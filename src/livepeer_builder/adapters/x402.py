"""The x402 media transport (ISS-68, PROTOTYPE.md): a standard HTTP/SSE client with an x402 interceptor.

On a 402 from the runner it parses the standard x402 challenge (a distinct format from the legacy
Livepeer-Payment format), delegates it to the caller-owned intent key through the ``X402Signer`` protocol,
and attaches the returned ``Payment`` header to the retry. A lost or malformed signer reply raises
``RunnerCallError`` with ``payment_unknown`` (D53): the job is ``uncertain`` and never fails over.

The transport is opt-in (``EngineSupply(transport=...)``): the default engine still ships
``SdkRunnerTransport`` (adapters/sdk.py). The x402 provider, signer and intent repo are wired by the
runtime layer (runtime/assembly.py), which may import services and adapters.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit

import httpx

from livepeer_builder.adapters.runner_call import json_object
from livepeer_builder.contracts import (
    JobRequest,
    Runner,
    RunnerReply,
    X402Challenge,
    X402IntentIdentity,
)
from livepeer_builder.errors import RunnerCallError, X402Refused, X402SignerUnknown
from livepeer_builder.protocols import OnSigned, X402Signer


def parse_x402_challenge(body: bytes, runner_url: str) -> X402Challenge:
    """Parse the standard x402 402 body. ``amount`` is in the asset's atomic units. ``origin`` is the host
    of the URL that challenged, so the authorization stays bound to it. A body without the shape fails the
    call before the signer is asked (nothing is signed)."""
    try:
        data = json.loads(body)
        x402 = data["x402"]
        accepts = x402.get("accepts") or []
        if not accepts:
            raise ValueError("x402 challenge carries no accepts")
        inner = accepts[0].get("x402") or {}
        payload = inner.get("payload") or {}
        return X402Challenge(
            origin=_origin(runner_url),
            pay_to=str(payload.get("payTo", "")),
            asset=str(payload.get("asset", "")),
            amount=Decimal(str(payload.get("amount", "0"))),
            nonce=str(payload.get("nonce", "")),
            response_format=str(inner.get("responseFormat", "x402")),
            exchange=str(inner.get("exchange", "ethereum")),
            min_fee=Decimal(str(payload.get("minGasPrice", 0))),
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise RunnerCallError(
            "http", f"invalid x402 challenge from {runner_url}", status_code=402, payment_sent=False
        ) from error


def _origin(url: str) -> str:
    """``scheme://host:port`` of a URL, like the orchestrator origin (services/failover.py)."""
    parts = urlsplit(url)
    scheme = (parts.scheme or "http").lower()
    host = (parts.hostname or "").lower()
    port = parts.port
    if port is None:
        port = 443 if scheme == "https" else 80
    if not host:
        return scheme + "://" + host
    return f"{scheme}://{host}:{port}"


class X402RunnerTransport:
    """The x402 media transport (PROTOTYPE.md #2): sends the request through httpx, pays the 402 through the
    ``X402Signer``, and streams the answer."""

    def __init__(self, signer: X402Signer, max_challenge_retries: int = 3) -> None:
        self._signer = signer
        self._max_retries = max_challenge_retries

    # -- RunnerTransport ---------------------------------------------------------------------------

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
        identity: X402IntentIdentity | None = None,
    ) -> RunnerReply:
        identity, _ = self._identity(identity)
        url = runner.url.rstrip("/") + (request.path or "")
        body, content_type, headers = _request_parts(request)
        async with httpx.AsyncClient(timeout=request.timeout_s) as _client:
            response = await _client.request(request.method, url, content=body, headers=headers)
            paid, response = await self._pay(
                _client,
                request.method,
                url,
                body,
                headers,
                response,
                runner.url,
                identity,
                credential,
                stream=False,
            )
            data = json_object(response.content, content_type)
            return RunnerReply(
                status_code=response.status_code,
                content=response.content,
                content_type=response.headers.get("Content-Type", content_type),
                data=data,
                manifest_ids=(),
                payment_sent=paid,
            )

    # -- StreamingRunnerTransport ------------------------------------------------------------------

    async def open(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
        identity: X402IntentIdentity | None = None,
    ) -> "X402RunnerStream":
        identity, _ = self._identity(identity)
        url = runner.url.rstrip("/") + (request.path or "")
        body, _content_type, headers = _request_parts(request)
        _client = httpx.AsyncClient()
        try:
            async with asyncio.timeout(request.first_byte_timeout_s):
                req = httpx.Request(request.method, url, content=body, headers=headers)
                response = await _client.send(req, stream=True)
            paid, response = await self._pay(
                _client,
                request.method,
                url,
                body,
                headers,
                response,
                runner.url,
                identity,
                credential,
                stream=True,
            )
            return X402RunnerStream(_client, response, paid)
        except TimeoutError as error:
            # The deadline passed: the payment may have been sent, so it is unknown (D53).
            await client.aclose()
            raise RunnerCallError(
                "timeout",
                f"no answer from {url} within {request.first_byte_timeout_s:g}s",
                payment_sent=True,
                payment_unknown=True,
            ) from error
        except BaseException:
            await client.aclose()
            raise

    # -- shared --------------------------------------------------------------------

    def _identity(self, identity: X402IntentIdentity | None) -> tuple[X402IntentIdentity, bool]:
        """The caller-owned intent key. A missing key is not signed: the intent is the caller's idempotency
        key (PROTOTYPE.md #3.1), so the transport fails loudly instead of minting one."""
        if identity is None:
            raise RunnerCallError(
                "payment",
                "the x402 transport requires the caller-owned intent key (job_id, attempt, cycle)",
                payment_sent=False,
            )
        return identity, False

    async def _pay(
        self,
        client: httpx.Client,
        method: str,
        url: str,
        body: bytes,
        headers: dict[str, str],
        response: httpx.Response,
        runner_url: str,
        identity: X402IntentIdentity,
        credential: str | None,
        stream: bool = False,
    ) -> tuple[bool, httpx.Response]:
        """Answer a 402 through the signer, attach the ``Payment`` header and retry. Returns whether a
        payment was sent on the answered request and the final response."""
        paid = False
        for _ in range(max(1, self._max_retries)):
            if response.status_code != 402:
                return (paid, response)
            chunk = await response.aread()
            await response.aclose()
            challenge = parse_x402_challenge(chunk, runner_url)
            try:
                auth = await self._signer.sign(identity, challenge, credential)
            except X402Refused as error:
                raise RunnerCallError(
                    "payment_refused",
                    f"the x402 signer refused the payment: {error}",
                    payment_sent=paid,
                    status_code=response.status_code,
                ) from error
            except X402SignerUnknown as error:
                # The signer was asked and gave no validated answer: it may have charged (PROTOTYPE.md #3.2).
                raise RunnerCallError(
                    "payment",
                    f"the x402 signer's reply is unknown: {error}",
                    payment_sent=True,
                    payment_unknown=True,
                ) from error
            if auth.payment is None:
                # A missing payment value: the signer answered but nothing to send; it may have charged.
                raise RunnerCallError(
                    "payment",
                    "the x402 signer gave no payment value",
                    payment_sent=True,
                    payment_unknown=True,
                )
            paid = True
            headers["Payment"] = auth.payment
            if stream:
                req = httpx.Request(method, url, content=body, headers=headers)
                response = await client.send(req, stream=True)
            else:
                response = await client.request(method, url, content=body, headers=headers)
        raise RunnerCallError(
            "http",
            f"{runner_url} kept asking for payment",
            status_code=402,
            payment_sent=paid,
        )


class X402RunnerStream:
    """The runner's answer as it arrives (docs/contracts/engine.md#streaming), for the x402 transport."""

    def __init__(self, client: httpx.Client, response: httpx.Response, payment_sent: bool) -> None:
        self._client = client
        self._response = response
        self._payment_sent = payment_sent

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> tuple[tuple[str, str], ...]:
        return tuple((name, value) for name, value in self._response.headers.multi_items())

    @property
    def content_type(self) -> str:
        return self._response.headers.get("Content-Type", "application/octet-stream")

    @property
    def manifest_ids(self) -> tuple[str, ...]:
        return ()  # the x402 correlation is the intent key, not a per-manifest session

    @property
    def payment_sent(self) -> bool:
        return self._payment_sent

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._response.aiter_bytes()

    async def aclose(self) -> None:
        await self._response.aclose()
        await self._client.aclose()


def _request_parts(request: JobRequest) -> tuple[bytes, str, dict[str, str]]:
    """The body, its content type and the request headers, as the runner expects (like adapters/sdk.py)."""
    if request.raw_body is not None:
        body, content_type = request.raw_body, request.content_type or "application/octet-stream"
    else:
        body = json.dumps({} if request.payload is None else request.payload).encode()
        content_type = request.content_type or "application/json"
    headers = {"Content-Type": content_type}
    headers.update(dict(request.headers))
    return body, content_type, headers
