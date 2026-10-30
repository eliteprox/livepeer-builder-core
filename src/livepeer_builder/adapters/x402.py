"""The x402 media transport (ISS-68, PROTOTYPE.md): a standard HTTP/SSE client with an x402 V2 interceptor.

On a 402 from the runner it decodes the standard x402 V2 ``PAYMENT-REQUIRED`` response header (base64
JSON, ``x402Version = 2``), delegates the validated challenge to the ``X402PaymentAuthorizer`` (the
provider boundary that owns the signer dispatch and the durable intent state, PROTOTYPE.md #2), and
retries with the signed ``PAYMENT-SIGNATURE`` request header. A lost or malformed signer reply surfaces
as ``RunnerCallError`` with ``payment_unknown`` (D53): the job is ``uncertain`` and never fails over.

The transport is opt-in (``EngineSupply(transport=...)``): the default engine still ships
``SdkRunnerTransport`` (adapters/sdk.py). The x402 provider is built in the runtime layer; the signer is
supplied explicitly (no HTTP signer, no LPB_SIGNER_URL auto-wiring) — tests supply a fake signer.
"""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from typing import Any, cast
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
from livepeer_builder.protocols import OnSigned, X402PaymentAuthorizer


def parse_x402_challenge(pay_required: str | None, runner_url: str) -> X402Challenge:
    """Decode the x402 V2 ``PAYMENT-REQUIRED`` response header (base64 JSON).

    Requires ``x402Version == 2``, a valid ``resource.url``, and one explicitly selected, fully-populated
    requirement (scheme, CAIP-2 network, positive integer atomic-unit amount, non-empty asset/payTo,
    positive ``maxTimeoutSeconds``, complete ``extra``). Malformed or oversized input, an unsupported
    version, or an unsafe or mismatched origin/resource fails the call before the signer is asked
    (nothing is signed)."""
    if not pay_required:
        raise RunnerCallError(
            "payment",
            f"the 402 from {runner_url} carries no PAYMENT-REQUIRED header",
            payment_sent=False,
        )
    try:
        raw = base64.b64decode(pay_required)
        data = json.loads(raw)
        version = data.get("x402Version")
        if version != 2:
            raise ValueError(f"x402Version must be 2, got {version!r}")
        resource = data.get("resource") or {}
        resource_url = str(resource.get("url", ""))
        if not _valid_url(resource_url):
            raise ValueError("the x402 challenge resource.url is missing or invalid")
        accepts = data.get("accepts") or []
        requirement = _select_requirement(accepts)
        extra = data.get("extra") or {}
        return X402Challenge(
            x402_version=2,
            origin=_origin(runner_url),
            pay_to=str(requirement["payTo"]),
            asset=str(requirement["asset"]),
            amount=Decimal(str(requirement["maxAmount"])),
            nonce=str(data.get("nonce", "")),
            resource_url=resource_url,
            scheme=str(requirement.get("scheme", "")),
            network=str(requirement.get("network", "")),
            max_timeout_seconds=int(requirement.get("maxTimeoutSeconds", 300)),
            extra=dict(extra),
        )
    except (ValueError, InvalidOperation) as error:
        raise RunnerCallError(
            "payment",
            f"invalid x402 PAYMENT-REQUIRED challenge from {runner_url}",
            payment_sent=False,
        ) from error


def _select_requirement(accepts: list[dict[str, Any]]) -> dict[str, Any]:
    """Explicitly select a supported requirement from ``accepts``: the first entry with a complete, valid
    requirement, not an arbitrary first item with zero/empty defaults."""
    for entry in accepts:
        candidate = entry.get("x402")
        requirement: dict[str, Any] = candidate or entry
        pay_to = str(requirement.get("payTo", ""))
        asset = str(requirement.get("asset", ""))
        amount_raw = requirement.get("maxAmount")
        if not pay_to or not asset or amount_raw in (None, "", 0, "0"):
            continue
        try:
            amount = Decimal(str(amount_raw))
        except InvalidOperation:
            continue
        if amount <= 0:
            continue
        if not requirement.get("scheme") or not str(requirement.get("network", "")):
            continue
        timeout = requirement.get("maxTimeoutSeconds", 300)
        try:
            if int(timeout) <= 0:
                continue
        except (TypeError, ValueError):
            continue
        return requirement
    raise ValueError("no supported x402 requirement")


def _valid_url(url: str) -> bool:
    """A valid http(s) URL with a host."""
    if not url:
        return False
    parts = urlsplit(url)
    return parts.scheme in ("http", "https") and bool(parts.hostname)


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
    ``X402PaymentAuthorizer``, and streams the answer.

    The legacy ``call``/``open`` signatures stay documented; the intent-aware ``call_with_intent`` /
    ``open_with_intent`` (protocols.IntentAwareTransport) carry the caller-owned key (ISS-68 review,
    priority A). Without the key the transport fails loudly instead of minting one (PROTOTYPE.md #3.1)."""

    def __init__(self, authorizer: X402PaymentAuthorizer, max_challenge_retries: int = 3) -> None:
        self._authorizer = authorizer
        self._max_retries = max_challenge_retries

    # -- RunnerTransport (legacy, identity=None fails loudly) -------------------------------------

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
    ) -> RunnerReply:
        return await self._run(runner, request, credential, on_signed, identity=None)

    async def open(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
    ) -> "X402RunnerStream":
        return await self._open_stream(runner, request, credential, on_signed, identity=None)

    # -- IntentAwareTransport (opt-in, ISS-68) -------------------------------------------------------

    async def call_with_intent(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
        identity: X402IntentIdentity | None = None,
    ) -> RunnerReply:
        return await self._run(runner, request, credential, on_signed, identity=identity)

    async def open_with_intent(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None = None,
        identity: X402IntentIdentity | None = None,
    ) -> "X402RunnerStream":
        return await self._open_stream(runner, request, credential, on_signed, identity=identity)

    # -- shared --------------------------------------------------------------------

    def _identity(self, identity: X402IntentIdentity | None) -> X402IntentIdentity:
        """The caller-owned intent key. A missing key is not signed: the intent is the caller's idempotency
        key (PROTOTYPE.md #3.1), so the transport fails loudly instead of minting one."""
        if identity is None:
            raise RunnerCallError(
                "payment",
                "the x402 transport requires the caller-owned intent key (job_id, attempt, cycle)",
                payment_sent=False,
            )
        return identity

    async def _run(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None,
        identity: X402IntentIdentity | None,
    ) -> RunnerReply:
        identity = self._identity(identity)
        url = runner.url.rstrip("/") + (request.path or "")
        body, content_type, headers = _request_parts(request)
        async with httpx.AsyncClient(timeout=request.timeout_s) as client:
            response = await client.request(request.method, url, content=body, headers=headers)
            paid, response = await self._pay(
                client,
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

    async def _open_stream(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
        on_signed: OnSigned | None,
        identity: X402IntentIdentity | None,
    ) -> "X402RunnerStream":
        identity = self._identity(identity)
        url = runner.url.rstrip("/") + (request.path or "")
        body, _content_type, headers = _request_parts(request)
        client = httpx.AsyncClient()
        try:
            async with asyncio.timeout(request.first_byte_timeout_s):
                req = httpx.Request(request.method, url, content=body, headers=headers)
                response = await client.send(req, stream=True)
            paid, response = await self._pay(
                client,
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
            return X402RunnerStream(client, response, paid)
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

    async def _pay(
        self,
        client: httpx.AsyncClient,
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
        """Answer a 402 through the payment authorizer, attach the signed ``PAYMENT-SIGNATURE`` header and
        retry. Returns whether a payment was sent on the answered request and the final response."""
        paid = False
        for _ in range(max(1, self._max_retries)):
            if response.status_code != 402:
                return (paid, response)
            challenge = parse_x402_challenge(response.headers.get("PAYMENT-REQUIRED"), runner_url)
            try:
                auth = await self._authorizer.authorize(identity, challenge, credential)
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
            # The HTTP boundary unwraps the secret (the secret value never rides on the record).
            headers["PAYMENT-SIGNATURE"] = auth.payment.get_secret_value()
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
    """The runner's answer as it arrives (docs/contracts/engine.md#streaming), for the x402 transport.

    Reads the x402 V2 ``PAYMENT-RESPONSE`` settlement header so a missing or mismatched receipt does not
    finalize a pending intent (authorization is not settlement)."""

    def __init__(self, client: httpx.AsyncClient, response: httpx.Response, payment_sent: bool) -> None:
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
        return cast(str, self._response.headers.get("Content-Type", "application/octet-stream"))

    @property
    def manifest_ids(self) -> tuple[str, ...]:
        return ()  # the x402 correlation is the intent key, not a per-manifest session

    @property
    def payment_sent(self) -> bool:
        return self._payment_sent

    def settlement_receipt(self) -> str | None:
        """The raw ``PAYMENT-RESPONSE`` header (base64 JSON receipt), or None if the runner gave no
        receipt. A missing receipt means the payment is not settled."""
        return cast("str | None", self._response.headers.get("PAYMENT-RESPONSE"))

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
