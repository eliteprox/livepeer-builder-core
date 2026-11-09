"""The x402 media transport end-to-end (ISS-68, PROTOTYPE.md #2): a loopback runner answers the standard
x402 V2 402 (a ``PAYMENT-REQUIRED`` base64 JSON header) until a request carries the ``PAYMENT-SIGNATURE``
header, and a minimal ``X402Signer`` supplies it. A refused signer fails the call (no failover); an unknown
signer leaves the payment unknown (D53); the caller-owned intent key is required.

The runner is a real aiohttp server (like tests/unit/livepeer_net.py), so the transport's httpx client,
the 402 parse, the signer dispatch and the retry all run for real.
"""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from multidict import CIMultiDict
from pydantic import SecretStr

from livepeer_builder.adapters.x402 import (
    X402RunnerTransport,
    _origin,
    _select_requirement,
    _valid_url,
    parse_x402_challenge,
)
from livepeer_builder.contracts import (
    JobRequest,
    NetworkRate,
    Runner,
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
)
from livepeer_builder.errors import RunnerCallError, X402Refused, X402SignerUnknown
from livepeer_builder.services.x402 import X402PaymentProvider
from livepeer_builder.testing import FakePaymentIntents, FixedClock, MemorySecretStore


@dataclass
class X402Net:
    """The x402 loopback runner: 402 with the standard body until the request carries a ``Payment`` header."""

    server: TestServer
    request_headers: list[CIMultiDict[str]] = field(default_factory=list)
    paid_requests: int = 0
    stream_chunks: tuple[bytes, ...] = ()

    @property
    def url(self) -> str:
        return str(self.server.make_url("")).rstrip("/")

    def runner(self) -> Runner:
        return Runner(
            url=f"{self.url}/app",
            app="example/echo",
            runner_id="r1",
            mode="single-shot",
            orchestrator_url=self.url,
            capacity=1,
            capacity_available=1,
            rate=NetworkRate(amount=Decimal("0.10"), currency="usd", unit="request"),
        )

    def _pay_required(self, method: str) -> str:
        """A base64 JSON x402 V2 ``PAYMENT-REQUIRED`` header bound to the runner's resource."""
        challenge = {
            "x402Version": 2,
            "nonce": "n1",
            "resource": {"url": f"{self.url}/app", "method": method},
            "accepts": [
                {
                    "x402": {
                        "scheme": "exact",
                        "network": "eip155:0x1",
                        "asset": "0xasset",
                        "payTo": "0xpay",
                        "maxAmount": "100000",
                        "maxTimeoutSeconds": 300,
                    }
                }
            ],
        }
        return base64.b64encode(json.dumps(challenge).encode()).decode()

    async def _app(self, request: web.Request) -> web.StreamResponse:
        self.request_headers.append(CIMultiDict(request.headers))
        if "PAYMENT-SIGNATURE" in request.headers:
            self.paid_requests += 1
            if self.stream_chunks:
                response = web.StreamResponse(
                    status=200, headers=[("Content-Type", "application/octet-stream")]
                )
                await response.prepare(request)
                for chunk in self.stream_chunks:
                    await response.write(chunk)
                    await asyncio.sleep(0.01)
                await response.write_eof()
                return response
            return web.json_response({"ok": True})
        return web.Response(status=402, headers={"PAYMENT-REQUIRED": self._pay_required(request.method)})

    async def _slow_app(self, request: web.Request) -> web.StreamResponse:
        # A runner whose first byte arrives after the caller's first-byte deadline: the payment may
        # already have been sent, so the outcome is unknown (D53).
        await asyncio.sleep(1.0)
        self.request_headers.append(CIMultiDict(request.headers))
        if "PAYMENT-SIGNATURE" in request.headers:
            self.paid_requests += 1
            return web.json_response({"ok": True})
        return web.Response(status=402, headers={"PAYMENT-REQUIRED": self._pay_required(request.method)})


@pytest.fixture
async def x402_net() -> AsyncIterator[X402Net]:
    app = web.Application()
    holder: list[X402Net] = []

    def route(name: str):
        async def handle(request: web.Request) -> web.StreamResponse:
            return await getattr(holder[0], name)(request)

        return handle

    app.router.add_route("*", "/app{path:.*}", route("_app"))
    async with TestServer(app) as server:
        holder.append(X402Net(server))
        yield holder[0]


@pytest.fixture
async def x402_slow_net() -> AsyncIterator[X402Net]:
    app = web.Application()
    holder: list[X402Net] = []

    def route(name: str):
        async def handle(request: web.Request) -> web.StreamResponse:
            return await getattr(holder[0], name)(request)

        return handle

    app.router.add_route("*", "/app{path:.*}", route("_slow_app"))
    async with TestServer(app) as server:
        holder.append(X402Net(server))
        yield holder[0]


class Signer:
    """A minimal ``X402Signer``: ``ok`` returns a payment, ``refused`` raises, ``unknown`` raises."""

    def __init__(self, behavior: str = "ok") -> None:
        self.behavior = behavior
        self.calls: list[X402IntentIdentity] = []

    async def sign(
        self, intent: X402IntentIdentity, challenge: X402Challenge, credential: str | None
    ) -> X402Authorization:
        self.calls.append(intent)
        if self.behavior == "refused":
            raise X402Refused("the orchestrator's params were rejected")
        if self.behavior == "unknown":
            raise X402SignerUnknown("the signer's reply was lost")
        return X402Authorization(
            intent_key=intent.intent_key,
            payment=SecretStr(f"pay-{intent.intent_key}"),
            signed_at=datetime.now(UTC),
        )


@pytest.fixture
def ident() -> X402IntentIdentity:
    return X402IntentIdentity(job_id=uuid4(), attempt=1, cycle=0)


@pytest.fixture
def job_request() -> JobRequest:
    return JobRequest(app="example/echo", path="/run", payload={"x": 1})


async def test_a_paid_call_retries_with_the_payment_header(
    x402_net: X402Net, ident: X402IntentIdentity, job_request: JobRequest
) -> None:
    net = x402_net
    signer = Signer()
    authorizer = X402PaymentProvider(signer, FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    reply = await transport.call_with_intent(net.runner(), job_request, "cred", None, identity=ident)
    assert (reply.status_code, reply.payment_sent) == (200, True)
    assert net.paid_requests == 1
    paid_headers = [h for h in net.request_headers if "PAYMENT-SIGNATURE" in h]
    assert len(paid_headers) == 1
    assert paid_headers[0]["PAYMENT-SIGNATURE"] == f"pay-{ident.intent_key}"
    assert len(signer.calls) == 1


async def test_a_missing_intent_key_fails_loudly(x402_net: X402Net, job_request: JobRequest) -> None:
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    with pytest.raises(RunnerCallError) as exc:
        await transport.call(x402_net.runner(), job_request, "cred")  # legacy call has no caller key
    assert exc.value.kind == "payment"


async def test_a_refused_signer_fails_the_call(
    x402_net: X402Net, ident: X402IntentIdentity, job_request: JobRequest
) -> None:
    authorizer = X402PaymentProvider(
        Signer("refused"), FakePaymentIntents(), FixedClock(), MemorySecretStore()
    )
    transport = X402RunnerTransport(authorizer)
    with pytest.raises(RunnerCallError) as exc:
        await transport.call_with_intent(x402_net.runner(), job_request, "cred", None, identity=ident)
    error = exc.value
    assert (error.kind, error.payment_sent) == ("payment_refused", False)  # no money moved: failover allowed


async def test_an_unknown_signer_marks_the_payment_unknown(
    x402_net: X402Net, ident: X402IntentIdentity, job_request: JobRequest
) -> None:
    authorizer = X402PaymentProvider(
        Signer("unknown"), FakePaymentIntents(), FixedClock(), MemorySecretStore()
    )
    transport = X402RunnerTransport(authorizer)
    with pytest.raises(RunnerCallError) as exc:
        await transport.call_with_intent(x402_net.runner(), job_request, "cred", None, identity=ident)
    error = exc.value
    assert error.kind == "payment" and error.payment_unknown is True  # it may have charged: no failover


async def test_an_open_stream_pays_then_streams(x402_net: X402Net, ident: X402IntentIdentity) -> None:
    x402_net.stream_chunks = (b"hello ", b"world")
    request = JobRequest(app="example/echo", path="/run", payload={"x": 1}, first_byte_timeout_s=5.0)
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    stream = await transport.open_with_intent(x402_net.runner(), request, "cred", None, identity=ident)
    chunks = [chunk async for chunk in stream]
    await stream.aclose()
    assert stream.status_code == 200 and stream.payment_sent
    assert stream.manifest_ids == ()  # the x402 correlation is the intent key, not a per-manifest session
    assert b"".join(chunks) == b"hello world"
    assert x402_net.paid_requests == 1
    status = authorizer.stream_status(ident.job_id, ident.attempt)
    assert status.mode == "single-shot" and status.phase == "closed"  # aclose closed it
    assert authorizer.renewal_task_count() == 0


async def test_a_persistent_stream_renews_only_while_open(
    x402_net: X402Net, ident: X402IntentIdentity
) -> None:
    request = JobRequest(app="example/echo", path="/run", payload={"x": 1}, first_byte_timeout_s=5.0)
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    runner = x402_net.runner().model_copy(update={"mode": "persistent"})
    stream = await transport.open_with_intent(runner, request, "cred", None, identity=ident)
    status = authorizer.stream_status(ident.job_id, ident.attempt)
    assert status.mode == "metered" and status.phase == "open"
    assert authorizer.renewal_task_count() == 1
    await stream.aclose()
    closed = authorizer.stream_status(ident.job_id, ident.attempt)
    assert closed.phase == "closed" and authorizer.renewal_task_count() == 0


def _valid_requirement() -> dict[str, object]:
    return {
        "x402": {
            "scheme": "exact",
            "network": "eip155:0x1",
            "asset": "0xasset",
            "payTo": "0xpay",
            "maxAmount": "100000",
            "maxTimeoutSeconds": 300,
        }
    }


def _b64_challenge(data: dict[str, object]) -> str:
    return base64.b64encode(json.dumps(data).encode()).decode()


async def test_parse_challenge_without_header_fails_loudly() -> None:
    with pytest.raises(RunnerCallError) as exc:
        parse_x402_challenge(None, "http://127.0.0.1:8443")
    assert exc.value.kind == "payment"


def test_parse_challenge_rejects_bad_base64() -> None:
    with pytest.raises(RunnerCallError):
        parse_x402_challenge("!!!not-base64!!!", "http://127.0.0.1:8443")


def test_parse_challenge_rejects_wrong_version() -> None:
    data = {
        "x402Version": 1,
        "resource": {"url": "http://127.0.0.1:8443/app"},
        "accepts": [_valid_requirement()],
    }
    with pytest.raises(RunnerCallError):
        parse_x402_challenge(_b64_challenge(data), "http://127.0.0.1:8443")


def test_parse_challenge_rejects_bad_resource_url() -> None:
    data = {"x402Version": 2, "resource": {"url": "not-a-url"}, "accepts": [_valid_requirement()]}
    with pytest.raises(RunnerCallError):
        parse_x402_challenge(_b64_challenge(data), "http://127.0.0.1:8443")


def test_parse_challenge_rejects_no_valid_requirement() -> None:
    data = {"x402Version": 2, "resource": {"url": "http://127.0.0.1:8443/app"}, "accepts": [{}]}
    with pytest.raises(RunnerCallError):
        parse_x402_challenge(_b64_challenge(data), "http://127.0.0.1:8443")


def test_select_requirement_skips_incomplete_entries() -> None:
    # an empty payTo is skipped
    with pytest.raises(ValueError, match="no supported x402 requirement"):
        _select_requirement([{"x402": {"payTo": "", "asset": "0xasset", "maxAmount": "100"}}])


def test_select_requirement_rejects_bad_amount() -> None:
    with pytest.raises(ValueError, match="no supported x402 requirement"):
        _select_requirement([{"x402": {"payTo": "0xpay", "asset": "0xasset", "maxAmount": "not-a-number"}}])


def test_select_requirement_rejects_non_positive_amount() -> None:
    with pytest.raises(ValueError, match="no supported x402 requirement"):
        _select_requirement([{"x402": {"payTo": "0xpay", "asset": "0xasset", "maxAmount": "0"}}])


def test_select_requirement_rejects_missing_scheme_or_network() -> None:
    with pytest.raises(ValueError, match="no supported x402 requirement"):
        _select_requirement(
            [{"x402": {"payTo": "0xpay", "asset": "0xasset", "maxAmount": "100", "scheme": ""}}]
        )


def test_select_requirement_rejects_bad_timeout() -> None:
    with pytest.raises(ValueError, match="no supported x402 requirement"):
        _select_requirement(
            [
                {
                    "x402": {
                        "payTo": "0xpay",
                        "asset": "0xasset",
                        "maxAmount": "100",
                        "scheme": "exact",
                        "network": "eip155:0x1",
                        "maxTimeoutSeconds": "not-a-number",
                    }
                }
            ]
        )


def test_select_requirement_returns_the_first_valid_entry() -> None:
    requirement = _select_requirement([_valid_requirement()])
    assert requirement["payTo"] == "0xpay"
    assert requirement["network"] == "eip155:0x1"


def test_valid_url_rejects_missing_or_non_http_urls() -> None:
    assert not _valid_url("")
    assert not _valid_url("not-a-url")
    assert _valid_url("http://127.0.0.1:8443/app")


def test_origin_normalizes_default_ports() -> None:
    assert _origin("https://example.com") == "https://example.com:443"
    assert _origin("http://example.com") == "http://example.com:80"
    assert _origin("http://") == "http://"


async def test_the_legacy_open_without_an_intent_key_fails_loudly(
    x402_net: X402Net, job_request: JobRequest
) -> None:
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    with pytest.raises(RunnerCallError) as exc:
        await transport.open(x402_net.runner(), job_request, "cred")
    assert exc.value.kind == "payment"


async def test_stream_exposes_content_type_and_settlement_receipt(
    x402_net: X402Net, ident: X402IntentIdentity
) -> None:
    x402_net.stream_chunks = (b"a", b"b")
    request = JobRequest(app="example/echo", path="/run", payload={"x": 1}, first_byte_timeout_s=5.0)
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    stream = await transport.open_with_intent(x402_net.runner(), request, "cred", None, identity=ident)
    assert stream.content_type == "application/octet-stream"
    assert stream.settlement_receipt() is None  # the loopback runner sends no PAYMENT-RESPONSE receipt
    await stream.aclose()


async def test_an_open_stream_times_out_during_the_first_byte(
    x402_slow_net: X402Net, ident: X402IntentIdentity
) -> None:
    request = JobRequest(app="example/echo", path="/run", payload={"x": 1}, first_byte_timeout_s=0.2)
    authorizer = X402PaymentProvider(Signer(), FakePaymentIntents(), FixedClock(), MemorySecretStore())
    transport = X402RunnerTransport(authorizer)
    with pytest.raises(RunnerCallError) as exc:
        await transport.open_with_intent(x402_slow_net.runner(), request, "cred", None, identity=ident)
    error = exc.value
    assert error.kind == "timeout"
    assert error.payment_sent and error.payment_unknown  # it may have charged: no failover
