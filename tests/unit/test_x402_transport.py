"""The x402 media transport end-to-end (ISS-68, PROTOTYPE.md #2): a loopback runner answers the standard
x402 402 until a request carries the ``Payment`` header, and a minimal ``X402Signer`` supplies it. A
refused signer fails the call (no failover); an unknown signer leaves the payment unknown (D53); the
caller-owned intent key is required.

The runner is a real aiohttp server (like tests/unit/livepeer_net.py), so the transport's httpx client,
the 402 parse, the signer dispatch and the retry all run for real.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from multidict import CIMultiDict

from livepeer_builder.adapters.x402 import X402RunnerTransport
from livepeer_builder.contracts import (
    JobRequest,
    NetworkRate,
    Runner,
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
)
from livepeer_builder.errors import RunnerCallError, X402Refused, X402SignerUnknown


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

    async def _app(self, request: web.Request) -> web.StreamResponse:
        self.request_headers.append(CIMultiDict(request.headers))
        if "Payment" in request.headers:
            self.paid_requests += 1
            if self.stream_chunks:
                response = web.StreamResponse(status=200, headers=[("Content-Type", "application/octet-stream")])
                await response.prepare(request)
                for chunk in self.stream_chunks:
                    await response.write(chunk)
                    await asyncio.sleep(0.01)
                await response.write_eof()
                return response
            return web.json_response({"ok": True})
        body = {
            "x402": {
                "accepts": [
                    {
                        "x402": {
                            "exchange": "ethereum",
                            "responseFormat": "x402",
                            "payload": {
                                "payTo": "0xpay",
                                "asset": "0xasset",
                                "amount": "100000",
                                "nonce": "n1",
                                "minGasPrice": 0,
                            },
                        }
                    }
                ]
            }
        }
        return web.json_response(body, status=402)


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
            payment=f"pay-{intent.intent_key}",
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
    transport = X402RunnerTransport(signer)
    reply = await transport.call(net.runner(), job_request, "cred", None, ident)
    assert (reply.status_code, reply.payment_sent) == (200, True)
    assert net.paid_requests == 1
    paid_headers = [h for h in net.request_headers if "Payment" in h]
    assert len(paid_headers) == 1
    assert paid_headers[0]["Payment"] == f"pay-{ident.intent_key}"
    assert len(signer.calls) == 1


async def test_a_missing_intent_key_fails_loudly(x402_net: X402Net, job_request: JobRequest) -> None:
    transport = X402RunnerTransport(Signer())
    with pytest.raises(RunnerCallError) as exc:
        await transport.call(x402_net.runner(), job_request, "cred")
    assert exc.value.kind == "payment"


async def test_a_refused_signer_fails_the_call(
    x402_net: X402Net, ident: X402IntentIdentity, job_request: JobRequest
) -> None:
    transport = X402RunnerTransport(Signer("refused"))
    with pytest.raises(RunnerCallError) as exc:
        await transport.call(x402_net.runner(), job_request, "cred", None, ident)
    error = exc.value
    assert (error.kind, error.payment_sent) == ("payment_refused", False)  # no money moved: failover allowed


async def test_an_unknown_signer_marks_the_payment_unknown(
    x402_net: X402Net, ident: X402IntentIdentity, job_request: JobRequest
) -> None:
    transport = X402RunnerTransport(Signer("unknown"))
    with pytest.raises(RunnerCallError) as exc:
        await transport.call(x402_net.runner(), job_request, "cred", None, ident)
    error = exc.value
    assert error.kind == "payment" and error.payment_unknown is True  # it may have charged: no failover


async def test_an_open_stream_pays_then_streams(
    x402_net: X402Net, ident: X402IntentIdentity
) -> None:
    x402_net.stream_chunks = (b"hello ", b"world")
    request = JobRequest(app="example/echo", path="/run", payload={"x": 1}, first_byte_timeout_s=5.0)
    transport = X402RunnerTransport(Signer())
    stream = await transport.open(x402_net.runner(), request, "cred", None, ident)
    chunks = [chunk async for chunk in stream]
    await stream.aclose()
    assert stream.status_code == 200 and stream.payment_sent
    assert stream.manifest_ids == ()  # the x402 correlation is the intent key, not a per-manifest session
    assert b"".join(chunks) == b"hello world"
    assert x402_net.paid_requests == 1
