"""The x402 engine flow end-to-end (ISS-68, PROTOTYPE.md): a real engine over a dedicated file-backed
``SqlStore``, a loopback x402 runner (a V2 ``PAYMENT-REQUIRED`` 402 until a request carries the
``PAYMENT-SIGNATURE`` header) and an explicitly supplied x402 signer (no ``X402HttpSigner``).

The engine-level guarantees: the job pays the 402 through the provider, the provider journals the durable
intent before asking the signer, and a provider restart replays the stored payment value from the encrypted
secret store instead of re-signing (PROTOTYPE.md #3.1/#3.2, ISS-68 review priorities B and C).
"""

import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from multidict import CIMultiDict
from pydantic import SecretStr

from livepeer_builder.adapters.secrets import EncryptedDbSecretStore
from livepeer_builder.adapters.static import StaticDiscoverySource
from livepeer_builder.adapters.x402 import parse_x402_challenge
from livepeer_builder.config import EngineSettings, generate_secret_key
from livepeer_builder.contracts import (
    ActorContext,
    JobRequest,
    Money,
    NetworkRate,
    ProvisionRequest,
    Runner,
    X402Authorization,
    X402Challenge,
    X402IntentIdentity,
)
from livepeer_builder.runtime import BuilderEngine
from livepeer_builder.runtime.assembly import EngineSupply
from livepeer_builder.services.x402 import X402PaymentProvider
from livepeer_builder.store import SqlStore
from livepeer_builder.testing import FakeAllocationManager, FakeUsageSource, FixedClock

APP = "example/echo"


@dataclass
class X402Net:
    """The x402 loopback runner: a V2 ``PAYMENT-REQUIRED`` 402 until the request carries a ``PAYMENT-SIGNATURE``."""

    server: TestServer
    request_headers: list[CIMultiDict[str]] = field(default_factory=list)
    paid_requests: int = 0

    @property
    def url(self) -> str:
        return str(self.server.make_url("")).rstrip("/")

    def runner(self) -> Runner:
        return Runner(
            url=f"{self.url}/app",
            app=APP,
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
            return web.json_response({"ok": True})
        return web.Response(status=402, headers={"PAYMENT-REQUIRED": self._pay_required(request.method)})


@dataclass
class SignerNet:
    """A loopback signer: answers /generate-live-payment with a payment bound to the intent key."""

    server: TestServer
    requests: list[dict[str, Any]] = field(default_factory=list)

    @property
    def url(self) -> str:
        return str(self.server.make_url("")).rstrip("/")

    async def _generate(self, request: web.Request) -> web.Response:
        payload = await request.json()
        self.requests.append(payload)
        return web.json_response({"payment": f"pay-{payload.get('IntentKey', '')}"})


class SignerNetSigner:
    """The explicitly supplied x402 signer: POSTs to the loopback ``SignerNet`` endpoint and returns a
    ``X402Authorization`` whose payment value is a protected ``SecretStr`` (ISS-68: no HTTP signer adapter)."""

    def __init__(self, signer_net: SignerNet, clock: FixedClock) -> None:
        self._signer_net = signer_net
        self._clock = clock

    async def sign(
        self, intent: X402IntentIdentity, challenge: X402Challenge, credential: str | None
    ) -> X402Authorization:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._signer_net.url}/generate-live-payment",
                json={"IntentKey": intent.intent_key, "Credential": credential},
            )
            resp.raise_for_status()
            payment = resp.json()["payment"]
        return X402Authorization(
            intent_key=intent.intent_key,
            payment=SecretStr(payment),
            signed_at=self._clock.now(),
        )


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
async def signer_net() -> AsyncIterator[SignerNet]:
    app = web.Application()
    holder: list[SignerNet] = []

    def route(name: str):
        async def handle(request: web.Request) -> web.StreamResponse:
            return await getattr(holder[0], name)(request)

        return handle

    app.router.add_post("/generate-live-payment", route("_generate"))
    async with TestServer(app) as server:
        holder.append(SignerNet(server))
        yield holder[0]


def _settings(database_url: str, secret_key: str) -> EngineSettings:
    return EngineSettings(
        database_url=database_url,
        allocation_mode="per_actor",
        secret_key=secret_key,
        log_json=False,
        x402=True,
    )


def _supply(
    x402_net: X402Net,
    signer_net: SignerNet,
    store: SqlStore,
    clock: FixedClock,
    manager: FakeAllocationManager,
    usage: FakeUsageSource,
) -> EngineSupply:
    """The shared parts; ``x402_signer`` is explicitly supplied (no HTTP signer adapter)."""
    return EngineSupply(
        store=store,
        clock=clock,
        discovery_source=StaticDiscoverySource((x402_net.runner(),)),
        allocation_manager=manager,
        grants_manager=manager,
        usage_source=usage,
        x402_signer=SignerNetSigner(signer_net, clock),
    )


def _file_url(tmp_path: Path) -> str:
    """A dedicated file-backed SQLite database, not the shared in-memory store (roots out the flaky full-run failure)."""
    return f"sqlite+aiosqlite:///{tmp_path / 'x402.sqlite'}"


async def test_a_x402_job_pays_the_runner_through_the_signer(
    x402_net: X402Net,
    signer_net: SignerNet,
    clock: FixedClock,
    admin: ActorContext,
    alice: ActorContext,
    manager: FakeAllocationManager,
    usage: FakeUsageSource,
    tmp_path: Path,
) -> None:
    database_url = _file_url(tmp_path)
    store = await SqlStore.connect(database_url)
    secret_key = generate_secret_key()
    engine = await BuilderEngine.create(
        _settings(database_url, secret_key),
        supply=_supply(x402_net, signer_net, store, clock, manager, usage),
    )
    await engine.start()
    try:
        assert engine.x402 is not None  # the provider is wired and the renewal task started
        await engine.payments.provision(
            admin,
            ProvisionRequest(
                operation_ref="provision:default:alice",
                application_id="default",
                actor_id="alice",
                amount=Money(amount=Decimal(5), currency="USD"),
                grant_id="grant_1",
            ),
        )
        result = await engine.jobs.run(alice, JobRequest(app=APP, path="/run", payload={"x": 1}))
        job = result.job
        assert job.state == "succeeded"
        assert x402_net.paid_requests == 1
        # The runner saw exactly one request carrying the PAYMENT-SIGNATURE header:
        paid_headers = [h for h in x402_net.request_headers if "PAYMENT-SIGNATURE" in h]
        assert len(paid_headers) == 1
        # The provider journaled one durable intent for the job (ISS-68, PROTOTYPE.md #3.1):
        intents = await store.intents_for_job(job.id)
        assert len(intents) == 1
        intent = intents[0]
        assert (intent.state, intent.attempt, intent.cycle) == ("allowed", 1, 0)
        assert intent.payment_ref is not None
        # The signer was asked once, with the caller-owned intent key:
        assert len(signer_net.requests) == 1
        assert signer_net.requests[0]["IntentKey"] == intent.intent_key
        # The PAYMENT-SIGNATURE header value lives in the encrypted secret store, not on the intent row:
        secrets = EncryptedDbSecretStore(store, secret_key, clock)
        assert await secrets.get(intent.payment_ref) == paid_headers[0]["PAYMENT-SIGNATURE"]
    finally:
        await engine.stop()
        await store.close()


async def test_a_provider_restart_replays_the_stored_payment(
    x402_net: X402Net,
    signer_net: SignerNet,
    clock: FixedClock,
    admin: ActorContext,
    alice: ActorContext,
    manager: FakeAllocationManager,
    usage: FakeUsageSource,
    tmp_path: Path,
) -> None:
    database_url = _file_url(tmp_path)
    store = await SqlStore.connect(database_url)
    secret_key = generate_secret_key()
    engine = await BuilderEngine.create(
        _settings(database_url, secret_key),
        supply=_supply(x402_net, signer_net, store, clock, manager, usage),
    )
    await engine.start()
    await engine.payments.provision(
        admin,
        ProvisionRequest(
            operation_ref="provision:default:alice",
            application_id="default",
            actor_id="alice",
            amount=Money(amount=Decimal(5), currency="USD"),
            grant_id="grant_1",
        ),
    )
    result = await engine.jobs.run(alice, JobRequest(app=APP, path="/run", payload={"x": 1}))
    job = result.job
    await engine.stop()  # the renewal task is stopped; the durable intent and secret stay in the store

    # A "restarted" provider: same supplied signer and same file-backed store, no engine in between.
    provider = X402PaymentProvider(
        SignerNetSigner(signer_net, clock), store, clock, EncryptedDbSecretStore(store, secret_key, clock)
    )
    [record] = await store.intents_for_job(job.id)
    identity = X402IntentIdentity(job_id=job.id, attempt=record.attempt, cycle=record.cycle)
    # Reconstruct the challenge exactly as the transport parsed it (V2 fields included) so its digest
    # matches the journaled ``record.challenge_digest``:
    pay_required = x402_net._pay_required("POST")
    challenge = parse_x402_challenge(pay_required, f"{x402_net.url}/app")
    auth = await provider.authorize(identity, challenge, None)  # held key: replays, never re-signs
    assert auth.payment.get_secret_value() == f"pay-{record.intent_key}"
    assert len(signer_net.requests) == 1  # the signer was asked exactly once
    assert record.challenge_digest == challenge.digest  # the journaled digest binds the challenge
    await store.close()
