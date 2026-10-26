"""Retry behaviour: transient failures are retried, definitive ones are not."""

from __future__ import annotations

import random
from dataclasses import replace

import httpx
import pytest

from fee_drag.http_client import HttpError, build_client, request_with_retry

URL = "https://example.test/thing.zip"


class ScriptedTransport(httpx.BaseTransport):
    """Replay a scripted list of outcomes, one per request."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = 0

    def handle_request(self, request):
        self.requests += 1
        outcome = self.outcomes[min(self.requests - 1, len(self.outcomes) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return httpx.Response(outcome, request=request)


def run(settings, outcomes):
    """Drive request_with_retry against a scripted transport, recording the sleeps."""
    slept: list[float] = []
    transport = ScriptedTransport(outcomes)
    with build_client(settings, transport=transport) as client:
        response = request_with_retry(
            client,
            "GET",
            URL,
            settings=settings,
            sleep=slept.append,
            rng=random.Random(0),
        )
    return response, transport, slept


def test_succeeds_without_sleeping_when_the_first_attempt_works(settings):
    response, transport, slept = run(settings, [200])

    assert response.status_code == 200
    assert transport.requests == 1
    assert slept == []


def test_retries_transport_errors_then_succeeds(settings):
    response, transport, slept = run(
        settings,
        [httpx.ConnectError("boom"), httpx.ReadTimeout("slow"), 200],
    )

    assert response.status_code == 200
    assert transport.requests == 3
    assert len(slept) == 2


def test_retries_retryable_status_then_succeeds(settings):
    response, transport, slept = run(settings, [503, 429, 200])

    assert response.status_code == 200
    assert transport.requests == 3
    assert len(slept) == 2


def test_does_not_retry_a_404(settings):
    slept: list[float] = []
    transport = ScriptedTransport([404])

    with (
        build_client(settings, transport=transport) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        request_with_retry(client, "GET", URL, settings=settings, sleep=slept.append)

    assert transport.requests == 1
    assert slept == []


def test_raises_http_error_once_the_budget_is_spent(settings):
    slept: list[float] = []
    transport = ScriptedTransport([503])

    with (
        build_client(settings, transport=transport) as client,
        pytest.raises(HttpError, match="failed after 3"),
    ):
        request_with_retry(client, "GET", URL, settings=settings, sleep=slept.append)

    # http_max_retries is 2 in the fixture, so three attempts and two sleeps.
    assert transport.requests == 3
    assert len(slept) == 2


def test_zero_retries_means_a_single_attempt(no_retry_settings):
    slept: list[float] = []
    transport = ScriptedTransport([httpx.ConnectError("boom")])

    with build_client(no_retry_settings, transport=transport) as client, pytest.raises(HttpError):
        request_with_retry(client, "GET", URL, settings=no_retry_settings, sleep=slept.append)

    assert transport.requests == 1
    assert slept == []


def test_backoff_grows_with_each_attempt(settings):
    """Full jitter is random, so assert the ceiling doubles rather than the value."""
    slept: list[float] = []
    transport = ScriptedTransport([503])
    settings_many = replace(settings, http_max_retries=4)

    with build_client(settings_many, transport=transport) as client, pytest.raises(HttpError):
        request_with_retry(
            client, "GET", URL, settings=settings_many, sleep=slept.append, rng=random.Random(1)
        )

    base = settings_many.http_backoff_base_seconds
    for attempt, value in enumerate(slept):
        assert 0.0 <= value <= base * (2.0**attempt)


def test_client_carries_an_explicit_timeout_and_user_agent(settings):
    with build_client(settings) as client:
        assert client.timeout.read == settings.http_timeout_seconds
        assert "fee-drag-br" in client.headers["user-agent"]
