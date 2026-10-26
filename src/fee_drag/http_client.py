"""HTTP access with an explicit timeout and bounded exponential backoff.

CVM and BCB are public endpoints with no SLA: they time out, they return 502 during
maintenance windows, and BCB rate-limits. Retrying is therefore part of correctness, not
a nicety. What we do *not* retry is a definitive answer: a 404 means the month is not
published and a 403 means we are blocked, and hammering either one only wastes time.

`sleep` is injected so tests exercise the backoff without spending wall-clock time.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

import httpx

from fee_drag.log import get_logger

if TYPE_CHECKING:
    from fee_drag.config import Settings

logger = get_logger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
USER_AGENT = "fee-drag-br/0.0.0 (+https://github.com/luizjf/fee-drag-br)"


class HttpError(RuntimeError):
    """Raised when a request keeps failing after every retry has been spent."""


def build_client(
    settings: Settings, *, transport: httpx.BaseTransport | None = None
) -> httpx.Client:
    """Create a client with an explicit timeout. Tests pass a `MockTransport` here."""
    return httpx.Client(
        timeout=httpx.Timeout(settings.http_timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
        transport=transport,
    )


def _backoff_seconds(attempt: int, base: float, rng: random.Random) -> float:
    """Exponential backoff with full jitter, so retries do not arrive in lockstep."""
    return rng.uniform(0.0, base * (2.0**attempt))


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    settings: Settings,
    headers: Mapping[str, str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> httpx.Response:
    """Issue a request, retrying transport errors and retryable status codes.

    Returns the first response whose status is neither retryable nor an error. Raises
    `HttpError` once the retry budget is exhausted, and `httpx.HTTPStatusError` straight
    away for a definitive failure such as 404.
    """
    jitter = rng or random.Random()
    attempts = settings.http_max_retries + 1
    last_error: Exception | None = None

    for attempt in range(attempts):
        try:
            response = client.request(method, url, headers=dict(headers or {}))
        except httpx.TransportError as exc:
            last_error = exc
            logger.warning(
                "http_transport_error",
                extra={
                    "url": url,
                    "attempt": attempt + 1,
                    "attempts": attempts,
                    "reason": str(exc),
                },
            )
        else:
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
                return response
            last_error = httpx.HTTPStatusError(
                f"{response.status_code} from {url}", request=response.request, response=response
            )
            logger.warning(
                "http_retryable_status",
                extra={
                    "url": url,
                    "status": response.status_code,
                    "attempt": attempt + 1,
                    "attempts": attempts,
                },
            )

        if attempt < attempts - 1:
            sleep(_backoff_seconds(attempt, settings.http_backoff_base_seconds, jitter))

    raise HttpError(f"{method} {url} failed after {attempts} attempt(s)") from last_error
