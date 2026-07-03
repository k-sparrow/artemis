"""Tenacity-based polling utilities shared across the test suite.

Four primitives cover every polling pattern in the tests:

  poll_until(fn, timeout, interval)
      Generic: call fn() until it returns truthy; raise TimeoutError on timeout.

  wait_for_http(url, timeout, interval)
      Block until an HTTP GET returns 2xx (retries on connection errors too).

  wait_for_kc_connector(kc_url, name, timeout, interval)
      Block until a Kafka Connect connector and all its tasks reach RUNNING.

  wait_for_celery_workers(broker_url, required_queues, timeout, interval)
      Block until every queue in required_queues has at least one live worker.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar

import httpx
from tenacity import RetryError, Retrying, TryAgain, stop_after_delay, wait_fixed

T = TypeVar("T")


def poll_until(
    fn: Callable[[], T],
    *,
    timeout: int,
    interval: float = 2.0,
) -> T:
    """Call fn() until it returns truthy; raise TimeoutError after *timeout* seconds."""
    value: Any = None
    try:
        for attempt in Retrying(
            stop=stop_after_delay(timeout), wait=wait_fixed(interval)
        ):
            with attempt:
                value = fn()
                if not value:
                    raise TryAgain
    except RetryError:
        raise TimeoutError(f"Condition not met within {timeout}s")
    return value


def wait_for_http(url: str, *, timeout: int = 300, interval: float = 3.0) -> None:
    """Block until *url* returns HTTP 2xx; raise TimeoutError on timeout."""
    try:
        for attempt in Retrying(
            stop=stop_after_delay(timeout), wait=wait_fixed(interval)
        ):
            with attempt:
                try:
                    r = httpx.get(url, timeout=5.0)
                    if not r.is_success:
                        raise TryAgain
                except httpx.RequestError:
                    raise TryAgain
    except RetryError:
        raise TimeoutError(f"{url} did not become ready within {timeout}s")


def wait_for_celery_workers(
    broker_url: str,
    required_queues: list[str],
    *,
    timeout: int = 120,
    interval: float = 3.0,
) -> None:
    """Block until every queue in required_queues has at least one live worker.

    Uses Celery inspect().active_queues() which returns a mapping of
    {worker_name: [queue_dict, ...]} for every worker that replied.
    Retries until all required queue names appear in the combined set.
    """
    from celery import Celery as _Celery

    check = _Celery(broker=broker_url)

    def _all_queues_covered() -> bool:
        try:
            result = check.control.inspect(timeout=2).active_queues()
        except Exception:
            return False
        if not result:
            return False
        active: set[str] = {
            q["name"] for queues in result.values() for q in (queues or [])
        }
        return all(q in active for q in required_queues)

    try:
        poll_until(_all_queues_covered, timeout=timeout, interval=interval)
    except TimeoutError:
        raise TimeoutError(
            f"Celery workers for queues {required_queues} "
            f"did not become ready within {timeout}s"
        )


def wait_for_kc_connector(
    kc_url: str,
    name: str,
    *,
    timeout: int = 120,
    interval: float = 3.0,
) -> None:
    """Block until a Kafka Connect connector and all tasks reach RUNNING."""

    def _is_running() -> bool:
        try:
            resp = httpx.get(f"{kc_url}/connectors/{name}/status", timeout=5.0)
            if not resp.is_success:
                return False
            s = resp.json()
            tasks = s.get("tasks", [])
            return (
                s.get("connector", {}).get("state") == "RUNNING"
                and bool(tasks)
                and all(t["state"] == "RUNNING" for t in tasks)
            )
        except httpx.RequestError:
            return False

    try:
        poll_until(_is_running, timeout=timeout, interval=interval)
    except TimeoutError:
        raise TimeoutError(
            f"Kafka Connect connector {name!r} did not reach RUNNING within {timeout}s"
        )
