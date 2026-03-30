"""Async benchmark for the distributed KV store HTTP API."""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass
from typing import List, Tuple

import httpx


@dataclass(frozen=True)
class BenchmarkResult:
    op: str
    num_requests: int
    concurrency: int
    total_s: float
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    errors: int


def _percentile_ms(samples_ms: List[float], percentile: float) -> float:
    if not samples_ms:
        return float("nan")
    if percentile <= 0:
        return min(samples_ms)
    if percentile >= 100:
        return max(samples_ms)
    xs = sorted(samples_ms)
    # Nearest-rank method.
    k = int((percentile / 100.0) * len(xs))
    k = max(1, min(len(xs), k))
    return xs[k - 1]


async def _run_puts(
    client: httpx.AsyncClient, *, num_requests: int, concurrency: int
) -> Tuple[List[float], int]:
    lat_ms: List[float] = []
    errors = 0

    async def _one(i: int) -> None:
        nonlocal errors
        key = f"bench-{i}"
        t0 = time.perf_counter()
        try:
            resp = await client.put(f"/keys/{key}", json={"value": i})
            resp.raise_for_status()
        except Exception:
            errors += 1
        finally:
            lat_ms.append((time.perf_counter() - t0) * 1000.0)

    for start in range(0, num_requests, concurrency):
        batch = [_one(i) for i in range(start, min(num_requests, start + concurrency))]
        await asyncio.gather(*batch)

    return lat_ms, errors


async def _run_gets(
    client: httpx.AsyncClient, *, num_requests: int, concurrency: int
) -> Tuple[List[float], int]:
    lat_ms: List[float] = []
    errors = 0

    async def _one(i: int) -> None:
        nonlocal errors
        key = f"bench-{i}"
        t0 = time.perf_counter()
        try:
            resp = await client.get(f"/keys/{key}")
            resp.raise_for_status()
        except Exception:
            errors += 1
        finally:
            lat_ms.append((time.perf_counter() - t0) * 1000.0)

    for start in range(0, num_requests, concurrency):
        batch = [_one(i) for i in range(start, min(num_requests, start + concurrency))]
        await asyncio.gather(*batch)

    return lat_ms, errors


async def run_benchmark(
    num_requests: int = 1000,
    concurrency: int = 50,
    *,
    base_url: str = "http://127.0.0.1:8001",
    timeout_s: float = 10.0,
) -> None:
    """Stress test PUT and GET against a single node and print a summary table."""

    if num_requests <= 0:
        raise ValueError("num_requests must be > 0")
    if concurrency <= 0:
        raise ValueError("concurrency must be > 0")

    timeout = httpx.Timeout(timeout_s)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        # PUT benchmark
        t0 = time.perf_counter()
        put_lat_ms, put_errors = await _run_puts(
            client, num_requests=num_requests, concurrency=concurrency
        )
        put_total_s = time.perf_counter() - t0
        put_rps = num_requests / put_total_s if put_total_s > 0 else float("inf")
        put = BenchmarkResult(
            op="PUT",
            num_requests=num_requests,
            concurrency=concurrency,
            total_s=put_total_s,
            rps=put_rps,
            p50_ms=_percentile_ms(put_lat_ms, 50),
            p95_ms=_percentile_ms(put_lat_ms, 95),
            p99_ms=_percentile_ms(put_lat_ms, 99),
            errors=put_errors,
        )

        # GET benchmark
        t1 = time.perf_counter()
        get_lat_ms, get_errors = await _run_gets(
            client, num_requests=num_requests, concurrency=concurrency
        )
        get_total_s = time.perf_counter() - t1
        get_rps = num_requests / get_total_s if get_total_s > 0 else float("inf")
        get = BenchmarkResult(
            op="GET",
            num_requests=num_requests,
            concurrency=concurrency,
            total_s=get_total_s,
            rps=get_rps,
            p50_ms=_percentile_ms(get_lat_ms, 50),
            p95_ms=_percentile_ms(get_lat_ms, 95),
            p99_ms=_percentile_ms(get_lat_ms, 99),
            errors=get_errors,
        )

    def fmt(result: BenchmarkResult) -> str:
        return (
            f"{result.op:<4}  "
            f"{result.num_requests:>7}  "
            f"{result.concurrency:>11}  "
            f"{result.total_s:>8.3f}  "
            f"{result.rps:>10.1f}  "
            f"{result.p50_ms:>9.2f}  "
            f"{result.p95_ms:>9.2f}  "
            f"{result.p99_ms:>9.2f}  "
            f"{result.errors:>6}"
        )

    header = (
        "op    requests  concurrency   total_s   req/s      p50_ms     p95_ms     p99_ms  errors"
    )
    print(header)
    print("-" * len(header))
    print(fmt(put))
    print(fmt(get))

    # Optional extra: show average latency if useful.
    try:
        put_avg = statistics.fmean(put_lat_ms)
        get_avg = statistics.fmean(get_lat_ms)
        print()
        print(f"avg_ms: PUT={put_avg:.2f}  GET={get_avg:.2f}")
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(run_benchmark())

