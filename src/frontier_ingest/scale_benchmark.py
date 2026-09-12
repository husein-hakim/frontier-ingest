from __future__ import annotations

import time
import tracemalloc
from dataclasses import asdict, dataclass

from frontier_ingest.core.hashing import stable_key


@dataclass(frozen=True, slots=True)
class ScaleBenchmark:
    benchmark: str
    records: int
    batch_size: int
    batches: int
    elapsed_seconds: float
    records_per_second: int
    peak_memory_mib: float
    checksum: str
    scope_note: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def run_scale_benchmark(records: int = 500_000, batch_size: int = 200) -> ScaleBenchmark:
    """Exercise the streaming key/batch hot path without materializing the dataset."""
    records = max(1, records)
    batch_size = max(1, batch_size)
    checksum = ""
    batches = 0
    tracemalloc.start()
    started = time.perf_counter()
    for index in range(records):
        checksum = stable_key("benchmark", f"record-{index}")
        if (index + 1) % batch_size == 0:
            batches += 1
    if records % batch_size:
        batches += 1
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return ScaleBenchmark(
        benchmark="streaming-key-and-batch-hot-path",
        records=records,
        batch_size=batch_size,
        batches=batches,
        elapsed_seconds=round(elapsed, 4),
        records_per_second=round(records / max(elapsed, 0.000001)),
        peak_memory_mib=round(peak / (1024 * 1024), 3),
        checksum=checksum,
        scope_note=(
            "Microbenchmark of bounded-memory transformation only; source latency, "
            "PostgreSQL I/O, and external rate limits are intentionally excluded."
        ),
    )
