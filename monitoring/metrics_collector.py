"""Lightweight metrics collector for pipeline observability."""

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pipeline.common.logging_config import get_logger

logger = get_logger("monitoring.metrics")


@dataclass
class PipelineMetrics:
    layer: str
    records_processed: int = 0
    batches_completed: int = 0
    errors: int = 0
    last_batch_duration_ms: float = 0.0
    _start_time: float = field(default_factory=time.monotonic, repr=False)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._start_time

    def record_batch(self, count: int, duration_ms: float):
        self.records_processed += count
        self.batches_completed += 1
        self.last_batch_duration_ms = duration_ms

    def record_error(self):
        self.errors += 1

    def snapshot(self) -> dict:
        return {
            "layer": self.layer,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "records_processed": self.records_processed,
            "batches_completed": self.batches_completed,
            "errors": self.errors,
            "last_batch_duration_ms": self.last_batch_duration_ms,
            "uptime_seconds": round(self.uptime_seconds, 2),
        }

    def log_snapshot(self):
        snap = self.snapshot()
        logger.info(
            "[%s] records=%d  batches=%d  errors=%d  last_batch=%.1fms  uptime=%.0fs",
            snap["layer"],
            snap["records_processed"],
            snap["batches_completed"],
            snap["errors"],
            snap["last_batch_duration_ms"],
            snap["uptime_seconds"],
        )
