from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    db_path: str = "output/sla_monitor.db"
    registry_path: str = "data/sample/dataset_registry.csv"
    landing_dir: str = "data/sample/landing"
    output_dir: str = "output"
    lookback_hours: int = 168            # evaluation window: partitions expected in (as_of - lookback, as_of]
    log_path: str | None = "output/pipeline.log"

    def landing_files(self) -> list[Path]:
        d = Path(self.landing_dir)
        return sorted(d.glob("*.csv")) if d.is_dir() else []
