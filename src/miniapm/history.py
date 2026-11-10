import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from miniapm.batch import BatchGenerationSettings, GenerationStats, iter_request_batches
from miniapm.models import ContractModel
from miniapm.topology import Topology


class EventFileWriter:
    def __init__(self, path_without_suffix: Path) -> None:
        self.jsonl_path = path_without_suffix.with_suffix(".jsonl")
        self.parquet_path = path_without_suffix.with_suffix(".parquet")
        self.jsonl_file = self.jsonl_path.open("w", encoding="utf-8")
        self.parquet_writer = None

    def write(self, events: list[BaseModel]) -> None:
        rows = [event.model_dump(mode="json") for event in events]
        for row in rows:
            self.jsonl_file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        if not rows:
            return
        nullable_strings = {"error_code", "parent_span_id", "trace_id", "span_id"}
        schema = pa.schema(
            [
                pa.field(
                    key,
                    pa.string()
                    if key in nullable_strings
                    else pa.infer_type([next(row[key] for row in rows if row[key] is not None)]),
                )
                for key in rows[0]
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        if self.parquet_writer is None:
            self.parquet_writer = pq.ParquetWriter(self.parquet_path, table.schema)
        self.parquet_writer.write_table(table)

    def close(self) -> None:
        self.jsonl_file.close()
        if self.parquet_writer is not None:
            self.parquet_writer.close()

    def __enter__(self) -> "EventFileWriter":
        return self

    def __exit__(self, *args) -> None:
        self.close()


class HistoricalWriteResult(ContractModel):
    output_directory: Path
    stats: GenerationStats
    truth_file: Path
    file_count: int
    total_file_bytes: int


def write_history(
    topology: Topology,
    request_count: int,
    start: datetime,
    output_root: Path,
    settings: BatchGenerationSettings | None = None,
) -> HistoricalWriteResult:
    output_directory = output_root / str(topology.dataset_id)
    observations = output_directory / "observations"
    truths_directory = output_directory / "truth"
    observations.mkdir(parents=True, exist_ok=True)
    truths_directory.mkdir(parents=True, exist_ok=True)
    truth_path = truths_directory / "requests.jsonl"

    total = GenerationStats()
    with (
        EventFileWriter(observations / "requests") as requests_writer,
        EventFileWriter(observations / "spans") as spans_writer,
        EventFileWriter(observations / "logs") as logs_writer,
        truth_path.open("w", encoding="utf-8") as truth_file,
    ):
        for batch in iter_request_batches(topology, request_count, start, settings=settings):
            requests_writer.write([item.request for item in batch.events])
            spans_writer.write([span for item in batch.events for span in item.spans])
            logs_writer.write([log for item in batch.events for log in item.logs])
            for truth in batch.truths:
                row = truth.model_dump(mode="json")
                truth_file.write(json.dumps(row, separators=(",", ":")) + "\n")
            total = total.add(batch.stats)

    files = [
        path
        for path in output_directory.rglob("*")
        if path.is_file() and path.suffix in {".jsonl", ".parquet"}
    ]
    return HistoricalWriteResult(
        output_directory=output_directory,
        stats=total,
        truth_file=truth_path,
        file_count=len(files),
        total_file_bytes=sum(path.stat().st_size for path in files),
    )
