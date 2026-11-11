import json
import subprocess
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from miniapm.models import SpanEvent

SCHEMA = pa.schema([
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("trace_id", pa.string(), nullable=False),
    pa.field("service", pa.string(), nullable=False),
    pa.field("instance", pa.string(), nullable=False),
    pa.field("version", pa.string(), nullable=False),
    pa.field("route", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("kind", pa.string(), nullable=False),
    pa.field("started_at", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("duration_seconds", pa.float64(), nullable=False),
])


def prepare_partitioned_spans(
    dataset_directory: Path, max_events: int = 100_000
) -> dict:
    source = dataset_directory / "observations" / "spans.jsonl"
    latest = {}
    with source.open(encoding="utf-8") as lines:
        for line in lines:
            event = SpanEvent.model_validate_json(line)
            key = str(event.event_id)
            current = latest.get(key)
            if current is None or (event.ingested_at, event.event_time) > (
                current.ingested_at,
                current.event_time,
            ):
                latest[key] = event
            if len(latest) > max_events:
                raise ValueError("input exceeds max_events")
    partitions = defaultdict(list)
    for event in latest.values():
        row = {
            "dataset_id": str(event.dataset_id),
            "event_id": str(event.event_id),
            "trace_id": event.trace_id,
            "service": event.service,
            "instance": event.instance,
            "version": event.version,
            "route": event.route,
            "status": event.status,
            "kind": event.kind,
            "started_at": event.started_at,
            "duration_seconds": (event.ended_at - event.started_at).total_seconds(),
        }
        partitions[event.started_at.date().isoformat()].append(row)
    output_root = dataset_directory / "hive"
    output_root.mkdir(parents=True, exist_ok=True)
    files = []
    for date, rows in sorted(partitions.items()):
        path = output_root / f"dt={date}" / "spans.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
        files.append(path)
    return {
        "logical_span_count": len(latest),
        "partitions": {date: len(rows) for date, rows in sorted(partitions.items())},
        "total_bytes": sum(path.stat().st_size for path in files),
        "output_root": str(output_root),
    }


def upload_partitions(dataset_directory: Path) -> str:
    dataset_id = dataset_directory.name
    hdfs_root = f"/miniapm/history/spans/dataset_id={dataset_id}"
    local_root = dataset_directory / "hive"
    subprocess.run(
        ["docker", "compose", "exec", "-T", "hive", "/opt/hadoop/bin/hdfs", "dfs", "-mkdir", "-p", hdfs_root],
        check=True,
    )
    for partition in sorted(local_root.glob("dt=*")):
        destination = f"{hdfs_root}/{partition.name}/spans.parquet"
        temporary = f"/tmp/{dataset_id}-{partition.name}.parquet"
        subprocess.run(
            ["docker", "compose", "exec", "-T", "hive", "/opt/hadoop/bin/hdfs", "dfs", "-mkdir", "-p", destination.rsplit("/", 1)[0]],
            check=True,
        )
        subprocess.run(
            ["docker", "compose", "cp", str(partition / "spans.parquet"), f"hive:{temporary}"],
            check=True,
        )
        subprocess.run(
            ["docker", "compose", "exec", "-T", "hive", "/opt/hadoop/bin/hdfs", "dfs", "-put", "-f", temporary, destination],
            check=True,
        )
    return hdfs_root
