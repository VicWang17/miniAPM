import argparse
import csv
import io
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

DDL = """
CREATE EXTERNAL TABLE IF NOT EXISTS miniapm_spans (
  event_id STRING,
  trace_id STRING,
  service STRING,
  instance STRING,
  version STRING,
  route STRING,
  status STRING,
  kind STRING,
  started_at TIMESTAMP,
  duration_seconds DOUBLE
)
PARTITIONED BY (dataset_id STRING, dt STRING)
STORED AS PARQUET
LOCATION '/miniapm/history/spans'
"""


def beeline(sql: str, timeout: int = 180) -> str:
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "hive", "/opt/hive/bin/beeline",
            "-u", "jdbc:hive2://127.0.0.1:10000", "-n", "root",
            "--silent=true", "--showHeader=true", "--outputformat=csv", "-e", sql,
        ],
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real Hive baseline aggregation")
    parser.add_argument("dataset_directory", type=Path)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    dataset_id = args.dataset_directory.name
    beeline(DDL)
    beeline("MSCK REPAIR TABLE miniapm_spans")
    query = f"""
SELECT service, route, dt,
       COUNT(*) AS request_count,
       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
       AVG(duration_seconds) AS avg_seconds,
       PERCENTILE_APPROX(duration_seconds, 0.95) AS p95_seconds
FROM miniapm_spans
WHERE dataset_id = '{dataset_id}'
  AND dt >= '{args.start_date}' AND dt <= '{args.end_date}'
  AND kind = 'server'
GROUP BY service, route, dt
ORDER BY service, route, dt
"""
    output = beeline(query)
    csv_rows = list(csv.reader(io.StringIO(output.strip())))
    headers = [value.strip("'") for value in csv_rows[0]]
    rows = [
        {key: value.strip("'") for key, value in zip(headers, values)}
        for values in csv_rows[1:]
    ]
    for row in rows:
        row["request_count"] = int(row["request_count"])
        row["error_count"] = int(row["error_count"])
        row["avg_seconds"] = float(row["avg_seconds"])
        row["p95_seconds"] = float(row["p95_seconds"])
    report = {
        "dataset_id": dataset_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "generated_at": datetime.now(UTC).isoformat(),
        "engine": "Hive 4.0.1 MapReduce",
        "query": query.strip(),
        "rows": rows,
    }
    output_path = args.output or args.dataset_directory / "baseline" / "hive-results.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"row_count": len(rows), "rows": rows, "output": str(output_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
