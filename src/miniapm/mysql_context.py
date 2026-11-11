from __future__ import annotations

import json
from datetime import UTC, datetime

import pymysql

from miniapm.models import ContractModel, TimeWindow
from miniapm.topology import Topology


class DeploymentInput(ContractModel):
    service: str
    version: str
    deployed_at: datetime
    summary: str


class MySQLContext:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3307,
        database: str = "miniapm",
    ) -> None:
        self.connection = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password="",
            database=database,
            autocommit=False,
        )

    def close(self) -> None:
        self.connection.close()

    @staticmethod
    def _mysql_time(value: datetime) -> datetime:
        return value.astimezone(UTC).replace(tzinfo=None)

    def import_context(
        self,
        topology: Topology,
        deployments: list[DeploymentInput],
    ) -> None:
        try:
            with self.connection.cursor() as cursor:
                for service_spec in topology.services:
                    dependencies = ("inventory",) if service_spec.service == "order" else ()
                    document = {
                        "instances": [
                            instance.model_dump() for instance in service_spec.instances
                        ],
                        "dependencies": dependencies,
                    }
                    cursor.execute(
                        """
                        INSERT INTO services (dataset_id, service, environment, topology_json)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            environment = VALUES(environment),
                            topology_json = VALUES(topology_json)
                        """,
                        (
                            str(topology.dataset_id),
                            service_spec.service,
                            topology.environment,
                            json.dumps(document),
                        ),
                    )
                for deployment in deployments:
                    cursor.execute(
                        """
                        INSERT INTO deployments
                            (dataset_id, service, version, deployed_at, summary)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            str(topology.dataset_id),
                            deployment.service,
                            deployment.version,
                            self._mysql_time(deployment.deployed_at),
                            deployment.summary,
                        ),
                    )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def service(self, dataset_id: str, service: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT environment, topology_json FROM services WHERE dataset_id=%s AND service=%s",
                (dataset_id, service),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        topology = json.loads(row[1])
        return {"environment": row[0], **topology}

    def deployments(
        self,
        dataset_id: str,
        service: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT version, deployed_at, summary FROM deployments
                WHERE dataset_id=%s AND service=%s AND deployed_at >= %s AND deployed_at < %s
                ORDER BY deployed_at
                """,
                (dataset_id, service, self._mysql_time(start), self._mysql_time(end)),
            )
            rows = cursor.fetchall()
        return [
            {"version": version, "deployed_at": deployed_at.isoformat(), "summary": summary}
            for version, deployed_at, summary in rows
        ]

    def create_investigation(
        self,
        investigation_id: str,
        dataset_id: str,
        question: str,
        service: str,
        window: TimeWindow,
        status: str = "running",
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO investigations
                    (id, dataset_id, question, service, window_start, window_end, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    investigation_id,
                    dataset_id,
                    question,
                    service,
                    self._mysql_time(window.start),
                    self._mysql_time(window.end),
                    status,
                ),
            )
        self.connection.commit()

    def complete_investigation(
        self,
        report: InvestigationReport,
        max_report_bytes: int = 100_000,
    ) -> None:
        from miniapm.reports import validate_report

        validate_report(report)
        document = report.model_dump_json()
        if len(document.encode("utf-8")) > max_report_bytes:
            raise ValueError("report_json exceeds the configured size limit")
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE investigations
                SET status='completed', report_json=%s, finished_at=%s, error_reason=NULL
                WHERE id=%s
                """,
                (document, self._mysql_time(datetime.now(UTC)), str(report.investigation_id)),
            )
            if cursor.rowcount != 1:
                raise LookupError("investigation not found")
        self.connection.commit()

    def update_investigation_status(
        self, investigation_id: str, status: str, error_reason: str | None = None
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE investigations
                SET status=%s, error_reason=%s,
                    finished_at=CASE WHEN %s IN ('failed', 'interrupted') THEN %s ELSE finished_at END
                WHERE id=%s
                """,
                (
                    status,
                    error_reason,
                    status,
                    self._mysql_time(datetime.now(UTC)),
                    investigation_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LookupError("investigation not found")
        self.connection.commit()

    def mark_interrupted(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE investigations
                SET status='interrupted', error_reason='application restarted', finished_at=%s
                WHERE status IN ('queued', 'running')
                """,
                (self._mysql_time(datetime.now(UTC)),),
            )
            count = cursor.rowcount
        self.connection.commit()
        return count

    def create_alert_investigation(
        self,
        investigation_id: str,
        dataset_id: str,
        question: str,
        service: str,
        window: TimeWindow,
        event_key: str,
        fingerprint: str,
        started_at: datetime,
        labels: dict,
    ) -> None:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO investigations
                        (id, dataset_id, question, service, window_start, window_end, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'queued')
                    """,
                    (
                        investigation_id,
                        dataset_id,
                        question,
                        service,
                        self._mysql_time(window.start),
                        self._mysql_time(window.end),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO alerts
                        (event_key, fingerprint, started_at, status, labels_json, investigation_id)
                    VALUES (%s, %s, %s, 'firing', %s, %s)
                    """,
                    (
                        event_key,
                        fingerprint,
                        self._mysql_time(started_at),
                        json.dumps(labels),
                        investigation_id,
                    ),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def resolve_alert(self, event_key: str, resolved_at: datetime) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alerts SET status='resolved', resolved_at=%s
                WHERE event_key=%s AND status='firing'
                """,
                (self._mysql_time(resolved_at), event_key),
            )
        self.connection.commit()
        return self.alert(event_key)

    def alert(self, event_key: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_key, status, investigation_id, resolved_at
                FROM alerts WHERE event_key=%s
                """,
                (event_key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "event_key": row[0],
            "status": row[1],
            "investigation_id": row[2],
            "resolved_at": row[3].isoformat() if row[3] else None,
        }

    def explain_deployments(
        self, dataset_id: str, service: str, start: datetime, end: datetime
    ) -> dict:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                EXPLAIN SELECT version, deployed_at, summary FROM deployments
                WHERE dataset_id=%s AND service=%s AND deployed_at >= %s AND deployed_at < %s
                ORDER BY deployed_at
                """,
                (dataset_id, service, self._mysql_time(start), self._mysql_time(end)),
            )
            row = cursor.fetchone()
            columns = [item[0] for item in cursor.description]
        return dict(zip(columns, row))

    def investigation(self, investigation_id: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, dataset_id, question, service, status, error_reason, report_json FROM investigations WHERE id=%s",
                (investigation_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "dataset_id": row[1],
            "question": row[2],
            "service": row[3],
            "status": row[4],
            "error_reason": row[5],
            "report_json": json.loads(row[6]) if row[6] else None,
        }

    # --- P12 knowledge cases -------------------------------------------------

    CASE_COLUMNS = (
        "id, dataset_id, service, alert_fingerprint, investigation_id, summary, "
        "root_cause, resolution, result_evidence, keywords, status, reject_reason, "
        "created_at, approved_at, hits, helped"
    )

    @staticmethod
    def _case_row(row) -> dict:
        return {
            "id": row[0],
            "dataset_id": row[1],
            "service": row[2],
            "alert_fingerprint": row[3],
            "investigation_id": row[4],
            "summary": row[5],
            "root_cause": row[6],
            "resolution": row[7],
            "result_evidence": row[8],
            "keywords": json.loads(row[9]) if row[9] else [],
            "status": row[10],
            "reject_reason": row[11],
            "created_at": row[12].isoformat() if row[12] else None,
            "approved_at": row[13].isoformat() if row[13] else None,
            "hits": int(row[14] or 0),
            "helped": int(row[15] or 0),
        }

    def upsert_case_candidate(
        self,
        case_id: str,
        investigation_id: str,
        dataset_id: str,
        service: str,
        alert_fingerprint: str | None,
        summary: str,
        root_cause: str,
        resolution: str,
        result_evidence: str | None,
        keywords: list[str],
    ) -> dict:
        """Insert or refresh a pending candidate; refuses to overwrite a decided case."""
        existing = self.case_by_investigation(investigation_id)
        payload = (
            summary,
            root_cause,
            resolution,
            result_evidence,
            json.dumps(keywords, ensure_ascii=False),
        )
        if existing is not None:
            if existing["status"] != "pending":
                raise ValueError(f"case for investigation {investigation_id} is already {existing['status']}")
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE cases SET summary=%s, root_cause=%s, resolution=%s,
                        result_evidence=%s, keywords=%s
                    WHERE investigation_id=%s
                    """,
                    (*payload, investigation_id),
                )
            self.connection.commit()
            return self.case_by_investigation(investigation_id)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO cases
                    (id, dataset_id, service, alert_fingerprint, investigation_id,
                     summary, root_cause, resolution, result_evidence, keywords, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (
                    case_id,
                    dataset_id,
                    service,
                    alert_fingerprint,
                    investigation_id,
                    *payload,
                ),
            )
        self.connection.commit()
        return self.case_by_investigation(investigation_id)

    def case_by_id(self, case_id: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {self.CASE_COLUMNS} FROM cases WHERE id=%s", (case_id,)
            )
            row = cursor.fetchone()
        return None if row is None else self._case_row(row)

    def alert_fingerprint(self, investigation_id: str) -> str | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT fingerprint FROM alerts WHERE investigation_id=%s LIMIT 1",
                (investigation_id,),
            )
            row = cursor.fetchone()
        return None if row is None else row[0]

    def alerts_between(self, start: datetime, end: datetime, limit: int = 500) -> list[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_key, fingerprint, status, started_at, resolved_at,
                       investigation_id, labels_json
                FROM alerts
                WHERE started_at >= %s AND started_at < %s
                ORDER BY started_at, event_key
                LIMIT %s
                """,
                (self._mysql_time(start), self._mysql_time(end), min(limit, 1000)),
            )
            rows = cursor.fetchall()
        result = []
        for row in rows:
            labels = json.loads(row[6]) if row[6] else {}
            result.append(
                {
                    "event_key": row[0],
                    "fingerprint": row[1],
                    "status": row[2],
                    "started_at": row[3].replace(tzinfo=UTC).isoformat() if row[3] else None,
                    "resolved_at": row[4].replace(tzinfo=UTC).isoformat() if row[4] else None,
                    "investigation_id": row[5],
                    "labels": labels,
                    "alertname": labels.get("alertname") or labels.get("summary") or "unknown",
                    "service": labels.get("service") or "unknown",
                }
            )
        return result

    def alert_fingerprint_stats(self, start: datetime, end: datetime) -> dict[str, dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fingerprint, COUNT(*) AS events,
                       SUM(status='firing') AS firing, SUM(status='resolved') AS resolved
                FROM alerts
                WHERE started_at >= %s AND started_at < %s
                GROUP BY fingerprint
                """,
                (self._mysql_time(start), self._mysql_time(end)),
            )
            rows = cursor.fetchall()
        return {
            row[0]: {"events": int(row[1]), "firing": int(row[2] or 0), "resolved": int(row[3] or 0)}
            for row in rows
        }

    def case_by_investigation(self, investigation_id: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {self.CASE_COLUMNS} FROM cases WHERE investigation_id=%s",
                (investigation_id,),
            )
            row = cursor.fetchone()
        return None if row is None else self._case_row(row)

    def approve_case(
        self,
        case_id: str,
        result_evidence: str,
        summary: str | None = None,
        root_cause: str | None = None,
        resolution: str | None = None,
        keywords: list[str] | None = None,
    ) -> dict:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE cases SET status='approved', result_evidence=%s, approved_at=%s,
                    summary=COALESCE(%s, summary), root_cause=COALESCE(%s, root_cause),
                    resolution=COALESCE(%s, resolution), keywords=COALESCE(%s, keywords),
                    reject_reason=NULL
                WHERE id=%s AND status IN ('pending', 'approved')
                """,
                (
                    result_evidence,
                    self._mysql_time(datetime.now(UTC)),
                    summary,
                    root_cause,
                    resolution,
                    json.dumps(keywords, ensure_ascii=False) if keywords is not None else None,
                    case_id,
                ),
            )
            if cursor.rowcount != 1:
                raise LookupError("case not found or already rejected")
        self.connection.commit()
        record = self.case_by_id(case_id)
        if record is None:
            raise LookupError("case not found after approval")
        return record

    def reject_case(self, case_id: str, reason: str) -> dict:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE cases SET status='rejected', reject_reason=%s
                WHERE id=%s AND status='pending'
                """,
                (reason[:500], case_id),
            )
            if cursor.rowcount != 1:
                raise LookupError("case not found or already decided")
        self.connection.commit()
        record = self.case_by_id(case_id)
        if record is None:
            raise LookupError("case not found after rejection")
        return record

    def search_cases(
        self,
        dataset_id: str,
        service: str,
        keywords: list[str] | None = None,
        error_code: str | None = None,
        route: str | None = None,
        limit: int = 3,
        scan_limit: int = 50,
    ) -> list[dict]:
        """Deterministic SQL filtering; ranking is done by the caller on the bounded set."""
        clauses = ["dataset_id=%s", "service=%s", "status='approved'"]
        params: list[object] = [dataset_id, service]
        haystack = (
            "CONCAT_WS(' ', summary, root_cause, resolution, "
            "IFNULL(result_evidence, ''), CAST(keywords AS CHAR))"
        )
        if keywords:
            keyword_clauses = []
            for keyword in keywords[:5]:
                keyword_clauses.append(f"{haystack} LIKE %s")
                params.append(f"%{keyword}%")
            clauses.append("(" + " OR ".join(keyword_clauses) + ")")
        if error_code:
            clauses.append(f"{haystack} LIKE %s")
            params.append(f"%{error_code}%")
        if route:
            clauses.append(f"{haystack} LIKE %s")
            params.append(f"%{route}%")
        params.append(min(scan_limit, 200))
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {self.CASE_COLUMNS} FROM cases WHERE "
                + " AND ".join(clauses)
                + " ORDER BY hits DESC, created_at DESC LIMIT %s",
                tuple(params),
            )
            rows = cursor.fetchall()
        return [self._case_row(row) for row in rows]

    def record_case_outcome(self, case_ids: list[str], helped: bool) -> int:
        """Record that approved cases were retrieved; optionally that they helped."""
        if not case_ids:
            return 0
        placeholders = ",".join(["%s"] * len(case_ids))
        with self.connection.cursor() as cursor:
            if helped:
                cursor.execute(
                    f"UPDATE cases SET hits=hits+1, helped=helped+1 WHERE id IN ({placeholders})",
                    tuple(case_ids),
                )
            else:
                cursor.execute(
                    f"UPDATE cases SET hits=hits+1 WHERE id IN ({placeholders})",
                    tuple(case_ids),
                )
            count = cursor.rowcount
        self.connection.commit()
        return count

