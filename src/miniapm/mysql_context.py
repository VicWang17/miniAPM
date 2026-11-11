import json
from datetime import UTC, datetime

import pymysql

from miniapm.models import ContractModel, TimeWindow
from miniapm.reports import InvestigationReport
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
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO investigations
                    (id, dataset_id, question, service, window_start, window_end, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'running')
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
        self.connection.commit()

    def complete_investigation(
        self,
        report: InvestigationReport,
        max_report_bytes: int = 100_000,
    ) -> None:
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
