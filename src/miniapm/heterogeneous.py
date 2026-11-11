import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily

from miniapm.adapters import (
    AdaptedRecord,
    AdaptationRule,
    adapt_nginx,
    adapt_otlp,
    adapt_prometheus,
)
from miniapm.models import LogEvent
from miniapm.mysql_context import MySQLContext
from miniapm.rabbitmq_publisher import RabbitMQPublisher


class HeterogeneousContext(MySQLContext):
    def resolve_identity(self, dataset_id, environment, source_id, event_time) -> dict:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT service, instance, version FROM identity_mappings
                WHERE dataset_id=%s AND environment=%s AND source_id=%s
                  AND valid_from <= %s AND (valid_to IS NULL OR valid_to > %s)
                """,
                (str(dataset_id), environment, source_id,
                 self._mysql_time(event_time), self._mysql_time(event_time)),
            )
            rows = cursor.fetchall()
        if not rows:
            return {"status": "missing"}
        if len(rows) > 1:
            return {"status": "ambiguous", "candidates": len(rows)}
        return {"status": "matched", "service": rows[0][0], "instance": rows[0][1], "version": rows[0][2]}

    def import_identity(self, dataset_id, environment, source_id, service, instance, version, valid_from, valid_to=None):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO identity_mappings
                (dataset_id, environment, source_id, service, instance, version, valid_from, valid_to)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (str(dataset_id), environment, source_id, service, instance, version,
                 self._mysql_time(valid_from), self._mysql_time(valid_to) if valid_to else None),
            )
        self.connection.commit()

    def save_pending(self, item) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO pending_records
                (event_id,dataset_id,environment,source_format,source_id,batch_id,record_locator,raw_text,reason,rule_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE reason=VALUES(reason), rule_version=VALUES(rule_version),
                    status='pending', resolved_at=NULL""",
                (str(item.event_id), str(item.dataset_id), item.environment, item.source_format,
                 item.source_id, item.batch_id, item.record_locator, item.raw_text, item.reason, item.rule_version),
            )
        self.connection.commit()

    def pending_summary(self, dataset_id: str, limit: int = 10) -> dict:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT source_format, reason, count(*) FROM pending_records
                WHERE dataset_id=%s AND status='pending' GROUP BY source_format, reason ORDER BY count(*) DESC LIMIT %s""",
                (dataset_id, max(1, min(limit, 50))),
            )
            groups = [{"source_format": row[0], "reason": row[1], "count": row[2]} for row in cursor.fetchall()]
            cursor.execute(
                """SELECT event_id, source_id, batch_id, record_locator, raw_text, reason, rule_version
                FROM pending_records WHERE dataset_id=%s AND status='pending' ORDER BY created_at DESC LIMIT %s""",
                (dataset_id, max(1, min(limit, 50))),
            )
            samples = [{"event_id": row[0], "source_id": row[1], "batch_id": row[2],
                        "record_locator": row[3], "raw_text": row[4][:500], "reason": row[5],
                        "rule_version": row[6]} for row in cursor.fetchall()]
        return {"groups": groups, "samples": samples}

    def pending_batch(self, dataset_id: str, batch_id: str) -> list[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT event_id, environment, source_format, source_id, record_locator, raw_text, rule_version
                FROM pending_records WHERE dataset_id=%s AND batch_id=%s AND status='pending'""",
                (dataset_id, batch_id),
            )
            return [{"event_id": row[0], "environment": row[1], "source_format": row[2],
                     "source_id": row[3], "record_locator": row[4], "raw_text": row[5],
                     "rule_version": row[6]} for row in cursor.fetchall()]

    def resolve_pending(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE pending_records SET status='resolved', resolved_at=%s WHERE event_id IN ({','.join(['%s'] * len(event_ids))})",
                [self._mysql_time(datetime.now(UTC)), *event_ids],
            )
        self.connection.commit()


class AdaptedMetrics:
    def __init__(self) -> None:
        self.snapshots: dict[tuple[str, str], list[dict]] = {}

    def replace(self, source_id: str, metrics: list[dict]) -> None:
        if metrics:
            key = (metrics[0]["dataset_id"], source_id)
            self.snapshots[key] = metrics

    def render(self) -> bytes:
        registry = CollectorRegistry()
        families = []
        grouped = {}
        for snapshots in self.snapshots.values():
            for metric in snapshots:
                grouped.setdefault((metric["dataset_id"], metric["name"], metric["type"]), []).append(metric)
        common_names = ["dataset_id", "environment", "service", "instance", "version"]
        for (_, name, kind), metrics in grouped.items():
            extra_names = sorted({key for metric in metrics for sample in metric["samples"]
                                  for key in sample["labels"] if key != "le"})
            label_names = [*common_names, *extra_names]
            if kind == "histogram":
                family = HistogramMetricFamily(name, "Normalized heterogeneous telemetry", labels=label_names)
            else:
                cls = CounterMetricFamily if kind == "counter" else GaugeMetricFamily
                family = cls(name, "Normalized heterogeneous telemetry", labels=label_names)
            families.append(family)
            for metric in metrics:
                common = {key: str(metric[key]) for key in common_names}
                if kind == "histogram":
                    buckets = []
                    total = 0
                    count = 0
                    labels = dict(common)
                    for sample in metric["samples"]:
                        labels.update({key: value for key, value in sample["labels"].items() if key != "le"})
                        if sample["name"].endswith("_bucket"):
                            buckets.append((sample["labels"]["le"], sample["value"]))
                        elif sample["name"].endswith("_sum"):
                            total = sample["value"]
                        elif sample["name"].endswith("_count"):
                            count = sample["value"]
                    family.add_metric([labels[key] for key in label_names], buckets=buckets, sum_value=total)
                    if buckets and count != buckets[-1][1]:
                        raise ValueError("histogram +Inf bucket must equal count")
                else:
                    for sample in metric["samples"]:
                        labels = {**common, **sample["labels"]}
                        family.add_metric([labels[key] for key in label_names], sample["value"])
        class FamilyCollector:
            def collect(self):
                return families

        registry.register(FamilyCollector())
        return generate_latest(registry)


class HeterogeneousPipeline:
    def __init__(self, context: HeterogeneousContext, publisher=None, metrics: AdaptedMetrics | None = None,
                 rules_path: str = "deploy/adaptation/rules.json") -> None:
        self.context = context
        self.publisher = publisher
        self.metrics = metrics or AdaptedMetrics()
        self.rules_path = Path(rules_path)
        document = json.loads(self.rules_path.read_text())
        self.rules = {rule.version: rule for rule in (AdaptationRule(**item) for item in document["rules"])}

    def rule(self, version: str) -> AdaptationRule:
        if version not in self.rules:
            raise LookupError("approved adaptation rule not found")
        return self.rules[version]

    def approve_rule(self, proposal_path: str, confirmed: bool) -> dict:
        proposal = json.loads(Path(proposal_path).read_text())
        if not confirmed:
            return {"approved": False, "status": "rejected"}
        rule = AdaptationRule(**proposal["rule"])
        if rule.version in self.rules:
            if self.rules[rule.version] != rule:
                raise ValueError("rule version already exists with different content")
            return {"approved": True, "version": rule.version, "existing": True}
        document = {"rules": [*self.rules.values(), rule]}
        temporary = self.rules_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"rules": [item.model_dump(mode="json") for item in document["rules"]]},
                                        ensure_ascii=False, indent=2) + "\n")
        temporary.replace(self.rules_path)
        self.rules[rule.version] = rule
        return {"approved": True, "version": rule.version}

    def adapt(self, source_format: str, content: str, dataset_id: str, environment: str,
              source_id: str, batch_id: str, rule_version: str, event_time: datetime | None = None) -> list[AdaptedRecord]:
        rule = self.rule(rule_version)
        if rule.source_format != source_format:
            raise ValueError("rule does not apply to source format")
        arguments = (content, rule, UUID(str(dataset_id)), environment, source_id, batch_id, self.context.resolve_identity)
        if source_format == "nginx":
            return adapt_nginx(*arguments)
        if source_format == "otlp":
            return adapt_otlp(*arguments)
        return adapt_prometheus(*arguments, event_time or datetime.now(UTC))

    def process(self, **kwargs) -> dict:
        records = self.adapt(**kwargs)
        published = 0
        pending_items = []
        metrics = []
        for record in records:
            if record.pending:
                self.context.save_pending(record.pending)
                pending_items.append(str(record.pending.event_id))
            elif isinstance(record.event, LogEvent) or record.event is not None:
                if self.publisher:
                    self.publisher.publish("logs" if isinstance(record.event, LogEvent) else "spans", record.event.model_dump(mode="json"))
                published += 1
            elif record.metric:
                metrics.append(record.metric)
        if metrics:
            self.metrics.replace(kwargs["source_id"], metrics)
        return {"records": len(records), "published": published, "pending": len(pending_items),
                "metrics": len(metrics), "pending_ids": pending_items}

    def replay(self, dataset_id: str, batch_id: str, rule_version: str, event_time: datetime | None = None) -> dict:
        items = self.context.pending_batch(dataset_id, batch_id)
        result = {"attempted": len(items), "published": 0, "still_pending": 0, "metrics": 0}
        for item in items:
            content = item["raw_text"]
            if item["source_format"] == "otlp":
                span = json.loads(content)
                content = json.dumps({"resourceSpans": [{"resource": {"attributes": []},
                    "scopeSpans": [{"spans": [span]}]}]})
            records = self.adapt(source_format=item["source_format"], content=content,
                dataset_id=dataset_id, environment=item["environment"], source_id=item["source_id"],
                batch_id=batch_id, rule_version=rule_version, event_time=event_time)
            for record in records:
                if record.pending:
                    corrected = record.pending.model_copy(update={"event_id": UUID(item["event_id"])})
                    self.context.save_pending(corrected)
                    result["still_pending"] += 1
                elif record.event:
                    event = record.event.model_copy(update={"event_id": UUID(item["event_id"])})
                    if self.publisher:
                        self.publisher.publish("logs" if isinstance(event, LogEvent) else "spans", event.model_dump(mode="json"))
                    result["published"] += 1
                    self.context.resolve_pending([item["event_id"]])
                elif record.metric:
                    self.metrics.replace(item["source_id"], [record.metric])
                    result["metrics"] += 1
                    self.context.resolve_pending([item["event_id"]])
        return result
