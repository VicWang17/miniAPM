import json

import redis

from miniapm.baseline import BaselineClickHouseBackend


def baseline_cache_key(
    dataset_id: str, service: str, route: str, start_date: str, end_date: str
) -> str:
    return f"miniapm:baseline:{dataset_id}:{service}:{route}:{start_date}:{end_date}"


class CachedBaselineBackend:
    def __init__(
        self,
        source: BaselineClickHouseBackend | None = None,
        redis_client=None,
        ttl_seconds: int = 300,
    ) -> None:
        self.source = source or BaselineClickHouseBackend()
        self.redis = redis_client or redis.Redis(
            host="127.0.0.1", port=6380, decode_responses=True,
            socket_connect_timeout=0.5, socket_timeout=0.5,
        )
        self.ttl_seconds = ttl_seconds

    def close(self) -> None:
        self.source.close()
        self.redis.close()

    def query_baseline(
        self, dataset_id: str, service: str, route: str, start_date: str, end_date: str
    ) -> dict | None:
        key = baseline_cache_key(dataset_id, service, route, start_date, end_date)
        try:
            cached = self.redis.get(key)
        except redis.RedisError:
            cached = None
            cache_status = "fallback_after_error"
        else:
            cache_status = "hit" if cached else "miss"
        if cached:
            return {**json.loads(cached), "cache_status": "hit"}
        baseline = self.source.query_baseline(
            dataset_id, service, route, start_date, end_date
        )
        if baseline is None:
            return None
        if cache_status != "fallback_after_error":
            try:
                self.redis.set(key, json.dumps(baseline), ex=self.ttl_seconds)
            except redis.RedisError:
                cache_status = "fallback_after_error"
        return {**baseline, "cache_status": cache_status}

    def invalidate(
        self, dataset_id: str, service: str, route: str, start_date: str, end_date: str
    ) -> None:
        key = baseline_cache_key(dataset_id, service, route, start_date, end_date)
        try:
            self.redis.delete(key)
        except redis.RedisError:
            return
