from __future__ import annotations

from main import PrometheusClient, PrometheusError


class FakeClient(PrometheusClient):
    """Ersetzt PrometheusClient ohne HTTP; Antworten oder Exceptions werden je Endpunkt vorgegeben."""

    def __init__(self, targets=None, query_result=None, tsdb=None, runtime=None):
        self.targets = targets if targets is not None else []
        self.query_result = query_result if query_result is not None else []
        self.tsdb = tsdb if tsdb is not None else {}
        self.runtime = runtime if runtime is not None else {}
        self.queries: list[str] = []
        self.tsdb_limits: list[int | None] = []

    @staticmethod
    def _answer(value):
        if isinstance(value, PrometheusError):
            raise value
        return value

    def active_targets(self):
        return self._answer(self.targets)

    def instant_query(self, promql):
        self.queries.append(promql)
        return self._answer(self.query_result)

    def tsdb_status(self, limit=None):
        self.tsdb_limits.append(limit)
        return self._answer(self.tsdb)

    def runtime_info(self):
        return self._answer(self.runtime)


def sample(name: str, job: str, instance: str, value: str) -> dict:
    return {"metric": {"__name__": name, "job": job, "instance": instance}, "value": [0, value]}
