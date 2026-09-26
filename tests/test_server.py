from __future__ import annotations

from datetime import datetime, timezone

import pytest
from conftest import FakeClient, sample

from main import PrometheusError
from server import PrometheusHelperCollector, parse_duration_to_seconds


@pytest.mark.parametrize(
    "value, expected",
    [
        ("15d", 15 * 86400),
        ("1w2d", 604800 + 2 * 86400),
        ("1h30m", 5400),
        ("500ms", 0.5),
        ("1y", 31536000),
        ("", None),
        (None, None),
        ("unbekannt", None),
    ],
)
def test_parse_duration_to_seconds(value, expected):
    assert parse_duration_to_seconds(value) == expected


def collect(client) -> dict[str, dict[tuple, float]]:
    """Liefert {metrikname: {labelwerte: wert}} aus einem Collector-Durchlauf."""
    result: dict[str, dict[tuple, float]] = {}
    for family in PrometheusHelperCollector(client, top_n=2).collect():
        result[family.name] = {tuple(s.labels.values()): s.value for s in family.samples}
    return result


def healthy_client() -> FakeClient:
    return FakeClient(
        targets=[
            {
                "labels": {"job": "node", "instance": "h1", "env": "prod"},
                "discoveredLabels": {"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"},
                "health": "up",
                "lastError": "",
            },
            {
                "labels": {"job": "node", "instance": "h2"},
                "health": "down",
                "lastError": "connection refused",
            },
        ],
        query_result=[
            sample("up", "node", "h1", "1"),
            sample("scrape_samples_scraped", "node", "h1", "100"),
            sample("scrape_samples_post_metric_relabeling", "node", "h1", "80"),
            sample("scrape_duration_seconds", "node", "h1", "0.5"),
            sample("scrape_series_added", "node", "h1", "3"),
            sample("up", "alloy", "r1", "1"),
        ],
        tsdb={
            "headStats": {"numSeries": 1000, "numLabelPairs": 50, "chunkCount": 2000},
            "seriesCountByMetricName": [
                {"name": "m1", "value": 10},
                {"name": "m2", "value": "kein-wert"},
                {"name": "m3", "value": 5},
            ],
        },
        runtime={
            "startTime": "2026-01-01T00:00:00.123Z",
            "reloadConfigSuccess": False,
            "goroutineCount": 77,
            "corruptionCount": 1,
            "storageRetention": "15d",
        },
    )


class TestCollector:
    def test_target_metrics(self):
        metrics = collect(healthy_client())

        assert metrics["prometheus_helper_target_info"] == {
            ("node", "h1", "scrape"): 1.0,
            ("node", "h2", "scrape"): 1.0,
            ("alloy", "r1", "remote_write"): 1.0,
        }
        assert metrics["prometheus_helper_target_up"] == {
            ("node", "h1"): 1.0,
            ("node", "h2"): 0.0,
            ("alloy", "r1"): 1.0,
        }
        # Remote-Write-Targets haben keine Labelinformationen
        assert metrics["prometheus_helper_target_label_count"] == {("node", "h1"): 3, ("node", "h2"): 2}
        assert metrics["prometheus_helper_target_discovered_label_count"] == {("node", "h1"): 5, ("node", "h2"): 0}
        assert metrics["prometheus_helper_target_relabel_dropped_labels"] == {("node", "h1"): 2, ("node", "h2"): -2}
        assert metrics["prometheus_helper_target_metric_count"] == {("node", "h1"): 100}
        assert metrics["prometheus_helper_target_metric_count_post_relabeling"] == {("node", "h1"): 80}
        assert metrics["prometheus_helper_target_scrape_duration_seconds"] == {("node", "h1"): 0.5}
        assert metrics["prometheus_helper_target_series_added"] == {("node", "h1"): 3}
        assert metrics["prometheus_helper_target_last_scrape_error"] == {
            ("node", "h1"): 0.0,
            ("node", "h2"): 1.0,
            ("alloy", "r1"): 0.0,
        }

    def test_remote_write_target_without_up_has_no_up_metric(self):
        client = FakeClient(query_result=[sample("scrape_samples_scraped", "alloy", "r1", "5")])
        metrics = collect(client)
        assert metrics["prometheus_helper_target_info"] == {("alloy", "r1", "remote_write"): 1.0}
        assert metrics["prometheus_helper_target_up"] == {}

    def test_tsdb_metrics_skip_non_numeric_and_respect_top_n(self):
        metrics = collect(healthy_client())

        assert metrics["prometheus_helper_tsdb_head_series"] == {(): 1000}
        assert metrics["prometheus_helper_tsdb_head_label_pairs"] == {(): 50}
        assert metrics["prometheus_helper_tsdb_head_chunks"] == {(): 2000}
        # top_n=2 schneidet m3 ab, m2 ist nicht numerisch
        assert metrics["prometheus_helper_tsdb_series_count_by_metric_name"] == {("m1",): 10.0}
        assert metrics["prometheus_helper_tsdb_memory_bytes_by_label_name"] == {}

    def test_runtime_metrics(self):
        metrics = collect(healthy_client())

        assert metrics["prometheus_helper_runtime_start_time_seconds"] == {
            (): datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
        }
        assert metrics["prometheus_helper_runtime_reload_config_success"] == {(): 0.0}
        assert metrics["prometheus_helper_runtime_goroutines"] == {(): 77}
        assert metrics["prometheus_helper_runtime_corruptions_total"] == {(): 1}
        assert metrics["prometheus_helper_runtime_storage_retention_seconds"] == {(): 15 * 86400}

    def test_all_sections_successful(self):
        metrics = collect(healthy_client())

        assert metrics["prometheus_helper_scrape_success"] == {("targets",): 1.0, ("tsdb",): 1.0, ("runtime",): 1.0}
        durations = metrics["prometheus_helper_scrape_duration_seconds"]
        assert set(durations) == {("targets",), ("tsdb",), ("runtime",)}
        assert all(v >= 0 for v in durations.values())

    def test_failed_sections_yield_empty_metrics(self, capsys):
        client = FakeClient(
            targets=PrometheusError("targets kaputt"),
            tsdb=PrometheusError("tsdb kaputt"),
            runtime=PrometheusError("runtime kaputt"),
        )

        metrics = collect(client)

        assert metrics["prometheus_helper_scrape_success"] == {("targets",): 0.0, ("tsdb",): 0.0, ("runtime",): 0.0}
        assert metrics["prometheus_helper_target_info"] == {}
        assert metrics["prometheus_helper_tsdb_head_series"] == {}
        assert metrics["prometheus_helper_runtime_goroutines"] == {}
        assert metrics["prometheus_helper_runtime_start_time_seconds"] == {}
        assert metrics["prometheus_helper_runtime_storage_retention_seconds"] == {}
        err = capsys.readouterr().err
        assert "'targets'" in err and "'tsdb'" in err and "'runtime'" in err

    def test_partial_query_error_marks_targets_failed_but_keeps_targets(self):
        client = healthy_client()
        client.query_result = PrometheusError("query kaputt")

        metrics = collect(client)

        assert metrics["prometheus_helper_scrape_success"][("targets",)] == 0.0
        assert metrics["prometheus_helper_scrape_success"][("tsdb",)] == 1.0
        assert set(metrics["prometheus_helper_target_info"]) == {("node", "h1", "scrape"), ("node", "h2", "scrape")}
        assert metrics["prometheus_helper_target_metric_count"] == {}
