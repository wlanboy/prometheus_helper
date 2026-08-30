from __future__ import annotations

import argparse
import re
import sys
import time

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily

from main import (
    PrometheusClient,
    PrometheusError,
    RuntimeStatus,
    TsdbStatus,
    add_common_args,
    collect_runtime_status,
    collect_target_status,
    collect_tsdb_status,
    create_client_or_exit,
)

_DURATION_RE = re.compile(r"(\d+)(ms|s|m|h|d|w|y)")
_DURATION_UNIT_SECONDS = {
    "ms": 0.001,
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "y": 31536000,
}


def parse_duration_to_seconds(value: str) -> float | None:
    matches = _DURATION_RE.findall(value or "")
    if not matches:
        return None
    return sum(int(amount) * _DURATION_UNIT_SECONDS[unit] for amount, unit in matches)


class PrometheusHelperCollector:
    def __init__(self, client: PrometheusClient, top_n: int = 5):
        self.client = client
        self.top_n = top_n

    def collect(self):
        target_up = GaugeMetricFamily(
            "prometheus_helper_target_up",
            "Health-Status eines Scrape-Targets (1 = up, 0 = nicht up)",
            labels=["job", "instance"],
        )
        target_labels = GaugeMetricFamily(
            "prometheus_helper_target_label_count",
            "Anzahl Labels eines Scrape-Targets nach dem Relabeling",
            labels=["job", "instance"],
        )
        target_discovered_labels = GaugeMetricFamily(
            "prometheus_helper_target_discovered_label_count",
            "Anzahl Labels eines Scrape-Targets vor dem Relabeling (discoveredLabels)",
            labels=["job", "instance"],
        )
        target_relabel_dropped_labels = GaugeMetricFamily(
            "prometheus_helper_target_relabel_dropped_labels",
            "Anzahl der durch Relabeling entfernten Labels (discoveredLabels - labels)",
            labels=["job", "instance"],
        )
        target_metrics = GaugeMetricFamily(
            "prometheus_helper_target_metric_count",
            "Anzahl gescrapter Samples eines Targets (scrape_samples_scraped)",
            labels=["job", "instance"],
        )
        target_duration = GaugeMetricFamily(
            "prometheus_helper_target_scrape_duration_seconds",
            "Dauer des letzten Scrapes eines Targets in Sekunden",
            labels=["job", "instance"],
        )
        target_series_added = GaugeMetricFamily(
            "prometheus_helper_target_series_added",
            "Neue Serien beim letzten Scrape eines Targets (scrape_series_added)",
            labels=["job", "instance"],
        )
        target_last_error = GaugeMetricFamily(
            "prometheus_helper_target_last_scrape_error",
            "1 wenn beim letzten Scrape ein Fehler auftrat, sonst 0",
            labels=["job", "instance", "error"],
        )

        try:
            statuses = collect_target_status(self.client)
        except PrometheusError as exc:
            print(f"Fehler beim Abfragen der Targets: {exc}", file=sys.stderr)
            statuses = []

        for s in statuses:
            target_up.add_metric([s.job, s.instance], 1.0 if s.health == "up" else 0.0)
            target_labels.add_metric([s.job, s.instance], s.label_count)
            target_discovered_labels.add_metric([s.job, s.instance], s.discovered_label_count)
            target_relabel_dropped_labels.add_metric(
                [s.job, s.instance], s.discovered_label_count - s.label_count
            )
            if s.metric_count is not None:
                target_metrics.add_metric([s.job, s.instance], s.metric_count)
            if s.scrape_duration is not None:
                target_duration.add_metric([s.job, s.instance], s.scrape_duration)
            if s.series_added is not None:
                target_series_added.add_metric([s.job, s.instance], s.series_added)
            target_last_error.add_metric(
                [s.job, s.instance, s.last_error], 1.0 if s.last_error else 0.0
            )

        yield target_up
        yield target_labels
        yield target_discovered_labels
        yield target_relabel_dropped_labels
        yield target_metrics
        yield target_duration
        yield target_series_added
        yield target_last_error

        try:
            tsdb = collect_tsdb_status(self.client, top_n=self.top_n)
        except PrometheusError as exc:
            print(f"Fehler beim Abfragen des TSDB-Status: {exc}", file=sys.stderr)
            tsdb = TsdbStatus(None, None, None, [], [], [])

        head_series = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_series", "Anzahl Serien im TSDB-Head"
        )
        head_label_pairs = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_label_pairs", "Anzahl Label-Paare im TSDB-Head"
        )
        head_chunks = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_chunks", "Anzahl Chunks im TSDB-Head"
        )
        if tsdb.num_series is not None:
            head_series.add_metric([], tsdb.num_series)
        if tsdb.num_label_pairs is not None:
            head_label_pairs.add_metric([], tsdb.num_label_pairs)
        if tsdb.chunk_count is not None:
            head_chunks.add_metric([], tsdb.chunk_count)
        yield head_series
        yield head_label_pairs
        yield head_chunks

        def top_metric_family(name: str, help_text: str, entries) -> GaugeMetricFamily:
            family = GaugeMetricFamily(name, help_text, labels=["name"])
            for entry in entries:
                try:
                    value = float(entry.value)
                except (TypeError, ValueError):
                    continue
                family.add_metric([entry.name], value)
            return family

        yield top_metric_family(
            "prometheus_helper_tsdb_series_count_by_metric_name",
            "Top-Metriken nach Serienanzahl",
            tsdb.top_series_by_metric,
        )
        yield top_metric_family(
            "prometheus_helper_tsdb_label_value_count_by_label_name",
            "Top-Labels nach Anzahl unterschiedlicher Werte (Cardinality)",
            tsdb.top_label_value_count,
        )
        yield top_metric_family(
            "prometheus_helper_tsdb_memory_bytes_by_label_name",
            "Top-Labels nach Speicherverbrauch in Bytes",
            tsdb.top_memory_by_label,
        )

        try:
            runtime = collect_runtime_status(self.client)
        except PrometheusError as exc:
            print(f"Fehler beim Abfragen der Runtime-Info: {exc}", file=sys.stderr)
            runtime = RuntimeStatus(None, None, None, None, None, None)

        reload_success = GaugeMetricFamily(
            "prometheus_helper_runtime_reload_config_success",
            "1 wenn der letzte Config-Reload erfolgreich war, sonst 0",
        )
        if runtime.reload_config_success is not None:
            reload_success.add_metric([], 1.0 if runtime.reload_config_success else 0.0)
        yield reload_success

        goroutines = GaugeMetricFamily(
            "prometheus_helper_runtime_goroutines",
            "Anzahl laufender Goroutinen im Prometheus-Prozess",
        )
        if runtime.goroutine_count is not None:
            goroutines.add_metric([], runtime.goroutine_count)
        yield goroutines

        corruptions = GaugeMetricFamily(
            "prometheus_helper_runtime_corruptions_total",
            "Anzahl erkannter WAL-/Storage-Korruptionen (corruptionCount)",
        )
        if runtime.corruption_count is not None:
            corruptions.add_metric([], runtime.corruption_count)
        yield corruptions

        retention_seconds = GaugeMetricFamily(
            "prometheus_helper_runtime_storage_retention_seconds",
            "Konfigurierte Storage-Retention in Sekunden",
        )
        retention_value = parse_duration_to_seconds(runtime.storage_retention or "")
        if retention_value is not None:
            retention_seconds.add_metric([], retention_value)
        yield retention_seconds


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stellt den Status eines Prometheus-Servers und dessen Scrape-Targets als Prometheus-Metriken bereit."
    )
    add_common_args(parser)
    parser.add_argument("--port", type=int, default=9877, help="Port fuer den Metrik-Endpunkt (Standard: 9877)")
    parser.add_argument(
        "--top-n", type=int, default=5, help="Anzahl der Top-Eintraege je TSDB-Rangliste (Standard: 5)"
    )
    args = parser.parse_args()

    client = create_client_or_exit(args)

    REGISTRY.register(PrometheusHelperCollector(client, top_n=args.top_n))
    start_http_server(args.port)
    print(f"Metrik-Endpunkt laeuft auf http://0.0.0.0:{args.port}/metrics")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
