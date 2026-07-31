from __future__ import annotations

import argparse
import sys
import time

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily

from main import (
    PrometheusClient,
    PrometheusError,
    collect_target_status,
    load_config,
)


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
            "Anzahl Labels eines Scrape-Targets",
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
        yield target_metrics
        yield target_duration
        yield target_series_added
        yield target_last_error

        try:
            tsdb = self.client.tsdb_status()
        except PrometheusError as exc:
            print(f"Fehler beim Abfragen des TSDB-Status: {exc}", file=sys.stderr)
            tsdb = {}

        head_stats = tsdb.get("headStats", {})
        head_series = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_series", "Anzahl Serien im TSDB-Head"
        )
        head_label_pairs = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_label_pairs", "Anzahl Label-Paare im TSDB-Head"
        )
        head_chunks = GaugeMetricFamily(
            "prometheus_helper_tsdb_head_chunks", "Anzahl Chunks im TSDB-Head"
        )
        if "numSeries" in head_stats:
            head_series.add_metric([], head_stats["numSeries"])
        if "numLabelPairs" in head_stats:
            head_label_pairs.add_metric([], head_stats["numLabelPairs"])
        if "chunkCount" in head_stats:
            head_chunks.add_metric([], head_stats["chunkCount"])
        yield head_series
        yield head_label_pairs
        yield head_chunks

        def top_metric_family(name: str, help_text: str, key: str) -> GaugeMetricFamily:
            family = GaugeMetricFamily(name, help_text, labels=["name"])
            for entry in tsdb.get(key, [])[: self.top_n]:
                try:
                    value = float(entry.get("value"))
                except (TypeError, ValueError):
                    continue
                family.add_metric([str(entry.get("name", "?"))], value)
            return family

        yield top_metric_family(
            "prometheus_helper_tsdb_series_count_by_metric_name",
            "Top-Metriken nach Serienanzahl",
            "seriesCountByMetricName",
        )
        yield top_metric_family(
            "prometheus_helper_tsdb_label_value_count_by_label_name",
            "Top-Labels nach Anzahl unterschiedlicher Werte (Cardinality)",
            "labelValueCountByLabelName",
        )
        yield top_metric_family(
            "prometheus_helper_tsdb_memory_bytes_by_label_name",
            "Top-Labels nach Speicherverbrauch in Bytes",
            "memoryInBytesByLabelName",
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stellt den Status eines Prometheus-Servers und dessen Scrape-Targets als Prometheus-Metriken bereit."
    )
    parser.add_argument("--env-file", help="Pfad zu einer .env-Datei (Standard: .env im aktuellen Verzeichnis)")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="SSL-Zertifikatspruefung deaktivieren (z.B. bei selbstsignierten Zertifikaten)",
    )
    parser.add_argument("--port", type=int, default=9877, help="Port fuer den Metrik-Endpunkt (Standard: 9877)")
    parser.add_argument(
        "--top-n", type=int, default=5, help="Anzahl der Top-Eintraege je TSDB-Rangliste (Standard: 5)"
    )
    args = parser.parse_args()

    try:
        url, user, password, verify_ssl = load_config(args.env_file)
        if args.insecure:
            verify_ssl = False
        client = PrometheusClient(url, user, password, verify_ssl=verify_ssl)
    except PrometheusError as exc:
        print(f"Fehler: {exc}", file=sys.stderr)
        sys.exit(1)

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
