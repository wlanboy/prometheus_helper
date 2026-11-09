from __future__ import annotations

import argparse
import re
import sys
import threading
import time
from collections.abc import Callable, Iterable
from typing import TypeVar

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
    parse_timestamp,
)

T = TypeVar("T")

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
    def __init__(self, client: PrometheusClient, top_n: int = 5, key_labels: tuple[str, ...] = ()):
        self.client = client
        self.top_n = top_n
        self.key_labels = key_labels

    def describe(self):
        # Verhindert, dass REGISTRY.register() fuer die Namenspruefung einen kompletten Abruf ausloest
        return []

    @staticmethod
    def _fetch(
        section: str,
        fetch: Callable[[list[PrometheusError]], T],
        default: T,
        success: GaugeMetricFamily,
        duration: GaugeMetricFamily,
    ) -> T:
        """Fuehrt fetch aus und erfasst Dauer und Erfolg. fetch kann Teilfehler in die uebergebene Liste
        eintragen, ohne abzubrechen; auch dann gilt der Abschnitt als nicht erfolgreich."""
        errors: list[PrometheusError] = []
        start = time.perf_counter()
        try:
            result = fetch(errors)
        except PrometheusError as exc:
            print(f"Fehler beim Abfragen von '{section}': {exc}", file=sys.stderr)
            errors.append(exc)
            result = default
        except Exception as exc:  # noqa: BLE001 - bewusst breit, siehe Kommentar
            # Unerwartete Antwortstrukturen duerfen nicht den ganzen /metrics-Endpunkt mit 500 abbrechen,
            # sonst fehlt genau scrape_success=0
            print(f"Unerwarteter Fehler beim Abfragen von '{section}': {exc!r}", file=sys.stderr)
            errors.append(PrometheusError(repr(exc)))
            result = default
        duration.add_metric([section], time.perf_counter() - start)
        success.add_metric([section], 0.0 if errors else 1.0)
        return result

    def collect(self):
        scrape_success = GaugeMetricFamily(
            "prometheus_helper_scrape_success",
            "1 wenn die Abfrage des Abschnitts (targets, tsdb, runtime) vollstaendig erfolgreich war, sonst 0",
            labels=["section"],
        )
        scrape_duration = GaugeMetricFamily(
            "prometheus_helper_scrape_duration_seconds",
            "Dauer der Abfrage eines Abschnitts (targets, tsdb, runtime) in Sekunden",
            labels=["section"],
        )

        target_labels_names = ["job", "instance", *self.key_labels]
        target_up = GaugeMetricFamily(
            "prometheus_helper_target_up",
            "Health-Status eines Scrape-Targets (1 = up, 0 = nicht up)",
            labels=target_labels_names,
        )
        target_info = GaugeMetricFamily(
            "prometheus_helper_target_info",
            "Herkunft eines Targets (source = scrape oder remote_write), Wert immer 1",
            labels=[*target_labels_names, "source"],
        )
        target_labels = GaugeMetricFamily(
            "prometheus_helper_target_label_count",
            "Anzahl Labels eines Scrape-Targets nach dem Relabeling",
            labels=target_labels_names,
        )
        target_discovered_labels = GaugeMetricFamily(
            "prometheus_helper_target_discovered_label_count",
            "Anzahl Labels eines Scrape-Targets vor dem Relabeling (discoveredLabels)",
            labels=target_labels_names,
        )
        target_relabel_dropped_labels = GaugeMetricFamily(
            "prometheus_helper_target_relabel_dropped_labels",
            "Anzahl der durch Relabeling entfernten Labels (discoveredLabels - labels)",
            labels=target_labels_names,
        )
        target_metrics = GaugeMetricFamily(
            "prometheus_helper_target_metric_count",
            "Anzahl gescrapter Samples eines Targets (scrape_samples_scraped)",
            labels=target_labels_names,
        )
        target_metrics_post_relabel = GaugeMetricFamily(
            "prometheus_helper_target_metric_count_post_relabeling",
            "Anzahl gespeicherter Samples eines Targets nach metric_relabel_configs (scrape_samples_post_metric_relabeling)",
            labels=target_labels_names,
        )
        target_duration = GaugeMetricFamily(
            "prometheus_helper_target_scrape_duration_seconds",
            "Dauer des letzten Scrapes eines Targets in Sekunden",
            labels=target_labels_names,
        )
        target_series_added = GaugeMetricFamily(
            "prometheus_helper_target_series_added",
            "Neue Serien beim letzten Scrape eines Targets (scrape_series_added)",
            labels=target_labels_names,
        )
        target_last_error = GaugeMetricFamily(
            "prometheus_helper_target_last_scrape_error",
            "1 wenn beim letzten Scrape ein Fehler auftrat, sonst 0 (Fehlertext: main.py)",
            labels=target_labels_names,
        )

        statuses = self._fetch(
            "targets",
            lambda errors: collect_target_status(self.client, errors, self.key_labels),
            [],
            scrape_success,
            scrape_duration,
        )

        for s in statuses:
            key = [s.job, s.instance, *s.key_values]
            target_info.add_metric([*key, s.source], 1.0)
            if s.health != "n/a":
                target_up.add_metric(key, 1.0 if s.health == "up" else 0.0)
            if s.label_count is not None:
                target_labels.add_metric(key, s.label_count)
            if s.discovered_label_count is not None:
                target_discovered_labels.add_metric(key, s.discovered_label_count)
            if s.relabel_dropped_labels is not None:
                target_relabel_dropped_labels.add_metric(key, s.relabel_dropped_labels)
            if s.metric_count is not None:
                target_metrics.add_metric(key, s.metric_count)
            if s.metric_count_post_relabel is not None:
                target_metrics_post_relabel.add_metric(key, s.metric_count_post_relabel)
            if s.scrape_duration is not None:
                target_duration.add_metric(key, s.scrape_duration)
            if s.series_added is not None:
                target_series_added.add_metric(key, s.series_added)
            target_last_error.add_metric(
                key, 1.0 if s.last_error or s.health == "down" else 0.0
            )

        yield target_info
        yield target_up
        yield target_labels
        yield target_discovered_labels
        yield target_relabel_dropped_labels
        yield target_metrics
        yield target_metrics_post_relabel
        yield target_duration
        yield target_series_added
        yield target_last_error

        tsdb = self._fetch(
            "tsdb",
            lambda _errors: collect_tsdb_status(self.client, top_n=self.top_n),
            TsdbStatus(None, None, None, [], [], []),
            scrape_success,
            scrape_duration,
        )

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

        runtime = self._fetch(
            "runtime",
            lambda _errors: collect_runtime_status(self.client),
            RuntimeStatus(None, None, None, None, None, None),
            scrape_success,
            scrape_duration,
        )

        start_time = GaugeMetricFamily(
            "prometheus_helper_runtime_start_time_seconds",
            "Startzeitpunkt des Prometheus-Prozesses als Unix-Timestamp (Uptime: time() - Wert)",
        )
        start_time_value = parse_timestamp(runtime.start_time)
        if start_time_value is not None:
            start_time.add_metric([], start_time_value.timestamp())
        yield start_time

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
            # Kein _total-Suffix: der Wert ist ein Gauge, kein Counter
            "prometheus_helper_runtime_corruptions",
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

        yield scrape_success
        yield scrape_duration


class CachingCollector:
    """Fragt den inneren Collector in einem Hintergrund-Thread periodisch ab und liefert bei jedem Scrape
    den letzten Stand. Scrapes antworten dadurch sofort, auch wenn Prometheus haengt, und mehrere scrapende
    Prometheus-Instanzen vervielfachen die Abfragen nicht."""

    def __init__(self, inner: PrometheusHelperCollector, interval: float):
        self.inner = inner
        self.interval = interval
        self._families: list = []
        self._last_refresh: float | None = None
        self._stop = threading.Event()

    def describe(self):
        return []

    def refresh(self) -> None:
        try:
            families = list(self.inner.collect())
        except Exception as exc:  # noqa: BLE001 - der Refresh-Thread darf nicht sterben
            print(f"Aktualisierung fehlgeschlagen: {exc!r}", file=sys.stderr)
            return
        # Zuweisung einer neuen Liste ist atomar, collect() sieht immer einen vollstaendigen Stand
        self._families = families
        self._last_refresh = time.time()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh()
            self._stop.wait(self.interval)

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, name="prometheus-helper-refresh", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()

    def collect(self) -> Iterable:
        yield from self._families
        last_refresh = GaugeMetricFamily(
            "prometheus_helper_last_refresh_timestamp_seconds",
            "Zeitpunkt der letzten abgeschlossenen Aktualisierung als Unix-Timestamp (Alter: time() - Wert)",
        )
        if self._last_refresh is not None:
            last_refresh.add_metric([], self._last_refresh)
        yield last_refresh


def non_negative_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"keine Zahl: {value!r}") from exc
    if number < 0:
        raise argparse.ArgumentTypeError(f"darf nicht negativ sein: {value!r}")
    return number


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stellt den Status eines Prometheus-Servers und dessen Scrape-Targets als Prometheus-Metriken bereit."
    )
    add_common_args(parser)
    parser.add_argument("--port", type=int, default=9877, help="Port fuer den Metrik-Endpunkt (Standard: 9877)")
    parser.add_argument(
        "--bind", default="0.0.0.0", help="Adresse, auf der der Metrik-Endpunkt lauscht (Standard: 0.0.0.0)"
    )
    parser.add_argument(
        "--refresh-interval",
        type=non_negative_float,
        default=30.0,
        help=(
            "Abstand der Hintergrund-Aktualisierung in Sekunden; 0 fragt bei jedem Scrape live ab "
            "(Standard: 30)"
        ),
    )
    parser.add_argument(
        "--top-n", type=int, default=5, help="Anzahl der Top-Eintraege je TSDB-Rangliste (Standard: 5)"
    )
    args = parser.parse_args()

    client = create_client_or_exit(args)

    collector = PrometheusHelperCollector(client, top_n=args.top_n, key_labels=tuple(dict.fromkeys(args.key_labels)))
    if args.refresh_interval > 0:
        cached = CachingCollector(collector, args.refresh_interval)
        cached.start()
        REGISTRY.register(cached)
    else:
        REGISTRY.register(collector)
    start_http_server(args.port, addr=args.bind)
    print(f"Metrik-Endpunkt laeuft auf http://{args.bind}:{args.port}/metrics")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
