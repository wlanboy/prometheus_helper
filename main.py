from __future__ import annotations

import argparse
import os
import statistics
import sys
from dataclasses import dataclass
from typing import NamedTuple

import requests
import urllib3
from dotenv import load_dotenv


class TargetKey(NamedTuple):
    job: str
    instance: str


SOURCE_SCRAPE = "scrape"
SOURCE_REMOTE_WRITE = "remote_write"


@dataclass
class TargetStatus:
    job: str
    instance: str
    source: str
    health: str
    label_count: int | None
    discovered_label_count: int | None
    metric_count: int | None
    scrape_duration: float | None
    series_added: int | None
    last_error: str

    @property
    def relabel_dropped_labels(self) -> int | None:
        if self.label_count is None or self.discovered_label_count is None:
            return None
        return self.discovered_label_count - self.label_count

    def conspicuous_flags(self, avg_metrics: float | None, avg_labels: float | None) -> list[str]:
        above_metrics = avg_metrics is not None and self.metric_count is not None and self.metric_count > avg_metrics
        above_labels = avg_labels is not None and self.label_count is not None and self.label_count > avg_labels
        if above_metrics and above_labels:
            return ["METRIKEN+LABELS"]
        if above_metrics:
            return ["METRIKEN"]
        if above_labels:
            return ["LABELS"]
        return []


@dataclass
class PrometheusConfig:
    url: str
    user: str | None
    password: str | None
    verify_ssl: bool
    ca_bundle: str | None = None


@dataclass
class TsdbTopEntry:
    name: str
    value: float | int | None


@dataclass
class TsdbStatus:
    num_series: int | None
    num_label_pairs: int | None
    chunk_count: int | None
    top_series_by_metric: list[TsdbTopEntry]
    top_label_value_count: list[TsdbTopEntry]
    top_memory_by_label: list[TsdbTopEntry]


@dataclass
class RuntimeStatus:
    start_time: str | None
    reload_config_success: bool | None
    last_config_time: str | None
    storage_retention: str | None
    corruption_count: int | None
    goroutine_count: int | None


class PrometheusError(Exception):
    pass


class PrometheusClient:
    def __init__(
        self,
        base_url: str,
        user: str | None,
        password: str | None,
        verify_ssl: bool = True,
        timeout: float = 10.0,
        ca_bundle: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Eine Session haelt die TLS-Verbindung ueber alle API-Aufrufe hinweg offen
        self.session = requests.Session()
        self.session.auth = (user, password) if user and password else None
        # requests akzeptiert fuer verify entweder bool oder den Pfad zu einem CA-Bundle
        self.session.verify = ca_bundle if verify_ssl and ca_bundle else verify_ssl
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.exceptions.RequestException as exc:
            raise PrometheusError(f"Verbindung zu {url} fehlgeschlagen: {exc}") from exc

        if response.status_code == 401:
            raise PrometheusError("Authentifizierung fehlgeschlagen (401) - Basic Auth Zugangsdaten pruefen")
        if not response.ok:
            raise PrometheusError(f"{url} antwortete mit HTTP {response.status_code}: {response.text[:200]}")

        payload = response.json()
        if payload.get("status") != "success":
            raise PrometheusError(f"Prometheus meldete Fehler: {payload.get('error', payload)}")
        return payload["data"]

    def active_targets(self) -> list[dict]:
        data = self._get("/api/v1/targets", params={"state": "active"})
        return data.get("activeTargets", [])

    def instant_query(self, promql: str) -> list[dict]:
        data = self._get("/api/v1/query", params={"query": promql})
        return data.get("result", [])

    def tsdb_status(self) -> dict:
        return self._get("/api/v1/status/tsdb")

    def runtime_info(self) -> dict:
        return self._get("/api/v1/status/runtimeinfo")


def query_metrics_by_target(client: PrometheusClient, metric_names: list[str]) -> dict[str, dict[TargetKey, float]]:
    values: dict[str, dict[TargetKey, float]] = {name: {} for name in metric_names}
    promql = '{__name__=~"' + "|".join(metric_names) + '"}'
    try:
        result = client.instant_query(promql)
    except PrometheusError as exc:
        print(f"Warnung: Abfrage von {', '.join(metric_names)} fehlgeschlagen: {exc}", file=sys.stderr)
        return values

    for entry in result:
        metric = entry.get("metric", {})
        name = metric.get("__name__")
        job = metric.get("job")
        instance = metric.get("instance")
        if name not in values or job is None or instance is None:
            continue
        try:
            values[name][TargetKey(job, instance)] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError):
            continue
    return values


def collect_target_status(client: PrometheusClient) -> list[TargetStatus]:
    targets = client.active_targets()
    metrics = query_metrics_by_target(
        client, ["up", "scrape_samples_scraped", "scrape_duration_seconds", "scrape_series_added"]
    )
    up = metrics["up"]
    samples_scraped = metrics["scrape_samples_scraped"]
    scrape_duration = metrics["scrape_duration_seconds"]
    series_added = metrics["scrape_series_added"]

    def build_status(key: TargetKey, **kwargs) -> TargetStatus:
        metric_value = samples_scraped.get(key)
        series_value = series_added.get(key)
        return TargetStatus(
            job=key.job,
            instance=key.instance,
            metric_count=int(metric_value) if metric_value is not None else None,
            scrape_duration=scrape_duration.get(key),
            series_added=int(series_value) if series_value is not None else None,
            **kwargs,
        )

    statuses = []
    for target in targets:
        labels = target.get("labels", {})
        key = TargetKey(labels.get("job", "unknown"), labels.get("instance", "unknown"))
        statuses.append(
            build_status(
                key,
                source=SOURCE_SCRAPE,
                health=target.get("health", "unknown"),
                label_count=len(labels),
                discovered_label_count=len(target.get("discoveredLabels", {})),
                last_error=target.get("lastError", ""),
            )
        )

    # Per Remote-Write eingelieferte Scrapes (z.B. Grafana Alloy) tauchen nicht in /api/v1/targets auf,
    # hinterlassen aber up/scrape_*-Serien. Labels und Fehlertext sind fuer diese nicht bekannt.
    known = {TargetKey(s.job, s.instance) for s in statuses}
    remote_keys = (up.keys() | samples_scraped.keys()) - known
    for key in sorted(remote_keys):
        up_value = up.get(key)
        statuses.append(
            build_status(
                key,
                source=SOURCE_REMOTE_WRITE,
                health="n/a" if up_value is None else ("up" if up_value == 1 else "down"),
                label_count=None,
                discovered_label_count=None,
                last_error="",
            )
        )
    return statuses


def collect_tsdb_status(client: PrometheusClient, top_n: int = 5) -> TsdbStatus:
    data = client.tsdb_status()
    head_stats = data.get("headStats", {})

    def top_entries(key: str) -> list[TsdbTopEntry]:
        return [
            TsdbTopEntry(name=str(entry.get("name", "?")), value=entry.get("value"))
            for entry in data.get(key, [])[:top_n]
        ]

    return TsdbStatus(
        num_series=head_stats.get("numSeries"),
        num_label_pairs=head_stats.get("numLabelPairs"),
        chunk_count=head_stats.get("chunkCount"),
        top_series_by_metric=top_entries("seriesCountByMetricName"),
        top_label_value_count=top_entries("labelValueCountByLabelName"),
        top_memory_by_label=top_entries("memoryInBytesByLabelName"),
    )


def collect_runtime_status(client: PrometheusClient) -> RuntimeStatus:
    data = client.runtime_info()
    return RuntimeStatus(
        start_time=data.get("startTime"),
        reload_config_success=data.get("reloadConfigSuccess"),
        last_config_time=data.get("lastConfigTime"),
        storage_retention=data.get("storageRetention"),
        corruption_count=data.get("corruptionCount"),
        goroutine_count=data.get("goroutineCount"),
    )


def print_report(statuses: list[TargetStatus]) -> None:
    if not statuses:
        print("Keine aktiven Targets gefunden.")
        return

    metric_counts = [s.metric_count for s in statuses if s.metric_count is not None]
    label_counts = [s.label_count for s in statuses if s.label_count is not None]

    avg_metrics = statistics.mean(metric_counts) if metric_counts else None
    avg_labels = statistics.mean(label_counts) if label_counts else None

    remote_count = sum(1 for s in statuses if s.source == SOURCE_REMOTE_WRITE)
    print(f"Targets: {len(statuses)} (davon {remote_count} per Remote-Write)")
    if avg_metrics is not None:
        print(f"Durchschnittliche Metrikanzahl je Target: {avg_metrics:.1f}")
    if avg_labels is not None:
        print(f"Durchschnittliche Labelanzahl je Target (nur gescrapte Targets): {avg_labels:.1f}")
    print()

    def or_na(value) -> str:
        return str(value) if value is not None else "n/a"

    rows = []
    for s in sorted(statuses, key=lambda s: (s.metric_count or -1), reverse=True):
        flags = s.conspicuous_flags(avg_metrics, avg_labels)
        rows.append(
            (
                f"{s.job}/{s.instance}",
                s.source,
                s.health,
                or_na(s.metric_count),
                or_na(s.label_count),
                or_na(s.discovered_label_count),
                or_na(s.relabel_dropped_labels),
                f"{s.scrape_duration:.3f}" if s.scrape_duration is not None else "n/a",
                or_na(s.series_added),
                ", ".join(flags),
            )
        )

    headers = (
        "TARGET",
        "QUELLE",
        "HEALTH",
        "METRIKEN",
        "LABELS",
        "LABELS_ROH",
        "RELABEL_ENTF",
        "DAUER(s)",
        "NEUE_SERIEN",
        "AUFFAELLIG",
    )
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in rows)) for i in range(len(headers))
    ]
    header_line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print("  ".join(c.ljust(w) for c, w in zip(row, widths)))

    conspicuous = [r for r in rows if r[-1]]
    print()
    if conspicuous:
        print(f"{len(conspicuous)} Target(s) auffaellig (ueberdurchschnittlich Metriken und/oder Labels).")
    else:
        print("Keine auffaelligen Targets gefunden.")

    failing = [s for s in statuses if s.last_error or s.health == "down"]
    if failing:
        print()
        print(f"Targets mit Scrape-Fehler ({len(failing)}):")
        for s in failing:
            print(f"  {s.job}/{s.instance}: {s.last_error or '(up == 0, kein Fehlertext verfuegbar)'}")


def print_tsdb_report(client: PrometheusClient, top_n: int = 5) -> None:
    try:
        tsdb = collect_tsdb_status(client, top_n=top_n)
    except PrometheusError as exc:
        print()
        print(f"TSDB-Status nicht verfuegbar: {exc}")
        return

    print()
    print("TSDB-Uebersicht (global):")
    if tsdb.num_series is not None or tsdb.num_label_pairs is not None or tsdb.chunk_count is not None:
        num_series = tsdb.num_series if tsdb.num_series is not None else "n/a"
        num_label_pairs = tsdb.num_label_pairs if tsdb.num_label_pairs is not None else "n/a"
        chunk_count = tsdb.chunk_count if tsdb.chunk_count is not None else "n/a"
        print(f"  Serien: {num_series}  Label-Paare: {num_label_pairs}  Chunks: {chunk_count}")

    def print_top(title: str, entries: list[TsdbTopEntry]) -> None:
        print(f"  {title}:")
        if not entries:
            print("    (keine Daten)")
            return
        for entry in entries:
            value = entry.value if entry.value is not None else "?"
            print(f"    {entry.name}: {value}")

    print_top(f"Top {top_n} Metriken nach Serienanzahl", tsdb.top_series_by_metric)
    print_top(f"Top {top_n} Labels nach Anzahl unterschiedlicher Werte (Cardinality)", tsdb.top_label_value_count)
    print_top(f"Top {top_n} Labels nach Speicherverbrauch", tsdb.top_memory_by_label)


def print_runtime_report(client: PrometheusClient) -> None:
    try:
        runtime = collect_runtime_status(client)
    except PrometheusError as exc:
        print()
        print(f"Runtime-Info nicht verfuegbar: {exc}")
        return

    def or_na(value):
        return value if value is not None else "n/a"

    print()
    print("Runtime-Info:")
    print(f"  Startzeit: {or_na(runtime.start_time)}")
    print(f"  Config-Reload erfolgreich: {or_na(runtime.reload_config_success)}")
    print(f"  Letzte Config-Aenderung: {or_na(runtime.last_config_time)}")
    print(f"  Storage-Retention: {or_na(runtime.storage_retention)}")
    print(f"  WAL-/Storage-Korruptionen: {or_na(runtime.corruption_count)}")
    print(f"  Goroutinen: {or_na(runtime.goroutine_count)}")


def str_to_bool(value: str) -> bool:
    return value.strip().lower() not in ("false", "0", "no", "off")


def load_config(env_file: str | None) -> PrometheusConfig:
    load_dotenv(dotenv_path=env_file) if env_file else load_dotenv()

    url = os.getenv("PROMETHEUS_URL")
    if not url:
        raise PrometheusError("PROMETHEUS_URL ist nicht gesetzt (Umgebungsvariable oder .env-Datei).")

    user = os.getenv("PROMETHEUS_USER")
    password = os.getenv("PROMETHEUS_PASSWORD")
    verify_ssl = str_to_bool(os.getenv("PROMETHEUS_VERIFY_SSL", "true"))
    ca_bundle = os.getenv("PROMETHEUS_CA_BUNDLE") or None
    return PrometheusConfig(url=url, user=user, password=password, verify_ssl=verify_ssl, ca_bundle=ca_bundle)


def build_client(config: PrometheusConfig, insecure: bool = False, ca_file: str | None = None) -> PrometheusClient:
    verify_ssl = False if insecure else config.verify_ssl
    ca_bundle = ca_file or config.ca_bundle
    if verify_ssl and ca_bundle and not os.path.isfile(ca_bundle):
        raise PrometheusError(f"CA-Bundle {ca_bundle} existiert nicht.")
    return PrometheusClient(config.url, config.user, config.password, verify_ssl=verify_ssl, ca_bundle=ca_bundle)


def exit_with_error(exc: PrometheusError) -> None:
    print(f"Fehler: {exc}", file=sys.stderr)
    sys.exit(1)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env-file", help="Pfad zu einer .env-Datei (Standard: .env im aktuellen Verzeichnis)")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="SSL-Zertifikatspruefung deaktivieren (z.B. bei selbstsignierten Zertifikaten)",
    )
    parser.add_argument(
        "--ca-file",
        help="Pfad zu einem CA-Bundle fuer die Zertifikatspruefung (ueberschreibt PROMETHEUS_CA_BUNDLE)",
    )


def create_client_or_exit(args: argparse.Namespace) -> PrometheusClient:
    try:
        config = load_config(args.env_file)
        return build_client(config, insecure=args.insecure, ca_file=args.ca_file)
    except PrometheusError as exc:
        exit_with_error(exc)
        raise AssertionError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser(description="Zeigt den Status eines Prometheus-Servers und dessen Scrape-Targets an.")
    add_common_args(parser)
    args = parser.parse_args()

    client = create_client_or_exit(args)
    try:
        statuses = collect_target_status(client)
        print_report(statuses)
        print_tsdb_report(client)
        print_runtime_report(client)
    except PrometheusError as exc:
        exit_with_error(exc)


if __name__ == "__main__":
    main()
