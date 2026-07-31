from __future__ import annotations

import argparse
import os
import statistics
import sys
from dataclasses import dataclass

import requests
import urllib3
from dotenv import load_dotenv


@dataclass
class TargetStatus:
    job: str
    instance: str
    health: str
    label_count: int
    discovered_label_count: int
    metric_count: int | None
    scrape_duration: float | None
    series_added: int | None
    last_error: str


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
    ):
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password) if user and password else None
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        try:
            response = requests.get(
                url, params=params, auth=self.auth, timeout=self.timeout, verify=self.verify_ssl
            )
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


def query_metric_by_target(client: PrometheusClient, metric_name: str) -> dict[tuple[str, str], float]:
    values: dict[tuple[str, str], float] = {}
    try:
        result = client.instant_query(metric_name)
    except PrometheusError:
        return values

    for entry in result:
        metric = entry.get("metric", {})
        job = metric.get("job")
        instance = metric.get("instance")
        if job is None or instance is None:
            continue
        try:
            values[(job, instance)] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError):
            continue
    return values


def collect_target_status(client: PrometheusClient) -> list[TargetStatus]:
    targets = client.active_targets()
    samples_scraped = query_metric_by_target(client, "scrape_samples_scraped")
    scrape_duration = query_metric_by_target(client, "scrape_duration_seconds")
    series_added = query_metric_by_target(client, "scrape_series_added")

    statuses = []
    for target in targets:
        labels = target.get("labels", {})
        discovered_labels = target.get("discoveredLabels", {})
        job = labels.get("job", "unknown")
        instance = labels.get("instance", "unknown")
        health = target.get("health", "unknown")
        last_error = target.get("lastError", "")
        key = (job, instance)

        metric_value = samples_scraped.get(key)
        duration_value = scrape_duration.get(key)
        series_value = series_added.get(key)

        statuses.append(
            TargetStatus(
                job=job,
                instance=instance,
                health=health,
                label_count=len(labels),
                discovered_label_count=len(discovered_labels),
                metric_count=int(metric_value) if metric_value is not None else None,
                scrape_duration=duration_value,
                series_added=int(series_value) if series_value is not None else None,
                last_error=last_error,
            )
        )
    return statuses


def print_report(statuses: list[TargetStatus]) -> None:
    if not statuses:
        print("Keine aktiven Targets gefunden.")
        return

    metric_counts = [s.metric_count for s in statuses if s.metric_count is not None]
    label_counts = [s.label_count for s in statuses]

    avg_metrics = statistics.mean(metric_counts) if metric_counts else None
    avg_labels = statistics.mean(label_counts) if label_counts else 0

    print(f"Aktive Targets: {len(statuses)}")
    if avg_metrics is not None:
        print(f"Durchschnittliche Metrikanzahl je Target: {avg_metrics:.1f}")
    print(f"Durchschnittliche Labelanzahl je Target: {avg_labels:.1f}")
    print()

    rows = []
    for s in sorted(statuses, key=lambda s: (s.metric_count or -1), reverse=True):
        above_metrics = avg_metrics is not None and s.metric_count is not None and s.metric_count > avg_metrics
        above_labels = s.label_count > avg_labels
        flags = []
        if above_metrics and above_labels:
            flags.append("METRIKEN+LABELS")
        elif above_metrics:
            flags.append("METRIKEN")
        elif above_labels:
            flags.append("LABELS")
        rows.append(
            (
                f"{s.job}/{s.instance}",
                s.health,
                str(s.metric_count) if s.metric_count is not None else "n/a",
                str(s.label_count),
                str(s.discovered_label_count),
                str(s.discovered_label_count - s.label_count),
                f"{s.scrape_duration:.3f}" if s.scrape_duration is not None else "n/a",
                str(s.series_added) if s.series_added is not None else "n/a",
                ", ".join(flags),
            )
        )

    headers = (
        "TARGET",
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

    failing = [s for s in statuses if s.last_error]
    if failing:
        print()
        print(f"Targets mit Scrape-Fehler ({len(failing)}):")
        for s in failing:
            print(f"  {s.job}/{s.instance}: {s.last_error}")


def print_tsdb_report(client: PrometheusClient, top_n: int = 5) -> None:
    try:
        data = client.tsdb_status()
    except PrometheusError as exc:
        print()
        print(f"TSDB-Status nicht verfuegbar: {exc}")
        return

    head_stats = data.get("headStats", {})
    print()
    print("TSDB-Uebersicht (global):")
    if head_stats:
        print(
            f"  Serien: {head_stats.get('numSeries', 'n/a')}  "
            f"Label-Paare: {head_stats.get('numLabelPairs', 'n/a')}  "
            f"Chunks: {head_stats.get('chunkCount', 'n/a')}"
        )

    def print_top(title: str, key: str) -> None:
        entries = data.get(key, [])[:top_n]
        print(f"  {title}:")
        if not entries:
            print("    (keine Daten)")
            return
        for entry in entries:
            print(f"    {entry.get('name', '?')}: {entry.get('value', '?')}")

    print_top(f"Top {top_n} Metriken nach Serienanzahl", "seriesCountByMetricName")
    print_top(f"Top {top_n} Labels nach Anzahl unterschiedlicher Werte (Cardinality)", "labelValueCountByLabelName")
    print_top(f"Top {top_n} Labels nach Speicherverbrauch", "memoryInBytesByLabelName")


def print_runtime_report(client: PrometheusClient) -> None:
    try:
        data = client.runtime_info()
    except PrometheusError as exc:
        print()
        print(f"Runtime-Info nicht verfuegbar: {exc}")
        return

    print()
    print("Runtime-Info:")
    print(f"  Startzeit: {data.get('startTime', 'n/a')}")
    print(f"  Config-Reload erfolgreich: {data.get('reloadConfigSuccess', 'n/a')}")
    print(f"  Letzte Config-Aenderung: {data.get('lastConfigTime', 'n/a')}")
    print(f"  Storage-Retention: {data.get('storageRetention', 'n/a')}")
    print(f"  WAL-/Storage-Korruptionen: {data.get('corruptionCount', 'n/a')}")
    print(f"  Goroutinen: {data.get('goroutineCount', 'n/a')}")


def str_to_bool(value: str) -> bool:
    return value.strip().lower() not in ("false", "0", "no", "off")


def load_config(env_file: str | None) -> tuple[str, str | None, str | None, bool]:
    load_dotenv(dotenv_path=env_file) if env_file else load_dotenv()

    url = os.getenv("PROMETHEUS_URL")
    if not url:
        raise PrometheusError("PROMETHEUS_URL ist nicht gesetzt (Umgebungsvariable oder .env-Datei).")

    user = os.getenv("PROMETHEUS_USER")
    password = os.getenv("PROMETHEUS_PASSWORD")
    verify_ssl = str_to_bool(os.getenv("PROMETHEUS_VERIFY_SSL", "true"))
    return url, user, password, verify_ssl


def main() -> None:
    parser = argparse.ArgumentParser(description="Zeigt den Status eines Prometheus-Servers und dessen Scrape-Targets an.")
    parser.add_argument("--env-file", help="Pfad zu einer .env-Datei (Standard: .env im aktuellen Verzeichnis)")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="SSL-Zertifikatspruefung deaktivieren (z.B. bei selbstsignierten Zertifikaten)",
    )
    args = parser.parse_args()

    try:
        url, user, password, verify_ssl = load_config(args.env_file)
        if args.insecure:
            verify_ssl = False
        client = PrometheusClient(url, user, password, verify_ssl=verify_ssl)
        statuses = collect_target_status(client)
        print_report(statuses)
        print_tsdb_report(client)
        print_runtime_report(client)
    except PrometheusError as exc:
        print(f"Fehler: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
