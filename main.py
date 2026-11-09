from __future__ import annotations

import argparse
import os
import re
import statistics
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NamedTuple

import requests
import urllib3
from dotenv import load_dotenv


class TargetKey(NamedTuple):
    job: str
    instance: str
    # Werte der per --key-label zusaetzlich unterschiedenen Labels, in Reihenfolge von key_labels
    extra: tuple[str, ...] = ()


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
    metric_count_post_relabel: int | None
    scrape_duration: float | None
    series_added: int | None
    last_error: str
    key_values: tuple[str, ...] = ()

    def display_name(self, key_labels: tuple[str, ...] = ()) -> str:
        extra = "".join(f" {name}={value}" for name, value in zip(key_labels, self.key_values))
        return f"{self.job}/{self.instance}{extra}"

    @property
    def relabel_dropped_labels(self) -> int | None:
        if self.label_count is None or self.discovered_label_count is None:
            return None
        return self.discovered_label_count - self.label_count

    def conspicuous_flags(self, metric_threshold: float | None, label_threshold: float | None) -> list[str]:
        above_metrics = (
            metric_threshold is not None and self.metric_count is not None and self.metric_count > metric_threshold
        )
        above_labels = (
            label_threshold is not None and self.label_count is not None and self.label_count > label_threshold
        )
        if above_metrics and above_labels:
            return ["METRIKEN+LABELS"]
        if above_metrics:
            return ["METRIKEN"]
        if above_labels:
            return ["LABELS"]
        return []


DEFAULT_CONSPICUOUS_FACTOR = 2.0


def conspicuous_threshold(values: list[int], factor: float) -> float | None:
    # Median statt Mittelwert: einzelne Ausreisser verschieben die Schwelle nicht
    return statistics.median(values) * factor if values else None


@dataclass
class PrometheusConfig:
    url: str
    user: str | None
    password: str | None
    verify_ssl: bool
    ca_bundle: str | None = None
    token: str | None = None


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
    server_time: str | None = None

    @property
    def uptime_seconds(self) -> float | None:
        start = parse_timestamp(self.start_time)
        if start is None:
            return None
        # serverTime vermeidet Abweichungen durch eine falsch gehende lokale Uhr
        now = parse_timestamp(self.server_time) or datetime.now(UTC)
        return (now - start).total_seconds()


def parse_timestamp(value: str | None) -> datetime | None:
    # Go liefert Nanosekunden und "Z"; datetime.fromisoformat verarbeitet beides seit Python 3.11
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    # Ohne Zeitzone liesse sich der Zeitpunkt nicht eindeutig einordnen
    return parsed if parsed.tzinfo is not None else None


def format_duration(seconds: float) -> str:
    total = int(seconds)
    days, rest = divmod(total, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


class PrometheusError(Exception):
    pass


DEFAULT_TIMEOUT = 10.0


class PrometheusClient:
    def __init__(
        self,
        base_url: str,
        user: str | None,
        password: str | None,
        verify_ssl: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
        ca_bundle: str | None = None,
        token: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Eine Session haelt die TLS-Verbindung ueber alle API-Aufrufe hinweg offen
        self.session = requests.Session()
        if token:
            # Bearer-Token (z.B. OpenShift, OAuth-Proxy) hat Vorrang vor Basic Auth
            self.session.auth = None
            self.session.headers["Authorization"] = f"Bearer {token}"
        else:
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
            raise PrometheusError("Authentifizierung fehlgeschlagen (401) - Zugangsdaten bzw. Token pruefen")
        if not response.ok:
            raise PrometheusError(f"{url} antwortete mit HTTP {response.status_code}: {response.text[:200]}")

        # Reverse-Proxies oder SSO-Loginseiten liefern oft HTML mit Status 200
        try:
            payload = response.json()
        except ValueError as exc:
            content_type = response.headers.get("Content-Type", "unbekannt")
            raise PrometheusError(
                f"{url} lieferte kein JSON (Content-Type: {content_type}): {response.text[:200]}"
            ) from exc
        if not isinstance(payload, dict):
            raise PrometheusError(f"{url} lieferte unerwartetes JSON: {str(payload)[:200]}")
        if payload.get("status") != "success":
            raise PrometheusError(f"Prometheus meldete Fehler: {payload.get('error', payload)}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise PrometheusError(f"{url} lieferte kein 'data'-Objekt: {str(payload)[:200]}")
        return data

    def active_targets(self) -> list[dict]:
        data = self._get("/api/v1/targets", params={"state": "active"})
        return data.get("activeTargets", [])

    def instant_query(self, promql: str) -> list[dict]:
        data = self._get("/api/v1/query", params={"query": promql})
        return data.get("result", [])

    def tsdb_status(self, limit: int | None = None) -> dict:
        return self._get("/api/v1/status/tsdb", params={"limit": limit} if limit is not None else None)

    def runtime_info(self) -> dict:
        return self._get("/api/v1/status/runtimeinfo")


def query_metrics_by_target(
    client: PrometheusClient,
    metric_names: list[str],
    errors: list[PrometheusError] | None = None,
    key_labels: tuple[str, ...] = (),
) -> dict[str, dict[TargetKey, float]]:
    values: dict[str, dict[TargetKey, float]] = {name: {} for name in metric_names}
    promql = '{__name__=~"' + "|".join(metric_names) + '"}'
    try:
        result = client.instant_query(promql)
    except PrometheusError as exc:
        print(f"Warnung: Abfrage von {', '.join(metric_names)} fehlgeschlagen: {exc}", file=sys.stderr)
        if errors is not None:
            errors.append(exc)
        return values

    duplicates: dict[str, set[TargetKey]] = {}
    for entry in result:
        metric = entry.get("metric", {})
        name = metric.get("__name__")
        job = metric.get("job")
        instance = metric.get("instance")
        if name not in values or job is None or instance is None:
            continue
        key = TargetKey(job, instance, tuple(metric.get(label, "") for label in key_labels))
        try:
            value = float(entry["value"][1])
        except (KeyError, IndexError, ValueError):
            continue
        if key in values[name]:
            duplicates.setdefault(name, set()).add(key)
        values[name][key] = value

    if duplicates:
        # Z.B. mehrere Alloy-Agents/Cluster, die per Remote-Write gleiche job/instance-Paare schicken
        keys = set().union(*duplicates.values())
        key_desc = "/".join(("job", "instance", *key_labels))
        example = min(keys)
        print(
            f"Warnung: {len(keys)} Target(s) mit mehreren Serien je {key_desc}, nur ein Wert wird verwendet "
            f"(z.B. {'/'.join((example.job, example.instance, *example.extra))} bei "
            f"{', '.join(sorted(duplicates))}). Unterscheidendes Label per --key-label angeben.",
            file=sys.stderr,
        )
    return values


def collect_target_status(
    client: PrometheusClient,
    errors: list[PrometheusError] | None = None,
    key_labels: tuple[str, ...] = (),
) -> list[TargetStatus]:
    """Fehler der Query-API werden nicht geworfen, sondern (falls uebergeben) in errors gesammelt;
    die Targets aus /api/v1/targets werden dann ohne Scrape-Werte zurueckgegeben."""
    targets = client.active_targets()
    metrics = query_metrics_by_target(
        client,
        [
            "up",
            "scrape_samples_scraped",
            "scrape_samples_post_metric_relabeling",
            "scrape_duration_seconds",
            "scrape_series_added",
        ],
        errors,
        key_labels,
    )
    up = metrics["up"]
    samples_scraped = metrics["scrape_samples_scraped"]
    samples_post_relabel = metrics["scrape_samples_post_metric_relabeling"]
    scrape_duration = metrics["scrape_duration_seconds"]
    series_added = metrics["scrape_series_added"]

    def build_status(key: TargetKey, **kwargs) -> TargetStatus:
        metric_value = samples_scraped.get(key)
        post_relabel_value = samples_post_relabel.get(key)
        series_value = series_added.get(key)
        return TargetStatus(
            job=key.job,
            instance=key.instance,
            key_values=key.extra,
            metric_count=int(metric_value) if metric_value is not None else None,
            metric_count_post_relabel=int(post_relabel_value) if post_relabel_value is not None else None,
            scrape_duration=scrape_duration.get(key),
            series_added=int(series_value) if series_value is not None else None,
            **kwargs,
        )

    statuses = []
    for target in targets:
        labels = target.get("labels", {})
        key = TargetKey(
            labels.get("job", "unknown"),
            labels.get("instance", "unknown"),
            tuple(labels.get(label, "") for label in key_labels),
        )
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
    known = {TargetKey(s.job, s.instance, s.key_values) for s in statuses}
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
    data = client.tsdb_status(limit=top_n)
    head_stats = data.get("headStats", {})

    def top_entries(key: str) -> list[TsdbTopEntry]:
        # Kuerzen nur als Absicherung fuer Prometheus-Versionen, die limit ignorieren
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
        server_time=data.get("serverTime"),
    )


def failing_targets(statuses: list[TargetStatus]) -> list[TargetStatus]:
    return [s for s in statuses if s.last_error or s.health == "down"]


def print_report(
    statuses: list[TargetStatus],
    factor: float = DEFAULT_CONSPICUOUS_FACTOR,
    key_labels: tuple[str, ...] = (),
) -> int:
    """Gibt die Target-Tabelle aus und liefert die Anzahl auffaelliger Targets."""
    if not statuses:
        print("Keine aktiven Targets gefunden.")
        return 0

    metric_counts = [s.metric_count for s in statuses if s.metric_count is not None]
    label_counts = [s.label_count for s in statuses if s.label_count is not None]

    metric_threshold = conspicuous_threshold(metric_counts, factor)
    label_threshold = conspicuous_threshold(label_counts, factor)

    remote_count = sum(1 for s in statuses if s.source == SOURCE_REMOTE_WRITE)
    print(f"Targets: {len(statuses)} (davon {remote_count} per Remote-Write)")
    if metric_threshold is not None:
        print(
            f"Metrikanzahl je Target: Median {statistics.median(metric_counts):.1f}, "
            f"auffaellig ab > {metric_threshold:.1f} ({factor:g}x Median)"
        )
    if label_threshold is not None:
        print(
            f"Labelanzahl je Target (nur gescrapte Targets): Median {statistics.median(label_counts):.1f}, "
            f"auffaellig ab > {label_threshold:.1f} ({factor:g}x Median)"
        )
    print()

    def or_na(value) -> str:
        return str(value) if value is not None else "n/a"

    rows = []
    for s in sorted(statuses, key=lambda s: -1 if s.metric_count is None else s.metric_count, reverse=True):
        flags = s.conspicuous_flags(metric_threshold, label_threshold)
        rows.append(
            (
                s.display_name(key_labels),
                s.source,
                s.health,
                or_na(s.metric_count),
                or_na(s.metric_count_post_relabel),
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
        "GESPEICHERT",
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
        print(f"{len(conspicuous)} Target(s) auffaellig (Metriken und/oder Labels > {factor:g}x Median).")
    else:
        print("Keine auffaelligen Targets gefunden.")

    failing = failing_targets(statuses)
    if failing:
        print()
        print(f"Targets mit Scrape-Fehler ({len(failing)}):")
        for s in failing:
            print(f"  {s.display_name(key_labels)}: {s.last_error or '(up == 0, kein Fehlertext verfuegbar)'}")

    return len(conspicuous)


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

    def print_top(title: str, entries: list[TsdbTopEntry], as_bytes: bool = False) -> None:
        print(f"  {title}:")
        if not entries:
            print("    (keine Daten)")
            return
        for entry in entries:
            if entry.value is None:
                value = "?"
            elif as_bytes:
                value = format_bytes(float(entry.value))
            else:
                value = entry.value
            print(f"    {entry.name}: {value}")

    print_top(f"Top {top_n} Metriken nach Serienanzahl", tsdb.top_series_by_metric)
    print_top(f"Top {top_n} Labels nach Anzahl unterschiedlicher Werte (Cardinality)", tsdb.top_label_value_count)
    print_top(f"Top {top_n} Labels nach Speicherverbrauch", tsdb.top_memory_by_label, as_bytes=True)


def print_runtime_report(client: PrometheusClient) -> RuntimeStatus | None:
    try:
        runtime = collect_runtime_status(client)
    except PrometheusError as exc:
        print()
        print(f"Runtime-Info nicht verfuegbar: {exc}")
        return None

    def or_na(value):
        return value if value is not None else "n/a"

    print()
    print("Runtime-Info:")
    uptime = runtime.uptime_seconds
    uptime_suffix = f" (Uptime: {format_duration(uptime)})" if uptime is not None else ""
    print(f"  Startzeit: {or_na(runtime.start_time)}{uptime_suffix}")
    print(f"  Config-Reload erfolgreich: {or_na(runtime.reload_config_success)}")
    print(f"  Letzte Config-Aenderung: {or_na(runtime.last_config_time)}")
    print(f"  Storage-Retention: {or_na(runtime.storage_retention)}")
    print(f"  WAL-/Storage-Korruptionen: {or_na(runtime.corruption_count)}")
    print(f"  Goroutinen: {or_na(runtime.goroutine_count)}")
    return runtime


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
    token = os.getenv("PROMETHEUS_TOKEN") or None
    return PrometheusConfig(
        url=url, user=user, password=password, verify_ssl=verify_ssl, ca_bundle=ca_bundle, token=token
    )


def build_client(
    config: PrometheusConfig,
    insecure: bool = False,
    ca_file: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> PrometheusClient:
    verify_ssl = False if insecure else config.verify_ssl
    ca_bundle = ca_file or config.ca_bundle
    if verify_ssl and ca_bundle and not os.path.isfile(ca_bundle):
        raise PrometheusError(f"CA-Bundle {ca_bundle} existiert nicht.")
    return PrometheusClient(
        config.url,
        config.user,
        config.password,
        verify_ssl=verify_ssl,
        timeout=timeout,
        ca_bundle=ca_bundle,
        token=config.token,
    )


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
    parser.add_argument(
        "--timeout",
        type=positive_float,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout je HTTP-Request an Prometheus in Sekunden (Standard: {DEFAULT_TIMEOUT:g})",
    )
    parser.add_argument(
        "--key-label",
        dest="key_labels",
        action="append",
        type=key_label,
        default=[],
        metavar="LABEL",
        help="Zusaetzliches Label, das Targets neben job/instance unterscheidet (z.B. cluster); mehrfach angebbar",
    )


_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# job/instance sind bereits Teil des Keys, source/name werden von server.py als Labels belegt
_RESERVED_KEY_LABELS = {"job", "instance", "source", "name", "section"}


def key_label(value: str) -> str:
    if not _LABEL_NAME_RE.match(value) or value.startswith("__"):
        raise argparse.ArgumentTypeError(f"ungueltiger Labelname: {value!r}")
    if value in _RESERVED_KEY_LABELS:
        raise argparse.ArgumentTypeError(f"Label {value!r} ist reserviert")
    return value


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"keine Zahl: {value!r}") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError(f"muss groesser als 0 sein: {value!r}")
    return number


def create_client_or_exit(args: argparse.Namespace) -> PrometheusClient:
    try:
        config = load_config(args.env_file)
        return build_client(config, insecure=args.insecure, ca_file=args.ca_file, timeout=args.timeout)
    except PrometheusError as exc:
        exit_with_error(exc)
        raise AssertionError("unreachable")


FAIL_ON_CONDITIONS = ("down", "conspicuous", "reload")
EXIT_FAIL_ON = 2


def fail_on_conditions(value: str) -> frozenset[str]:
    conditions = {c.strip() for c in value.split(",") if c.strip()}
    unknown = conditions - set(FAIL_ON_CONDITIONS)
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unbekannte Bedingung(en): {', '.join(sorted(unknown))} (erlaubt: {', '.join(FAIL_ON_CONDITIONS)})"
        )
    return frozenset(conditions)


def triggered_conditions(
    fail_on: frozenset[str],
    failing_count: int,
    conspicuous_count: int,
    runtime: RuntimeStatus | None,
) -> list[str]:
    triggered = []
    if "down" in fail_on and failing_count:
        triggered.append(f"down ({failing_count} Target(s))")
    if "conspicuous" in fail_on and conspicuous_count:
        triggered.append(f"conspicuous ({conspicuous_count} Target(s))")
    # Ist die Runtime-Info nicht abrufbar, ist der Reload-Status unbekannt und zaehlt nicht als Fehlschlag
    if "reload" in fail_on and runtime is not None and runtime.reload_config_success is False:
        triggered.append("reload")
    return triggered


def main() -> None:
    parser = argparse.ArgumentParser(description="Zeigt den Status eines Prometheus-Servers und dessen Scrape-Targets an.")
    add_common_args(parser)
    parser.add_argument(
        "--factor",
        type=float,
        default=DEFAULT_CONSPICUOUS_FACTOR,
        help=f"Target gilt als auffaellig ab diesem Vielfachen des Medians (Standard: {DEFAULT_CONSPICUOUS_FACTOR:g})",
    )
    parser.add_argument(
        "--fail-on",
        type=fail_on_conditions,
        default=frozenset(),
        metavar="BEDINGUNGEN",
        help=(
            "Kommagetrennt aus " + ",".join(FAIL_ON_CONDITIONS) + f": Exit-Code {EXIT_FAIL_ON}, wenn eine "
            "Bedingung zutrifft (down = Target down oder mit Scrape-Fehler, conspicuous = auffaellige Targets, "
            "reload = letzter Config-Reload fehlgeschlagen)"
        ),
    )
    args = parser.parse_args()
    key_labels = tuple(dict.fromkeys(args.key_labels))

    client = create_client_or_exit(args)
    try:
        statuses = collect_target_status(client, key_labels=key_labels)
        conspicuous_count = print_report(statuses, factor=args.factor, key_labels=key_labels)
        print_tsdb_report(client)
        runtime = print_runtime_report(client)
    except PrometheusError as exc:
        exit_with_error(exc)
        return

    triggered = triggered_conditions(
        args.fail_on,
        failing_count=len(failing_targets(statuses)),
        conspicuous_count=conspicuous_count,
        runtime=runtime,
    )
    if triggered:
        print(f"Fehlschlag wegen --fail-on: {', '.join(triggered)}", file=sys.stderr)
        sys.exit(EXIT_FAIL_ON)


if __name__ == "__main__":
    main()
