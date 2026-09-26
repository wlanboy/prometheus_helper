# Prometheus Helper

Kommandozeilen-Tool, das den Status eines Prometheus-Servers abfragt und einen Überblick über dessen aktive Scrape-Targets liefert. Die Verbindung erfolgt per HTTP Basic Auth. Für jedes Target werden folgende Werte ermittelt:

- Anzahl der gescrapten Metriken (`scrape_samples_scraped`)
- Anzahl der Target-Labels nach dem Relabeling sowie roh vor dem Relabeling (`discoveredLabels`) – die Differenz zeigt, wie viele Labels durch Relabeling entfernt werden
- Scrape-Dauer (`scrape_duration_seconds`)
- Anzahl neu hinzugekommener Serien pro Scrape (`scrape_series_added`)
- letzter Scrape-Fehler (`lastError`)

Neben den aktiven Targets aus `/api/v1/targets` werden auch Targets berücksichtigt, deren Scrape-Ergebnisse nur per Remote-Write ankommen (z. B. von Grafana Alloy). Sie werden über die Serien `up`/`scrape_samples_scraped` erkannt und mit der Quelle `remote_write` gekennzeichnet. Labelanzahl und Fehlertext sind für sie nicht verfügbar; die Health wird aus `up` abgeleitet.

Targets, deren Metrik- und/oder Labelanzahl über dem Durchschnitt aller Targets liegt, werden in der Ausgabe als auffällig markiert – so lassen sich Targets identifizieren, die besonders viele Metriken oder besonders viele Labels liefern. Targets mit fehlgeschlagenem letzten Scrape werden gesondert aufgelistet.

Zusätzlich wird eine globale TSDB-Übersicht (`/api/v1/status/tsdb`) ausgegeben: Serien-/Label-Paar-/Chunk-Anzahl sowie die Top-Metriken und -Labels nach Serienanzahl und Speicherverbrauch – nützlich, um Cardinality-Probleme unabhängig von einzelnen Targets zu erkennen.

Außerdem wird eine Runtime-Info-Übersicht (`/api/v1/status/runtimeinfo`) ausgegeben: Startzeit, letzter Config-Reload, konfigurierte Storage-Retention sowie erkannte WAL-/Storage-Korruptionen – nützlich, um den Prometheus-Server selbst (nicht nur seine Targets) im Blick zu behalten.

## Bauen

Das Projekt nutzt [uv](https://docs.astral.sh/uv/) zur Verwaltung der virtuellen Umgebung und der Abhängigkeiten.

```bash
uv sync
```

Damit wird die virtuelle Umgebung angelegt und `requests`, `python-dotenv` sowie `prometheus-client` installiert.

## Konfigurieren

Die Zugangsdaten werden über Umgebungsvariablen bzw. eine `.env`-Datei bereitgestellt. Als Vorlage dient `.env.example`:

```bash
cp .env.example .env
```

Anschließend in der `.env` anpassen:

| Variable | Pflicht | Beschreibung |
|---|---|---|
| `PROMETHEUS_URL` | ja | Basis-URL des Prometheus-Servers, z. B. `https://prometheus.example.com` |
| `PROMETHEUS_USER` | nein | Benutzername für Basic Auth |
| `PROMETHEUS_PASSWORD` | nein | Passwort für Basic Auth |
| `PROMETHEUS_VERIFY_SSL` | nein | `true`/`false` – SSL-Zertifikatsprüfung aktivieren/deaktivieren (Standard: `true`) |
| `PROMETHEUS_CA_BUNDLE` | nein | Pfad zu einem CA-Bundle, z. B. `/etc/ssl/certs/ca-certificates.crt` für eine eigene CA |

Ist das Zertifikat von einer eigenen CA signiert, `PROMETHEUS_CA_BUNDLE` bzw. `--ca-file` auf das CA-Bundle setzen. `requests` nutzt standardmäßig nur das certifi-Bundle und nicht den System-Store. Nur als Notlösung: `PROMETHEUS_VERIFY_SSL=false` oder `--insecure`.

## Starten

```bash
uv run main.py
```

Optionale Parameter:

```bash
uv run main.py --env-file /pfad/zu/anderer.env   # abweichende .env-Datei verwenden
uv run main.py --insecure                        # SSL-Zertifikatsprüfung für diesen Lauf deaktivieren
uv run main.py --ca-file /pfad/zu/ca.pem         # CA-Bundle für die Zertifikatsprüfung
```

## Metrik-Endpunkt (`server.py`)

Neben der einmaligen Kommandozeilen-Ausgabe kann `server.py` dieselben Werte dauerhaft als Prometheus-Metriken unter `/metrics` bereitstellen, sodass ein Prometheus-Server diesen Helper selbst scrapen kann. Die Werte werden bei jedem Scrape live über die Ziel-Prometheus-API neu abgefragt (kein Caching).

```bash
uv run server.py
```

Optionale Parameter:

```bash
uv run server.py --env-file /pfad/zu/anderer.env   # abweichende .env-Datei verwenden
uv run server.py --insecure                        # SSL-Zertifikatsprüfung für diesen Lauf deaktivieren
uv run server.py --port 9877                        # Port des Metrik-Endpunkts (Standard: 9877)
uv run server.py --top-n 5                           # Anzahl der Top-Einträge je TSDB-Rangliste (Standard: 5)
```

Bereitgestellte Metriken:

| Metrik | Beschreibung |
|---|---|
| `prometheus_helper_target_info{job,instance,source}` | Herkunft des Targets (`scrape` oder `remote_write`), Wert immer 1 |
| `prometheus_helper_target_up{job,instance}` | 1 = Target-Health `up`, sonst 0 |
| `prometheus_helper_target_label_count{job,instance}` | Anzahl der Target-Labels nach dem Relabeling |
| `prometheus_helper_target_discovered_label_count{job,instance}` | Anzahl der Target-Labels vor dem Relabeling (`discoveredLabels`) |
| `prometheus_helper_target_relabel_dropped_labels{job,instance}` | Durch Relabeling entfernte Labels (`discoveredLabels` - `labels`) |
| `prometheus_helper_target_metric_count{job,instance}` | Anzahl gescrapter Metriken (`scrape_samples_scraped`) |
| `prometheus_helper_target_scrape_duration_seconds{job,instance}` | Scrape-Dauer (`scrape_duration_seconds`) |
| `prometheus_helper_target_series_added{job,instance}` | Neue Serien pro Scrape (`scrape_series_added`) |
| `prometheus_helper_target_last_scrape_error{job,instance}` | 1 wenn letzter Scrape fehlgeschlagen ist (bei Remote-Write-Targets: `up == 0`). Den Fehlertext zeigt `main.py`; als Label würde er pro Fehlermeldung eine neue Serie erzeugen |
| `prometheus_helper_tsdb_head_series` | Serienanzahl im TSDB-Head |
| `prometheus_helper_tsdb_head_label_pairs` | Label-Paar-Anzahl im TSDB-Head |
| `prometheus_helper_tsdb_head_chunks` | Chunk-Anzahl im TSDB-Head |
| `prometheus_helper_tsdb_series_count_by_metric_name{name}` | Top-Metriken nach Serienanzahl |
| `prometheus_helper_tsdb_label_value_count_by_label_name{name}` | Top-Labels nach Cardinality |
| `prometheus_helper_tsdb_memory_bytes_by_label_name{name}` | Top-Labels nach Speicherverbrauch |
| `prometheus_helper_runtime_reload_config_success` | 1 wenn der letzte Config-Reload erfolgreich war, sonst 0 |
| `prometheus_helper_runtime_goroutines` | Anzahl laufender Goroutinen im Prometheus-Prozess |
| `prometheus_helper_runtime_corruptions_total` | Anzahl erkannter WAL-/Storage-Korruptionen (`corruptionCount`) |
| `prometheus_helper_runtime_storage_retention_seconds` | Konfigurierte Storage-Retention in Sekunden (aus `storageRetention`, z. B. `15d`) |
| `prometheus_helper_scrape_success{section}` | 1 wenn die Abfrage des Abschnitts (`targets`, `tsdb`, `runtime`) vollständig erfolgreich war, sonst 0 – auch bei Teilfehlern wie gestörter Query-API |
| `prometheus_helper_scrape_duration_seconds{section}` | Dauer der Abfrage je Abschnitt in Sekunden |

Beispiel-Alert, der anschlägt, wenn der Helper den Ziel-Prometheus nicht (vollständig) abfragen kann:

```yaml
- alert: PrometheusHelperScrapeFailed
  expr: prometheus_helper_scrape_success == 0
  for: 5m
```
