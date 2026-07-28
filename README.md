# Prometheus Helper

Kommandozeilen-Tool, das den Status eines Prometheus-Servers abfragt und einen Überblick über dessen aktive Scrape-Targets liefert. Die Verbindung erfolgt per HTTP Basic Auth. Für jedes Target werden folgende Werte ermittelt:

- Anzahl der gescrapten Metriken (`scrape_samples_scraped`)
- Anzahl der Target-Labels
- Scrape-Dauer (`scrape_duration_seconds`)
- Anzahl neu hinzugekommener Serien pro Scrape (`scrape_series_added`)
- letzter Scrape-Fehler (`lastError`)

Targets, deren Metrik- und/oder Labelanzahl über dem Durchschnitt aller Targets liegt, werden in der Ausgabe als auffällig markiert – so lassen sich Targets identifizieren, die besonders viele Metriken oder besonders viele Labels liefern. Targets mit fehlgeschlagenem letzten Scrape werden gesondert aufgelistet.

Zusätzlich wird eine globale TSDB-Übersicht (`/api/v1/status/tsdb`) ausgegeben: Serien-/Label-Paar-/Chunk-Anzahl sowie die Top-Metriken und -Labels nach Serienanzahl und Speicherverbrauch – nützlich, um Cardinality-Probleme unabhängig von einzelnen Targets zu erkennen.

## Bauen

Das Projekt nutzt [uv](https://docs.astral.sh/uv/) zur Verwaltung der virtuellen Umgebung und der Abhängigkeiten.

```bash
uv sync
```

Damit wird die virtuelle Umgebung angelegt und `requests` sowie `python-dotenv` installiert.

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

Bei selbstsignierten Zertifikaten `PROMETHEUS_VERIFY_SSL=false` setzen oder das CLI-Flag `--insecure` verwenden.

## Starten

```bash
uv run main.py
```

Optionale Parameter:

```bash
uv run main.py --env-file /pfad/zu/anderer.env   # abweichende .env-Datei verwenden
uv run main.py --insecure                        # SSL-Zertifikatsprüfung für diesen Lauf deaktivieren
```
