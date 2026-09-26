from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import requests
from conftest import FakeClient, sample

import main
from main import (
    SOURCE_REMOTE_WRITE,
    SOURCE_SCRAPE,
    PrometheusClient,
    PrometheusConfig,
    PrometheusError,
    RuntimeStatus,
    TargetKey,
    TargetStatus,
    build_client,
    collect_runtime_status,
    collect_target_status,
    collect_tsdb_status,
    conspicuous_threshold,
    format_bytes,
    format_duration,
    load_config,
    parse_timestamp,
    query_metrics_by_target,
    str_to_bool,
)

REAL_LOAD_DOTENV = main.load_dotenv


def make_status(**overrides) -> TargetStatus:
    values = {
        "job": "job",
        "instance": "host:9100",
        "source": SOURCE_SCRAPE,
        "health": "up",
        "label_count": 5,
        "discovered_label_count": 8,
        "metric_count": 100,
        "metric_count_post_relabel": 90,
        "scrape_duration": 0.1,
        "series_added": 0,
        "last_error": "",
    }
    values.update(overrides)
    return TargetStatus(**values)


class TestTargetStatus:
    def test_relabel_dropped_labels(self):
        assert make_status(label_count=5, discovered_label_count=8).relabel_dropped_labels == 3

    @pytest.mark.parametrize("label_count, discovered", [(None, 8), (5, None), (None, None)])
    def test_relabel_dropped_labels_unknown(self, label_count, discovered):
        status = make_status(label_count=label_count, discovered_label_count=discovered)
        assert status.relabel_dropped_labels is None

    @pytest.mark.parametrize(
        "metric_count, label_count, expected",
        [
            (200, 10, ["METRIKEN+LABELS"]),
            (200, 3, ["METRIKEN"]),
            (50, 10, ["LABELS"]),
            (50, 3, []),
            # Gleichstand mit der Schwelle ist nicht auffaellig
            (100, 5, []),
        ],
    )
    def test_conspicuous_flags(self, metric_count, label_count, expected):
        status = make_status(metric_count=metric_count, label_count=label_count)
        assert status.conspicuous_flags(metric_threshold=100, label_threshold=5) == expected

    def test_conspicuous_flags_ignores_missing_values(self):
        status = make_status(metric_count=None, label_count=None)
        assert status.conspicuous_flags(metric_threshold=1, label_threshold=1) == []
        assert make_status().conspicuous_flags(metric_threshold=None, label_threshold=None) == []


class TestPrometheusClient:
    session: MagicMock

    def make_client(self, response=None, exc=None) -> PrometheusClient:
        client = PrometheusClient("https://prom.example/", "u", "p")
        self.session = MagicMock()
        if exc is not None:
            self.session.get.side_effect = exc
        else:
            self.session.get.return_value = response
        client.session = self.session
        return client

    @staticmethod
    def response(status_code=200, payload=None, text=""):
        resp = MagicMock()
        resp.status_code = status_code
        resp.ok = status_code < 400
        resp.text = text
        resp.json.return_value = payload
        return resp

    def test_get_returns_data_and_strips_trailing_slash(self):
        client = self.make_client(self.response(payload={"status": "success", "data": {"result": [1]}}))
        assert client.instant_query("up") == [1]
        self.session.get.assert_called_once_with(
            "https://prom.example/api/v1/query", params={"query": "up"}, timeout=10.0
        )

    def test_get_wraps_connection_errors(self):
        client = self.make_client(exc=requests.exceptions.ConnectionError("boom"))
        with pytest.raises(PrometheusError, match="Verbindung"):
            client.runtime_info()

    def test_get_401(self):
        client = self.make_client(self.response(status_code=401))
        with pytest.raises(PrometheusError, match="401"):
            client.tsdb_status()

    def test_get_http_error_truncates_body(self):
        client = self.make_client(self.response(status_code=500, text="x" * 500))
        with pytest.raises(PrometheusError, match="HTTP 500") as excinfo:
            client.tsdb_status()
        assert "x" * 200 in str(excinfo.value)
        assert "x" * 201 not in str(excinfo.value)

    def test_get_prometheus_error_status(self):
        client = self.make_client(self.response(payload={"status": "error", "error": "bad query"}))
        with pytest.raises(PrometheusError, match="bad query"):
            client.instant_query("{")

    def test_tsdb_status_passes_limit_only_when_set(self):
        client = self.make_client(self.response(payload={"status": "success", "data": {}}))
        client.tsdb_status()
        client.tsdb_status(limit=3)
        assert [c.kwargs["params"] for c in self.session.get.call_args_list] == [None, {"limit": 3}]

    def test_active_targets_defaults_to_empty_list(self):
        client = self.make_client(self.response(payload={"status": "success", "data": {}}))
        assert client.active_targets() == []

    @pytest.mark.parametrize(
        "user, password, expected",
        [("u", "p", ("u", "p")), ("u", None, None), (None, "p", None), (None, None, None)],
    )
    def test_auth_only_with_user_and_password(self, user, password, expected):
        assert PrometheusClient("http://x", user, password).session.auth == expected

    @pytest.mark.parametrize(
        "verify_ssl, ca_bundle, expected",
        [
            (True, None, True),
            (True, "/ca.pem", "/ca.pem"),
            (False, "/ca.pem", False),
            (False, None, False),
        ],
    )
    def test_verify_setting(self, verify_ssl, ca_bundle, expected):
        client = PrometheusClient("http://x", None, None, verify_ssl=verify_ssl, ca_bundle=ca_bundle)
        assert client.session.verify == expected


class TestQueryMetricsByTarget:
    def test_groups_values_by_name_and_target(self):
        client = FakeClient(
            query_result=[
                sample("up", "a", "i1", "1"),
                sample("up", "b", "i2", "0"),
                sample("scrape_samples_scraped", "a", "i1", "42"),
            ]
        )
        values = query_metrics_by_target(client, ["up", "scrape_samples_scraped"])

        assert client.queries == ['{__name__=~"up|scrape_samples_scraped"}']
        assert values == {
            "up": {TargetKey("a", "i1"): 1.0, TargetKey("b", "i2"): 0.0},
            "scrape_samples_scraped": {TargetKey("a", "i1"): 42.0},
        }

    def test_skips_incomplete_or_unknown_entries(self):
        client = FakeClient(
            query_result=[
                sample("other_metric", "a", "i1", "1"),
                {"metric": {"__name__": "up", "instance": "i1"}, "value": [0, "1"]},
                {"metric": {"__name__": "up", "job": "a"}, "value": [0, "1"]},
                {"metric": {"__name__": "up", "job": "a", "instance": "i1"}},
                {"metric": {"__name__": "up", "job": "a", "instance": "i2"}, "value": [0]},
                sample("up", "a", "i3", "not-a-number"),
                sample("up", "a", "ok", "1"),
            ]
        )
        assert query_metrics_by_target(client, ["up"]) == {"up": {TargetKey("a", "ok"): 1.0}}

    def test_query_error_returns_empty_values_and_records_error(self, capsys):
        error = PrometheusError("kaputt")
        errors: list[PrometheusError] = []

        values = query_metrics_by_target(FakeClient(query_result=error), ["up", "x"], errors)

        assert values == {"up": {}, "x": {}}
        assert errors == [error]
        assert "kaputt" in capsys.readouterr().err

    def test_query_error_without_error_list(self):
        values = query_metrics_by_target(FakeClient(query_result=PrometheusError("kaputt")), ["up"])
        assert values == {"up": {}}


class TestCollectTargetStatus:
    def test_scrape_target_with_metrics(self):
        client = FakeClient(
            targets=[
                {
                    "labels": {"job": "node", "instance": "h1:9100", "env": "prod"},
                    "discoveredLabels": {"a": "1", "b": "2", "c": "3", "d": "4"},
                    "health": "down",
                    "lastError": "timeout",
                }
            ],
            query_result=[
                sample("up", "node", "h1:9100", "0"),
                sample("scrape_samples_scraped", "node", "h1:9100", "123"),
                sample("scrape_samples_post_metric_relabeling", "node", "h1:9100", "100"),
                sample("scrape_duration_seconds", "node", "h1:9100", "0.25"),
                sample("scrape_series_added", "node", "h1:9100", "7"),
            ],
        )

        assert collect_target_status(client) == [
            TargetStatus(
                job="node",
                instance="h1:9100",
                source=SOURCE_SCRAPE,
                health="down",
                label_count=3,
                discovered_label_count=4,
                metric_count=123,
                metric_count_post_relabel=100,
                scrape_duration=0.25,
                series_added=7,
                last_error="timeout",
            )
        ]

    def test_target_without_labels_uses_defaults(self):
        (status,) = collect_target_status(FakeClient(targets=[{}]))
        assert (status.job, status.instance, status.health) == ("unknown", "unknown", "unknown")
        assert status.label_count == 0
        assert status.discovered_label_count == 0
        assert status.metric_count is None
        assert status.metric_count_post_relabel is None
        assert status.scrape_duration is None
        assert status.series_added is None

    def test_remote_write_targets_are_added_sorted(self):
        client = FakeClient(
            targets=[{"labels": {"job": "node", "instance": "h1"}}],
            query_result=[
                sample("up", "node", "h1", "1"),
                sample("up", "rw", "b", "0"),
                sample("up", "rw", "a", "1"),
                # Nur Samples, kein up -> Health unbekannt
                sample("scrape_samples_scraped", "rw", "c", "10"),
                # Nur Dauer ohne up/samples -> kein eigenes Target
                sample("scrape_duration_seconds", "rw", "d", "1"),
            ],
        )

        statuses = collect_target_status(client)

        assert [(s.job, s.instance, s.source, s.health) for s in statuses] == [
            ("node", "h1", SOURCE_SCRAPE, "unknown"),
            ("rw", "a", SOURCE_REMOTE_WRITE, "up"),
            ("rw", "b", SOURCE_REMOTE_WRITE, "down"),
            ("rw", "c", SOURCE_REMOTE_WRITE, "n/a"),
        ]
        remote = statuses[3]
        assert remote.label_count is None
        assert remote.discovered_label_count is None
        assert remote.metric_count == 10
        assert remote.last_error == ""

    def test_query_error_keeps_targets_and_collects_error(self):
        errors: list[PrometheusError] = []
        client = FakeClient(
            targets=[{"labels": {"job": "node", "instance": "h1"}, "health": "up"}],
            query_result=PrometheusError("query failed"),
        )

        statuses = collect_target_status(client, errors)

        assert len(errors) == 1
        assert [(s.job, s.health, s.metric_count) for s in statuses] == [("node", "up", None)]

    def test_targets_error_is_raised(self):
        with pytest.raises(PrometheusError):
            collect_target_status(FakeClient(targets=PrometheusError("down")))


class TestCollectTsdbStatus:
    def test_maps_head_stats_and_limits_top_entries(self):
        client = FakeClient(
            tsdb={
                "headStats": {"numSeries": 10, "numLabelPairs": 20, "chunkCount": 30},
                "seriesCountByMetricName": [{"name": f"m{i}", "value": i} for i in range(10)],
                "labelValueCountByLabelName": [{"value": 5}],
            }
        )

        tsdb = collect_tsdb_status(client, top_n=3)

        assert client.tsdb_limits == [3]

        assert (tsdb.num_series, tsdb.num_label_pairs, tsdb.chunk_count) == (10, 20, 30)
        assert [(e.name, e.value) for e in tsdb.top_series_by_metric] == [("m0", 0), ("m1", 1), ("m2", 2)]
        assert [(e.name, e.value) for e in tsdb.top_label_value_count] == [("?", 5)]
        assert tsdb.top_memory_by_label == []

    def test_empty_response(self):
        tsdb = collect_tsdb_status(FakeClient(tsdb={}))
        assert (tsdb.num_series, tsdb.num_label_pairs, tsdb.chunk_count) == (None, None, None)


def test_collect_runtime_status_maps_fields():
    client = FakeClient(
        runtime={
            "startTime": "2026-01-01T00:00:00Z",
            "reloadConfigSuccess": True,
            "lastConfigTime": "2026-01-02T00:00:00Z",
            "storageRetention": "15d",
            "corruptionCount": 0,
            "goroutineCount": 42,
            "serverTime": "2026-01-03T00:00:00Z",
        }
    )
    runtime = collect_runtime_status(client)
    assert runtime.start_time == "2026-01-01T00:00:00Z"
    assert runtime.reload_config_success is True
    assert runtime.last_config_time == "2026-01-02T00:00:00Z"
    assert runtime.storage_retention == "15d"
    assert runtime.corruption_count == 0
    assert runtime.goroutine_count == 42
    assert runtime.server_time == "2026-01-03T00:00:00Z"


@pytest.mark.parametrize(
    "values, factor, expected",
    [
        ([1, 2, 100], 2.0, 4.0),
        ([1, 2, 3, 4], 1.0, 2.5),
        ([], 2.0, None),
    ],
)
def test_conspicuous_threshold_uses_median(values, factor, expected):
    assert conspicuous_threshold(values, factor) == expected


class TestParseTimestamp:
    def test_go_timestamp_with_nanoseconds_and_z(self):
        assert parse_timestamp("2026-01-02T03:04:05.123456789Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_offset(self):
        parsed = parse_timestamp("2026-01-02T03:04:05+02:00")
        assert parsed == datetime(2026, 1, 2, 1, 4, 5, tzinfo=timezone.utc)

    @pytest.mark.parametrize("value", [None, "", "2026-01-02", "2026-01-02T03:04:05", "kein datum"])
    def test_invalid(self, value):
        assert parse_timestamp(value) is None


class TestUptime:
    @staticmethod
    def runtime(start_time, server_time=None) -> RuntimeStatus:
        return RuntimeStatus(start_time, None, None, None, None, None, server_time=server_time)

    def test_uses_server_time(self):
        runtime = self.runtime("2026-01-01T00:00:00Z", "2026-01-01T01:00:30.5Z")
        assert runtime.uptime_seconds == 3630

    def test_falls_back_to_local_clock(self):
        start = datetime.now(timezone.utc) - timedelta(minutes=5)
        uptime = self.runtime(start.strftime("%Y-%m-%dT%H:%M:%SZ")).uptime_seconds
        assert uptime is not None
        assert 299 <= uptime <= 310

    def test_unknown_start_time(self):
        assert self.runtime(None, "2026-01-01T00:00:00Z").uptime_seconds is None


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (0, "0s"),
        (59.9, "59s"),
        (61, "1m 1s"),
        (3600, "1h 0m"),
        (3 * 86400 + 2 * 3600 + 5 * 60 + 7, "3d 2h 5m"),
    ],
)
def test_format_duration(seconds, expected):
    assert format_duration(seconds) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, "0 B"),
        (1023, "1023 B"),
        (1024, "1.0 KiB"),
        (1536 * 1024, "1.5 MiB"),
        (5 * 1024**3, "5.0 GiB"),
        (2048 * 1024**3, "2048.0 GiB"),
    ],
)
def test_format_bytes(value, expected):
    assert format_bytes(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("anything", True),
        ("false", False),
        (" FALSE ", False),
        ("0", False),
        ("no", False),
        ("Off", False),
    ],
)
def test_str_to_bool(value, expected):
    assert str_to_bool(value) is expected


class TestLoadConfig:
    ENV_VARS = (
        "PROMETHEUS_URL",
        "PROMETHEUS_USER",
        "PROMETHEUS_PASSWORD",
        "PROMETHEUS_VERIFY_SSL",
        "PROMETHEUS_CA_BUNDLE",
    )

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        for name in self.ENV_VARS:
            # setenv vor delenv, damit monkeypatch auch von load_dotenv gesetzte Variablen wieder entfernt
            monkeypatch.setenv(name, "")
            monkeypatch.delenv(name)
        # Keine echte .env aus dem Arbeitsverzeichnis einlesen
        monkeypatch.setattr(main, "load_dotenv", lambda **kwargs: False)

    def test_missing_url_raises(self):
        with pytest.raises(PrometheusError, match="PROMETHEUS_URL"):
            load_config(None)

    def test_defaults(self, monkeypatch):
        monkeypatch.setenv("PROMETHEUS_URL", "http://prom")
        assert load_config(None) == PrometheusConfig(
            url="http://prom", user=None, password=None, verify_ssl=True, ca_bundle=None
        )

    def test_all_values(self, monkeypatch):
        monkeypatch.setenv("PROMETHEUS_URL", "http://prom")
        monkeypatch.setenv("PROMETHEUS_USER", "u")
        monkeypatch.setenv("PROMETHEUS_PASSWORD", "p")
        monkeypatch.setenv("PROMETHEUS_VERIFY_SSL", "false")
        monkeypatch.setenv("PROMETHEUS_CA_BUNDLE", "/ca.pem")
        assert load_config(None) == PrometheusConfig(
            url="http://prom", user="u", password="p", verify_ssl=False, ca_bundle="/ca.pem"
        )

    def test_empty_ca_bundle_is_none(self, monkeypatch):
        monkeypatch.setenv("PROMETHEUS_URL", "http://prom")
        monkeypatch.setenv("PROMETHEUS_CA_BUNDLE", "")
        assert load_config(None).ca_bundle is None

    def test_reads_env_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(main, "load_dotenv", REAL_LOAD_DOTENV)
        env_file = tmp_path / ".env"
        env_file.write_text("PROMETHEUS_URL=http://from-file\nPROMETHEUS_USER=fileuser\n")

        config = load_config(str(env_file))

        assert config.url == "http://from-file"
        assert config.user == "fileuser"


class TestBuildClient:
    @staticmethod
    def config(**overrides) -> PrometheusConfig:
        values = {"url": "https://prom", "user": "u", "password": "p", "verify_ssl": True, "ca_bundle": None}
        values.update(overrides)
        return PrometheusConfig(**values)

    def test_insecure_overrides_config_and_ignores_missing_ca(self):
        client = build_client(self.config(ca_bundle="/does/not/exist"), insecure=True)
        assert client.session.verify is False

    def test_missing_ca_bundle_raises(self):
        with pytest.raises(PrometheusError, match="CA-Bundle"):
            build_client(self.config(ca_bundle="/does/not/exist"))

    def test_ca_file_argument_overrides_config(self, tmp_path):
        ca_file = tmp_path / "ca.pem"
        ca_file.write_text("dummy")
        client = build_client(self.config(ca_bundle="/does/not/exist"), ca_file=str(ca_file))
        assert client.session.verify == str(ca_file)

    def test_verify_ssl_false_from_config(self):
        client = build_client(self.config(verify_ssl=False, ca_bundle="/does/not/exist"))
        assert client.session.verify is False
