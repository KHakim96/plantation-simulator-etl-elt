"""Phase 9 orchestration contract/unit tests (OFFLINE — no Azure/Databricks).

These tests validate the Phase 9 artifacts on disk and the ADF REST logic of
``databricks/orchestrator/trigger_adf.py`` using mocks only. They never touch
the network, Azure, Databricks, or real credentials.

Covered:
  * ADF REST URL construction + fixed Azure coordinates.
  * clear error on missing auth configuration.
  * no credential/token leakage in logs.
  * createRun runId extraction (mocked).
  * poll terminal states: Succeeded/Failed/Cancelled/timeout (mocked).
  * workflow JSON validity, DAG ordering, DQ gating, streaming independence.
  * script-path references exist; no hard-coded secrets.
  * bronze_to_silver / silver_to_gold use sys.exit(main()).
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TRIGGER_ADF = REPO_ROOT / "databricks" / "orchestrator" / "trigger_adf.py"
BATCH_WF = REPO_ROOT / "databricks" / "workflows" / "plantation_batch.json"
STREAM_WF = REPO_ROOT / "databricks" / "workflows" / "sensor_streaming.json"

EXPECTED_SUBSCRIPTION = "afec86b2-072d-4bdb-83a9-4fe370a3a0fc"
EXPECTED_RG = "plantation-simulator-rg"
EXPECTED_FACTORY = "plantation-simulator-adf"
EXPECTED_PIPELINE = "PL_Ingest_Landing_To_Bronze"
EXPECTED_API_VERSION = "2018-06-01"


def _load_trigger_adf():
    """Import trigger_adf.py as a module without requiring Azure/Databricks."""
    spec = importlib.util.spec_from_file_location("trigger_adf", TRIGGER_ADF)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["trigger_adf"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def ta():
    return _load_trigger_adf()


def _load_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ==============================================================================
# 1-2. URL construction + fixed coordinates
# ==============================================================================


class TestUrlConstruction:
    def test_fixed_coordinates(self, ta):
        assert ta.SUBSCRIPTION_ID == EXPECTED_SUBSCRIPTION
        assert ta.RESOURCE_GROUP == EXPECTED_RG
        assert ta.FACTORY_NAME == EXPECTED_FACTORY
        assert ta.PIPELINE_NAME == EXPECTED_PIPELINE
        assert ta.API_VERSION == EXPECTED_API_VERSION
        assert ta.ARM_SCOPE == "https://management.azure.com/.default"

    def test_trigger_url(self, ta):
        url = ta.build_trigger_url()
        assert url == (
            "https://management.azure.com/subscriptions/"
            f"{EXPECTED_SUBSCRIPTION}/resourceGroups/{EXPECTED_RG}"
            "/providers/Microsoft.DataFactory/factories/"
            f"{EXPECTED_FACTORY}/pipelines/{EXPECTED_PIPELINE}/createRun"
            f"?api-version={EXPECTED_API_VERSION}"
        )

    def test_run_url(self, ta):
        url = ta.build_run_url("RUN123")
        assert url == (
            "https://management.azure.com/subscriptions/"
            f"{EXPECTED_SUBSCRIPTION}/resourceGroups/{EXPECTED_RG}"
            "/providers/Microsoft.DataFactory/factories/"
            f"{EXPECTED_FACTORY}/pipelineruns/RUN123"
            f"?api-version={EXPECTED_API_VERSION}"
        )


# ==============================================================================
# 3. Missing auth configuration -> clear error (and no secret leakage)
# ==============================================================================


class TestAuthConfig:
    def test_missing_local_credentials_raises_config_error(self, ta, monkeypatch):
        # Force the non-Databricks path and make DefaultAzureCredential fail.
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

        class _Boom:
            def __init__(self, *a, **k):
                pass

            def get_token(self, scope):
                raise RuntimeError("no credential chain available")

        monkeypatch.setattr(ta, "DefaultAzureCredential", _Boom)
        with pytest.raises(ta.AdfConfigError):
            ta.get_access_token()

    def test_databricks_secret_read_failure_raises_config_error(
        self, ta, monkeypatch
    ):
        # Force the Databricks path with a dbutils that cannot read secrets.
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")

        class _Secrets:
            def get(self, scope, key):
                raise RuntimeError("scope does not exist")

        class _DBUtils:
            secrets = _Secrets()

        monkeypatch.setattr(ta, "_get_dbutils", lambda: _DBUtils())
        with pytest.raises(ta.AdfConfigError):
            ta.get_access_token()

    def test_no_secret_or_token_in_error_messages(self, ta, monkeypatch, capsys):
        # Ensure that even when secret material exists in the failing call,
        # it is never echoed to stdout/stderr.
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
        secret_value = "super-secret-value-12345"

        class _Secrets:
            def get(self, scope, key):
                return secret_value

        class _DBUtils:
            secrets = _Secrets()

        class _CredBoom:
            def __init__(self, *a, **k):
                pass

            def get_token(self, scope):
                raise RuntimeError("token endpoint unreachable")

        monkeypatch.setattr(ta, "_get_dbutils", lambda: _DBUtils())
        monkeypatch.setattr(ta, "ClientSecretCredential", _CredBoom)
        with pytest.raises(ta.AdfConfigError):
            ta.get_access_token()
        out = capsys.readouterr()
        assert secret_value not in out.out
        assert secret_value not in out.err


# ==============================================================================
# 5-9. Trigger + poll behaviour (mocked HTTP)
# ==============================================================================


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


class TestTriggerAndPoll:
    def test_trigger_extracts_run_id(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        monkeypatch.setattr(
            ta.requests,
            "post",
            lambda *a, **k: _Resp(200, {"runId": "run-abc-123"}),
        )
        assert ta.trigger_pipeline() == "run-abc-123"

    def test_trigger_missing_runid_raises(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        monkeypatch.setattr(ta.requests, "post", lambda *a, **k: _Resp(200, {}))
        with pytest.raises(ta.AdfConfigError):
            ta.trigger_pipeline()

    def test_poll_succeeded(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        statuses = iter(["InProgress", "Succeeded"])
        monkeypatch.setattr(
            ta.requests,
            "get",
            lambda *a, **k: _Resp(200, {"status": next(statuses)}),
        )
        monkeypatch.setattr(ta.time, "sleep", lambda s: None)
        assert (
            ta.poll_until_terminal("run1", timeout_seconds=10, poll_interval_seconds=0)
            == "Succeeded"
        )

    def test_poll_failed(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        monkeypatch.setattr(
            ta.requests, "get", lambda *a, **k: _Resp(200, {"status": "Failed"})
        )
        monkeypatch.setattr(ta.time, "sleep", lambda s: None)
        assert (
            ta.poll_until_terminal("run1", timeout_seconds=10, poll_interval_seconds=0)
            == "Failed"
        )

    def test_poll_cancelled(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        monkeypatch.setattr(
            ta.requests, "get", lambda *a, **k: _Resp(200, {"status": "Cancelled"})
        )
        monkeypatch.setattr(ta.time, "sleep", lambda s: None)
        assert (
            ta.poll_until_terminal("run1", timeout_seconds=10, poll_interval_seconds=0)
            == "Cancelled"
        )

    def test_poll_timeout(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "_auth_headers", lambda: {"Authorization": "Bearer x"})
        monkeypatch.setattr(
            ta.requests, "get", lambda *a, **k: _Resp(200, {"status": "InProgress"})
        )
        monkeypatch.setattr(ta.time, "sleep", lambda s: None)
        # Force monotonic to jump past the deadline after the first status check.
        times = iter([0.0, 100.0])
        monkeypatch.setattr(ta.time, "monotonic", lambda: next(times))
        with pytest.raises(ta.AdfConfigError):
            ta.poll_until_terminal("run1", timeout_seconds=10, poll_interval_seconds=0)

    def test_main_returns_zero_on_success(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "trigger_pipeline", lambda: "run1")
        monkeypatch.setattr(ta, "poll_until_terminal", lambda *a, **k: "Succeeded")
        assert ta.main() == 0

    def test_main_returns_one_on_failure(self, ta, monkeypatch):
        monkeypatch.setattr(ta, "trigger_pipeline", lambda: "run1")
        monkeypatch.setattr(ta, "poll_until_terminal", lambda *a, **k: "Failed")
        assert ta.main() == 1


# ==============================================================================
# 10-17. Workflow JSON contract tests
# ==============================================================================


def _task_map(wf: dict) -> dict[str, dict]:
    return {t["task_key"]: t for t in wf["tasks"]}


def _dependency_chain(wf: dict) -> list[str]:
    """Order tasks by following depends_on from the single root task."""
    tasks = _task_map(wf)
    roots = [k for k, t in tasks.items() if not t.get("depends_on")]
    assert len(roots) == 1, f"expected a single root task, got {roots}"
    order = [roots[0]]
    while len(order) < len(tasks):
        nxt = [
            k
            for k, t in tasks.items()
            if [d["task_key"] for d in t.get("depends_on", [])] == [order[-1]]
        ]
        assert len(nxt) == 1, f"expected a linear chain after {order[-1]}"
        order.append(nxt[0])
    return order


class TestBatchWorkflow:
    def test_parses(self):
        wf = _load_json(BATCH_WF)
        assert wf["name"] == "plantation_batch"

    def test_exact_dag_order(self):
        wf = _load_json(BATCH_WF)
        assert _dependency_chain(wf) == [
            "trigger_adf",
            "bronze_to_silver",
            "dq_checks",
            "silver_to_gold",
        ]

    def test_silver_to_gold_depends_on_dq(self):
        wf = _load_json(BATCH_WF)
        tasks = _task_map(wf)
        deps = [d["task_key"] for d in tasks["silver_to_gold"]["depends_on"]]
        assert "dq_checks" in deps

    def test_no_dq_bypass(self):
        # Every task (except the root) must have a depends_on; Gold's only
        # upstream is dq_checks, and dq_checks' only upstream is bronze_to_silver.
        wf = _load_json(BATCH_WF)
        tasks = _task_map(wf)
        assert [d["task_key"] for d in tasks["silver_to_gold"]["depends_on"]] == [
            "dq_checks"
        ]
        assert [d["task_key"] for d in tasks["dq_checks"]["depends_on"]] == [
            "bronze_to_silver"
        ]
        assert [d["task_key"] for d in tasks["bronze_to_silver"]["depends_on"]] == [
            "trigger_adf"
        ]

    def test_script_paths_exist(self):
        wf = _load_json(BATCH_WF)
        for task in wf["tasks"]:
            rel = task["spark_python_task"]["python_file"]
            assert (REPO_ROOT / rel).is_file(), f"missing script: {rel}"

    def test_expected_scripts(self):
        wf = _load_json(BATCH_WF)
        tasks = _task_map(wf)
        assert (
            tasks["trigger_adf"]["spark_python_task"]["python_file"]
            == "databricks/orchestrator/trigger_adf.py"
        )
        assert (
            tasks["bronze_to_silver"]["spark_python_task"]["python_file"]
            == "databricks/batch/bronze_to_silver.py"
        )
        assert (
            tasks["dq_checks"]["spark_python_task"]["python_file"]
            == "databricks/batch/dq_checks.py"
        )
        assert (
            tasks["silver_to_gold"]["spark_python_task"]["python_file"]
            == "databricks/batch/silver_to_gold.py"
        )


class TestStreamingWorkflow:
    def test_parses(self):
        wf = _load_json(STREAM_WF)
        assert wf["name"] == "sensor_streaming"

    def test_single_task(self):
        wf = _load_json(STREAM_WF)
        assert [t["task_key"] for t in wf["tasks"]] == ["sensors_stream"]

    def test_references_sensors_stream(self):
        wf = _load_json(STREAM_WF)
        rel = wf["tasks"][0]["spark_python_task"]["python_file"]
        assert rel == "databricks/streaming/sensors_stream.py"
        assert (REPO_ROOT / rel).is_file()

    def test_independent_of_batch(self):
        wf = _load_json(STREAM_WF)
        task = wf["tasks"][0]
        assert not task.get("depends_on"), "streaming task must have no depends_on"
        text = json.dumps(wf)
        for forbidden in (
            "trigger_adf",
            "dq_checks",
            "silver_to_gold",
            "bronze_to_silver",
            "createRun",
            "PL_Ingest_Landing_To_Bronze",
            "synapse",
        ):
            assert forbidden not in text, f"streaming must not reference {forbidden}"

    def test_has_schedule(self):
        wf = _load_json(STREAM_WF)
        assert "schedule" in wf
        assert wf["schedule"]["quartz_cron_expression"]
        assert wf["schedule"]["pause_status"] in ("PAUSED", "UNPAUSED")


# ==============================================================================
# 17. No hard-coded secrets in Phase 9 artifacts
# ==============================================================================


# Match genuine secret *assignments* (e.g. JSON "client_secret": "value",
# "password": "value", connection-string material) and token literals — but NOT
# constant/env-var NAMES such as ADF_SECRET_KEY_CLIENT_SECRET (which are
# configuration names, never values).
SECRET_PATTERNS = re.compile(
    r"(\"client_secret\"\s*:\s*\"[^\"]+\"|"
    r"\"clientsecret\"\s*:\s*\"[^\"]+\"|"
    r"\"password\"\s*:\s*\"[^\"]+\"|"
    r"accountKey|sasToken|sig=|"
    r"Bearer\s+[A-Za-z0-9._-]{20,}|"
    r"-----BEGIN)",
    re.IGNORECASE,
)


class TestNoHardcodedSecrets:
    @pytest.mark.parametrize(
        "path", [TRIGGER_ADF, BATCH_WF, STREAM_WF], ids=lambda p: p.name
    )
    def test_no_secret_literals(self, path):
        text = path.read_text(encoding="utf-8")
        assert not SECRET_PATTERNS.search(text), (
            f"possible secret literal found in {path.name}"
        )

    def test_trigger_adf_does_not_log_secrets(self):
        text = TRIGGER_ADF.read_text(encoding="utf-8")
        # It must never print the token/secret variables.
        assert "print(token" not in text
        assert "print(client_secret" not in text
        assert ".token)" not in text.replace("get_token", "")


# ==============================================================================
# 18. sys.exit(main()) in Bronze->Silver and Silver->Gold
# ==============================================================================


class TestExitCodePropagation:
    @pytest.mark.parametrize(
        "rel",
        [
            "databricks/batch/bronze_to_silver.py",
            "databricks/batch/silver_to_gold.py",
            "databricks/orchestrator/trigger_adf.py",
        ],
    )
    def test_main_guard_uses_sys_exit(self, rel):
        text = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert re.search(r'if __name__ == "__main__":\s*\n\s*sys\.exit\(main\(\)\)', text), (
            f"{rel} must call sys.exit(main()) in its __main__ guard"
        )
