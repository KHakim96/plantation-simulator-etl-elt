"""Phase 9: Trigger and poll the existing ADF pipeline via the ADF REST API.

Orchestration entry point for the Databricks batch workflow
(``databricks/workflows/plantation_batch.json``). It triggers the existing
``PL_Ingest_Landing_To_Bronze`` pipeline with an empty body (the pipeline
parameters all have defaults) and polls the run until a terminal state.

Terminal behaviour:
  * ``Succeeded``           -> exit 0
  * ``Failed``/``Cancelled`` -> exit 1
  * timeout                  -> exit 1

Authentication (Service Principal — never hard-coded, never logged):
  * On Databricks (``DATABRICKS_RUNTIME_VERSION`` set): the client id, client
    secret, and tenant id are read from a Databricks **secret scope** via
    ``dbutils.secrets.get`` and used with ``ClientSecretCredential``. The scope
    and key NAMES are configurable through environment variables (no secret
    value is ever placed in code, logs, or workflow JSON).
  * Local development: falls back to ``DefaultAzureCredential`` (env SP vars or
    the authenticated Azure CLI), so the module can be exercised offline of a
    workspace without a secret scope.

No access token, client secret, or other credential is ever printed. Only the
ADF run ID and run state are logged.

Uses ``requests`` + ``azure-identity`` (already project dependencies).
"""

from __future__ import annotations

import os
import sys
import time

import requests
from azure.identity import ClientSecretCredential, DefaultAzureCredential

# --- Fixed Azure coordinates for the EXISTING ADF pipeline (audited facts). ---
SUBSCRIPTION_ID = "afec86b2-072d-4bdb-83a9-4fe370a3a0fc"
RESOURCE_GROUP = "plantation-simulator-rg"
FACTORY_NAME = "plantation-simulator-adf"
PIPELINE_NAME = "PL_Ingest_Landing_To_Bronze"
API_VERSION = "2018-06-01"

ARM_SCOPE = "https://management.azure.com/.default"
ARM_BASE = (
    "https://management.azure.com/subscriptions/"
    f"{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
    "/providers/Microsoft.DataFactory/factories/"
    f"{FACTORY_NAME}"
)

# Terminal ADF run statuses.
STATUS_SUCCEEDED = "Succeeded"
STATUS_FAILED = "Failed"
STATUS_CANCELLED = "Cancelled"
TERMINAL_STATUSES = frozenset(
    {STATUS_SUCCEEDED, STATUS_FAILED, STATUS_CANCELLED}
)

# Defaults (overridable via non-secret environment variables).
DEFAULT_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
DEFAULT_POLL_INTERVAL_SECONDS = 30

# Environment-variable NAMES (never values) used to configure secret lookup.
ENV_SECRET_SCOPE = "ADF_SECRET_SCOPE"
ENV_KEY_CLIENT_ID = "ADF_SECRET_KEY_CLIENT_ID"
ENV_KEY_CLIENT_SECRET = "ADF_SECRET_KEY_CLIENT_SECRET"
ENV_KEY_TENANT_ID = "ADF_SECRET_KEY_TENANT_ID"
# Default secret-scope/key names (configurable; these are NAMES, not secrets).
DEFAULT_SECRET_SCOPE = "adf-sp"
DEFAULT_KEY_CLIENT_ID = "client-id"
DEFAULT_KEY_CLIENT_SECRET = "client-secret"
DEFAULT_KEY_TENANT_ID = "tenant-id"


class AdfConfigError(RuntimeError):
    """Raised when required configuration/credentials are unavailable."""


class AdfRunError(RuntimeError):
    """Raised when the ADF run ends in a non-success terminal state."""


# ==============================================================================
# URL construction
# ==============================================================================


def build_trigger_url() -> str:
    """Return the createRun endpoint for the existing pipeline."""
    return (
        f"{ARM_BASE}/pipelines/{PIPELINE_NAME}/createRun"
        f"?api-version={API_VERSION}"
    )


def build_run_url(run_id: str) -> str:
    """Return the pipeline-run polling endpoint for a run id."""
    return f"{ARM_BASE}/pipelineruns/{run_id}?api-version={API_VERSION}"


# ==============================================================================
# Authentication (secret scope on Databricks; DefaultAzureCredential locally)
# ==============================================================================


def _is_databricks() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def _get_dbutils():
    """Return a dbutils handle on Databricks, else None.

    No secret is touched here; this only resolves the secrets accessor.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:  # noqa: BLE001 - fall back to the IPython-attached dbutils
        try:  # pragma: no cover - Databricks notebook/jobs attach dbutils
            import __main__  # type: ignore

            return getattr(__main__, "dbutils", None)
        except Exception:  # noqa: BLE001
            return None


def _read_secret(scope: str, key: str) -> str:
    """Read a single secret value from a Databricks secret scope.

    Raises AdfConfigError if the secret cannot be read. The value is returned
    to the caller only (never logged).
    """
    dbutils = _get_dbutils()
    if dbutils is None:
        raise AdfConfigError(
            "dbutils is unavailable; cannot read Databricks secret scope "
            f"{scope!r}. Run on Databricks or configure local auth."
        )
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:
        raise AdfConfigError(
            f"Unable to read secret {key!r} from scope {scope!r}. Ensure the "
            "scope exists and the job has READ access. (Secret not exposed.)"
        ) from exc


def get_access_token() -> str:
    """Acquire an Azure AD access token for ARM.

    Databricks: SP credentials from the configured secret scope ->
    ClientSecretCredential. Local: DefaultAzureCredential. The token is
    returned to the caller and never logged.
    """
    if _is_databricks():
        scope = os.getenv(ENV_SECRET_SCOPE, DEFAULT_SECRET_SCOPE)
        client_id = _read_secret(
            scope, os.getenv(ENV_KEY_CLIENT_ID, DEFAULT_KEY_CLIENT_ID)
        )
        client_secret = _read_secret(
            scope, os.getenv(ENV_KEY_CLIENT_SECRET, DEFAULT_KEY_CLIENT_SECRET)
        )
        tenant_id = _read_secret(
            scope, os.getenv(ENV_KEY_TENANT_ID, DEFAULT_KEY_TENANT_ID)
        )
        if not (client_id and client_secret and tenant_id):
            raise AdfConfigError(
                "Service Principal credentials from secret scope are "
                "incomplete. (Values not exposed.)"
            )
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        credential = DefaultAzureCredential()

    try:
        token = credential.get_token(ARM_SCOPE).token
    except Exception as exc:
        raise AdfConfigError(
            "Failed to acquire an Azure AD token for ARM. Check SP "
            "credentials / local Azure CLI login. (Token not exposed.)"
        ) from exc
    if not token:
        raise AdfConfigError("Acquired an empty access token.")
    return token


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {get_access_token()}"}


# ==============================================================================
# ADF REST operations
# ==============================================================================


def trigger_pipeline() -> str:
    """Trigger the existing pipeline (empty body: defaults apply).

    Returns the ADF run id.
    """
    url = build_trigger_url()
    print(f"Triggering ADF pipeline: {PIPELINE_NAME} (factory {FACTORY_NAME})")
    try:
        response = requests.post(url, headers=_auth_headers(), json={}, timeout=60)
    except Exception as exc:
        raise AdfConfigError(f"Failed to call ADF createRun: {exc}") from exc

    if response.status_code not in (200, 201, 202):
        raise AdfConfigError(
            f"ADF createRun returned HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )
    try:
        run_id = response.json()["runId"]
    except (ValueError, KeyError) as exc:
        raise AdfConfigError(
            f"ADF createRun response did not contain a runId: "
            f"{response.text[:300]}"
        ) from exc
    print(f"ADF run triggered. runId: {run_id}")
    return run_id


def get_run_status(run_id: str) -> str:
    """Return the current ADF pipeline-run status string."""
    url = build_run_url(run_id)
    response = requests.get(url, headers=_auth_headers(), timeout=60)
    if response.status_code != 200:
        raise AdfConfigError(
            f"ADF pipeline-run poll returned HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )
    try:
        return response.json()["status"]
    except (ValueError, KeyError) as exc:
        raise AdfConfigError(
            f"ADF pipeline-run response had no status: {response.text[:300]}"
        ) from exc


def poll_until_terminal(
    run_id: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
) -> str:
    """Poll the run until a terminal status or timeout.

    Returns the terminal status string. Raises AdfConfigError on timeout.
    """
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while True:
        status = get_run_status(run_id)
        if status != last_status:
            print(f"ADF run {run_id} status: {status}")
            last_status = status
        if status in TERMINAL_STATUSES:
            return status
        if time.monotonic() >= deadline:
            raise AdfConfigError(
                f"Timed out after {timeout_seconds}s waiting for ADF run "
                f"{run_id} (last status: {last_status})."
            )
        time.sleep(poll_interval_seconds)


# ==============================================================================
# Entry point
# ==============================================================================


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def main() -> int:
    """Trigger the ADF pipeline and poll to a terminal state.

    Exit 0 on Succeeded; 1 on Failed/Cancelled/timeout/error.
    """
    timeout_seconds = _int_env("ADF_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    poll_interval = _int_env(
        "ADF_POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS
    )
    try:
        run_id = trigger_pipeline()
        final_status = poll_until_terminal(
            run_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval,
        )
    except (AdfConfigError, AdfRunError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if final_status == STATUS_SUCCEEDED:
        print(f"ADF pipeline {PIPELINE_NAME} Succeeded. runId: {run_id}")
        return 0
    print(
        f"ADF pipeline {PIPELINE_NAME} ended with status "
        f"{final_status}. runId: {run_id}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
