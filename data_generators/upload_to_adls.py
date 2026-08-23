"""
ADLS Gen2 Landing uploader for Smart Plantation Analytics (Phase 1).

Simulates external systems delivering raw batch files into the platform. It
reads the locally generated CSVs from ``data/raw/<source>/`` and uploads them to
the **Landing** container on Azure Data Lake Storage Gen2, preserving the
per-source folder structure:

    data/raw/harvest/harvest_transactions.csv
        -> https://<account>.dfs.core.windows.net/landing/harvest/harvest_transactions.csv

Architecture rules enforced here (ARCHITECTURE.md §6/§7, AGENTS.md §5):
  * This writes to **Landing only**. ADF owns Landing -> Bronze ingestion.
  * It must NEVER bypass ADF and write directly to Bronze/Silver/Gold. The
    destination container is hard-fixed to the Landing container name resolved
    from configuration, and any attempt to pass a different container is
    rejected.
  * Credentials come from environment variables ONLY. Nothing is hard-coded and
    no secret is ever written to disk or logs.

Authentication (environment variables):
  * ``AZURE_STORAGE_ACCOUNT``         - ADLS Gen2 storage account name (required)
  * ``AZURE_STORAGE_ACCOUNT_KEY``     - account key (optional; enables real upload)
  * ``ADLS_LANDING_CONTAINER``        - Landing container name (default: "landing")

Environment loading: when this module is executed as a CLI entrypoint (``__main__``)
it loads ``.env`` from the repository root via ``python-dotenv`` (existing real
environment variables always take precedence over ``.env`` values). No secret is
ever written to disk or logs.

If ``AZURE_STORAGE_ACCOUNT_KEY`` is not set the uploader runs in DRY-RUN mode:
it reports exactly what it *would* upload (and validates the Landing-only
contract) without making any network calls. This keeps Phase 1 fully testable
without fabricating any Azure state.

The implementation uses only the Python standard library (HTTPS + the ADLS Gen2
REST API) so that no dependency beyond ``pyyaml`` is required.

Usage (from the repository root):
    python3 -m data_generators.upload_to_adls
    python3 data_generators/upload_to_adls.py            # dry-run if no key set
"""

import base64
import hashlib
import hmac
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import quote

import yaml

# Allow both invocation styles by ensuring the repo root is importable:
#   python3 -m data_generators.upload_to_adls
#   python3 data_generators/upload_to_adls.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# The ONLY ADLS layer this module is ever permitted to write to. ADF owns
# Landing -> Bronze; Bronze/Silver/Gold are strictly out of scope here.
ALLOWED_LAYER = "landing"

# Local source directories (under data/raw/) whose CSV files are delivered.
# The sensor source is excluded: it belongs to the Phase 7 streaming path and
# is delivered to ADLS Incoming, not Landing.
BATCH_SOURCES: List[str] = ["weather", "harvest", "fertilizer", "equipment", "hr", "finance"]


def load_config(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """Load and return master YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class LandingOnlyViolation(Exception):
    """Raised if any code path attempts to target a non-Landing destination."""


def resolve_landing_container() -> str:
    """
    Resolve the Landing container name from the environment.

    Defaults to ``landing``. Whatever value is resolved is treated as the
    Landing container; it is the only destination this module will ever use.
    """
    return os.getenv("ADLS_LANDING_CONTAINER", "landing")


def _guard_landing_only(container: str) -> str:
    """
    Enforce the Landing-only contract.

    The uploader is architecturally forbidden from writing to Bronze/Silver/
    Gold (ADF owns Landing -> Bronze). This guard rejects any container that is
    explicitly one of those downstream layers.
    """
    forbidden = {"bronze", "silver", "gold", "incoming", "live-bronze", "live-silver", "checkpoints"}
    if container.strip().lower() in forbidden:
        raise LandingOnlyViolation(
            f"Refusing to upload: container '{container}' is not the Landing layer. "
            "upload_to_adls.py writes to ADLS Landing ONLY (ADF owns Landing -> Bronze)."
        )
    return container


def collect_batch_files(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Build the list of local CSV files to upload and their Landing blob paths.

    Returns a list of dicts: {source, local_path, blob_path}. The blob path
    preserves the per-source folder structure (``<source>/<filename>``).
    """
    output_paths = config.get("output_settings", {}).get("output_paths", {})
    files: List[Dict[str, str]] = []

    for source in BATCH_SOURCES:
        src_dir = Path(output_paths.get(source, f"data/raw/{source}"))
        if not src_dir.is_dir():
            print(f"  [skip] {source}: local directory not found: {src_dir}")
            continue
        for csv_file in sorted(src_dir.glob("*.csv")):
            files.append(
                {
                    "source": source,
                    "local_path": str(csv_file),
                    "blob_path": f"{source}/{csv_file.name}",
                }
            )
    return files


def _shared_key_authorization(
    account: str, key: str, method: str, path: str, headers: Dict[str, str], length: int = 0
) -> str:
    """
    Build an Azure Storage Shared Key Authorization header value.

    Only called when a real account key is supplied via the environment. The
    key is used purely in-memory to sign the request and is never persisted.

    Shared Key string-to-sign format (Azure Storage Services REST API):

        VERB
        Content-Encoding / Content-Language / Content-Length / Content-MD5 /
        Content-Type / Date / If-Modified-Since / If-Match / If-None-Match /
        If-Unmodified-Since / Range            (one blank line each)
        CanonicalizedHeaders                   (x-ms-* headers, one per line)
        CanonicalizedResource                  (/account/container[/path])
        [query parameter lines: name:value]    (sorted by name, lowercase name)

    ``path`` may contain a query string (``?a=b&c=d``); it is split here so
    callers can pass the full request path. Query parameters are appended as
    their own ``name:value`` lines after the canonicalized resource — they must
    NOT be embedded inside the resource path (that produced HTTP 403
    AuthenticationFailed; verified against the real account).
    """
    # Split path into the canonicalized resource and its query parameters.
    raw_path, _, raw_query = path.partition("?")
    query_pairs = []
    for part in raw_query.split("&"):
        if not part:
            continue
        name, _, value = part.partition("=")
        query_pairs.append((name.lower(), f"{name.lower()}:{value}"))
    # Shared Key requires query parameters sorted by (lowercased) parameter name.
    query_pairs.sort(key=lambda kv: kv[0])
    query_lines = [line for _, line in query_pairs]

    string_to_sign = "\n".join(
        [
            method,
            "",  # Content-Encoding
            "",  # Content-Language
            str(length) if length else "",  # Content-Length
            "",  # Content-MD5
            headers.get("Content-Type", ""),
            "",  # Date
            "",  # If-Modified-Since
            "",  # If-Match
            "",  # If-None-Match
            "",  # If-Unmodified-Since
            "",  # Range
            "x-ms-date:" + headers["x-ms-date"],
            "x-ms-version:" + headers["x-ms-version"],
            f"/{account}/{raw_path.lstrip('/')}",
        ]
        + query_lines
    )
    signature = base64.b64encode(
        hmac.new(base64.b64decode(key), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    return f"SharedKey {account}:{signature}"


def _adls_request(
    account: str, key: str, method: str, resource_path: str, data: Optional[bytes] = None
) -> None:
    """Perform a single ADLS Gen2 REST call signed with the account key."""
    url = f"https://{account}.dfs.core.windows.net/{resource_path}"
    headers = {
        "x-ms-date": datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-ms-version": "2021-08-06",
    }
    length = len(data) if data else 0
    if data is not None:
        headers["Content-Type"] = "application/octet-stream"
    headers["Authorization"] = _shared_key_authorization(
        account, key, method, resource_path, headers, length
    )
    req = urlrequest.Request(url, data=data, headers=headers, method=method)
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            resp.read()
    except HTTPError as err:
        # Surface the real Azure error; never log the key.
        raise RuntimeError(
            f"ADLS request failed: {method} {resource_path} -> HTTP {err.code} {err.reason}"
        ) from err
    except URLError as err:
        raise RuntimeError(
            f"ADLS request failed: {method} {resource_path} -> {err.reason}"
        ) from err


def upload_file_to_landing(
    account: str, key: str, container: str, blob_path: str, local_path: str
) -> None:
    """
    Upload one local file to the Landing container using the ADLS Gen2
    create/append/flush REST flow.
    """
    _guard_landing_only(container)
    with open(local_path, "rb") as f:
        payload = f.read()

    encoded_path = "/".join(quote(part) for part in f"{container}/{blob_path}".split("/"))
    # create (resource=file)
    _adls_request(account, key, "PUT", f"{encoded_path}?resource=file")
    # append
    if payload:
        _adls_request(account, key, "PATCH", f"{encoded_path}?action=append&position=0", payload)
    # flush
    _adls_request(
        account, key, "PATCH", f"{encoded_path}?action=flush&position={len(payload)}"
    )


def upload_to_landing(config_path: str = "data_generators/config.yaml") -> Dict[str, Any]:
    """
    Deliver all generated batch CSVs into the ADLS Landing container.

    Returns a summary dict. In DRY-RUN mode (no account key in the environment)
    no network calls are made; the summary describes what would be uploaded.
    """
    config = load_config(config_path)
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    container = _guard_landing_only(resolve_landing_container())

    files = collect_batch_files(config)
    dry_run = not bool(key)

    print("ADLS Landing uploader")
    print(f"  destination layer : {ALLOWED_LAYER} (Landing only)")
    print(f"  container         : {container}")
    print(f"  storage account   : {account or '(not set)'}")
    print(f"  mode              : {'DRY-RUN (no AZURE_STORAGE_ACCOUNT_KEY)' if dry_run else 'LIVE UPLOAD'}")
    print(f"  files discovered  : {len(files)}")

    if not dry_run and not account:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT is not set. Both AZURE_STORAGE_ACCOUNT and "
            "AZURE_STORAGE_ACCOUNT_KEY are required for a live upload."
        )

    # In live mode both values are guaranteed present by the checks above; bind
    # them to narrowed locals so the type checker (and reader) can rely on that.
    live_account: Optional[str] = account if not dry_run else None
    live_key: Optional[str] = key if not dry_run else None

    uploaded: List[str] = []
    for item in files:
        target = f"{container}/{item['blob_path']}"
        if dry_run:
            print(f"  [dry-run] would upload {item['local_path']} -> {target}")
        else:
            print(f"  [upload]  {item['local_path']} -> {target}")
            # live_account/live_key are non-None here because dry_run is False.
            upload_file_to_landing(
                str(live_account), str(live_key), container, item["blob_path"], item["local_path"]
            )
        uploaded.append(target)

    summary = {
        "container": container,
        "layer": ALLOWED_LAYER,
        "dry_run": dry_run,
        "file_count": len(files),
        "destinations": uploaded,
    }
    print(
        f"\n{'DRY-RUN complete' if dry_run else 'Upload complete'}: "
        f"{len(files)} file(s) -> container '{container}' (Landing only)."
    )
    return summary


def _load_dotenv_if_running_as_cli() -> None:
    """
    Load ``.env`` from the repository root (python-dotenv) when this module is
    run as a CLI entrypoint.

    Real environment variables always win over ``.env`` values (``override=False``).
    This is deliberately NOT executed on import, so library users (and tests)
    keep full control of the environment; only interactive CLI runs pick up the
    local ``.env``.
    """
    if __package__ in (None, "") or __name__ == "__main__":
        try:
            from dotenv import load_dotenv  # python-dotenv (in requirements.txt)

            load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
        except ImportError:  # pragma: no cover - dotenv is a declared dependency
            print("  [warn] python-dotenv not installed; relying on the real environment only.")


if __name__ == "__main__":
    _load_dotenv_if_running_as_cli()
    upload_to_landing()
