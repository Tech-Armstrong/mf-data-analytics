"""
sif/config/blob_io.py

Thin helpers for the WRITE path to upload parquet to the SIF Azure Blob
container.

The READ path (queries) does NOT use this module — DuckDB's azure extension
reads parquet directly. This is only for putting parquet INTO the container.

Auth: the same AZURE_STORAGE_CONNECTION_STRING env var used by the query layer.
Identical to config/blob_io.py except it imports the SIF constants (so it
targets the sifnavdata container).
"""

import io
from pathlib import Path

import polars as pl
from azure.storage.blob import BlobServiceClient

from sif.config.constants import AZURE_CONNECTION_STRING, BLOB_CONTAINER
from sif.config.logging_utils import get_logger

log = get_logger("blob_io")


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize a Polars DataFrame to parquet bytes (for upload)."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def _container_client():
    if not AZURE_CONNECTION_STRING:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING is not set — cannot upload to Blob. "
            "Set it in .env (local) or as a GitHub Actions secret (CI)."
        )
    svc = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    return svc.get_container_client(BLOB_CONTAINER)


def upload_bytes(data: bytes, blob_path: str) -> None:
    """Upload raw bytes to <container>/<blob_path>, overwriting if present."""
    cc = _container_client()
    cc.upload_blob(name=blob_path, data=io.BytesIO(data), overwrite=True)
    log.info("uploaded → az://%s/%s (%d bytes)", BLOB_CONTAINER, blob_path, len(data))


def upload_file(local_path: Path, blob_path: str) -> None:
    """Upload a local file to <container>/<blob_path>, overwriting if present."""
    upload_bytes(local_path.read_bytes(), blob_path)


def download_bytes(blob_path: str) -> bytes | None:
    """Download a blob's bytes, or None if it does not exist."""
    cc = _container_client()
    bc = cc.get_blob_client(blob_path)
    if not bc.exists():
        return None
    return bc.download_blob().readall()


def blob_exists(blob_path: str) -> bool:
    return _container_client().get_blob_client(blob_path).exists()
