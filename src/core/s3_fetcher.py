"""
S3 document fetcher for Iqidis documents.
Downloads from iqidis-artifact bucket using artifact.storage_key.
"""
import os
import threading
from typing import Optional

S3_BUCKET = "iqidis-artifact"

# Bound how long a single S3 call can hold a request thread. Previous behavior
# had no timeout — a hung S3 connection would sit on a gunicorn thread until
# the 600s worker timeout. Read timeout is generous for large PDFs.
_S3_CONNECT_TIMEOUT_S = 5
_S3_READ_TIMEOUT_S = 60

_s3_client = None
_s3_client_lock = threading.Lock()


def _get_s3_client():
    """Cache the boto3 S3 client. Previous code rebuilt the client per
    download, so every call paid TLS handshake + creds-lookup overhead."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise ImportError("boto3 required. pip install boto3")

    with _s3_client_lock:
        if _s3_client is not None:
            return _s3_client
        region = os.getenv("AWS_REGION", "us-east-1")
        _s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            config=Config(
                connect_timeout=_S3_CONNECT_TIMEOUT_S,
                read_timeout=_S3_READ_TIMEOUT_S,
                retries={"max_attempts": 3, "mode": "standard"},
                max_pool_connections=32,
            ),
        )
        return _s3_client


def download_from_s3(storage_key: str, bucket: str = S3_BUCKET) -> Optional[bytes]:
    """Download document bytes from S3."""
    try:
        from botocore.exceptions import ClientError, BotoCoreError
    except ImportError:
        raise ImportError("boto3 required. pip install boto3")

    client = _get_s3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=storage_key)
        return response["Body"].read()
    except (ClientError, BotoCoreError) as e:
        print(f"S3 download failed for {storage_key}: {e}")
        return None
