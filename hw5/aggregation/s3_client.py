import logging
import time

import boto3
from botocore.exceptions import ClientError

from config import Settings

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.s3_endpoint_url,
                aws_access_key_id=self._settings.s3_access_key,
                aws_secret_access_key=self._settings.s3_secret_key,
                region_name="us-east-1",
            )
            logger.info(f"S3 client connected to {self._settings.s3_endpoint_url}")
        return self._client

    def ensure_bucket(self) -> None:
        client = self._get_client()
        bucket = self._settings.s3_bucket
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchBucket"):
                client.create_bucket(Bucket=bucket)
                logger.info(f"S3 bucket '{bucket}' created")
            else:
                raise

    def put_object(self, key: str, body: str, retries: int = 3, delay: float = 2.0) -> None:
        for attempt in range(1, retries + 1):
            try:
                self._get_client().put_object(
                    Bucket=self._settings.s3_bucket,
                    Key=key,
                    Body=body.encode("utf-8"),
                    ContentType="application/json",
                )
                logger.info(f"Uploaded s3://{self._settings.s3_bucket}/{key}")
                return
            except Exception as exc:
                logger.warning(f"S3 upload error (attempt {attempt}/{retries}): {exc}")
                self._client = None
                if attempt < retries:
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
