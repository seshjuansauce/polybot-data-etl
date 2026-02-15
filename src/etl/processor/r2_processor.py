from __future__ import annotations

import io
import json
import gzip
import hashlib
from typing import Any, Dict, Optional, Union, List

import boto3
import botocore
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.etl.models.config.r2_config import R2Config


JsonLike = Union[Dict[str, Any], List[Any]]


class R2Processor:
    """
    Small utility wrapper around Cloudflare R2 (S3-compatible) for:
      - uploading/downloading JSON
      - uploading/downloading Parquet (via PyArrow)
      - listing keys, checking existence, deleting
    """

    def __init__(
        self,
        config: R2Config,
        *,
        default_parquet_compression: str = "zstd",
        default_parquet_row_group_size: Optional[int] = None,
        connect_timeout_s: int = 10,
        read_timeout_s: int = 60,
        max_pool_connections: int = 10,
    ) -> None:
        self._config = config
        self._endpoint_url = config.endpoint_template.format(account_id=config.account_id)

        boto_cfg = botocore.config.Config(
            region_name=config.region_name,
            connect_timeout=connect_timeout_s,
            read_timeout=read_timeout_s,
            retries={"max_attempts": 10, "mode": "standard"},
            max_pool_connections=max_pool_connections,
        )

        self._s3 = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            config=boto_cfg,
        )

        self._default_parquet_compression = default_parquet_compression
        self._default_parquet_row_group_size = default_parquet_row_group_size

    # -----------------------------
    # Core helpers
    # -----------------------------
    @property
    def bucket(self) -> str:
        return self._config.bucket

    def _md5_hex(self, b: bytes) -> str:
        return hashlib.md5(b).hexdigest()

    # -----------------------------
    # Existence / metadata / listing
    # -----------------------------
    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def head(self, key: str) -> Dict[str, Any]:
        return self._s3.head_object(Bucket=self.bucket, Key=key)

    def list_keys(
        self,
        *,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> List[str]:
        keys: List[str] = []
        token: Optional[str] = None

        while True:
            kwargs: Dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": prefix,
                "MaxKeys": max_keys,
            }
            if token:
                kwargs["ContinuationToken"] = token

            resp = self._s3.list_objects_v2(**kwargs)
            for item in resp.get("Contents", []) or []:
                k = item.get("Key")
                if k:
                    keys.append(k)

            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break

        return keys

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    # -----------------------------
    # Raw bytes
    # -----------------------------
    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        cache_control: Optional[str] = None,
        content_encoding: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
        }
        if cache_control:
            kwargs["CacheControl"] = cache_control
        if content_encoding:
            kwargs["ContentEncoding"] = content_encoding
        if metadata:
            kwargs["Metadata"] = metadata

        return self._s3.put_object(**kwargs)

    def get_bytes(self, key: str) -> bytes:
        resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    # -----------------------------
    # JSON helpers
    # -----------------------------
    def put_json(
        self,
        key: str,
        obj: JsonLike,
        *,
        compress_gzip: bool = False,
        ensure_ascii: bool = False,
        indent: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        raw = json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent).encode("utf-8")

        if compress_gzip:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(raw)
            data = buf.getvalue()
            return self.put_bytes(
                key,
                data,
                content_type="application/json",
                content_encoding="gzip",
                metadata=metadata,
            )

        return self.put_bytes(
            key,
            raw,
            content_type="application/json",
            metadata=metadata,
        )

    def get_json(self, key: str) -> JsonLike:
        raw = self.get_bytes(key)
        # If gzipped, boto3 does not automatically decompress. We can attempt detect.
        if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))

    # -----------------------------
    # Parquet helpers (PyArrow)
    # -----------------------------
    def df_to_parquet_bytes(
        self,
        df: pd.DataFrame,
        *,
        schema: Optional[pa.Schema] = None,
        compression: Optional[str] = None,
        row_group_size: Optional[int] = None,
        use_dictionary: bool = True,
        write_statistics: bool = True,
        preserve_index: bool = False,
    ) -> bytes:
        comp = compression or self._default_parquet_compression
        rgs = row_group_size if row_group_size is not None else self._default_parquet_row_group_size

        table = pa.Table.from_pandas(df, schema=schema, preserve_index=preserve_index)

        buf = io.BytesIO()
        pq.write_table(
            table,
            buf,
            compression=comp,
            row_group_size=rgs,
            use_dictionary=use_dictionary,
            write_statistics=write_statistics,
        )
        return buf.getvalue()

    def put_parquet_df(
        self,
        key: str,
        df: pd.DataFrame,
        *,
        schema: Optional[pa.Schema] = None,
        compression: Optional[str] = None,
        row_group_size: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None,
        content_type: str = "application/octet-stream",
    ) -> Dict[str, Any]:
        body = self.df_to_parquet_bytes(
            df,
            schema=schema,
            compression=compression,
            row_group_size=row_group_size,
        )
        return self.put_bytes(
            key,
            body,
            content_type=content_type,
            metadata=metadata,
        )

    def get_parquet_table(self, key: str) -> pa.Table:
        data = self.get_bytes(key)
        buf = io.BytesIO(data)
        return pq.read_table(buf)

    def get_parquet_df(self, key: str) -> pd.DataFrame:
        table = self.get_parquet_table(key)
        return table.to_pandas()

    # -----------------------------
    # Convenience: multipart upload for large payloads
    # (S3 client will auto-multipart for some higher-level APIs, but put_object won't.)
    # For most bronze ETL parts (< ~100-200MB), put_object is fine.
    # -----------------------------
    def put_large_bytes_multipart(
        self,
        key: str,
        data: bytes,
        *,
        part_size_mb: int = 16,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        part_size = max(5, part_size_mb) * 1024 * 1024  # S3 minimum part size is 5MB (except last part)

        create_kwargs: Dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "ContentType": content_type,
        }
        if metadata:
            create_kwargs["Metadata"] = metadata

        mpu = self._s3.create_multipart_upload(**create_kwargs)
        upload_id = mpu["UploadId"]

        parts = []
        try:
            part_number = 1
            for start in range(0, len(data), part_size):
                chunk = data[start : start + part_size]
                resp = self._s3.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk,
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                part_number += 1

            complete = self._s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return complete

        except Exception:
            self._s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
            raise