import logging
import uuid
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional

from feast.errors import (
    RegistryCASConflictError,
    S3RegistryBucketForbiddenAccess,
    S3RegistryBucketNotExist,
)
from feast.infra.registry.s3 import S3RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.utils import _utc_now

try:
    from botocore.exceptions import ClientError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))

logger = logging.getLogger(__name__)


class CASS3RegistryStore(S3RegistryStore):
    """S3 registry store with compare-and-swap (optimistic concurrency) support.

    The base ``S3RegistryStore`` does a blind ``put_object`` on every write,
    making concurrent registry updates a last-write-wins race. This subclass
    captures the S3 ETag on every read and passes it as ``IfMatch`` on every
    write, so S3 returns ``412 PreconditionFailed`` when another process
    has written in between.

    On 412, :class:`RegistryCASConflictError` is raised so callers can
    re-read, re-apply their mutation, and retry.
    """

    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        super().__init__(registry_config, repo_path)
        self._expected_etag: Optional[str] = None

    def get_registry_proto(self) -> RegistryProto:
        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            bucket = self.s3_client.Bucket(self._bucket)
            self.s3_client.meta.client.head_bucket(Bucket=bucket.name)
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                raise S3RegistryBucketNotExist(self._bucket)
            else:
                raise S3RegistryBucketForbiddenAccess(self._bucket) from e

        try:
            response = self.s3_client.meta.client.get_object(
                Bucket=self._bucket, Key=self._key
            )
            self._expected_etag = response.get("ETag", "")
            if self._expected_etag:
                self._expected_etag = self._expected_etag.strip('"')
            file_obj.write(response["Body"].read())
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        except ClientError as e:
            raise FileNotFoundError(
                f"Error while trying to locate Registry at path {self._uri.geturl()}"
            ) from e

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)

        extra_args = dict(self._boto_extra_args)
        if self._expected_etag:
            extra_args["IfMatch"] = self._expected_etag

        try:
            response = self.s3_client.Bucket(self._bucket).put_object(
                Body=file_obj, Key=self._key, **extra_args
            )
            if response is not None and response.e_tag:
                self._expected_etag = response.e_tag.strip('"')
        except ClientError as e:
            error_code = int(e.response.get("Error", {}).get("Code", "0"))
            if error_code == 412:
                raise RegistryCASConflictError(
                    f"Registry CAS conflict: another process modified the S3 registry "
                    f"object at {self._uri.geturl()} between read and write. "
                    f"Re-read and retry."
                ) from e
            raise
