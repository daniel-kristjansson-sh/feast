import json
import logging
import uuid
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional
from urllib.parse import urlparse

from feast.errors import RegistryCASConflictError
from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.utils import _utc_now

logger = logging.getLogger(__name__)

try:
    from google.cloud.exceptions import PreconditionFailed as GCSPreconditionFailed
except ImportError:
    GCSPreconditionFailed = type("GCSPreconditionFailed", (Exception,), {})


class GCSRegistryStore(RegistryStore):
    """GCS registry store with compare-and-swap (optimistic concurrency) support.

    Captures the blob generation on every read and passes it as
    ``if_generation_match`` on every write, so GCS returns 412 when another
    process has written in between. On conflict, raises
    :class:`RegistryCASConflictError`.
    """

    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        uri = registry_config.path
        try:
            import google.cloud.storage as storage
        except ImportError as e:
            from feast.errors import FeastExtrasDependencyImportError

            raise FeastExtrasDependencyImportError("gcp", str(e))

        self.gcs_client = storage.Client()
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._blob = self._uri.path.lstrip("/")
        self._expected_generation: Optional[int] = None

    def get_registry_proto(self) -> RegistryProto:
        import google.cloud.storage as storage
        from google.cloud.exceptions import NotFound

        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            bucket = self.gcs_client.get_bucket(self._bucket)
        except NotFound:
            raise Exception(
                f"No bucket named {self._bucket} exists; please create it first."
            )
        blob = storage.Blob(bucket=bucket, name=self._blob)
        if blob.exists(self.gcs_client):
            blob.reload(self.gcs_client)
            self._expected_generation = blob.generation
            blob.download_to_file(file_obj)
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self._uri.geturl()}". Have you run "feast apply"?'
        )

    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        from google.cloud.exceptions import NotFound

        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        try:
            gs_bucket.delete_blob(self._blob)
        except NotFound:
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(_utc_now())
        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        blob = gs_bucket.blob(self._blob)
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        try:
            blob.upload_from_file(
                file_obj, if_generation_match=self._expected_generation
            )
            blob.reload(self.gcs_client)
            self._expected_generation = blob.generation
        except GCSPreconditionFailed as e:
            raise RegistryCASConflictError(
                f"Registry CAS conflict: another process modified the GCS "
                f"registry object at {self._uri.geturl()} between read "
                f"and write. Re-read and retry."
            ) from e

    def set_project_metadata(self, project: str, key: str, value: str):
        registry_proto = self.get_registry_proto()
        found = False
        for pm in registry_proto.project_metadata:
            if pm.project == project:
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}
                if not isinstance(meta, dict):
                    meta = {}
                meta[key] = value
                pm.project_uuid = json.dumps(meta)
                found = True
                break
        if not found:
            from feast.project_metadata import ProjectMetadata

            pm = ProjectMetadata(project_name=project)
            pm.project_uuid = json.dumps({key: value})
            registry_proto.project_metadata.append(pm.to_proto())
        self.update_registry_proto(registry_proto)

    def get_project_metadata(self, project: str, key: str) -> Optional[str]:
        registry_proto = self.get_registry_proto()
        for pm in registry_proto.project_metadata:
            if pm.project == project:
                try:
                    meta = json.loads(pm.project_uuid) if pm.project_uuid else {}
                except Exception:
                    meta = {}
                if not isinstance(meta, dict):
                    return None
                return meta.get(key, None)
        return None
