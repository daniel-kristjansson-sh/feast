from unittest.mock import MagicMock, patch

import pytest

from feast.errors import RegistryCASConflictError
from feast.infra.registry.registry import Registry, cas_retry
from feast.infra.registry.s3 import S3RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class TestS3RegistryStoreCAS:
    """Tests for S3RegistryStore ETag capture and If-Match behavior."""

    @pytest.fixture
    def mock_s3_store(self):
        store = S3RegistryStore.__new__(S3RegistryStore)
        store._bucket = "test-bucket"
        store._key = "registry.db"
        store._uri = MagicMock()
        store._uri.geturl.return_value = "s3://test-bucket/registry.db"
        store._boto_extra_args = {}
        store._expected_etag = None
        store.s3_client = MagicMock()
        return store

    def test_get_registry_proto_captures_etag(self, mock_s3_store):
        proto = RegistryProto()
        proto.registry_schema_version = "1"

        mock_body = MagicMock()
        mock_body.read.return_value = proto.SerializeToString()
        mock_s3_store.s3_client.meta.client.get_object.return_value = {
            "ETag": '"abc123"',
            "Body": mock_body,
        }

        result = mock_s3_store.get_registry_proto()
        assert result.registry_schema_version == "1"
        assert mock_s3_store._expected_etag == "abc123"

    def test_write_registry_passes_if_match(self, mock_s3_store):
        mock_s3_store._expected_etag = "abc123"
        mock_response = MagicMock()
        mock_response.e_tag = '"def456"'
        mock_bucket = MagicMock()
        mock_bucket.put_object.return_value = mock_response
        mock_s3_store.s3_client.Bucket.return_value = mock_bucket

        proto = RegistryProto()
        mock_s3_store._write_registry(proto)

        call_kwargs = mock_bucket.put_object.call_args
        assert call_kwargs.kwargs["IfMatch"] == "abc123"
        assert mock_s3_store._expected_etag == "def456"

    def test_write_registry_without_etag_skips_if_match(self, mock_s3_store):
        mock_s3_store._expected_etag = None
        mock_response = MagicMock()
        mock_response.e_tag = '"def456"'
        mock_bucket = MagicMock()
        mock_bucket.put_object.return_value = mock_response
        mock_s3_store.s3_client.Bucket.return_value = mock_bucket

        proto = RegistryProto()
        mock_s3_store._write_registry(proto)

        call_kwargs = mock_bucket.put_object.call_args
        assert "IfMatch" not in call_kwargs.kwargs

    def test_write_registry_raises_cas_conflict_on_412(self, mock_s3_store):
        from botocore.exceptions import ClientError

        mock_s3_store._expected_etag = "abc123"
        mock_bucket = MagicMock()
        mock_bucket.put_object.side_effect = ClientError(
            {"Error": {"Code": "412", "Message": "Precondition Failed"}},
            "PutObject",
        )
        mock_s3_store.s3_client.Bucket.return_value = mock_bucket

        proto = RegistryProto()
        with pytest.raises(RegistryCASConflictError):
            mock_s3_store._write_registry(proto)


class TestCASRetryDecorator:
    """Tests for the cas_retry decorator behavior."""

    def test_cas_retry_succeeds_on_first_attempt(self):
        call_count = 0

        class FakeRegistry:
            @cas_retry
            def apply_entity(self, entity, project, commit=True):
                nonlocal call_count
                call_count += 1
                return "success"

        reg = FakeRegistry()
        result = reg.apply_entity("entity", "project")
        assert result == "success"
        assert call_count == 1

    def test_cas_retry_retries_on_conflict(self):
        call_count = 0

        class FakeRegistry:
            @cas_retry
            def apply_entity(self, entity, project, commit=True):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise RegistryCASConflictError("conflict")
                return "success"

        reg = FakeRegistry()
        with patch("feast.infra.registry.registry.time.sleep"):
            result = reg.apply_entity("entity", "project")
        assert result == "success"
        assert call_count == 3

    def test_cas_retry_raises_after_max_retries(self):
        call_count = 0

        class FakeRegistry:
            @cas_retry
            def apply_entity(self, entity, project, commit=True):
                nonlocal call_count
                call_count += 1
                raise RegistryCASConflictError("always conflict")

        reg = FakeRegistry()
        with patch("feast.infra.registry.registry.time.sleep"):
            with pytest.raises(RegistryCASConflictError):
                reg.apply_entity("entity", "project")
        assert call_count == 5

    def test_cas_retry_does_not_retry_non_cas_errors(self):
        call_count = 0

        class FakeRegistry:
            @cas_retry
            def apply_entity(self, entity, project, commit=True):
                nonlocal call_count
                call_count += 1
                raise ValueError("not a CAS error")

        reg = FakeRegistry()
        with pytest.raises(ValueError):
            reg.apply_entity("entity", "project")
        assert call_count == 1


class TestRegistryWriteMethodsWrapped:
    """Verify that all registry write methods are wrapped with cas_retry."""

    WRITE_METHODS = [
        "update_infra",
        "apply_entity",
        "apply_data_source",
        "delete_data_source",
        "apply_feature_service",
        "apply_feature_view",
        "delete_label_view",
        "apply_materialization",
        "delete_feature_service",
        "delete_feature_view",
        "delete_entity",
        "apply_saved_dataset",
        "delete_saved_dataset",
        "apply_validation_reference",
        "delete_validation_reference",
        "apply_permission",
        "delete_permission",
        "apply_project",
        "delete_project",
    ]

    def test_all_write_methods_are_wrapped(self):
        for method_name in self.WRITE_METHODS:
            method = getattr(Registry, method_name, None)
            assert method is not None, f"Registry.{method_name} not found"
            assert hasattr(method, "__wrapped__"), (
                f"Registry.{method_name} is not wrapped with cas_retry"
            )


class TestGCSRegistryStoreCAS:
    """Tests for GCSRegistryStore generation capture and if_generation_match."""

    @pytest.fixture
    def mock_gcs_store(self):
        from feast.infra.registry.gcs import GCSRegistryStore

        store = GCSRegistryStore.__new__(GCSRegistryStore)
        store._bucket = "test-bucket"
        store._blob = "registry.db"
        store._uri = MagicMock()
        store._uri.geturl.return_value = "gs://test-bucket/registry.db"
        store._expected_generation = None
        store.gcs_client = MagicMock()
        return store

    def test_get_registry_proto_captures_generation(self, mock_gcs_store):
        proto = RegistryProto()
        proto.registry_schema_version = "1"

        mock_bucket = MagicMock()
        mock_gcs_store.gcs_client.get_bucket.return_value = mock_bucket

        mock_blob = MagicMock()
        mock_blob.generation = 12345
        mock_blob.exists.return_value = True
        mock_blob.download_to_file.side_effect = lambda f: f.write(
            proto.SerializeToString()
        )

        with patch("google.cloud.storage.Blob", return_value=mock_blob):
            result = mock_gcs_store.get_registry_proto()

        assert result.registry_schema_version == "1"
        assert mock_gcs_store._expected_generation == 12345

    def test_write_registry_passes_if_generation_match(self, mock_gcs_store):
        mock_gcs_store._expected_generation = 12345
        mock_bucket = MagicMock()
        mock_gcs_store.gcs_client.get_bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_blob.generation = 67890
        mock_bucket.blob.return_value = mock_blob

        proto = RegistryProto()
        mock_gcs_store._write_registry(proto)

        call_kwargs = mock_blob.upload_from_file.call_args
        assert call_kwargs.kwargs["if_generation_match"] == 12345


class TestAzBlobRegistryStoreCAS:
    """Tests for AzBlobRegistryStore ETag capture and if_match."""

    @pytest.fixture
    def mock_azure_store(self):
        from feast.infra.registry.contrib.azure.azure_registry_store import (
            AzBlobRegistryStore,
        )

        store = AzBlobRegistryStore.__new__(AzBlobRegistryStore)
        store._container = "test-container"
        store._path = "registry.db"
        store._uri = MagicMock()
        store._uri.geturl.return_value = (
            "https://test.blob.core.windows.net/test-container/registry.db"
        )
        store._expected_etag = None
        store.blob = MagicMock()
        return store

    def test_get_registry_proto_captures_etag(self, mock_azure_store):
        proto = RegistryProto()
        proto.registry_schema_version = "1"

        mock_azure_store.blob.exists.return_value = True
        mock_download = MagicMock()
        mock_download.properties.etag = '"abc123"'
        mock_download.readall.return_value = proto.SerializeToString()
        mock_azure_store.blob.download_blob.return_value = mock_download

        result = mock_azure_store.get_registry_proto()
        assert result.registry_schema_version == "1"
        assert mock_azure_store._expected_etag == '"abc123"'

    def test_write_registry_passes_if_match(self, mock_azure_store):
        mock_azure_store._expected_etag = '"abc123"'
        mock_upload = MagicMock()
        mock_upload.etag = '"def456"'
        mock_azure_store.blob.upload_blob.return_value = mock_upload

        proto = RegistryProto()
        mock_azure_store._write_registry(proto)

        call_kwargs = mock_azure_store.blob.upload_blob.call_args
        assert call_kwargs.kwargs["if_match"] == '"abc123"'
        assert mock_azure_store._expected_etag == '"def456"'

    def test_write_registry_without_etag_passes_none(self, mock_azure_store):
        mock_azure_store._expected_etag = None
        mock_upload = MagicMock()
        mock_upload.etag = '"def456"'
        mock_azure_store.blob.upload_blob.return_value = mock_upload

        proto = RegistryProto()
        mock_azure_store._write_registry(proto)

        call_kwargs = mock_azure_store.blob.upload_blob.call_args
        assert call_kwargs.kwargs["if_match"] is None

    def test_write_registry_raises_cas_conflict_on_resource_modified(
        self, mock_azure_store
    ):
        from feast.infra.registry.contrib.azure.azure_registry_store import (
            ResourceModifiedError,
        )

        mock_azure_store._expected_etag = '"abc123"'
        mock_azure_store.blob.upload_blob.side_effect = ResourceModifiedError(
            "The blob has been modified"
        )

        proto = RegistryProto()
        with pytest.raises(RegistryCASConflictError):
            mock_azure_store._write_registry(proto)
