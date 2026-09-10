from unittest.mock import MagicMock, patch

import pytest

from feast.errors import RegistryCASConflictError
from feast.infra.registry.cas_s3_registry_store import CASS3RegistryStore
from feast.infra.registry.registry import Registry, cas_retry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class TestCASS3RegistryStore:
    """Tests for CASS3RegistryStore ETag capture and If-Match behavior."""

    @pytest.fixture
    def mock_s3_store(self):
        with patch(
            "feast.infra.registry.cas_s3_registry_store.CASS3RegistryStore.__init__",
            return_value=None,
        ):
            store = CASS3RegistryStore.__new__(CASS3RegistryStore)
            store._bucket = "test-bucket"
            store._key = "registry.db"
            store._uri = MagicMock()
            store._uri.geturl.return_value = "s3://test-bucket/registry.db"
            store._boto_extra_args = {}
            store._expected_etag = None
            store.s3_client = MagicMock()
            yield store

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
