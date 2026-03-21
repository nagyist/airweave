"""Tests for compute_status with NEEDS_REAUTH derivation."""

from types import SimpleNamespace

from airweave.core.shared_models import SourceConnectionStatus, SyncJobStatus
from airweave.schemas.source_connection import compute_status


def test_needs_reauth_when_failed_with_error_category():
    """FAILED job + error_category → NEEDS_REAUTH."""
    source_conn = SimpleNamespace(
        is_authenticated=True,
        is_active=True,
        _error_category="oauth_credentials_expired",
    )
    status = compute_status(source_conn, SyncJobStatus.FAILED)
    assert status == SourceConnectionStatus.NEEDS_REAUTH


def test_error_when_failed_without_error_category():
    """FAILED job without error_category → ERROR."""
    source_conn = SimpleNamespace(
        is_authenticated=True,
        is_active=True,
        _error_category=None,
    )
    status = compute_status(source_conn, SyncJobStatus.FAILED)
    assert status == SourceConnectionStatus.ERROR


def test_active_when_completed():
    """COMPLETED job → ACTIVE."""
    source_conn = SimpleNamespace(
        is_authenticated=True,
        is_active=True,
    )
    status = compute_status(source_conn, SyncJobStatus.COMPLETED)
    assert status == SourceConnectionStatus.ACTIVE


def test_syncing_when_running():
    """RUNNING job → SYNCING."""
    source_conn = SimpleNamespace(
        is_authenticated=True,
        is_active=True,
    )
    status = compute_status(source_conn, SyncJobStatus.RUNNING)
    assert status == SourceConnectionStatus.SYNCING


def test_pending_auth_when_not_authenticated():
    """Not authenticated → PENDING_AUTH regardless of job status."""
    source_conn = SimpleNamespace(is_authenticated=False)
    status = compute_status(source_conn, SyncJobStatus.FAILED)
    assert status == SourceConnectionStatus.PENDING_AUTH
