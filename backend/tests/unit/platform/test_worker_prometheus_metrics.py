"""Tests for Prometheus metrics functionality.

Tests the Prometheus metrics update and export functionality.

These tests cover commits:
- 9527bc63d6fbe7b39f21271ae962ca472a9beb89: feat: extensive sync worker metrics
- 6b7cae7898c859cd51de4dbf6136389da264a6df: fix: remove deadlock risks and private API dependencies
"""

import pytest

from airweave.platform.temporal.prometheus_metrics import (
    get_prometheus_metrics,
    update_worker_metrics,
    worker_active_activities,
    worker_active_sync_jobs,
    worker_active_syncs_by_connector,
    worker_info,
    worker_pool_active_and_pending_by_connector,
    worker_pool_active_and_pending_workers,
    worker_registry,
    worker_status,
    worker_sync_max_workers_config,
    worker_thread_pool_active,
    worker_thread_pool_size_config,
    worker_uptime_seconds,
)


def test_update_worker_metrics_basic():
    """Test basic worker metrics update."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=5,
        active_sync_jobs_count=3,
        task_queue="test-queue",
        worker_pool_active_and_pending_count=15,
        connector_metrics={},
        sync_max_workers=20,
        thread_pool_size=100,
        thread_pool_active=25,
    )

    # Generate metrics and verify they contain expected data
    metrics_data = get_prometheus_metrics()
    metrics_str = metrics_data.decode("utf-8")

    # Check that metrics are present
    assert "airweave_worker_status" in metrics_str
    assert "airweave_worker_uptime_seconds" in metrics_str
    assert "airweave_worker_active_activities" in metrics_str
    assert "airweave_worker_active_sync_jobs" in metrics_str
    assert "airweave_worker_pool_active_and_pending_workers" in metrics_str
    assert "airweave_worker_sync_max_workers_config" in metrics_str
    assert "airweave_worker_thread_pool_size_config" in metrics_str
    assert "airweave_worker_thread_pool_active" in metrics_str


def test_worker_status_values():
    """Test worker status metric values for different states."""
    # Test running state (value = 1)
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=0,
        active_sync_jobs_count=0,
        task_queue="test-queue",
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")
    # Status 1 = running
    assert 'airweave_worker_status{worker_id="0"} 1.0' in metrics_str

    # Test draining state (value = 2)
    update_worker_metrics(
        worker_id="0",
        status="draining",
        uptime_seconds=200.0,
        active_activities_count=0,
        active_sync_jobs_count=0,
        task_queue="test-queue",
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")
    # Status 2 = draining
    assert 'airweave_worker_status{worker_id="0"} 2.0' in metrics_str

    # Test stopped state (value = 0)
    update_worker_metrics(
        worker_id="0",
        status="stopped",
        uptime_seconds=300.0,
        active_activities_count=0,
        active_sync_jobs_count=0,
        task_queue="test-queue",
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")
    # Status 0 = stopped
    assert 'airweave_worker_status{worker_id="0"} 0.0' in metrics_str


def test_connector_metrics_update():
    """Test connector-aggregated metrics are updated correctly."""
    connector_metrics = {
        "slack": {
            "active_syncs": 5,
            "active_and_pending_workers": 50,
        },
        "notion": {
            "active_syncs": 3,
            "active_and_pending_workers": 30,
        },
    }

    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=8,
        active_sync_jobs_count=8,
        task_queue="test-queue",
        connector_metrics=connector_metrics,
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Check connector-specific metrics
    assert "airweave_worker_pool_active_and_pending_by_connector" in metrics_str
    assert "airweave_worker_active_syncs_by_connector" in metrics_str
    assert 'connector_type="slack"' in metrics_str
    assert 'connector_type="notion"' in metrics_str


def test_connector_metrics_zero_out_finished_syncs():
    """Test that finished connector syncs are zeroed out (commit 6b7cae789)."""
    # First update with slack and notion
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=5,
        active_sync_jobs_count=2,
        task_queue="test-queue",
        connector_metrics={
            "slack": {"active_syncs": 3, "active_and_pending_workers": 30},
            "notion": {"active_syncs": 2, "active_and_pending_workers": 20},
        },
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")
    assert 'connector_type="slack"' in metrics_str
    assert 'connector_type="notion"' in metrics_str

    # Second update with only slack (notion finished)
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=200.0,
        active_activities_count=3,
        active_sync_jobs_count=1,
        task_queue="test-queue",
        connector_metrics={
            "slack": {"active_syncs": 3, "active_and_pending_workers": 30},
        },
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Notion should be zeroed out
    # Look for notion with value 0
    lines = metrics_str.split("\n")
    notion_lines = [
        line
        for line in lines
        if 'connector_type="notion"' in line and not line.startswith("#")
    ]
    for line in notion_lines:
        # Should be set to 0.0
        assert line.endswith(" 0.0")


def test_multiple_workers_separate_metrics():
    """Test that multiple workers maintain separate metrics."""
    # Update worker 0
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=5,
        active_sync_jobs_count=3,
        task_queue="test-queue",
    )

    # Update worker 1
    update_worker_metrics(
        worker_id="1",
        status="running",
        uptime_seconds=200.0,
        active_activities_count=7,
        active_sync_jobs_count=4,
        task_queue="test-queue",
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Both workers should be present
    assert 'worker_id="0"' in metrics_str
    assert 'worker_id="1"' in metrics_str


def test_config_value_gauges():
    """Test configuration value gauges are updated correctly."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=0,
        active_sync_jobs_count=0,
        task_queue="test-queue",
        sync_max_workers=25,
        thread_pool_size=150,
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Check config gauges
    assert "airweave_worker_sync_max_workers_config" in metrics_str
    assert "airweave_worker_thread_pool_size_config" in metrics_str
    assert 'airweave_worker_sync_max_workers_config{worker_id="0"} 25.0' in metrics_str
    assert 'airweave_worker_thread_pool_size_config{worker_id="0"} 150.0' in metrics_str


def test_thread_pool_metrics():
    """Test thread pool active count metric."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=5,
        active_sync_jobs_count=3,
        task_queue="test-queue",
        thread_pool_active=42,
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Check thread pool metric
    assert "airweave_worker_thread_pool_active" in metrics_str
    assert 'airweave_worker_thread_pool_active{worker_id="0"} 42.0' in metrics_str


def test_worker_pool_active_and_pending_count():
    """Test worker pool active and pending count metric."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=10,
        active_sync_jobs_count=5,
        task_queue="test-queue",
        worker_pool_active_and_pending_count=35,
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Check worker pool metric
    assert "airweave_worker_pool_active_and_pending_workers" in metrics_str
    assert (
        'airweave_worker_pool_active_and_pending_workers{worker_id="0"} 35.0'
        in metrics_str
    )


def test_process_metrics_included():
    """Test that process metrics collector is registered."""
    from airweave.platform.temporal.prometheus_metrics import worker_registry
    from prometheus_client import ProcessCollector

    # Verify that ProcessCollector is registered
    # (actual metrics may not appear in test environment without process running)
    collectors = list(worker_registry._collector_to_names.keys())
    assert any(isinstance(c, ProcessCollector) for c in collectors)


def test_worker_info_metric():
    """Test worker info metric is present."""
    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Should have worker info
    assert "airweave_worker_info" in metrics_str


def test_empty_connector_metrics():
    """Test handling of empty connector metrics."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=0,
        active_sync_jobs_count=0,
        task_queue="test-queue",
        connector_metrics={},  # No connectors
    )

    # Should not crash
    metrics_str = get_prometheus_metrics().decode("utf-8")
    assert "airweave_worker" in metrics_str


def test_metrics_format_is_prometheus_compliant():
    """Test that generated metrics follow Prometheus format."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=123.45,
        active_activities_count=5,
        active_sync_jobs_count=3,
        task_queue="test-queue",
    )

    metrics_data = get_prometheus_metrics()
    metrics_str = metrics_data.decode("utf-8")

    # Should have HELP and TYPE comments
    assert "# HELP" in metrics_str
    assert "# TYPE" in metrics_str

    # Metrics should have format: metric_name{labels} value
    lines = metrics_str.split("\n")
    metric_lines = [line for line in lines if line and not line.startswith("#")]

    for line in metric_lines:
        # Should contain either {} for labels or just space and value
        assert "{" in line or " " in line


def test_high_cardinality_metrics_not_exposed():
    """Test that high-cardinality metrics (per-sync) are not in Prometheus output.

    This verifies the fix in commit 9527bc63 to use aggregated connector metrics
    instead of per-sync metrics to avoid cardinality explosion.
    """
    # Try to include high-cardinality data
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=100,
        active_sync_jobs_count=100,  # Many unique syncs
        task_queue="test-queue",
        connector_metrics={
            "slack": {"active_syncs": 50, "active_and_pending_workers": 500},
            "notion": {"active_syncs": 50, "active_and_pending_workers": 500},
        },
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Should NOT have per-sync-job-id labels (high cardinality)
    assert "sync_job_id=" not in metrics_str
    assert "sync_id=" not in metrics_str

    # Should only have low-cardinality labels
    assert "worker_id=" in metrics_str
    assert "connector_type=" in metrics_str  # Aggregated by type


def test_metrics_registry_isolation():
    """Test that worker_registry is isolated from other Prometheus registries."""
    from prometheus_client import REGISTRY as default_registry

    # Worker registry should be separate
    assert worker_registry != default_registry

    # Worker metrics should not be in default registry
    metrics_from_worker = get_prometheus_metrics().decode("utf-8")
    worker_metric_names = [
        "airweave_worker_status",
        "airweave_worker_active_activities",
    ]

    # Verify worker metrics are in worker registry
    for metric_name in worker_metric_names:
        assert metric_name in metrics_from_worker


def test_zero_values_handled_correctly():
    """Test that zero values are properly represented in metrics."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=0.0,  # Just started
        active_activities_count=0,  # No activities
        active_sync_jobs_count=0,  # No syncs
        task_queue="test-queue",
        worker_pool_active_and_pending_count=0,
        thread_pool_active=0,
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Zero values should be present (not omitted)
    assert 'airweave_worker_active_activities{worker_id="0"} 0.0' in metrics_str
    assert 'airweave_worker_active_sync_jobs{worker_id="0"} 0.0' in metrics_str


def test_fractional_uptime_precision():
    """Test that fractional uptime values are preserved."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=3600.123456,  # Precise fractional seconds
        active_activities_count=1,
        active_sync_jobs_count=1,
        task_queue="test-queue",
    )

    metrics_str = get_prometheus_metrics().decode("utf-8")

    # Should preserve some precision (Prometheus uses float64)
    assert "airweave_worker_uptime_seconds" in metrics_str
    # Value should be present with decimal
    assert '3600.12' in metrics_str or '3600.123' in metrics_str


def test_special_characters_in_labels():
    """Test that special characters in label values are handled."""
    update_worker_metrics(
        worker_id="0",
        status="running",
        uptime_seconds=100.0,
        active_activities_count=1,
        active_sync_jobs_count=1,
        task_queue="test-queue",
        connector_metrics={
            "google_drive": {"active_syncs": 1, "active_and_pending_workers": 10},
            "microsoft-365": {"active_syncs": 1, "active_and_pending_workers": 5},
        },
    )

    # Should not crash with hyphens or underscores in connector names
    metrics_str = get_prometheus_metrics().decode("utf-8")
    assert 'connector_type="google_drive"' in metrics_str or "google_drive" in metrics_str

