"""Unit tests for AccessControlPipeline incremental ACL reconciliation.

Tests the reconciliation path that runs when DirSync uses BASIC flags (0x0)
instead of INCREMENTAL_VALUES. With BASIC flags, DirSync returns full member
lists as all ADDs with zero REMOVEs, so the pipeline must diff against the DB
to detect removed members.

Key methods under test:
- _reconcile_modified_groups: Diffs DB memberships against DirSync to find stale entries
- _apply_membership_changes: Applies ADDs/REMOVEs and triggers reconciliation for BASIC flags
- _process_incremental: Full incremental flow including deleted group handling
"""

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Set
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from airweave.platform.access_control.schemas import ACLChangeType, MembershipChange
from airweave.platform.sync.access_control_pipeline import AccessControlPipeline


# ---------------------------------------------------------------------------
# Helpers: lightweight fakes for SyncContext, SyncRuntime, DirSync results
# ---------------------------------------------------------------------------


@dataclass
class FakeSyncContext:
    """Minimal SyncContext for pipeline tests."""

    organization_id: object = field(default_factory=uuid4)
    source_connection_id: object = field(default_factory=uuid4)
    logger: object = field(default_factory=lambda: MagicMock())


@dataclass
class FakeCursor:
    """Minimal cursor supporting .data and .update()."""

    data: dict = field(default_factory=dict)

    def update(self, **kwargs):
        self.data.update(kwargs)


@dataclass
class FakeRuntime:
    """Minimal SyncRuntime."""

    cursor: FakeCursor = field(default_factory=FakeCursor)


@dataclass
class FakeDirSyncResult:
    """DirSync result returned by source.get_acl_changes()."""

    changes: list = field(default_factory=list)
    modified_group_ids: Set[str] = field(default_factory=set)
    deleted_group_ids: Set[str] = field(default_factory=set)
    incremental_values: bool = False
    cookie_b64: str = "new_cookie"


@dataclass
class FakeMembership:
    """Mimics ORM AccessControlMembership for get_memberships_by_groups results."""

    group_id: str
    member_id: str
    member_type: str


def _make_change(change_type, member_id, member_type, group_id, group_name=None):
    """Helper to build a MembershipChange."""
    return MembershipChange(
        change_type=change_type,
        member_id=member_id,
        member_type=member_type,
        group_id=group_id,
        group_name=group_name,
    )


def _make_pipeline():
    """Create an AccessControlPipeline with mocked internals."""
    return AccessControlPipeline(
        resolver=MagicMock(),
        dispatcher=MagicMock(),
        tracker=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Tests: _reconcile_modified_groups
# ---------------------------------------------------------------------------


class TestReconcileModifiedGroups:
    """Tests for the reconciliation logic that detects and removes stale memberships."""

    @pytest.mark.asyncio
    async def test_removes_stale_members_not_in_dirsync(self):
        """Members in DB but not in DirSync result should be deleted."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()

        # DB has 3 members in group-A: alice, bob, charlie
        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
            FakeMembership("group-A", "charlie@acme.com", "user"),
        ]

        # DirSync only reports alice and charlie — bob was removed
        dirsync_members = {
            "group-A": {("alice@acme.com", "user"), ("charlie@acme.com", "user")}
        }

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )
            mock_crud.access_control_membership.delete_by_key = AsyncMock()

            removes = await pipeline._reconcile_modified_groups(
                db,
                modified_group_ids={"group-A"},
                dirsync_members_by_group=dirsync_members,
                sync_context=ctx,
            )

        assert removes == 1
        mock_crud.access_control_membership.delete_by_key.assert_called_once_with(
            db,
            member_id="bob@acme.com",
            member_type="user",
            group_id="group-A",
            source_connection_id=ctx.source_connection_id,
            organization_id=ctx.organization_id,
        )

    @pytest.mark.asyncio
    async def test_no_removals_when_all_members_present(self):
        """No deletions when DirSync and DB agree on membership."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()

        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
        ]
        dirsync_members = {
            "group-A": {("alice@acme.com", "user"), ("bob@acme.com", "user")}
        }

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )
            mock_crud.access_control_membership.delete_by_key = AsyncMock()

            removes = await pipeline._reconcile_modified_groups(
                db,
                modified_group_ids={"group-A"},
                dirsync_members_by_group=dirsync_members,
                sync_context=ctx,
            )

        assert removes == 0
        mock_crud.access_control_membership.delete_by_key.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconciles_multiple_groups(self):
        """Reconciliation handles multiple groups, removing stale members from each."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()

        # Group-A: alice stays, bob removed
        # Group-B: charlie stays, dave removed
        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
            FakeMembership("group-B", "charlie@acme.com", "user"),
            FakeMembership("group-B", "dave@acme.com", "user"),
        ]
        dirsync_members = {
            "group-A": {("alice@acme.com", "user")},
            "group-B": {("charlie@acme.com", "user")},
        }

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )
            mock_crud.access_control_membership.delete_by_key = AsyncMock()

            removes = await pipeline._reconcile_modified_groups(
                db,
                modified_group_ids={"group-A", "group-B"},
                dirsync_members_by_group=dirsync_members,
                sync_context=ctx,
            )

        assert removes == 2
        assert mock_crud.access_control_membership.delete_by_key.call_count == 2

    @pytest.mark.asyncio
    async def test_empty_db_means_no_removals(self):
        """No stale removals when DB has no existing memberships for the groups."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=[]
            )
            mock_crud.access_control_membership.delete_by_key = AsyncMock()

            removes = await pipeline._reconcile_modified_groups(
                db,
                modified_group_ids={"group-A"},
                dirsync_members_by_group={"group-A": {("alice@acme.com", "user")}},
                sync_context=ctx,
            )

        assert removes == 0

    @pytest.mark.asyncio
    async def test_group_not_in_dirsync_results_removes_all_members(self):
        """If a modified group has no DirSync entries, all its DB members are stale."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()

        # Group-A is modified but DirSync returned no ADD entries for it
        # (e.g., all members removed from the group)
        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
        ]

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )
            mock_crud.access_control_membership.delete_by_key = AsyncMock()

            removes = await pipeline._reconcile_modified_groups(
                db,
                modified_group_ids={"group-A"},
                dirsync_members_by_group={},  # empty — no ADDs for group-A
                sync_context=ctx,
            )

        assert removes == 2
        assert mock_crud.access_control_membership.delete_by_key.call_count == 2


# ---------------------------------------------------------------------------
# Tests: _apply_membership_changes
# ---------------------------------------------------------------------------


class TestApplyMembershipChanges:
    """Tests for _apply_membership_changes with both INCREMENTAL and BASIC flags."""

    @pytest.mark.asyncio
    async def test_incremental_values_applies_adds_and_removes(self):
        """With incremental_values=True, ADDs are upserted and REMOVEs are deleted."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()
        source = SimpleNamespace(_short_name="sp2019v2")

        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
                _make_change(ACLChangeType.REMOVE, "bob@acme.com", "user", "group-A"),
                _make_change(ACLChangeType.ADD, "charlie@acme.com", "user", "group-B"),
            ],
            modified_group_ids={"group-A", "group-B"},
            incremental_values=True,
        )

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.delete_by_key = AsyncMock()
            # Should NOT call get_memberships_by_groups (no reconciliation)
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock()

            adds, removes = await pipeline._apply_membership_changes(
                db, result, source, ctx, uses_incremental_values=True
            )

        assert adds == 2
        assert removes == 1
        assert mock_crud.access_control_membership.upsert.call_count == 2
        assert mock_crud.access_control_membership.delete_by_key.call_count == 1
        # No reconciliation with incremental values
        mock_crud.access_control_membership.get_memberships_by_groups.assert_not_called()

    @pytest.mark.asyncio
    async def test_basic_flags_triggers_reconciliation(self):
        """With incremental_values=False, reconciliation is triggered after applying ADDs."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()
        source = SimpleNamespace(_short_name="sp2019v2")

        # BASIC flags: DirSync returns full member list as ADDs only
        # group-A: alice and charlie in DirSync, bob was removed
        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
                _make_change(ACLChangeType.ADD, "charlie@acme.com", "user", "group-A"),
            ],
            modified_group_ids={"group-A"},
            incremental_values=False,
        )

        # DB has alice, bob, charlie — bob should be removed by reconciliation
        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
            FakeMembership("group-A", "charlie@acme.com", "user"),
        ]

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.delete_by_key = AsyncMock()
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )

            adds, removes = await pipeline._apply_membership_changes(
                db, result, source, ctx, uses_incremental_values=False
            )

        assert adds == 2
        assert removes == 1  # bob removed by reconciliation
        # Reconciliation was called
        mock_crud.access_control_membership.get_memberships_by_groups.assert_called_once()

    @pytest.mark.asyncio
    async def test_basic_flags_no_reconciliation_when_no_modified_groups(self):
        """Even with BASIC flags, skip reconciliation if modified_group_ids is empty."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()
        source = SimpleNamespace(_short_name="sp2019v2")

        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
            ],
            modified_group_ids=set(),  # empty
            incremental_values=False,
        )

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.delete_by_key = AsyncMock()
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock()

            adds, removes = await pipeline._apply_membership_changes(
                db, result, source, ctx, uses_incremental_values=False
            )

        assert adds == 1
        assert removes == 0
        mock_crud.access_control_membership.get_memberships_by_groups.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_passes_correct_fields(self):
        """Verify upsert is called with all correct keyword arguments."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        db = MagicMock()
        source = SimpleNamespace(_short_name="sp2019v2")

        result = FakeDirSyncResult(
            changes=[
                _make_change(
                    ACLChangeType.ADD, "alice@acme.com", "user", "group-eng", "Engineering"
                ),
            ],
            modified_group_ids=set(),
            incremental_values=True,
        )

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud:
            mock_crud.access_control_membership.upsert = AsyncMock()

            await pipeline._apply_membership_changes(
                db, result, source, ctx, uses_incremental_values=True
            )

        mock_crud.access_control_membership.upsert.assert_called_once_with(
            db,
            member_id="alice@acme.com",
            member_type="user",
            group_id="group-eng",
            group_name="Engineering",
            organization_id=ctx.organization_id,
            source_connection_id=ctx.source_connection_id,
            source_name="sp2019v2",
        )


# ---------------------------------------------------------------------------
# Tests: _process_incremental (full flow)
# ---------------------------------------------------------------------------


class TestProcessIncremental:
    """Integration tests for the full _process_incremental flow with mocked DB."""

    @pytest.mark.asyncio
    async def test_deleted_groups_remove_all_memberships(self):
        """Deleted AD groups should have all their memberships removed via delete_by_group."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime(
            cursor=FakeCursor(data={"acl_dirsync_cookie": "old_cookie"})
        )
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(),
        )

        result = FakeDirSyncResult(
            changes=[],
            modified_group_ids=set(),
            deleted_group_ids={"deleted-group-1", "deleted-group-2"},
            incremental_values=True,
            cookie_b64="new_cookie_123",
        )
        source.get_acl_changes.return_value = result

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud, \
             patch("airweave.platform.sync.access_control_pipeline.get_db_context") as mock_db_ctx:
            mock_db = MagicMock()
            mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_crud.access_control_membership.delete_by_group = AsyncMock(return_value=5)

            total = await pipeline._process_incremental(source, ctx, runtime)

        assert total == 10  # 5 members x 2 groups
        assert mock_crud.access_control_membership.delete_by_group.call_count == 2

    @pytest.mark.asyncio
    async def test_no_changes_returns_zero_and_updates_cookie(self):
        """When DirSync reports zero changes, return 0 but still advance the cookie."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime(
            cursor=FakeCursor(data={"acl_dirsync_cookie": "old_cookie"})
        )
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(),
        )

        result = FakeDirSyncResult(
            changes=[],
            modified_group_ids=set(),
            deleted_group_ids=set(),
            cookie_b64="advanced_cookie",
        )
        source.get_acl_changes.return_value = result

        total = await pipeline._process_incremental(source, ctx, runtime)

        assert total == 0
        assert runtime.cursor.data["acl_dirsync_cookie"] == "advanced_cookie"

    @pytest.mark.asyncio
    async def test_fallback_to_full_sync_on_exception(self):
        """If get_acl_changes raises, fall back to full sync."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime()
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(side_effect=Exception("LDAP timeout")),
        )

        # Mock _process_full to track fallback
        pipeline._process_full = AsyncMock(return_value=42)

        total = await pipeline._process_incremental(source, ctx, runtime)

        assert total == 42
        pipeline._process_full.assert_called_once_with(source, ctx, runtime)

    @pytest.mark.asyncio
    async def test_full_flow_with_adds_removes_and_deletes(self):
        """End-to-end: ADDs + REMOVEs + deleted groups in one incremental sync."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime(
            cursor=FakeCursor(data={"acl_dirsync_cookie": "old"})
        )
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(),
        )

        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
                _make_change(ACLChangeType.ADD, "bob@acme.com", "user", "group-A"),
                _make_change(ACLChangeType.REMOVE, "charlie@acme.com", "user", "group-B"),
            ],
            modified_group_ids={"group-A", "group-B"},
            deleted_group_ids={"dead-group"},
            incremental_values=True,
            cookie_b64="final_cookie",
        )
        source.get_acl_changes.return_value = result

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud, \
             patch("airweave.platform.sync.access_control_pipeline.get_db_context") as mock_db_ctx:
            mock_db = MagicMock()
            mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.delete_by_key = AsyncMock()
            mock_crud.access_control_membership.delete_by_group = AsyncMock(return_value=3)

            total = await pipeline._process_incremental(source, ctx, runtime)

        # 2 adds + 1 remove + 3 group-deletion removals = 6
        assert total == 6
        assert mock_crud.access_control_membership.upsert.call_count == 2
        assert mock_crud.access_control_membership.delete_by_key.call_count == 1
        assert mock_crud.access_control_membership.delete_by_group.call_count == 1
        # Cookie should be updated
        assert runtime.cursor.data["acl_dirsync_cookie"] == "final_cookie"

    @pytest.mark.asyncio
    async def test_incremental_values_flag_skips_reconciliation(self):
        """With incremental_values=True, no reconciliation happens even with modified groups."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime(
            cursor=FakeCursor(data={"acl_dirsync_cookie": "old"})
        )
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(),
        )

        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
            ],
            modified_group_ids={"group-A"},
            deleted_group_ids=set(),
            incremental_values=True,
        )
        source.get_acl_changes.return_value = result

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud, \
             patch("airweave.platform.sync.access_control_pipeline.get_db_context") as mock_db_ctx:
            mock_db = MagicMock()
            mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock()

            total = await pipeline._process_incremental(source, ctx, runtime)

        assert total == 1
        # Reconciliation was NOT triggered
        mock_crud.access_control_membership.get_memberships_by_groups.assert_not_called()

    @pytest.mark.asyncio
    async def test_basic_flags_triggers_reconciliation_in_full_flow(self):
        """With incremental_values=False, _process_incremental reconciles against DB."""
        pipeline = _make_pipeline()
        ctx = FakeSyncContext()
        runtime = FakeRuntime(
            cursor=FakeCursor(data={"acl_dirsync_cookie": "old"})
        )
        source = SimpleNamespace(
            _short_name="sp2019v2",
            get_acl_changes=AsyncMock(),
        )

        # BASIC flags: full member list as ADDs only
        result = FakeDirSyncResult(
            changes=[
                _make_change(ACLChangeType.ADD, "alice@acme.com", "user", "group-A"),
            ],
            modified_group_ids={"group-A"},
            deleted_group_ids=set(),
            incremental_values=False,
        )
        source.get_acl_changes.return_value = result

        # DB has alice + bob — bob is stale
        existing = [
            FakeMembership("group-A", "alice@acme.com", "user"),
            FakeMembership("group-A", "bob@acme.com", "user"),
        ]

        with patch("airweave.platform.sync.access_control_pipeline.crud") as mock_crud, \
             patch("airweave.platform.sync.access_control_pipeline.get_db_context") as mock_db_ctx:
            mock_db = MagicMock()
            mock_db_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_db)
            mock_db_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_crud.access_control_membership.upsert = AsyncMock()
            mock_crud.access_control_membership.delete_by_key = AsyncMock()
            mock_crud.access_control_membership.get_memberships_by_groups = AsyncMock(
                return_value=existing
            )

            total = await pipeline._process_incremental(source, ctx, runtime)

        # 1 add + 1 reconciliation remove = 2
        assert total == 2
        mock_crud.access_control_membership.get_memberships_by_groups.assert_called_once()
