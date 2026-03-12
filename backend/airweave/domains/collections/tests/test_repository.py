"""Unit tests for CollectionRepository — status computation and federated lookup."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from airweave.core.shared_models import CollectionStatus
from airweave.domains.collections.repository import CollectionRepository
from airweave.domains.source_connections.fakes.repository import FakeSourceConnectionRepository

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_registry(federated_map: dict[str, bool] | None = None):
    """Build a fake source registry that returns federated_search flags."""
    federated_map = federated_map or {}

    def _get(short_name: str):
        if short_name not in federated_map:
            raise KeyError(short_name)
        return SimpleNamespace(federated_search=federated_map[short_name])

    reg = MagicMock()
    reg.get = MagicMock(side_effect=_get)
    return reg


def _repo(
    federated_map: dict[str, bool] | None = None,
    sc_repo: FakeSourceConnectionRepository | None = None,
) -> CollectionRepository:
    return CollectionRepository(
        source_registry=_source_registry(federated_map),
        sc_repo=sc_repo or FakeSourceConnectionRepository(),
    )


# ---------------------------------------------------------------------------
# _federated_lookup
# ---------------------------------------------------------------------------


class TestFederatedLookup:
    def test_returns_true_when_federated(self):
        repo = _repo({"slack": True})
        assert repo._federated_lookup("slack") is True

    def test_returns_false_when_not_federated(self):
        repo = _repo({"github": False})
        assert repo._federated_lookup("github") is False

    def test_returns_false_for_unknown_source(self):
        repo = _repo({})
        assert repo._federated_lookup("nonexistent") is False


# ---------------------------------------------------------------------------
# _compute_collection_status
# ---------------------------------------------------------------------------


class TestComputeCollectionStatus:
    def test_empty_connections_returns_needs_source(self):
        assert CollectionRepository._compute_collection_status([]) == CollectionStatus.NEEDS_SOURCE

    def test_no_authenticated_connections_returns_needs_source(self):
        conns = [{"is_authenticated": False, "federated_search": False, "last_job": None}]
        result = CollectionRepository._compute_collection_status(conns)
        assert result == CollectionStatus.NEEDS_SOURCE

    def test_federated_authenticated_returns_active(self):
        conns = [{"is_authenticated": True, "federated_search": True, "last_job": None}]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ACTIVE

    def test_completed_sync_returns_active(self):
        conns = [
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "completed"},
            }
        ]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ACTIVE

    def test_running_sync_returns_active(self):
        conns = [
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "running"},
            }
        ]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ACTIVE

    def test_cancelling_sync_returns_active(self):
        conns = [
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "cancelling"},
            }
        ]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ACTIVE

    def test_all_failed_returns_error(self):
        conns = [
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "failed"},
            },
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "failed"},
            },
        ]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ERROR

    def test_mixed_failed_and_working_returns_active(self):
        conns = [
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "failed"},
            },
            {
                "is_authenticated": True,
                "federated_search": False,
                "last_job": {"status": "completed"},
            },
        ]
        assert CollectionRepository._compute_collection_status(conns) == CollectionStatus.ACTIVE

    def test_no_jobs_yet_returns_needs_source(self):
        conns = [{"is_authenticated": True, "federated_search": False, "last_job": None}]
        result = CollectionRepository._compute_collection_status(conns)
        assert result == CollectionStatus.NEEDS_SOURCE

    def test_last_job_empty_dict_returns_needs_source(self):
        conns = [{"is_authenticated": True, "federated_search": False, "last_job": {}}]
        result = CollectionRepository._compute_collection_status(conns)
        assert result == CollectionStatus.NEEDS_SOURCE


# ---------------------------------------------------------------------------
# _attach_ephemeral_status
# ---------------------------------------------------------------------------


def _sc(
    id=None, readable_collection_id="col", short_name="github", is_authenticated=True
):
    """Build a fake SourceConnection-like object for seeding."""
    obj = MagicMock()
    obj.id = id or uuid4()
    obj.readable_collection_id = readable_collection_id
    obj.short_name = short_name
    obj.is_authenticated = is_authenticated
    return obj


@pytest.mark.asyncio
class TestAttachEphemeralStatus:
    async def test_empty_collections_returns_empty(self):
        repo = _repo()
        result = await repo._attach_ephemeral_status(MagicMock(), [], MagicMock())
        assert result == []

    async def test_no_source_connections_sets_needs_source(self):
        repo = _repo()

        col = MagicMock()
        col.readable_id = "test-col"

        ctx = MagicMock()
        ctx.organization.id = uuid4()

        result = await repo._attach_ephemeral_status(MagicMock(), [col], ctx)
        assert len(result) == 1
        assert result[0].status == CollectionStatus.NEEDS_SOURCE  # type: ignore[attr-defined]

    async def test_with_connections_computes_status(self):
        sc = _sc(readable_collection_id="my-col", short_name="github")

        sc_repo = FakeSourceConnectionRepository()
        sc_repo.seed(sc.id, sc)
        sc_repo.seed_last_jobs({sc.id: {"status": "completed"}})
        repo = _repo({"github": False}, sc_repo=sc_repo)

        col = MagicMock()
        col.readable_id = "my-col"

        ctx = MagicMock()
        ctx.organization.id = uuid4()

        result = await repo._attach_ephemeral_status(MagicMock(), [col], ctx)

        assert len(result) == 1
        assert result[0].status == CollectionStatus.ACTIVE  # type: ignore[attr-defined]

    async def test_federated_source_sets_active(self):
        sc = _sc(readable_collection_id="slack-col", short_name="slack")

        sc_repo = FakeSourceConnectionRepository()
        sc_repo.seed(sc.id, sc)
        repo = _repo({"slack": True}, sc_repo=sc_repo)

        col = MagicMock()
        col.readable_id = "slack-col"

        ctx = MagicMock()
        ctx.organization.id = uuid4()

        result = await repo._attach_ephemeral_status(MagicMock(), [col], ctx)

        assert result[0].status == CollectionStatus.ACTIVE  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Delegating methods (get, get_by_readable_id, get_multi)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDelegatingMethods:
    async def test_get_returns_none_when_not_found(self):
        repo = _repo()

        with patch("airweave.domains.collections.repository.crud") as mock_crud:
            mock_crud.collection.get = AsyncMock(return_value=None)

            result = await repo.get(MagicMock(), id=uuid4(), ctx=MagicMock())

        assert result is None

    async def test_get_attaches_status(self):
        repo = _repo()
        col = MagicMock()
        col.readable_id = "test"

        with patch("airweave.domains.collections.repository.crud") as mock_crud:
            mock_crud.collection.get = AsyncMock(return_value=col)

            attach = patch.object(repo, "_attach_ephemeral_status", new_callable=AsyncMock)
            with attach as mock_attach:
                mock_attach.return_value = [col]

                result = await repo.get(MagicMock(), id=uuid4(), ctx=MagicMock())

        assert result is col
        mock_attach.assert_awaited_once()

    async def test_get_by_readable_id_returns_none_on_not_found(self):
        repo = _repo()

        with patch("airweave.domains.collections.repository.crud") as mock_crud:
            from airweave.core.exceptions import NotFoundException

            mock_crud.collection.get_by_readable_id = AsyncMock(
                side_effect=NotFoundException("not found")
            )

            result = await repo.get_by_readable_id(MagicMock(), readable_id="nope", ctx=MagicMock())

        assert result is None

    async def test_get_by_readable_id_attaches_status(self):
        repo = _repo()
        col = MagicMock()
        col.readable_id = "found"

        with patch("airweave.domains.collections.repository.crud") as mock_crud:
            mock_crud.collection.get_by_readable_id = AsyncMock(return_value=col)

            attach = patch.object(repo, "_attach_ephemeral_status", new_callable=AsyncMock)
            with attach as mock_attach:
                mock_attach.return_value = [col]

                result = await repo.get_by_readable_id(
                    MagicMock(), readable_id="found", ctx=MagicMock()
                )

        assert result is col
        mock_attach.assert_awaited_once()

    async def test_get_multi_attaches_status(self):
        repo = _repo()
        cols = [MagicMock(), MagicMock()]

        with patch("airweave.domains.collections.repository.crud") as mock_crud:
            mock_crud.collection.get_multi = AsyncMock(return_value=cols)

            attach = patch.object(repo, "_attach_ephemeral_status", new_callable=AsyncMock)
            with attach as mock_attach:
                mock_attach.return_value = cols

                result = await repo.get_multi(MagicMock(), ctx=MagicMock())

        assert result == cols
        mock_attach.assert_awaited_once()
