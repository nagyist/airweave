"""Base source class — v2 contract with constructor DI and explicit method params."""

from __future__ import annotations

import base64
import json
import re
import time
from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Optional,
    Union,
)

if TYPE_CHECKING:
    from airweave.platform.access_control.schemas import MembershipTuple

import httpx
from pydantic import BaseModel

from airweave.core.logging import ContextualLogger
from airweave.core.shared_models import RateLimitLevel
from airweave.domains.browse_tree.types import NodeSelectionData
from airweave.domains.sources.token_providers.protocol import SourceAuthProvider
from airweave.domains.storage.file_service import FileService
from airweave.domains.syncs.cursors.cursor import SyncCursor
from airweave.platform.entities._base import BaseEntity
from airweave.platform.http_client.airweave_client import AirweaveHttpClient
from airweave.schemas.source_connection import AuthenticationMethod, OAuthType


class BaseSource:
    """Base class for all sources.

    Construction-time deps (auth, logger, http_client) are injected via
    ``create()`` and stored on self. Operation-time deps (cursor, files,
    node_selections) are passed as params to ``generate_entities()``.
    """

    # Identity (set by @source decorator — required)
    is_source: ClassVar[bool] = False
    source_name: ClassVar[str] = ""
    short_name: ClassVar[str] = ""

    # Auth (set by @source decorator)
    auth_methods: ClassVar[list[AuthenticationMethod]] = []
    oauth_type: ClassVar[Optional[OAuthType]] = None
    requires_byoc: ClassVar[bool] = False
    auth_config_class: ClassVar[Optional[type[BaseModel]]] = None
    config_class: ClassVar[Optional[type[BaseModel]]] = None

    # Capabilities (set by @source decorator)
    supports_continuous: ClassVar[bool] = False
    federated_search: ClassVar[bool] = False
    supports_temporal_relevance: ClassVar[bool] = True
    supports_access_control: ClassVar[bool] = False
    supports_browse_tree: ClassVar[bool] = False
    cursor_class: ClassVar[Optional[type]] = None
    rate_limit_level: ClassVar[Optional[RateLimitLevel]] = None

    # Metadata (set by @source decorator)
    labels: ClassVar[list[str]] = []
    feature_flag: ClassVar[Optional[str]] = None
    internal: ClassVar[bool] = False

    def __init__(
        self,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
    ) -> None:
        """Initialize with injected dependencies.

        Args:
            auth: Auth provider (TokenProviderProtocol or DirectCredentialProvider).
            logger: Contextual logger with sync/search metadata.
            http_client: Pre-built AirweaveHttpClient with rate limiting.
        """
        self._auth = auth
        self._logger = logger
        self._http_client = http_client

    # ------------------------------------------------------------------
    # Properties — construction-time deps
    # ------------------------------------------------------------------

    @property
    def auth(self) -> SourceAuthProvider:
        """The auth provider for this source."""
        return self._auth

    @property
    def logger(self) -> ContextualLogger:
        """Contextual logger with sync/search metadata."""
        return self._logger

    @property
    def http_client(self) -> AirweaveHttpClient:
        """Pre-built HTTP client with rate limiting."""
        return self._http_client

    # ------------------------------------------------------------------
    # Auth convenience methods
    # ------------------------------------------------------------------

    async def get_access_token(self) -> str:
        """Get a valid access token via the auth provider.

        Delegates to ``self._auth.get_token()`` for TokenProviderProtocol
        implementations.
        """
        if hasattr(self._auth, "get_token"):
            return await self._auth.get_token()
        raise RuntimeError(
            f"{self.__class__.__name__}: auth provider {type(self._auth).__name__} "
            f"does not support get_token(). Use DirectCredentialProvider.credentials instead."
        )

    async def refresh_on_unauthorized(self) -> str:
        """Force-refresh the token after a 401 error."""
        if not hasattr(self._auth, "force_refresh"):
            from airweave.domains.sources.exceptions import SourceAuthError

            raise SourceAuthError(
                f"{self.__class__.__name__}: auth provider does not support refresh",
                source_short_name=self.short_name,
                status_code=401,
                token_provider_kind=self.auth.provider_kind,
            )
        return await self._auth.force_refresh()

    async def get_token_for_resource(self, resource_scope: str) -> Optional[str]:
        """Get a token for a different resource scope (SharePoint Online only)."""
        if hasattr(self._auth, "get_token_for_resource"):
            return await self._auth.get_token_for_resource(resource_scope)
        return None

    # ------------------------------------------------------------------
    # Class metadata methods
    # ------------------------------------------------------------------

    @classmethod
    def is_internal(cls) -> bool:
        """Check if this is an internal/test source."""
        return cls.internal

    @classmethod
    def supports_auth_method(cls, method: AuthenticationMethod) -> bool:
        """Check if source supports a given authentication method."""
        methods = cls.get_supported_auth_methods()
        return method in methods

    @classmethod
    def get_supported_auth_methods(cls) -> list[AuthenticationMethod]:
        """Get all supported authentication methods."""
        methods = list(cls.auth_methods)
        if (
            AuthenticationMethod.OAUTH_BROWSER in methods
            and AuthenticationMethod.OAUTH_BYOC not in methods
        ):
            methods.append(AuthenticationMethod.OAUTH_BYOC)
        return methods

    @classmethod
    def get_oauth_type(cls) -> Optional[OAuthType]:
        """Get OAuth token type if this is an OAuth source."""
        return cls.oauth_type

    @classmethod
    def is_oauth_source(cls) -> bool:
        """Check if this is an OAuth-based source."""
        return AuthenticationMethod.OAUTH_BROWSER in cls.auth_methods

    @classmethod
    def requires_refresh_token(cls) -> bool:
        """Check if source requires refresh token."""
        return cls.oauth_type in [OAuthType.WITH_REFRESH, OAuthType.WITH_ROTATING_REFRESH]

    @classmethod
    def does_require_byoc(cls) -> bool:
        """Check if source requires user to bring their own OAuth client credentials."""
        return cls.requires_byoc

    # ------------------------------------------------------------------
    # Abstract methods — the source contract
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    async def create(
        cls,
        *,
        auth: SourceAuthProvider,
        logger: ContextualLogger,
        http_client: AirweaveHttpClient,
        config: BaseModel,
    ) -> BaseSource:
        """Create a new source instance.

        Auth is fully handled by the ``auth`` provider — sources never
        touch raw credentials. Source-specific runtime configuration
        (repo name, site URL, etc.) comes from ``config`` as a typed
        Pydantic model (the source's ``config_class``).

        Args:
            auth: Auth provider — ``TokenProviderProtocol`` or ``DirectCredentialProvider``.
            logger: Contextual logger with sync/search metadata.
            http_client: Pre-built AirweaveHttpClient with rate limiting.
            config: Typed config instance (source's config_class, parsed by lifecycle).
        """
        pass

    @abstractmethod
    async def generate_entities(
        self,
        *,
        cursor: SyncCursor | None = None,
        files: FileService | None = None,
        node_selections: list[NodeSelectionData] | None = None,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Generate entities for the source.

        Args:
            cursor: SyncCursor for incremental sync tracking.
            files: FileService for downloading files.
            node_selections: Node selections for targeted sync.
        """
        return
        yield  # type: ignore[misc]

    async def generate_access_control_memberships(
        self,
    ) -> AsyncGenerator[MembershipTuple, None]:
        r"""Generate access control membership tuples.

        Only implement this if your source has @source(supports_access_control=True).
        """
        return
        yield  # type: ignore[misc]

    @abstractmethod
    async def validate(self) -> bool:
        """Validate that this source is reachable and credentials are usable."""
        raise NotImplementedError

    async def get_browse_children(
        self,
        parent_node_id: Optional[str] = None,
    ) -> list:
        """Get child nodes for browse tree display."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support browse tree")

    def parse_browse_node_id(self, node_id: str) -> tuple:
        """Parse an encoded browse node ID into (node_type, metadata_dict)."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support browse tree")

    async def search(self, query: str, limit: int) -> AsyncGenerator[BaseEntity, None]:
        """Search the source for entities matching the query."""
        if not getattr(self.__class__, "federated_search", False):
            raise NotImplementedError(
                f"Source {self.__class__.__name__} does not support federated search"
            )
        raise NotImplementedError(
            f"Source {self.__class__.__name__} has federated_search=True but "
            "search() method is not implemented"
        )

    # ------------------------------------------------------------------
    # Backward compat — to be removed once sharepoint2019v2 + google_drive
    # stop stashing cursor on self (they should use the generate_entities param).
    # ------------------------------------------------------------------

    @property
    def cursor(self):
        """Temporary shim: sharepoint2019v2 and google_drive read self.cursor."""
        return getattr(self, "_cursor", None)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def clean_content_for_embedding(self, content: str) -> str:
        """Clean content for embedding by removing huge URLs and cleaning up formatting."""
        if not content:
            return ""

        pattern_images_query = r"!\[([^\]]*)\]\([^\?\)]+\?[^\)]+\)"
        content = re.sub(pattern_images_query, r"[Image: \1]", content)

        pattern_images_long = r"!\[([^\]]*)\]\([^\)]{200,}\)"
        content = re.sub(pattern_images_long, r"[Image: \1]", content)

        pattern_links = r"\[([^\]]+)\]\(https?://[^\s\)]+\?[^\)]{100,}\)"
        content = re.sub(pattern_links, r"[\1]", content)

        pattern_bare = r"(https?://[^\s]+\?[^\s]{100,})"
        content = re.sub(pattern_bare, "[link]", content)

        content = re.sub(r"\n{3,}", "\n\n", content)
        content = content.strip()

        return content

    async def _validate_oauth2(  # noqa: C901
        self,
        *,
        introspection_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        ping_url: Optional[str] = None,
        access_token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
    ) -> bool:
        """Generic OAuth2 validation: introspection and/or a bearer ping."""
        token = access_token or await self.get_access_token()
        if not token:
            self.logger.error("OAuth2 validation failed: no access token available.")
            return False

        def _is_jwt_unexpired(tok: str) -> Optional[bool]:
            try:
                parts = tok.split(".")
                if len(parts) != 3:
                    return None
                pad = "=" * (-len(parts[1]) % 4)
                payload_bytes = base64.urlsafe_b64decode(parts[1] + pad)
                payload = json.loads(payload_bytes.decode("utf-8"))
                exp = payload.get("exp")
                if exp is None:
                    return None
                return time.time() < float(exp)
            except Exception:
                return None

        async def _do_ping(bearer: str) -> bool:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    hdrs = {"Authorization": f"Bearer {bearer}"}
                    if headers:
                        hdrs.update(headers)
                    resp = await client.get(ping_url, headers=hdrs)
                    if 200 <= resp.status_code < 300:
                        return True
                    if resp.status_code == 401:
                        self.logger.info("Ping unauthorized (401); attempting token refresh.")
                        new_token = await self.refresh_on_unauthorized()
                        if new_token:
                            hdrs["Authorization"] = f"Bearer {new_token}"
                            resp = await client.get(ping_url, headers=hdrs)
                            return 200 <= resp.status_code < 300
                    self.logger.warning(f"Ping failed: HTTP {resp.status_code} - {resp.text[:200]}")
                    return False
            except httpx.RequestError as e:
                self.logger.error(f"Ping request error: {e}")
                return False

        if introspection_url:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    auth = (client_id, client_secret) if client_id and client_secret else None
                    data = {"token": token, "token_type_hint": "access_token"}
                    resp = await client.post(
                        introspection_url,
                        data=data,
                        auth=auth,
                        headers={"Accept": "application/json", **(headers or {})},
                    )
                    if resp.status_code == 401:
                        self.logger.info(
                            "Introspection unauthorized (401); attempting token refresh."
                        )
                        new_token = await self.refresh_on_unauthorized()
                        if new_token:
                            data["token"] = new_token
                            resp = await client.post(
                                introspection_url,
                                data=data,
                                auth=auth,
                                headers={"Accept": "application/json", **(headers or {})},
                            )

                    resp.raise_for_status()
                    body = resp.json()
                    active = bool(body.get("active", False))

                    exp = body.get("exp")
                    if exp is not None:
                        try:
                            if time.time() >= float(exp):
                                active = False
                        except Exception:
                            pass

                    if active:
                        return True

                    peek = _is_jwt_unexpired(token)
                    if peek is True:
                        self.logger.debug(
                            "Token appears unexpired by JWT payload, "
                            "but introspection returned inactive."
                        )
                    else:
                        self.logger.warning("Token reported inactive by introspection.")
            except httpx.HTTPStatusError as e:
                status = e.response.status_code if getattr(e, "response", None) else "N/A"
                self.logger.error(f"Introspection HTTP error {status}: {e}")
            except httpx.RequestError as e:
                self.logger.error(f"Introspection request error: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected introspection error: {e}")

        if ping_url:
            return await _do_ping(token)

        peek = _is_jwt_unexpired(token)
        if peek is not None:
            self.logger.debug("Validated via JWT 'exp' claim peek.")
            return peek

        self.logger.warning(
            "OAuth2 validation inconclusive: no endpoints provided and token format is opaque."
        )
        return False

    # ------------------------------------------------------------------
    # Concurrency / batching helpers
    # ------------------------------------------------------------------

    async def process_entities_concurrent(
        self,
        items: Union[Iterable[Any], AsyncIterable[Any]],
        worker: Callable[[Any], AsyncIterable[BaseEntity]],
        *,
        batch_size: int = 10,
        preserve_order: bool = False,
        stop_on_error: bool = False,
        max_queue_size: int = 100,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Generic bounded-concurrency driver."""
        import asyncio as _asyncio

        pool = self._create_bounded_pool(
            items, worker, batch_size=batch_size, max_queue_size=max_queue_size
        )

        try:
            if preserve_order:
                async for ent in self._drain_results_preserve_order(
                    pool["results"],
                    pool["all_tasks"],
                    pool["producer_finished"],
                    pool["get_total_items"],
                    stop_on_error,
                    pool["sentinel"],
                ):
                    yield ent
            else:
                async for ent in self._drain_results_unordered(
                    pool["results"],
                    pool["all_tasks"],
                    pool["producer_finished"],
                    pool["get_total_items"],
                    stop_on_error,
                    pool["sentinel"],
                ):
                    yield ent
        finally:
            for t in pool["all_tasks"]:
                t.cancel()
            await _asyncio.gather(*pool["all_tasks"], return_exceptions=True)

    def _create_bounded_pool(  # noqa: C901
        self,
        items: Union[Iterable[Any], AsyncIterable[Any]],
        worker: Callable[[Any], AsyncIterable[BaseEntity]],
        *,
        batch_size: int,
        max_queue_size: int,
    ) -> Dict[str, Any]:
        """Create a bounded producer + fixed worker pool."""
        import asyncio as _asyncio

        results: _asyncio.Queue = _asyncio.Queue(maxsize=max_queue_size)
        items_queue: _asyncio.Queue = _asyncio.Queue(maxsize=batch_size)
        sentinel = object()
        items_done = object()
        producer_finished = _asyncio.Event()
        total_items_cell: list[int] = [0]

        async def _producer() -> None:
            import time as _time

            try:
                idx = 0
                last_yield_time = _time.monotonic()

                if hasattr(items, "__aiter__"):
                    async for item in items:  # type: ignore[union-attr]
                        now = _time.monotonic()
                        gap = now - last_yield_time
                        if gap > 60:
                            self.logger.warning(
                                f"Source producer resumed after {int(gap)}s gap "
                                f"(item {idx}, {total_items_cell[0]} total)"
                            )
                        await items_queue.put((idx, item))
                        idx += 1
                        total_items_cell[0] = idx
                        last_yield_time = _time.monotonic()
                else:
                    for item in items:  # type: ignore[union-attr]
                        await items_queue.put((idx, item))
                        idx += 1
                        total_items_cell[0] = idx
            finally:
                await items_queue.put(items_done)
                producer_finished.set()
                await results.put(None)

        async def _pool_worker() -> None:
            while True:
                msg = await items_queue.get()
                if msg is items_done:
                    await items_queue.put(items_done)
                    return
                idx, item = msg
                try:
                    agen = worker(item)
                    if not hasattr(agen, "__aiter__"):
                        raise TypeError(
                            "worker(item) must return an async iterator (async generator)."
                        )
                    async for entity in agen:
                        await results.put((idx, entity, None))
                except BaseException as e:
                    await results.put((idx, None, e))
                finally:
                    await results.put((idx, sentinel, None))

        producer_task = _asyncio.create_task(_producer())
        pool_tasks = [_asyncio.create_task(_pool_worker()) for _ in range(batch_size)]

        return {
            "results": results,
            "all_tasks": [producer_task] + pool_tasks,
            "producer_finished": producer_finished,
            "get_total_items": lambda: total_items_cell[0],
            "sentinel": sentinel,
        }

    async def _drain_results_unordered(
        self,
        results,
        tasks,
        producer_finished,
        get_total_items: Callable[[], int],
        stop_on_error: bool,
        sentinel: object,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Yield results as they arrive; stop early on error if requested."""
        done_items = 0
        while True:
            if producer_finished.is_set() and done_items >= get_total_items():
                break
            msg = await results.get()
            if msg is None:
                continue
            i, payload, err = msg
            if payload is sentinel:
                done_items += 1
                continue
            if err:
                self.logger.error(f"Worker {i} error: {err}", exc_info=True)
                if stop_on_error:
                    for t in tasks:
                        t.cancel()
                    raise err
                continue
            yield payload  # type: ignore[misc]

    async def _drain_results_preserve_order(
        self,
        results,
        tasks,
        producer_finished,
        get_total_items: Callable[[], int],
        stop_on_error: bool,
        sentinel: object,
    ) -> AsyncGenerator[BaseEntity, None]:
        """Buffer per-item results and yield in input order."""
        buffers: Dict[int, list[BaseEntity]] = {}
        finished: set[int] = set()
        next_idx = 0
        done_items = 0

        while True:
            if producer_finished.is_set() and done_items >= get_total_items():
                break
            msg = await results.get()
            if msg is None:
                continue
            i, payload, err = msg
            if payload is sentinel:
                finished.add(i)
                done_items += 1
            elif err:
                self.logger.error(f"Worker {i} error: {err}", exc_info=True)
                if stop_on_error:
                    for t in tasks:
                        t.cancel()
                    raise err
            else:
                buffers.setdefault(i, []).append(payload)  # type: ignore[arg-type]

            while next_idx in finished:
                for ent in buffers.pop(next_idx, []):
                    yield ent
                next_idx += 1


class Relation(BaseModel):
    """A relation between two entities."""

    source_entity_type: type[BaseEntity]
    source_entity_id_attribute: str
    target_entity_type: type[BaseEntity]
    target_entity_id_attribute: str
    relation_type: str
