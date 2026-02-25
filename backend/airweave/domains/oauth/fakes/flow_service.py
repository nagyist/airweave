"""Fake OAuthFlowService for testing."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.db.unit_of_work import UnitOfWork
from airweave.domains.oauth.types import OAuth1TokenResponse
from airweave.platform.auth.schemas import OAuth1Settings, OAuth2TokenResponse


class FakeOAuthFlowService:
    """In-memory fake for OAuthFlowServiceProtocol."""

    def __init__(self) -> None:
        self._calls: List[Tuple[str, ...]] = []
        self._oauth2_token_response: Optional[OAuth2TokenResponse] = None
        self._oauth1_token_response: Optional[OAuth1TokenResponse] = None
        self._last_create_init_session_kwargs: Dict[str, Any] = {}
        self._last_initiate_oauth2_kwargs: Dict[str, Any] = {}
        self._last_initiate_oauth1_kwargs: Dict[str, Any] = {}
        self._auth_url: str = "https://provider.example.com/auth"
        self._code_verifier: Optional[str] = "fake_verifier"
        self._oauth1_overrides: Dict[str, str] = {
            "oauth_token": "fake_token",
            "oauth_token_secret": "fake_secret",
            "consumer_key": "key",
            "consumer_secret": "secret",
        }

    def seed_oauth2_response(self, response: OAuth2TokenResponse) -> None:
        self._oauth2_token_response = response

    def seed_oauth1_response(self, response: OAuth1TokenResponse) -> None:
        self._oauth1_token_response = response

    async def initiate_oauth2(
        self,
        short_name: str,
        state: str,
        *,
        client_id: Optional[str] = None,
        template_configs: Optional[dict] = None,
        ctx: ApiContext,
    ) -> Tuple[str, Optional[str]]:
        self._calls.append(("initiate_oauth2", short_name, state))
        self._last_initiate_oauth2_kwargs = {
            "short_name": short_name,
            "state": state,
            "client_id": client_id,
            "template_configs": template_configs,
        }
        return self._auth_url, self._code_verifier

    async def initiate_oauth1(
        self,
        short_name: str,
        *,
        consumer_key: str,
        consumer_secret: str,
        ctx: ApiContext,
    ) -> Tuple[str, Dict[str, str]]:
        self._calls.append(("initiate_oauth1", short_name))
        self._last_initiate_oauth1_kwargs = {
            "short_name": short_name,
            "consumer_key": consumer_key,
            "consumer_secret": consumer_secret,
        }
        return self._auth_url, self._oauth1_overrides

    async def complete_oauth2_callback(
        self,
        short_name: str,
        code: str,
        overrides: Dict[str, Any],
        ctx: ApiContext,
    ) -> OAuth2TokenResponse:
        self._calls.append(("complete_oauth2_callback", short_name, code))
        if self._oauth2_token_response:
            return self._oauth2_token_response
        return OAuth2TokenResponse(access_token="fake_access_token", token_type="bearer")

    async def complete_oauth1_callback(
        self,
        short_name: str,
        verifier: str,
        overrides: Dict[str, Any],
        oauth_settings: OAuth1Settings,
        ctx: ApiContext,
    ) -> OAuth1TokenResponse:
        self._calls.append(("complete_oauth1_callback", short_name, verifier))
        if self._oauth1_token_response:
            return self._oauth1_token_response
        return OAuth1TokenResponse(oauth_token="fake_access", oauth_token_secret="fake_secret")

    async def create_init_session(
        self,
        db: AsyncSession,
        *,
        short_name: str,
        state: str,
        payload: Dict[str, Any],
        ctx: ApiContext,
        uow: UnitOfWork,
        redirect_session_id: Optional[UUID] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        oauth_client_mode: Optional[str] = None,
        redirect_url: Optional[str] = None,
        template_configs: Optional[dict] = None,
        additional_overrides: Optional[Dict[str, Any]] = None,
    ) -> Any:
        self._calls.append(("create_init_session", short_name, state))
        self._last_create_init_session_kwargs = {
            "short_name": short_name,
            "state": state,
            "payload": payload,
            "redirect_session_id": redirect_session_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "oauth_client_mode": oauth_client_mode,
            "redirect_url": redirect_url,
            "template_configs": template_configs,
            "additional_overrides": additional_overrides,
        }
        return type("InitSession", (), {"id": uuid4()})()

    async def create_proxy_url(
        self,
        db: AsyncSession,
        provider_auth_url: str,
        ctx: ApiContext,
        uow: Optional[UnitOfWork] = None,
    ) -> Tuple[str, datetime, UUID]:
        self._calls.append(("create_proxy_url", provider_auth_url))
        return (
            "https://api.example.com/source-connections/authorize/abc12345",
            datetime.now(timezone.utc) + timedelta(hours=24),
            uuid4(),
        )
