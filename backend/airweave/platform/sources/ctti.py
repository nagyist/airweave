"""CTTI source implementation.

This source connects to the AACT Clinical Trials PostgreSQL database, queries the nct_id column
from the studies table, and creates WebEntity instances with ClinicalTrials.gov URLs.
"""

import asyncio
import random
from typing import Any, AsyncGenerator, Dict, Optional, Union

import asyncpg

from airweave.core.logging import logger
from airweave.platform.auth.schemas import AuthType
from airweave.platform.configs.auth import CTTIAuthConfig
from airweave.platform.decorators import source
from airweave.platform.entities._base import Breadcrumb
from airweave.platform.entities.ctti import CTTIWebEntity
from airweave.platform.sources._base import BaseSource


async def _retry_with_backoff(func, *args, max_retries=3, **kwargs):
    """Retry a function with exponential backoff.

    Args:
        func: The async function to retry
        *args: Arguments to pass to the function
        max_retries: Maximum number of retry attempts
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The result of the function call

    Raises:
        The last exception if all retries fail
    """
    last_exception = None

    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e

            # Log the full error details
            error_type = type(e).__name__
            error_msg = str(e)

            # Don't retry on certain permanent errors
            if isinstance(
                e,
                (
                    asyncpg.InvalidPasswordError,
                    asyncpg.InvalidCatalogNameError,
                    ValueError,  # Our credential validation errors
                ),
            ):
                logger.error(f"Non-retryable database error: {error_type}: {error_msg}")
                raise e

            if attempt < max_retries:
                # Calculate delay with exponential backoff and jitter
                base_delay = 2**attempt  # 1s, 2s, 4s
                jitter = random.uniform(0.1, 0.5)  # Add randomness
                delay = base_delay + jitter

                logger.warning(
                    f"Database operation attempt {attempt + 1}/{max_retries + 1} failed with "
                    f"{error_type}: {error_msg}. Retrying in {delay:.2f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"All {max_retries + 1} database operation attempts failed. "
                    f"Final error {error_type}: {error_msg}"
                )

    # Re-raise the last exception if all retries failed
    raise last_exception


@source(
    name="CTTI AACT",
    short_name="ctti",
    auth_type=AuthType.config_class,
    auth_config_class="CTTIAuthConfig",
    config_class="CTTIConfig",
    labels=["Clinical Trials", "Database"],
)
class CTTISource(BaseSource):
    """CTTI source implementation.

    This source connects to the AACT Clinical Trials PostgreSQL database and queries
    the nct_id column from the studies table to create WebEntity instances with
    ClinicalTrials.gov URLs.

    Connection details are hardcoded to the public AACT database:
    - Host: aact-db.ctti-clinicaltrials.org
    - Port: 5432
    - Database: aact
    - Schema: ctgov
    - Table: studies
    """

    # Hardcoded AACT database connection details
    AACT_HOST = "aact-db.ctti-clinicaltrials.org"
    AACT_PORT = 5432
    AACT_DATABASE = "aact"
    AACT_SCHEMA = "ctgov"
    AACT_TABLE = "studies"

    def __init__(self):
        """Initialize the CTTI source."""
        self.conn: Optional[asyncpg.Connection] = None

    @classmethod
    async def create(
        cls,
        credentials: Union[Dict[str, Any], CTTIAuthConfig],
        config: Optional[Dict[str, Any]] = None,
    ) -> "CTTISource":
        """Create a new CTTI source instance.

        Args:
            credentials: CTTIAuthConfig object or dictionary containing AACT database credentials:
                - username: Username for AACT database
                - password: Password for AACT database
            config: Optional configuration parameters:
                - limit: Maximum number of studies to fetch (default: 10000)
                - skip: Number of studies to skip for pagination (default: 0)
        """
        instance = cls()
        instance.credentials = credentials  # Store credentials separately
        instance.config = config or {}  # Store config separately
        return instance

    def _get_credential(self, key: str) -> str:
        """Get a credential value from either dict or config object.

        Args:
            key: The credential key to retrieve

        Returns:
            The credential value

        Raises:
            ValueError: If the credential is missing or empty
        """
        # Try to get from object attribute first (CTTIAuthConfig)
        value = getattr(self.credentials, key, None)

        # If not found and credentials is a dict, try dict access
        if value is None and isinstance(self.credentials, dict):
            value = self.credentials.get(key)

        # Validate the value exists and is not empty
        if not value:
            raise ValueError(f"Missing or empty credential: {key}")

        return value

    async def _connect(self) -> None:
        """Establish connection to the AACT Clinical Trials database with retry logic."""
        if not self.conn:

            async def _establish_connection():
                try:
                    username = self._get_credential("username")
                    password = self._get_credential("password")

                    logger.info(f"Connecting to AACT database at {self.AACT_HOST}:{self.AACT_PORT}")
                    conn = await asyncpg.connect(
                        host=self.AACT_HOST,
                        port=self.AACT_PORT,
                        user=username,
                        password=password,
                        database=self.AACT_DATABASE,
                        timeout=30.0,
                        command_timeout=60.0,
                    )
                    logger.info("Successfully connected to AACT database")
                    return conn
                except ValueError as e:
                    # Re-raise credential validation errors with more context
                    raise ValueError(f"Invalid AACT database credentials: {str(e)}") from e
                except asyncpg.InvalidPasswordError as e:
                    raise ValueError(
                        "Invalid AACT database credentials: Authentication failed"
                    ) from e
                except asyncpg.InvalidCatalogNameError as e:
                    raise ValueError(f"AACT database '{self.AACT_DATABASE}' does not exist") from e
                except (
                    OSError,
                    asyncpg.CannotConnectNowError,
                    asyncpg.ConnectionDoesNotExistError,
                ) as e:
                    raise RuntimeError(
                        f"Could not connect to AACT database at {self.AACT_HOST}:{self.AACT_PORT}. "
                        f"Please check your internet connection. Error: {str(e)}"
                    ) from e
                except Exception as e:
                    raise RuntimeError(f"AACT database connection failed: {str(e)}") from e

            # Use retry logic for connection establishment
            self.conn = await _retry_with_backoff(_establish_connection)

    async def generate_entities(self) -> AsyncGenerator[Union[CTTIWebEntity], None]:
        """Generate WebEntity instances for each nct_id in the AACT studies table."""
        try:
            await self._connect()

            # Get the limit and skip from config
            limit = self.config.get("limit", 10000)
            skip = self.config.get("skip", 0)

            # Simple query - URL construction in Python is fine
            query = f'''
                SELECT nct_id
                FROM "{self.AACT_SCHEMA}"."{self.AACT_TABLE}"
                WHERE nct_id IS NOT NULL
                ORDER BY nct_id
                LIMIT {limit}
                OFFSET {skip}
            '''

            async def _execute_query():
                if skip > 0:
                    logger.info(
                        f"Executing query to fetch {limit} clinical trials from AACT database "
                        f"(skipping first {skip} records)"
                    )
                else:
                    logger.info(
                        f"Executing query to fetch {limit} clinical trials from AACT database"
                    )
                records = await self.conn.fetch(query)
                logger.info(f"Successfully fetched {len(records)} clinical trial records")
                return records

            # Use retry logic for query execution
            records = await _retry_with_backoff(_execute_query)

            # Process each nct_id
            for record in records:
                nct_id = record["nct_id"]

                # Skip if nct_id is empty or None
                if not nct_id or not str(nct_id).strip():
                    continue

                # Clean the nct_id (remove whitespace)
                clean_nct_id = str(nct_id).strip()

                # Create the ClinicalTrials.gov URL
                url = f"https://clinicaltrials.gov/study/{clean_nct_id}"

                # Create entity_id using the nct_id
                entity_id = f"CTTI:study:{clean_nct_id}"

                # Create WebEntity
                entity = CTTIWebEntity(
                    entity_id=entity_id,
                    url=url,
                    title=f"Clinical Trial {clean_nct_id}",
                    description=(
                        f"Clinical trial study from ClinicalTrials.gov with NCT ID: {clean_nct_id}"
                    ),
                    nct_id=clean_nct_id,
                    study_url=url,
                    data_source="ClinicalTrials.gov",
                    breadcrumbs=[
                        Breadcrumb(
                            entity_id="CTTI:source", name="CTTI Clinical Trials", type="source"
                        ),
                        Breadcrumb(
                            entity_id=entity_id,
                            name=f"Clinical Trial {clean_nct_id}",
                            type="clinical_trial",
                        ),
                    ],
                    metadata={
                        "source": "CTTI",
                        "database_host": self.AACT_HOST,
                        "database_name": self.AACT_DATABASE,
                        "database_schema": self.AACT_SCHEMA,
                        "database_table": self.AACT_TABLE,
                        "original_nct_id": nct_id,  # Keep original in case it had formatting
                        "limit_used": limit,
                        "skip_used": skip,
                        "total_fetched": len(records),
                    },
                )

                # Check if CTTI entity is already processed globally
                from airweave.platform.storage import storage_manager

                if await storage_manager.is_ctti_entity_processed(entity_id):
                    logger.info(
                        f"CTTI entity {entity_id} already processed globally, marking as KEPT"
                    )
                    entity.is_fully_processed = True

                yield entity

        except Exception as e:
            logger.error(f"Error in CTTI source generate_entities: {str(e)}")
            raise
        finally:
            if self.conn:
                await self.conn.close()
                self.conn = None
                logger.info("Closed AACT database connection")
