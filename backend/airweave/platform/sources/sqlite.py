"""SQLite source implementation.

This source connects to a SQLite database and generates entities for each table
based on its schema structure. It dynamically creates entity classes at runtime
using the PolymorphicEntity system.
"""

from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Type

import aiosqlite

from airweave.platform.auth.schemas import AuthType
from airweave.platform.decorators import source
from airweave.platform.entities._base import ChunkEntity, PolymorphicEntity
from airweave.platform.sources._base import BaseSource

# Mapping of SQLite types to Python types
SQLITE_TYPE_MAP = {
    "integer": int,
    "int": int,
    "tinyint": int,
    "smallint": int,
    "mediumint": int,
    "bigint": int,
    "unsigned big int": int,
    "int2": int,
    "int8": int,
    "real": float,
    "double": float,
    "double precision": float,
    "float": float,
    "decimal": float,
    "character": str,
    "varchar": str,
    "varying character": str,
    "nchar": str,
    "native character": str,
    "nvarchar": str,
    "text": str,
    "clob": str,
    "blob": bytes,
    "date": datetime,
    "datetime": datetime,
    "timestamp": datetime,
    "boolean": bool,
}


@source(
    name="SQLite",
    short_name="sqlite",
    auth_type=AuthType.config_class,
    auth_config_class="SQLiteAuthConfig",
    config_class="SQLiteConfig",
    labels=["Database"],
)
class SQLiteSource(BaseSource):
    """SQLite source implementation.

    This source connects to a SQLite database and generates entities for each table.
    It uses database introspection to:
    1. Discover tables and their structures
    2. Create appropriate entity classes dynamically
    3. Generate entities for each table's data
    """

    def __init__(self):
        """Initialize the SQLite source."""
        self.conn: Optional[aiosqlite.Connection] = None
        self.entity_classes: Dict[str, Type[PolymorphicEntity]] = {}

    @classmethod
    async def create(
        cls, credentials: Dict[str, Any], config: Optional[Dict[str, Any]] = None
    ) -> "SQLiteSource":
        """Create a new SQLite source instance.

        Args:
            credentials: Dictionary containing connection details:
                - database: Path to SQLite database file
                - tables: Table to sync (defaults to '*')
            config: Optional configuration parameters for the SQLite source.
        """
        instance = cls()
        instance.config = credentials.model_dump()
        return instance

    async def _connect(self) -> None:
        """Establish database connection with timeout and error handling."""
        if not self.conn:
            try:
                self.conn = await aiosqlite.connect(
                    database=self.config["database"],
                    timeout=10.0,
                )
            except aiosqlite.Error as e:
                raise ValueError(f"Database connection failed: {str(e)}") from e

    async def _get_table_info(self, table: str) -> Dict[str, Any]:
        """Get table structure information.

        Args:
            table: Table name

        Returns:
            Dictionary containing column information and primary keys
        """
        # Get column information using PRAGMA table_info
        async with self.conn.execute(f"PRAGMA table_info({table})") as cursor:
            columns = await cursor.fetchall()

        # Get primary key information from the same PRAGMA
        primary_keys = [col[1] for col in columns if col[5]]  # col[5] is pk flag

        # Build column metadata
        column_info = {}
        for col in columns:
            # col[1] is name, col[2] is type
            sqlite_type = col[2].lower().split("(")[0]  # Remove size constraints
            python_type = SQLITE_TYPE_MAP.get(sqlite_type, Any)

            column_info[col[1]] = {
                "python_type": python_type,
                "nullable": not col[3],  # col[3] is notnull
                "default": col[4],  # col[4] is dflt_value
                "sqlite_type": sqlite_type,
            }

        return {
            "columns": column_info,
            "primary_keys": primary_keys,
        }

    async def _create_entity_class(self, table: str) -> Type[PolymorphicEntity]:
        """Create a entity class for a specific table.

        Args:
            table: Table name

        Returns:
            Dynamically created entity class for the table
        """
        table_info = await self._get_table_info(table)

        return PolymorphicEntity.create_table_entity_class(
            table_name=table,
            schema_name="main",  # SQLite uses 'main' as default schema
            columns=table_info["columns"],
            primary_keys=table_info["primary_keys"],
        )

    async def _get_tables(self) -> List[str]:
        """Get list of tables.

        Returns:
            List of table names
        """
        async with self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ) as cursor:
            tables = await cursor.fetchall()
            return [table[0] for table in tables]

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate entities for all tables."""
        try:
            await self._connect()

            tables_config = self.config.get("tables", "*")

            # Handle both wildcard and CSV list of tables
            if tables_config == "*":
                tables = await self._get_tables()
            else:
                # Split by comma and strip whitespace
                tables = [t.strip() for t in tables_config.split(",")]
                # Validate that all specified tables exist
                available_tables = await self._get_tables()
                invalid_tables = set(tables) - set(available_tables)
                if invalid_tables:
                    raise ValueError(f"Tables not found: {', '.join(invalid_tables)}")

            for table in tables:
                # Create entity class if not already created
                if f"main.{table}" not in self.entity_classes:
                    self.entity_classes[f"main.{table}"] = await self._create_entity_class(table)

                entity_class = self.entity_classes[f"main.{table}"]

                # Fetch and yield data
                BATCH_SIZE = 50
                offset = 0

                while True:
                    # Fetch records in batches using LIMIT and OFFSET
                    batch_query = f"""
                        SELECT *
                        FROM "{table}"
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                    """
                    async with self.conn.execute(batch_query) as cursor:
                        records = await cursor.fetchall()

                        # Break if no more records
                        if not records:
                            break

                        # Get column names from cursor description
                        columns = [column[0] for column in cursor.description]

                        # Process the batch
                        for record in records:
                            # Convert record to dictionary using column names
                            data = dict(zip(columns, record, strict=False))
                            model_fields = entity_class.model_fields
                            primary_keys = model_fields["primary_key_columns"].default_factory()
                            pk_values = [str(data[pk]) for pk in primary_keys]
                            entity_id = f"main.{table}:" + ":".join(pk_values)

                            entity = entity_class(entity_id=entity_id, **data)
                            yield entity

                    # Increment offset for next batch
                    offset += BATCH_SIZE

        finally:
            if self.conn:
                await self.conn.close()
                self.conn = None
