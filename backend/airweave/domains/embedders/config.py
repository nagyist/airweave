"""Deployment-wide embedding configuration.

Python constants — the single source of truth for what this deployment uses.
Replaces both core/embedding_validation.py and platform/embedders/config.py
for startup validation purposes.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.core.config import settings
from airweave.core.logging import logger
from airweave.domains.embedders.registry_data import (
    DenseEmbedderSpec,
    SparseEmbedderSpec,
    get_dense_spec,
    get_sparse_spec,
)
from airweave.models.vector_db_deployment_metadata import VectorDbDeploymentMetadata

# ---------------------------------------------------------------------------
# Constants — the single source of truth for the embedding stack
# ---------------------------------------------------------------------------

DENSE_EMBEDDER = "openai_text_embedding_3_large"
EMBEDDING_DIMENSIONS = 3072
SPARSE_EMBEDDER = "fastembed_bm25"


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------


class EmbeddingConfigError(Exception):
    """Hard error raised when embedding config is invalid or mismatched."""


async def validate_embedding_config(db: AsyncSession) -> None:
    """Validate the embedding configuration at startup.

    Steps:
    1. Registry check — verify DENSE_EMBEDDER / SPARSE_EMBEDDER exist.
    2. Dimensions check — verify EMBEDDING_DIMENSIONS is valid for the model.
    3. Credentials check — verify required API keys are set.
    4. DB reconciliation — compare against the vector_db_deployment_metadata table.

    Raises:
        EmbeddingConfigError on any mismatch.
    """
    try:
        dense_spec = get_dense_spec(DENSE_EMBEDDER)
    except KeyError:
        raise EmbeddingConfigError(f"Dense embedder '{DENSE_EMBEDDER}' not found in registry.")

    try:
        sparse_spec = get_sparse_spec(SPARSE_EMBEDDER)
    except KeyError:
        raise EmbeddingConfigError(f"Sparse embedder '{SPARSE_EMBEDDER}' not found in registry.")

    _validate_dimensions(dense_spec)
    _validate_credentials(dense_spec, sparse_spec)
    await _reconcile_db(db)


def _validate_dimensions(dense_spec: DenseEmbedderSpec) -> None:
    """Validate EMBEDDING_DIMENSIONS against the dense embedder spec."""
    if dense_spec.supports_matryoshka:
        if EMBEDDING_DIMENSIONS > dense_spec.max_dimensions:
            raise EmbeddingConfigError(
                f"EMBEDDING_DIMENSIONS={EMBEDDING_DIMENSIONS} exceeds max_dimensions="
                f"{dense_spec.max_dimensions} for dense embedder '{DENSE_EMBEDDER}'."
            )
    elif EMBEDDING_DIMENSIONS != dense_spec.max_dimensions:
        raise EmbeddingConfigError(
            f"Dense embedder '{DENSE_EMBEDDER}' does not support Matryoshka dimensions — "
            f"EMBEDDING_DIMENSIONS must be exactly {dense_spec.max_dimensions}, "
            f"got {EMBEDDING_DIMENSIONS}."
        )


def _validate_credentials(
    dense_spec: DenseEmbedderSpec,
    sparse_spec: SparseEmbedderSpec,
) -> None:
    """Check that required API keys / settings are present."""
    if dense_spec.required_setting:
        value = getattr(settings, dense_spec.required_setting, None)
        if not value:
            raise EmbeddingConfigError(
                f"Dense embedder '{DENSE_EMBEDDER}' requires setting "
                f"'{dense_spec.required_setting}' but it is not set."
            )

    if sparse_spec.required_setting:
        value = getattr(settings, sparse_spec.required_setting, None)
        if not value:
            raise EmbeddingConfigError(
                f"Sparse embedder '{SPARSE_EMBEDDER}' requires setting "
                f"'{sparse_spec.required_setting}' but it is not set."
            )


async def _reconcile_db(db: AsyncSession) -> None:
    """Reconcile code config against the vector_db_deployment_metadata table."""
    result = await db.execute(select(VectorDbDeploymentMetadata).limit(1))
    row = result.scalar_one_or_none()

    if row is None:
        row = VectorDbDeploymentMetadata(
            dense_embedder=DENSE_EMBEDDER,
            embedding_dimensions=EMBEDDING_DIMENSIONS,
            sparse_embedder=SPARSE_EMBEDDER,
        )
        db.add(row)
        await db.commit()
        logger.info(
            f"[EmbeddingConfig] First deploy — created vector_db_deployment_metadata row: "
            f"dense={DENSE_EMBEDDER}, dims={EMBEDDING_DIMENSIONS}, sparse={SPARSE_EMBEDDER}"
        )
        return

    mismatches = []
    if row.dense_embedder != DENSE_EMBEDDER:
        mismatches.append(f"dense_embedder: code={DENSE_EMBEDDER}, db={row.dense_embedder}")
    if row.embedding_dimensions != EMBEDDING_DIMENSIONS:
        mismatches.append(
            f"embedding_dimensions: code={EMBEDDING_DIMENSIONS}, db={row.embedding_dimensions}"
        )
    if row.sparse_embedder != SPARSE_EMBEDDER:
        mismatches.append(f"sparse_embedder: code={SPARSE_EMBEDDER}, db={row.sparse_embedder}")

    if mismatches:
        detail = "; ".join(mismatches)
        raise EmbeddingConfigError(
            f"Embedding config mismatch: {detail}. "
            f"Changing embedding model or dimensions makes all synced data "
            f"unsearchable — you would have to delete all data and resync."
        )

    logger.info("[EmbeddingConfig] vector_db_deployment_metadata row matches code config — OK")
