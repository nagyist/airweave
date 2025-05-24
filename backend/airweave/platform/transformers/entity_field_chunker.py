"""Entity chunker for chunking large text fields in entities."""

import math
from typing import Dict, List, Tuple

from chonkie import SemanticChunker

from airweave.core.logging import logger
from airweave.platform.decorators import transformer
from airweave.platform.entities._base import BaseEntity
from airweave.platform.transformers.utils import (
    MARGIN_OF_ERROR,
    MAX_CHUNK_SIZE,
    METADATA_SIZE,
    count_tokens,
)

# Fields that should never be chunked (system fields)
NON_CHUNKABLE_FIELDS = {
    "entity_id",
    "breadcrumbs",
    "db_entity_id",
    "source_name",
    "sync_id",
    "sync_job_id",
    "url",
    "sync_metadata",
    "parent_entity_id",
    "vector",
    "chunk_index",
}


def create_semantic_chunker(content_size: int, target_size: int) -> SemanticChunker:
    """Create a semantic chunker optimized for minimal splitting.

    Args:
        content_size: Size of the content to chunk in tokens
        target_size: Target size for each chunk

    Returns:
        Configured SemanticChunker
    """
    # Calculate ideal number of chunks (round up)
    ideal_chunks = math.ceil(content_size / target_size)

    # Calculate chunk size - slightly less than target to allow some flexibility
    chunk_size = int(target_size * 0.95)

    # Adjust similarity threshold based on number of chunks needed
    # More chunks needed = lower threshold (more aggressive splitting)
    if ideal_chunks <= 2:
        similarity_threshold = 0.65  # Conservative splitting for just 1-2 chunks
    elif ideal_chunks <= 4:
        similarity_threshold = 0.6  # Moderate splitting
    else:
        similarity_threshold = 0.55  # More aggressive splitting for many chunks

    # Create chunker with optimized parameters
    return SemanticChunker(
        embedding_model="text-embedding-ada-002",
        chunk_size=chunk_size,
        threshold=similarity_threshold,
        mode="window",
        min_sentences=1,
        similarity_window=2 if ideal_chunks <= 2 else 3,
    )


def calculate_entity_size(entity_dict: Dict) -> Tuple[int, Dict[str, int]]:
    """Calculate the total size of an entity and sizes of individual fields.

    Args:
        entity_dict: Entity dictionary from model_dump()

    Returns:
        Tuple of (total_size, field_sizes_dict)
    """
    total_size = 0
    field_sizes = {}

    for field_name, field_value in entity_dict.items():
        if isinstance(field_value, str):
            size = count_tokens(field_value)
            field_sizes[field_name] = size
            total_size += size
        elif isinstance(field_value, (dict, list)):
            # Approximate size for complex fields
            # This is a rough estimate based on JSON serialization
            size = count_tokens(str(field_value))
            field_sizes[field_name] = size
            total_size += size

    return total_size, field_sizes


def find_field_to_chunk(entity_dict: Dict, field_sizes: Dict[str, int]) -> Tuple[str, int]:
    """Find the largest chunkable field that contributes most to entity size.

    Args:
        entity_dict: Entity dictionary
        field_sizes: Dictionary of field sizes

    Returns:
        Tuple of (field_name, field_size)
    """
    largest_field = None
    largest_field_size = 0

    for field_name, field_size in field_sizes.items():
        if (
            field_name not in NON_CHUNKABLE_FIELDS
            and isinstance(entity_dict[field_name], str)
            and field_size > largest_field_size
        ):
            largest_field = field_name
            largest_field_size = field_size

    return largest_field, largest_field_size


@transformer(name="Entity Chunker")
async def entity_chunker(entity: BaseEntity) -> List[BaseEntity]:
    """Chunk large text fields in an entity with minimal cuts.

    This transformer ensures both individual fields and the overall entity size
    remain under MAX_CHUNK_SIZE by:
    1. Calculating the total entity size
    2. Finding the largest field to chunk if needed
    3. Creating appropriately sized chunks to keep the total entity size under MAX_CHUNK_SIZE

    Args:
        entity: The BaseEntity to process

    Returns:
        List[BaseEntity]: Multiple copies of the entity with chunks of the large field
    """
    # Skip if already chunked
    if getattr(entity, "chunk_index", None) is not None:
        return [entity]

    # Get entity data and calculate sizes
    entity_dict = entity.model_dump()
    total_size, field_sizes = calculate_entity_size(entity_dict)

    # If entity is small enough, return as is
    if total_size <= MAX_CHUNK_SIZE - MARGIN_OF_ERROR:
        return [entity]

    # Find the largest field to chunk
    largest_field, largest_field_size = find_field_to_chunk(entity_dict, field_sizes)

    # If no suitable field found, log warning and return original
    if not largest_field:
        logger.warning(
            f"Entity {entity.entity_id} exceeds max size ({total_size} > {MAX_CHUNK_SIZE}), "
            f"but no suitable field found for chunking"
        )
        return [entity]

    logger.info(
        f"Chunking entity {entity.entity_id} (total size: {total_size}, max: {MAX_CHUNK_SIZE})"
        f" using field '{largest_field}' (size: {largest_field_size})"
    )

    # Calculate overhead - size of entity excluding the field to chunk
    overhead = total_size - largest_field_size

    # Calculate target size for chunks to ensure total entity size
    # stays under MAX_CHUNK_SIZE - METADATA_SIZE
    target_chunk_size = MAX_CHUNK_SIZE - overhead - METADATA_SIZE

    if target_chunk_size <= 0:
        logger.warning(
            f"Entity overhead ({overhead}) exceeds MAX_CHUNK_SIZE ({MAX_CHUNK_SIZE}), "
            f"chunking may not be effective"
        )
        # Fall back to a minimum chunk size
        target_chunk_size = max(100, int(MAX_CHUNK_SIZE * 0.2))

    # Create semantic chunker optimized for this content
    chunker = create_semantic_chunker(largest_field_size, target_chunk_size)
    chunks = chunker.chunk(entity_dict[largest_field])

    # Log chunk distribution
    chunk_sizes = [count_tokens(chunk.text) for chunk in chunks]
    estimated_entity_sizes = [overhead + size for size in chunk_sizes]

    logger.info(
        f"Created {len(chunks)} chunks with token distribution: "
        f"{', '.join(str(size) for size in chunk_sizes)}"
    )
    logger.info(
        f"Estimated entity sizes after chunking: "
        f"{', '.join(str(size) for size in estimated_entity_sizes)}"
    )

    # Create a copy of the entity for each chunk
    chunked_entities = []
    entity_class = type(entity)

    for i, chunk in enumerate(chunks):
        # Create a new entity with the chunked field
        chunked_entity_data = {**entity_dict, largest_field: chunk.text, "chunk_index": i}
        chunked_entity = entity_class(**chunked_entity_data)
        chunked_entities.append(chunked_entity)

    return chunked_entities
