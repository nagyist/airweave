"""Utils for transformers."""

import tiktoken

# Max chunk size for embedding models (e.g. OpenAI's text-embedding-ada-002)
MAX_CHUNK_SIZE = 8191
MARGIN_OF_ERROR = 250
METADATA_SIZE = 1000


def count_tokens(text: str) -> int:
    """Count tokens using the cl100k_base tokenizer (used by OpenAI's text-embedding models)."""
    encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(text))
