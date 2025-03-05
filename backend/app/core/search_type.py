"""Search type enumeration for different search strategies."""

from enum import Enum


class SearchType(str, Enum):
    """Search type enumeration.

    Defines the different types of search that can be performed:
    - VECTOR: Traditional vector search using embeddings
    - GRAPH: Graph-based search using relationships
    - HYBRID: Combined vector and graph search
    """

    VECTOR = "vector"
    GRAPH = "graph"
    HYBRID = "hybrid"
