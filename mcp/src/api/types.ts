// Type definitions for Airweave API responses

export interface AirweaveConfig {
    apiKey: string;
    collection: string;
    baseUrl: string;
    organizationId?: string;
}

// ── Search V2 types ──────────────────────────────────────────────────────────

export type SearchTier = "instant" | "classic" | "agentic";
export type RetrievalStrategy = "hybrid" | "semantic" | "keyword";

export interface FilterCondition {
    field: string;
    operator: string;
    value: string | number | boolean | string[] | number[];
}

export interface FilterGroup {
    conditions: FilterCondition[];
}

// Tier-specific request bodies (sent to backend)

export interface InstantSearchRequestBody {
    query: string;
    retrieval_strategy?: RetrievalStrategy;
    filter?: FilterGroup[];
    limit?: number;
    offset?: number;
}

export interface ClassicSearchRequestBody {
    query: string;
    filter?: FilterGroup[];
    limit?: number;
    offset?: number;
}

export interface AgenticSearchRequestBody {
    query: string;
    thinking?: boolean;
    filter?: FilterGroup[];
    limit?: number;
}

// Response types

export interface SearchBreadcrumb {
    entity_id: string;
    name: string;
    entity_type: string;
}

export interface SearchSystemMetadata {
    source_name: string;
    entity_type: string;
    original_entity_id: string;
    chunk_index: number;
    sync_id?: string;
    sync_job_id?: string;
}

export interface SearchAccessControl {
    viewers?: string[] | null;
    is_public?: boolean | null;
}

export interface SearchResult {
    entity_id: string;
    name: string;
    relevance_score: number;
    breadcrumbs: SearchBreadcrumb[];
    created_at?: string | null;
    updated_at?: string | null;
    textual_representation: string;
    airweave_system_metadata: SearchSystemMetadata;
    access: SearchAccessControl;
    web_url: string;
    url?: string | null;
    raw_source_fields: Record<string, unknown>;
}

export interface SearchV2Response {
    results: SearchResult[];
}
