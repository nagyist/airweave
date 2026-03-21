// Airweave API client — uses SDK for collections, direct HTTP for V2 search

import { AirweaveSDKClient } from '@airweave/sdk';
import type {
    AirweaveConfig,
    SearchV2Response,
    InstantSearchRequestBody,
    ClassicSearchRequestBody,
    AgenticSearchRequestBody,
} from './types.js';
import { VERSION } from '../config/constants.js';

export class AirweaveClient {
    private client: AirweaveSDKClient;

    constructor(private config: AirweaveConfig) {
        const headers: Record<string, string> = {
            'Authorization': `Bearer ${config.apiKey}`,
            'X-Client-Name': 'airweave-mcp-search',
            'X-Client-Version': VERSION,
        };
        if (config.organizationId) {
            headers['X-Organization-ID'] = config.organizationId;
        }
        this.client = new AirweaveSDKClient({
            apiKey: config.apiKey,
            baseUrl: config.baseUrl,
            headers,
        });
    }

    async listCollections(limit = 25): Promise<{ readable_id: string; name?: string }[]> {
        try {
            const response = await this.client.collections.list({ limit });
            return (response as any[]).map((c: any) => ({
                readable_id: c.readable_id ?? c.readableId ?? c.id,
                name: c.name,
            }));
        } catch (error: unknown) {
            const err = error as { statusCode?: number; message?: string };
            throw new Error(`Failed to list collections: ${err.message || 'Unknown error'}`);
        }
    }

    // ── V2 tiered search methods (direct HTTP) ──────────────────────────────

    async searchInstant(body: InstantSearchRequestBody): Promise<SearchV2Response> {
        return this.searchV2('instant', body as unknown as Record<string, unknown>);
    }

    async searchClassic(body: ClassicSearchRequestBody): Promise<SearchV2Response> {
        return this.searchV2('classic', body as unknown as Record<string, unknown>);
    }

    async searchAgentic(body: AgenticSearchRequestBody): Promise<SearchV2Response> {
        return this.searchV2('agentic', body as unknown as Record<string, unknown>);
    }

    /**
     * Call a V2 search endpoint directly via fetch.
     *
     * The @airweave/sdk doesn't expose the new tiered endpoints yet, so we
     * make raw HTTP calls.  Once the SDK is regenerated (same CI job) callers
     * can switch to SDK methods with no behavioural change.
     */
    private async searchV2(
        tier: 'instant' | 'classic' | 'agentic',
        body: Record<string, unknown>,
    ): Promise<SearchV2Response> {
        // Mock mode for testing
        if (this.config.apiKey === 'test-key' && this.config.baseUrl.includes('localhost')) {
            return this.getMockResponse(body as { query?: string; limit?: number });
        }

        const url = `${this.config.baseUrl}/collections/${this.config.collection}/search/${tier}`;
        console.log(`[search/${tier}] collection=${this.config.collection} baseUrl=${this.config.baseUrl} orgId=${this.config.organizationId || 'none'}`);

        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this.config.apiKey}`,
            'X-Client-Name': 'airweave-mcp-search',
            'X-Client-Version': VERSION,
        };
        if (this.config.organizationId) {
            headers['X-Organization-ID'] = this.config.organizationId;
        }

        const response = await fetch(url, {
            method: 'POST',
            headers,
            body: JSON.stringify(body),
        });

        if (!response.ok) {
            const text = await response.text();
            throw new Error(
                `Airweave API error (${response.status}): ${response.statusText}\nBody: ${text}`,
            );
        }

        return (await response.json()) as SearchV2Response;
    }

    private getMockResponse(request: { query?: string; limit?: number }): SearchV2Response {
        const { query = '', limit = 100 } = request;
        const resultCount = Math.min(limit, 3);
        const results = [];

        for (let i = 0; i < resultCount; i++) {
            results.push({
                entity_id: `mock_${i + 1}`,
                name: `Mock Document ${i + 1} about "${query}"`,
                relevance_score: 0.95 - i * 0.1,
                breadcrumbs: [
                    { entity_id: 'ws-1', name: 'Mock Workspace', entity_type: 'WorkspaceEntity' },
                ],
                created_at: new Date(Date.now() - i * 24 * 60 * 60 * 1000).toISOString(),
                updated_at: new Date().toISOString(),
                textual_representation: `This is mock content for the query "${query}".`,
                airweave_system_metadata: {
                    source_name: 'mock',
                    entity_type: 'MockDocumentEntity',
                    original_entity_id: `mock_${i + 1}`,
                    chunk_index: 0,
                },
                access: { viewers: null, is_public: null },
                web_url: `https://example.com/doc/${i + 1}`,
                url: null,
                raw_source_fields: {},
            });
        }

        return { results };
    }
}
