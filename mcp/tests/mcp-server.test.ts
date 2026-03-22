/**
 * MCP Server Tests - V2 tiered search
 *
 * Verifies:
 * 1. Tool creation with correct name, schema, description
 * 2. Tier routing: instant → searchInstant, classic → searchClassic, agentic → searchAgentic
 * 3. Parameters pass through correctly per tier
 * 4. Response formatting for the new SearchResult shape
 * 5. Error handling (validation, API errors)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { AirweaveSDKClient } from '@airweave/sdk';
import { createSearchTool } from '../src/tools/search-tool.js';
import { createConfigTool } from '../src/tools/config-tool.js';
import { AirweaveClient } from '../src/api/airweave-client.js';

// Mock the Airweave SDK
vi.mock('@airweave/sdk');

// Mock prometheus metrics to avoid prom-client registration conflicts
vi.mock('../src/metrics/prometheus.js', () => ({
    searchDuration: { startTimer: () => () => {} },
    searchTotal: { inc: () => {} },
    register: { metrics: () => '', contentType: 'text/plain' },
}));

// Mock global fetch for V2 search calls
const mockFetch = vi.fn();
vi.stubGlobal('fetch', mockFetch);

const MOCK_V2_RESPONSE = {
    results: [
        {
            entity_id: 'page-abc123',
            name: 'Test Document',
            relevance_score: 0.95,
            breadcrumbs: [
                { entity_id: 'ws-1', name: 'Workspace', entity_type: 'WorkspaceEntity' },
                { entity_id: 'db-1', name: 'Engineering', entity_type: 'DatabaseEntity' },
            ],
            created_at: '2025-02-10T09:15:00Z',
            updated_at: '2025-03-18T16:30:00Z',
            textual_representation: 'This is test content about machine learning algorithms.',
            airweave_system_metadata: {
                source_name: 'notion',
                entity_type: 'NotionPageEntity',
                original_entity_id: 'page-abc123',
                chunk_index: 0,
            },
            access: { viewers: null, is_public: null },
            web_url: 'https://notion.so/Test-abc123',
            url: null,
            raw_source_fields: { icon: '📄' },
        },
    ],
};

function mockFetchOk(body: unknown = MOCK_V2_RESPONSE) {
    mockFetch.mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve(body),
    });
}

function mockFetchError(status: number, text: string) {
    mockFetch.mockResolvedValue({
        ok: false,
        status,
        statusText: text,
        text: () => Promise.resolve(text),
    });
}

describe('MCP Server - V2 Tiered Search', () => {
    let airweaveClient: AirweaveClient;

    beforeEach(() => {
        vi.clearAllMocks();

        // SDK is only used for listCollections — mock it minimally
        vi.mocked(AirweaveSDKClient).mockImplementation(() => ({
            collections: { list: vi.fn().mockResolvedValue([]) },
        }) as any);

        airweaveClient = new AirweaveClient({
            apiKey: 'test-api-key',
            collection: 'test-collection',
            baseUrl: 'https://api.airweave.ai',
        });

        mockFetchOk();
    });

    // ── Tool creation ────────────────────────────────────────────────────

    describe('Search Tool Creation', () => {
        it('should create search tool with correct name', () => {
            const tool = createSearchTool('search-my-collection', 'my-collection', airweaveClient);

            expect(tool.name).toBe('search-my-collection');
            expect(tool.description).toContain('my-collection');
            expect(tool.handler).toBeDefined();
        });

        it('should have the new V2 schema parameters', () => {
            const tool = createSearchTool('search-test', 'test', airweaveClient);

            expect(tool.schema).toHaveProperty('query');
            expect(tool.schema).toHaveProperty('tier');
            expect(tool.schema).toHaveProperty('retrieval_strategy');
            expect(tool.schema).toHaveProperty('limit');
            expect(tool.schema).toHaveProperty('offset');
            expect(tool.schema).toHaveProperty('thinking');
            expect(tool.schema).toHaveProperty('filter');

            // Old params should NOT be present
            expect(tool.schema).not.toHaveProperty('response_type');
            expect(tool.schema).not.toHaveProperty('recency_bias');
            expect(tool.schema).not.toHaveProperty('score_threshold');
            expect(tool.schema).not.toHaveProperty('search_method');
            expect(tool.schema).not.toHaveProperty('expansion_strategy');
            expect(tool.schema).not.toHaveProperty('enable_reranking');
            expect(tool.schema).not.toHaveProperty('enable_query_interpretation');
        });
    });

    // ── Tier routing ─────────────────────────────────────────────────────

    describe('Tier Routing', () => {
        it('should default to classic tier', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'test' });

            expect(mockFetch).toHaveBeenCalledWith(
                'https://api.airweave.ai/collections/test-collection/search/classic',
                expect.any(Object),
            );
        });

        it('should route instant tier to /search/instant', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'test', tier: 'instant' });

            expect(mockFetch).toHaveBeenCalledWith(
                'https://api.airweave.ai/collections/test-collection/search/instant',
                expect.any(Object),
            );
        });

        it('should route classic tier to /search/classic', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'test', tier: 'classic' });

            expect(mockFetch).toHaveBeenCalledWith(
                'https://api.airweave.ai/collections/test-collection/search/classic',
                expect.any(Object),
            );
        });

        it('should route agentic tier to /search/agentic', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'test', tier: 'agentic' });

            expect(mockFetch).toHaveBeenCalledWith(
                'https://api.airweave.ai/collections/test-collection/search/agentic',
                expect.any(Object),
            );
        });
    });

    // ── Parameter passing ────────────────────────────────────────────────

    describe('Parameter Passing', () => {
        it('should pass basic parameters for classic tier', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'machine learning', limit: 5, offset: 10 });

            const body = JSON.parse(mockFetch.mock.calls[0][1].body);
            expect(body).toMatchObject({
                query: 'machine learning',
                limit: 5,
                offset: 10,
            });
        });

        it('should pass retrieval_strategy for instant tier', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({
                query: 'test',
                tier: 'instant',
                retrieval_strategy: 'semantic',
            });

            const body = JSON.parse(mockFetch.mock.calls[0][1].body);
            expect(body).toMatchObject({
                query: 'test',
                retrieval_strategy: 'semantic',
            });
        });

        it('should pass thinking for agentic tier', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({
                query: 'test',
                tier: 'agentic',
                thinking: true,
            });

            const body = JSON.parse(mockFetch.mock.calls[0][1].body);
            expect(body).toMatchObject({
                query: 'test',
                thinking: true,
            });
        });

        it('should pass filter groups correctly', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);
            const filter = [
                {
                    conditions: [
                        { field: 'airweave_system_metadata.source_name', operator: 'equals', value: 'notion' },
                    ],
                },
            ];

            await tool.handler({ query: 'test', filter });

            const body = JSON.parse(mockFetch.mock.calls[0][1].body);
            expect(body.filter).toEqual(filter);
        });

        it('should include auth headers in request', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            await tool.handler({ query: 'test' });

            const headers = mockFetch.mock.calls[0][1].headers;
            expect(headers['Authorization']).toBe('Bearer test-api-key');
            expect(headers['Content-Type']).toBe('application/json');
            expect(headers['X-Client-Name']).toBe('airweave-mcp-search');
        });
    });

    // ── Response formatting ──────────────────────────────────────────────

    describe('Response Formatting', () => {
        it('should format V2 search results correctly', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            const result = await tool.handler({ query: 'test' });

            expect(result).toHaveProperty('content');
            expect(result.content[0].type).toBe('text');
            expect(result.content[0].text).toContain('Test Document');
            expect(result.content[0].text).toContain('0.950');
            expect(result.content[0].text).toContain('notion');
            expect(result.content[0].text).toContain('Workspace > Engineering');
            expect(result.content[0].text).toContain('https://notion.so/Test-abc123');
        });

        it('should include tier in response summary', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            const result = await tool.handler({ query: 'test', tier: 'agentic' });

            expect(result.content[0].text).toContain('**Tier:** agentic');
        });

        it('should handle empty results gracefully', async () => {
            mockFetchOk({ results: [] });

            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);
            const result = await tool.handler({ query: 'nonexistent query' });

            expect(result.content[0].text).toContain('No results found');
        });
    });

    // ── Error handling ───────────────────────────────────────────────────

    describe('Error Handling', () => {
        it('should handle validation errors correctly', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            const result = await tool.handler({ limit: 5 }); // missing query

            expect(result.content[0].text).toContain('Parameter Validation Errors');
            expect(result.content[0].text).toContain('query');
        });

        it('should handle API errors correctly', async () => {
            mockFetchError(404, 'Collection not found');

            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);
            const result = await tool.handler({ query: 'test' });

            expect(result.content[0].text).toContain('Failed to search collection');
            expect(result.content[0].text).toContain('404');
        });

        it('should handle network errors correctly', async () => {
            mockFetch.mockRejectedValue(new Error('Network error: timeout'));

            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);
            const result = await tool.handler({ query: 'test' });

            expect(result.content[0].text).toContain('Failed to search collection');
            expect(result.content[0].text).toContain('Network error');
        });

        it('should validate string parameter types', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            const result = await tool.handler({ query: 123 });

            expect(result.content[0].text).toContain('Parameter Validation Errors');
        });

        it('should validate enum parameter values', async () => {
            const tool = createSearchTool('search-test', 'test-collection', airweaveClient);

            const result = await tool.handler({ query: 'test', tier: 'invalid' });

            expect(result.content[0].text).toContain('Parameter Validation Errors');
        });
    });

    // ── Config tool ──────────────────────────────────────────────────────

    describe('Config Tool', () => {
        it('should create config tool correctly', () => {
            const tool = createConfigTool('search-test', 'test-collection', 'https://api.airweave.ai', 'test-key');

            expect(tool.name).toBe('get-config');
            expect(tool.description).toContain('configuration');
        });

        it('should return correct configuration', async () => {
            const tool = createConfigTool('search-test', 'my-collection', 'https://api.airweave.ai', 'test-key-123');

            const result = await tool.handler({});

            expect(result.content[0].text).toContain('my-collection');
            expect(result.content[0].text).toContain('https://api.airweave.ai');
            expect(result.content[0].text).toContain('Configured');
        });
    });

    // ── MCP server integration ───────────────────────────────────────────

    describe('MCP Server Integration', () => {
        it('should register tools correctly on MCP server', () => {
            const server = new McpServer({
                name: 'test-server',
                version: '1.0.0'
            }, {
                capabilities: { tools: {} }
            });

            const searchTool = createSearchTool('search-test', 'test', airweaveClient);
            const configTool = createConfigTool('search-test', 'test', 'https://api.airweave.ai', 'key');

            server.tool(searchTool.name, searchTool.description, searchTool.schema, searchTool.handler);
            server.tool(configTool.name, configTool.description, configTool.schema, configTool.handler);

            expect(server).toBeDefined();
        });
    });
});
