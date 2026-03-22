// Error handling and response formatting utilities

import { SearchV2Response, SearchResult, SearchTier } from "../api/types.js";

export function formatSearchResponse(
    searchResponse: SearchV2Response,
    tier: SearchTier,
    collection: string,
) {
    const results = searchResponse.results ?? [];
    const formattedResults = results
        .map((result: SearchResult, index: number) => {
            const parts = [
                `**Result ${index + 1} (Score: ${result.relevance_score.toFixed(3)}):**`,
            ];

            // Name + source
            const source = result.airweave_system_metadata?.source_name;
            parts.push(source ? `${result.name} (${source})` : result.name);

            // Breadcrumbs
            if (result.breadcrumbs?.length > 0) {
                const trail = result.breadcrumbs.map(b => b.name).join(" > ");
                parts.push(`📍 ${trail}`);
            }

            // Content
            if (result.textual_representation) {
                parts.push(result.textual_representation);
            }

            // Link
            if (result.web_url) {
                parts.push(`🔗 ${result.web_url}`);
            }

            return parts.join("\n");
        })
        .join("\n\n---\n\n");

    const summaryText = [
        `**Collection:** ${collection} | **Tier:** ${tier}`,
        `**Results:** ${results.length}`,
        "",
        formattedResults || "No results found.",
    ].join("\n");

    return {
        content: [
            {
                type: "text" as const,
                text: summaryText,
            },
        ],
    };
}

export function formatErrorResponse(
    error: Error,
    searchRequest: any,
    collection: string,
    baseUrl: string,
) {
    return {
        content: [
            {
                type: "text" as const,
                text: `**Error:** Failed to search collection.\n\n**Details:** ${error.message}\n\n**Debugging Info:**\n- Collection: ${collection}\n- Base URL: ${baseUrl}\n- Parameters: ${JSON.stringify(searchRequest, null, 2)}`,
            },
        ],
    };
}
