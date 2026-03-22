// Type definitions for Airweave MCP server
//
// Response and filter types are re-exported from the SDK.
// Only MCP-specific types (AirweaveConfig, SearchTier) are defined here.

import { AirweaveSDK } from '@airweave/sdk';

// ── MCP-specific types ──────────────────────────────────────────────────────

export interface AirweaveConfig {
    apiKey: string;
    collection: string;
    baseUrl: string;
    organizationId?: string;
}

export type SearchTier = "instant" | "classic" | "agentic";

// ── Re-exports from SDK ─────────────────────────────────────────────────────

export type FilterCondition = AirweaveSDK.FilterCondition;
export type FilterGroup = AirweaveSDK.FilterGroup;
export type SearchResult = AirweaveSDK.SearchResult;
export type SearchV2Response = AirweaveSDK.SearchV2Response;
export type SearchBreadcrumb = AirweaveSDK.SearchBreadcrumb;
export type SearchSystemMetadata = AirweaveSDK.SearchSystemMetadata;
export type SearchAccessControl = AirweaveSDK.SearchAccessControl;

// Request body types — re-exported from SDK
export type InstantSearchRequestBody = AirweaveSDK.InstantSearchRequest;
export type ClassicSearchRequestBody = AirweaveSDK.ClassicSearchRequest;
export type AgenticSearchRequestBody = AirweaveSDK.AgenticSearchRequest;
