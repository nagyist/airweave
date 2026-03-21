import React, { useState, useCallback, useMemo } from 'react';
import { cn } from '@/lib/utils';
import { ChevronDown, ChevronRight, ExternalLink, Copy, Check, Download, FileText } from 'lucide-react';
import { getAppIconUrl } from '@/lib/utils/icons';
import { useTheme } from '@/lib/theme-provider';

interface EntityResultCardProps {
    result: any;
    index: number;
    isDark: boolean;
    onEntityIdClick?: (entityId: string) => void;
}

// ── Helpers ───────────────────────────────────────────────────────────

function formatDate(dateString: string): string {
    try {
        const date = new Date(dateString);
        if (isNaN(date.getTime())) return dateString;
        return date.toLocaleDateString('en-US', {
            year: 'numeric', month: 'short', day: 'numeric',
        });
    } catch {
        return dateString;
    }
}

function formatContentSize(text: string): string {
    const len = text.length;
    if (len < 1000) return `${len} chars`;
    return `${(len / 1000).toFixed(1)}k chars`;
}

interface BreadcrumbData {
    entity_id: string;
    name: string;
    entity_type: string;
}

interface ExtractedFields {
    sourceName: string;
    entityType: string;
    breadcrumbs: BreadcrumbData[];
    webUrl: string | undefined;
    downloadUrl: string | undefined;
    hasDownload: boolean;
    entityId: string;
    name: string;
    textualRepresentation: string;
    updatedAt: string | undefined;
    createdAt: string | undefined;
}

function extractFields(result: any): ExtractedFields {
    const sysMetadata = result.airweave_system_metadata || result.system_metadata || {};
    const sourceName = sysMetadata.source_name || 'unknown';
    const entityType = sysMetadata.entity_type || 'Entity';

    const rawBreadcrumbs = result.breadcrumbs || [];
    const breadcrumbs: BreadcrumbData[] = rawBreadcrumbs.map((b: any) => {
        if (typeof b === 'string') return { entity_id: '', name: b, entity_type: '' };
        return { entity_id: b.entity_id || '', name: b.name || '', entity_type: b.entity_type || '' };
    }).filter((b: BreadcrumbData) => b.name);

    const sourceFields = result.source_fields || {};
    const rawWebUrl = sourceFields.web_url || result.web_url;
    const rawDownloadUrl = sourceFields.url || result.url;
    // If only url exists (no web_url), promote it to webUrl so the link still renders
    const webUrl = rawWebUrl || rawDownloadUrl;
    const downloadUrl = rawDownloadUrl;
    const hasDownload = Boolean(rawDownloadUrl && rawWebUrl && rawDownloadUrl !== rawWebUrl);

    return {
        sourceName,
        entityType,
        breadcrumbs,
        webUrl,
        downloadUrl,
        hasDownload,
        entityId: result.entity_id || '',
        name: result.name || 'Untitled',
        textualRepresentation: result.textual_representation || '',
        updatedAt: result.updated_at,
        createdAt: result.created_at,
    };
}

// ── Source icon ────────────────────────────────────────────────────────

function SourceIcon({ sourceName, theme, className }: { sourceName: string; theme?: string; className?: string }) {
    const url = getAppIconUrl(sourceName, theme);
    return (
        <img
            src={url}
            alt={sourceName}
            className={cn("object-contain", className)}
            onError={(e) => { e.currentTarget.style.display = 'none'; }}
        />
    );
}

// ── Main component ────────────────────────────────────────────────────

const EntityResultCardComponent: React.FC<EntityResultCardProps> = ({
    result,
    index,
    isDark,
}) => {
    const { resolvedTheme } = useTheme();
    const [copiedField, setCopiedField] = useState<string | null>(null);
    const [contentExpanded, setContentExpanded] = useState(false);

    const handleCopy = useCallback(async (text: string, field: string) => {
        try {
            await navigator.clipboard.writeText(text);
            setCopiedField(field);
            setTimeout(() => setCopiedField(null), 2000);
        } catch (error) {
            console.error('Failed to copy:', error);
        }
    }, []);

    const fields = useMemo(() => extractFields(result), [result]);

    const muted = isDark ? 'text-gray-500' : 'text-gray-400';
    const label = isDark ? 'text-gray-600' : 'text-gray-400';
    const val = isDark ? 'text-gray-300' : 'text-gray-600';
    const linkColor = isDark
        ? 'text-blue-400 hover:text-blue-300'
        : 'text-blue-600 hover:text-blue-500';
    const separator = isDark ? 'text-gray-700' : 'text-gray-300';

    const hasLinks = fields.webUrl || fields.hasDownload;
    const hasTimestamps = fields.createdAt || fields.updatedAt;

    const sectionBorder = isDark ? 'border-gray-800/50' : 'border-gray-200/50';

    return (
        <div
            data-entity-id={fields.entityId}
            className={cn(
                "pl-3 pr-2 py-2.5 font-mono relative",
                index > 0 && "border-t-2",
                isDark ? "border-t-gray-700" : "border-t-gray-300"
            )}
        >
            {/* ── Copy raw — top right ── */}
            <button
                onClick={() => handleCopy(JSON.stringify(result, null, 2), 'json')}
                className={cn(
                    "absolute top-2.5 right-2 inline-flex items-center gap-1 text-[10px] font-mono transition-colors",
                    isDark ? "text-gray-600 hover:text-gray-400" : "text-gray-400 hover:text-gray-600"
                )}
                title="Copy full entity as JSON"
            >
                {copiedField === 'json'
                    ? <><Check className="h-2.5 w-2.5" /> copied</>
                    : <><Copy className="h-2.5 w-2.5" /> copy raw</>
                }
            </button>

            {/* ── Section 1: Header + identity ── */}
            <div>
                <div className="flex items-center gap-2 pr-16">
                    <SourceIcon sourceName={fields.sourceName} theme={resolvedTheme} className="h-4 w-4 shrink-0" />
                    <span className={cn("text-[11px] font-medium truncate", isDark ? "text-gray-200" : "text-gray-800")}>
                        {fields.name}
                    </span>
                </div>
                <div className={cn("flex items-center gap-0 mt-0.5 text-[10px]", muted)}>
                    <span className={val}>{fields.sourceName}</span>
                    <span className={cn("mx-1", label)}>·</span>
                    <span className={val}>{fields.entityType}</span>
                    <span className={cn("mx-1", label)}>·</span>
                    <span className="opacity-60">{fields.entityId}</span>
                </div>
            </div>

            {/* ── Section 2: Links ── */}
            {hasLinks && (
                <div className={cn("mt-1 pt-1 border-t", sectionBorder)}>
                    <span className={cn("text-[10px] font-mono font-semibold", val)}>links</span>
                    <div className="flex items-center gap-3 mt-0.5">
                        {fields.webUrl && (
                            <a
                                href={fields.webUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className={cn("inline-flex items-center gap-1 text-[10px] font-mono transition-colors", linkColor)}
                            >
                                <ExternalLink className="h-3 w-3" />
                                Open in source
                            </a>
                        )}
                        {fields.hasDownload && (
                            <a
                                href={fields.downloadUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className={cn("inline-flex items-center gap-1 text-[10px] font-mono transition-colors", linkColor)}
                            >
                                <Download className="h-3 w-3" />
                                Download file
                            </a>
                        )}
                    </div>
                </div>
            )}

            {/* ── Section 3: Breadcrumbs (tree) ── */}
            {fields.breadcrumbs.length > 0 && (
                <div className={cn("mt-1 pt-1 border-t", sectionBorder)}>
                    <span className={cn("text-[10px] font-mono font-semibold", val)}>path</span>
                    <div className={cn("text-[10px] font-mono mt-0.5", muted)}>
                        {fields.breadcrumbs.map((crumb, i) => {
                            const isLast = i === fields.breadcrumbs.length - 1;
                            const prefix = i === 0 ? '' : '│   '.repeat(i - 1) + (isLast ? '└── ' : '├── ');
                            return (
                                <div key={i} className="leading-[18px] whitespace-pre">
                                    <span className={label}>{prefix}</span>
                                    <span className={val}>{crumb.name}</span>
                                    {crumb.entity_type && (
                                        <span className={label}> [{crumb.entity_type}]</span>
                                    )}
                                    {crumb.entity_id && (
                                        <span className={cn("ml-1 opacity-50", muted)}>{crumb.entity_id}</span>
                                    )}
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* ── Section 4: Timestamps ── */}
            {hasTimestamps && (
                <div className={cn("mt-1 pt-1 border-t", sectionBorder)}>
                    <span className={cn("text-[10px] font-mono font-semibold", val)}>dates</span>
                    <div className={cn("flex items-center gap-3 text-[10px] font-mono mt-0.5", muted)}>
                        {fields.createdAt && (
                            <span>
                                <span className={label}>created </span>
                                <span className={val}>{formatDate(fields.createdAt)}</span>
                            </span>
                        )}
                        {fields.createdAt && fields.updatedAt && (
                            <span className={label}>·</span>
                        )}
                        {fields.updatedAt && (
                            <span>
                                <span className={label}>updated </span>
                                <span className={val}>{formatDate(fields.updatedAt)}</span>
                            </span>
                        )}
                    </div>
                </div>
            )}

            {/* ── Section 5: Content expander ── */}
            {fields.textualRepresentation && (
                <div className={cn("mt-1 pt-1 border-t", sectionBorder)}>
                    <button
                        onClick={() => setContentExpanded(!contentExpanded)}
                        className={cn(
                            "inline-flex items-center gap-1 text-[12px] font-mono font-medium px-2 py-1 -ml-2 rounded-md transition-colors",
                            isDark
                                ? "text-gray-200 hover:text-white hover:bg-gray-800/60"
                                : "text-gray-700 hover:text-gray-900 hover:bg-gray-100/60"
                        )}
                    >
                        {contentExpanded
                            ? <ChevronDown className={cn("h-4 w-4", isDark ? "text-blue-400" : "text-blue-500")} />
                            : <ChevronRight className={cn("h-4 w-4", isDark ? "text-blue-400" : "text-blue-500")} />
                        }
                        <FileText className="h-3.5 w-3.5 -ml-0.5" strokeWidth={1.5} />
                        <span>Content</span>
                        <span className={cn("text-[10px] font-normal ml-1", muted)}>
                            {formatContentSize(fields.textualRepresentation)}
                        </span>
                    </button>

                    {contentExpanded && (
                        <pre className={cn(
                            "mt-1.5 text-[11px] font-mono leading-relaxed whitespace-pre-wrap",
                            "max-h-[400px] overflow-auto raw-data-scrollbar",
                            "pl-4 border-l-2 py-1",
                            isDark
                                ? "text-gray-400 border-gray-800"
                                : "text-gray-600 border-gray-200"
                        )}>
                            {fields.textualRepresentation}
                        </pre>
                    )}
                </div>
            )}
        </div>
    );
};

const arePropsEqual = (prev: EntityResultCardProps, next: EntityResultCardProps) => {
    return (
        prev.index === next.index &&
        prev.isDark === next.isDark &&
        prev.result.entity_id === next.result.entity_id &&
        prev.result.relevance_score === next.result.relevance_score
    );
};

export const EntityResultCard = React.memo(EntityResultCardComponent, arePropsEqual);
