import { useState, useEffect, useRef, useCallback } from "react";
import { useParams, useNavigate, useSearchParams } from "react-router-dom";
import { Alert } from "@/components/ui/alert";
import { AlertCircle, RefreshCw, Pencil, Trash, Plus, Clock, Play, Plug, Copy, Check, Loader2 } from "lucide-react";
import { apiClient } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { toast } from "@/hooks/use-toast";
import { getAppIconUrl } from "@/lib/utils/icons";
import { useTheme } from "@/lib/theme-provider";
import { cn } from "@/lib/utils";
import { StatusBadge } from "@/components/ui/StatusBadge";
import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import SourceConnectionDetailView from "@/components/collection/SourceConnectionDetailView";
import { emitCollectionEvent, COLLECTION_DELETED } from "@/lib/events";
import { QueryToolAndLiveDoc } from '@/components/collection/QueryToolAndLiveDoc';
import { ConnectFlow } from '@/components/shared';

// DeleteCollectionDialog component
interface DeleteCollectionDialogProps {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: () => void;
    collectionReadableId: string;
    confirmText: string;
    setConfirmText: (text: string) => void;
}

const DeleteCollectionDialog = ({
    open,
    onOpenChange,
    onConfirm,
    collectionReadableId,
    confirmText,
    setConfirmText
}: DeleteCollectionDialogProps) => {
    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === 'dark';

    return (
        <AlertDialog open={open} onOpenChange={onOpenChange}>
            <AlertDialogContent className={cn(
                "border-border",
                isDark ? "bg-card-solid text-foreground" : "bg-white"
            )}>
                <AlertDialogHeader>
                    <AlertDialogTitle className="text-foreground">Delete Collection</AlertDialogTitle>
                    <AlertDialogDescription className={isDark ? "text-gray-300" : "text-foreground"}>
                        <p className="mb-4">This will permanently delete this collection and all its source connections. This action cannot be undone.</p>

                        <div className="mt-4">
                            <label htmlFor="confirm-delete" className="text-sm font-medium block mb-2">
                                Type <span className="font-bold">{collectionReadableId}</span> to confirm deletion
                            </label>
                            <Input
                                id="confirm-delete"
                                value={confirmText}
                                onChange={(e) => setConfirmText(e.target.value)}
                                className="w-full"
                                placeholder={collectionReadableId}
                            />
                        </div>
                    </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                    <AlertDialogCancel className={isDark ? "bg-gray-800 text-white hover:bg-gray-700" : ""}>
                        Cancel
                    </AlertDialogCancel>
                    <AlertDialogAction
                        onClick={onConfirm}
                        disabled={confirmText !== collectionReadableId}
                        className="bg-red-600 text-white hover:bg-red-700 dark:bg-red-500 dark:text-white dark:hover:bg-red-600 disabled:opacity-50"
                    >
                        Delete Collection
                    </AlertDialogAction>
                </AlertDialogFooter>
            </AlertDialogContent>
        </AlertDialog>
    );
};

interface Collection {
    name: string;
    readable_id: string;
    id: string;
    created_at: string;
    modified_at: string;
    organization_id: string;
    created_by_email: string;
    modified_by_email: string;
    status?: string;
}

interface SourceConnection {
    id: string;
    name: string;
    description?: string;
    short_name: string;
    config_fields?: Record<string, any>;
    sync_id?: string;
    organization_id: string;
    created_at: string;
    modified_at: string;
    connection_id?: string;
    collection: string;
    created_by_email: string;
    modified_by_email: string;
    auth_fields?: Record<string, any> | string;
    status?: string;
    latest_sync_job_status?: string;
    latest_sync_job_id?: string;
    latest_sync_job_started_at?: string;
    latest_sync_job_completed_at?: string;
    cron_schedule?: string;
}

const Collections = () => {
    /********************************************
     * COMPONENT STATE
     ********************************************/
    const { readable_id } = useParams();
    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === 'dark';
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const isFromOAuthSuccess = searchParams.get("connected") === "success";

    // Page state
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [isReloading, setIsReloading] = useState(false);

    // Collection state
    const [collection, setCollection] = useState<Collection | null>(null);
    const [isEditingName, setIsEditingName] = useState(false);
    const nameInputRef = useRef<HTMLDivElement>(null);

    // Source connection state
    const [sourceConnections, setSourceConnections] = useState<SourceConnection[]>([]);
    const [selectedConnection, setSelectedConnection] = useState<SourceConnection | null>(null);

    // Add state for delete dialog
    const [showDeleteDialog, setShowDeleteDialog] = useState(false);
    const [confirmText, setConfirmText] = useState('');
    const [isDeleting, setIsDeleting] = useState(false);

    // Add state for copy animation
    const [isCopied, setIsCopied] = useState(false);

    // Add state for add source dialog
    const [showAddSourceDialog, setShowAddSourceDialog] = useState(false);

    // Add state for refreshing all sources
    const [isRefreshingAll, setIsRefreshingAll] = useState(false);
    const [refreshingSourceIds, setRefreshingSourceIds] = useState<string[]>([]);

    /********************************************
     * API AND DATA FETCHING FUNCTIONS
     ********************************************/

    // Fetch source connections for a collection
    const fetchSourceConnections = async (collectionId: string) => {
        try {
            console.log("Fetching source connections for collection:", collectionId);
            const response = await apiClient.get(`/source-connections/?collection=${collectionId}`);

            if (response.ok) {
                const data = await response.json();
                console.log("Loaded source connections:", data);
                setSourceConnections(data);

                // Always select the first connection when loading a new collection
                if (data.length > 0) {
                    console.log("Auto-selecting first connection:", data[0]);
                    setSelectedConnection(data[0]);
                } else {
                    // If no connections, ensure selectedConnection is null
                    setSelectedConnection(null);
                    console.log("No connections to select");
                }
            } else {
                console.error("Failed to load source connections:", await response.text());
                setSourceConnections([]);
                setSelectedConnection(null);
            }
        } catch (err) {
            console.error("Error fetching source connections:", err);
            setSourceConnections([]);
            setSelectedConnection(null);
        } finally {
            setIsLoading(false);
        }
    };

    // Update the existing fetchCollection function to use our new function
    const fetchCollection = async () => {
        if (!readable_id) return;

        setIsLoading(true);
        setError(null);

        try {
            const response = await apiClient.get(`/collections/${readable_id}`);

            if (response.ok) {
                const data = await response.json();
                setCollection(data);
                // After successful collection fetch, fetch source connections
                fetchSourceConnections(data.readable_id);
            } else {
                if (response.status === 404) {
                    setError("Collection not found");
                } else {
                    const errorText = await response.text();
                    setError(`Failed to load collection: ${errorText}`);
                }
                setIsLoading(false);
            }
        } catch (err) {
            setError(`An error occurred: ${err instanceof Error ? err.message : String(err)}`);
            setIsLoading(false);
        }
    };

    /********************************************
     * UI EVENT HANDLERS
     ********************************************/

    // Update selected connection
    const handleSelectConnection = async (connection: SourceConnection) => {
        console.log("Manually selecting connection:", connection.id);
        setSelectedConnection(connection);
    };

    // Handle name editing
    const startEditingName = () => {
        setIsEditingName(true);
        setTimeout(() => {
            if (nameInputRef.current) {
                nameInputRef.current.innerText = collection?.name || "";
                // Place cursor at the end of text
                const range = document.createRange();
                const selection = window.getSelection();
                const textNode = nameInputRef.current.firstChild || nameInputRef.current;
                const textLength = nameInputRef.current.innerText.length;

                // Position at the end of text
                range.setStart(textNode, textLength);
                range.setEnd(textNode, textLength);

                if (selection) {
                    selection.removeAllRanges();
                    selection.addRange(range);
                }
                nameInputRef.current.focus();
            }
        }, 0);
    };

    const handleSaveNameChange = async () => {
        const newName = nameInputRef.current?.innerText.trim() || "";

        if (!newName || newName === collection?.name) {
            setIsEditingName(false);
            return;
        }

        try {
            const response = await apiClient.patch(`/collections/${readable_id}`, { name: newName });
            if (!response.ok) throw new Error("Failed to update collection name");

            // Update local state after successful API call
            setCollection(prev => prev ? { ...prev, name: newName } : null);
            setIsEditingName(false);

            toast({
                title: "Success",
                description: "Collection name updated successfully"
            });
        } catch (error) {
            console.error("Error updating collection name:", error);
            toast({
                title: "Error",
                description: "Failed to update collection name",
                variant: "destructive"
            });
            setIsEditingName(false);
        }
    };

    // Handle copy to clipboard
    const handleCopyId = () => {
        if (collection?.readable_id) {
            navigator.clipboard.writeText(collection.readable_id);
            setIsCopied(true);

            // Reset after animation completes
            setTimeout(() => {
                setIsCopied(false);
            }, 1500);

            toast({
                title: "Copied",
                description: "ID copied to clipboard"
            });
        }
    };

    /********************************************
     * UTILITY FUNCTIONS
     ********************************************/

    // Reload data
    const reloadData = async () => {
        if (!readable_id) return;

        setIsReloading(true);
        try {
            await fetchCollection();
        } finally {
            setIsReloading(false);
        }
    };

    /********************************************
     * SIDE EFFECTS
     ********************************************/

    // Initial data loading
    useEffect(() => {
        console.log(`\nFetching collection because of readable id change\n`)
        setSelectedConnection(null);
        fetchCollection();
    }, [readable_id]);

    // Handle collection deletion
    const handleDeleteCollection = async () => {
        if (!readable_id || confirmText !== readable_id) return;

        setIsDeleting(true);
        try {
            const response = await apiClient.delete(`/collections/${readable_id}`);

            if (response.ok) {
                // Emit event that collection was deleted
                emitCollectionEvent(COLLECTION_DELETED, { id: readable_id });

                toast({
                    title: "Success",
                    description: "Collection deleted successfully"
                });
                // Navigate back to dashboard after successful deletion
                navigate("/dashboard");
            } else {
                const errorText = await response.text();
                throw new Error(`Failed to delete collection: ${errorText}`);
            }
        } catch (err) {
            console.error("Error deleting collection:", err);
            toast({
                title: "Error",
                description: err instanceof Error ? err.message : "Failed to delete collection",
                variant: "destructive"
            });
        } finally {
            setIsDeleting(false);
            setShowDeleteDialog(false);
            setConfirmText(''); // Reset confirm text
        }
    };

    // Add right before the source connections section in render
    useEffect(() => {
        console.log("Source connections state:", {
            count: sourceConnections.length,
            connections: sourceConnections,
            selectedId: selectedConnection?.id
        });
    }, [sourceConnections, selectedConnection]);

    // Add this in the Source Connections Section right above the mapping
    console.log("Rendering source connections section", {
        count: sourceConnections.length,
        selectedId: selectedConnection?.id
    });

    // Handle refreshing all sources
    const handleRefreshAllSources = async () => {
        if (!collection?.readable_id) return;

        setIsRefreshingAll(true);
        try {
            const response = await apiClient.post(`/collections/${collection.readable_id}/refresh_all`);

            if (response.ok) {
                const jobs = await response.json();
                // Store IDs of sources being refreshed
                const sourceIds = jobs.map(job => job.source_connection_id);
                setRefreshingSourceIds(sourceIds);

                toast({
                    title: "Success",
                    description: `Started refreshing ${sourceIds.length} source connection${sourceIds.length !== 1 ? 's' : ''}`
                });

                // Reload data after starting the jobs
                reloadData();

                // If you have a currently selected connection, reload its data too
                if (selectedConnection) {
                    // Force a complete re-render by briefly setting selectedConnection to null
                    // This will ensure the SourceConnectionDetailView fully reloads
                    setSelectedConnection(null);
                    setTimeout(() => {
                        // After a brief delay, select the connection again
                        const reselect = sourceConnections.find(sc => sc.id === selectedConnection.id);
                        if (reselect) setSelectedConnection(reselect);
                        // Or reselect the first connection
                        else if (sourceConnections.length > 0) setSelectedConnection(sourceConnections[0]);
                    }, 50);
                }
            } else {
                throw new Error("Failed to refresh sources");
            }
        } catch (error) {
            console.error("Error refreshing sources:", error);
            toast({
                title: "Error",
                description: "Failed to refresh all sources",
                variant: "destructive"
            });
        }
    };

    const handleSubscriptionComplete = useCallback(() => {
        setRefreshingSourceIds(prev => {
            const newIds = prev.filter(id => id !== selectedConnection?.id);
            if (newIds.length === 0) {
                setIsRefreshingAll(false);
            }
            return newIds;
        });
    }, [selectedConnection?.id]);

    if (error) {
        return (
            <div className="container mx-auto py-6">
                <h1 className="text-3xl font-bold mb-6 text-foreground">Collection Error</h1>
                <Alert variant="destructive">
                    <AlertCircle className="h-4 w-4" />
                    <div className="font-medium">Error</div>
                    <div>{error}</div>
                </Alert>
                <div className="mt-4">
                    <Button onClick={() => navigate("/dashboard")}>
                        Return to Dashboard
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className={cn(
            "container mx-auto py-6",
            isDark ? "text-foreground" : ""
        )}>
            {/* Show loading state when isLoading or no collection data yet */}
            {(isLoading || !collection) && !error ? (
                <div className="w-full h-48 flex flex-col items-center justify-center space-y-4">
                    <Loader2 className="h-10 w-10 animate-spin text-primary" />
                    <p className="text-muted-foreground">Loading collection data...</p>
                </div>
            ) : (
                <>
                    {/* Header with Title and Status Badge */}
                    <div className="flex items-center justify-between py-4">
                        <div className="flex items-center gap-4">
                            {/* Source Icons */}
                            <div className="flex justify-start" style={{ minWidth: "4.5rem" }}>
                                {sourceConnections.map((connection, index) => (
                                    <div
                                        key={connection.id}
                                        className={cn(
                                            "w-14 h-14 rounded-md border p-1 flex items-center justify-center overflow-hidden",
                                            isDark ? "bg-gray-800 border-gray-700" : "bg-background border-gray-300"
                                        )}
                                        style={{
                                            marginLeft: index > 0 ? `-${Math.min(index * 8, 24)}px` : "0px",
                                            zIndex: sourceConnections.length - index
                                        }}
                                    >
                                        <img
                                            src={getAppIconUrl(connection.short_name, resolvedTheme)}
                                            alt={connection.name}
                                            className="max-w-full max-h-full w-auto h-auto object-contain"
                                        />
                                    </div>
                                ))}
                            </div>

                            <div className="flex flex-col justify-center">
                                {isEditingName ? (
                                    <div className="flex items-center gap-2">
                                        <div
                                            ref={nameInputRef}
                                            contentEditable
                                            className="text-3xl font-bold tracking-tight text-foreground outline-none p-1 pl-0 pr-3 rounded border border-border/40 transition-all duration-150"
                                            onKeyDown={(e) => {
                                                if (e.key === 'Enter') {
                                                    e.preventDefault();
                                                    handleSaveNameChange();
                                                }
                                                if (e.key === 'Escape') {
                                                    e.preventDefault();
                                                    setIsEditingName(false);
                                                }
                                            }}
                                            onBlur={handleSaveNameChange}
                                        />
                                    </div>
                                ) : (
                                    <div className="flex items-center gap-2">
                                        <h1 className="text-3xl font-bold tracking-tight text-foreground py-1 pl-0">{collection?.name}</h1>
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                                            onClick={startEditingName}
                                        >
                                            <Pencil className="h-3 w-3" />
                                        </Button>
                                        {collection?.status && (
                                            <StatusBadge status={collection.status} />
                                        )}
                                    </div>
                                )}
                                <p className="text-muted-foreground text-sm group relative flex items-center">
                                    {collection?.readable_id}
                                    <button
                                        className="ml-1.5 opacity-0 group-hover:opacity-100 transition-opacity focus:opacity-100 focus:outline-none"
                                        onClick={handleCopyId}
                                        title="Copy ID"
                                    >
                                        {isCopied ? (
                                            <Check className="h-3.5 w-3.5 text-muted-foreground  transition-all" />
                                        ) : (
                                            <Copy className="h-3.5 w-3.5 text-muted-foreground transition-all" />
                                        )}
                                    </button>
                                </p>
                            </div>
                        </div>

                        {/* Header action buttons */}
                        <div className="flex items-center gap-2">
                            <Button
                                variant="ghost"
                                size="icon"
                                onClick={reloadData}
                                disabled={isReloading}
                                className={cn(
                                    "h-8 w-8 rounded-full transition-all duration-200",
                                    isDark ? "hover:bg-gray-800" : "hover:bg-gray-100",
                                )}
                                title="Reload page"
                            >
                                <RefreshCw className={cn(
                                    "h-4 w-4 transition-transform duration-500",
                                    isReloading ? "animate-spin" : "hover:rotate-90"
                                )} />
                            </Button>

                            <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => setShowDeleteDialog(true)}
                                className={cn(
                                    "h-8 w-8 rounded-full transition-all duration-200",
                                    isDark ? "hover:bg-gray-800" : "hover:bg-gray-100"
                                )}
                                title="Delete collection"
                            >
                                <Trash className="h-4 w-4" />
                            </Button>
                        </div>
                    </div>

                    <div className="flex justify-end gap-2 mb-0">
                        <Button
                            variant="outline"
                            onClick={() => setShowAddSourceDialog(true)}
                            className={cn(
                                "gap-1 text-xs font-medium h-8 px-3",
                                isDark ? "border-gray-700 hover:bg-gray-800" : "border-gray-300 hover:bg-gray-100"
                            )}
                        >
                            <Plus className="h-3.5 w-3.5" />
                            Add Source
                        </Button>
                        <Button
                            variant="outline"
                            onClick={handleRefreshAllSources}
                            className={cn(
                                "gap-1 text-xs font-medium h-8 px-3",
                                isDark ? "border-gray-700 hover:bg-gray-800" : "border-gray-300 hover:bg-gray-100"
                            )}
                        >
                            <Plug className="h-3.5 w-3.5 mr-1" />
                            Refresh all sources
                        </Button>
                    </div>

                    {/* Add QueryToolAndLiveDoc when a connection with syncId is selected */}
                    {selectedConnection?.sync_id && (
                        <>
                            <div className='py-3 space-y-2 mt-1'>
                                <QueryToolAndLiveDoc
                                    collectionReadableId={collection?.readable_id || ''}
                                />
                            </div>
                        </>
                    )}

                    <hr className={cn(
                        "border-t my-2 max-w-full",
                        isDark ? "border-gray-700/30" : "border-gray-300/30"
                    )} />

                    {/* Source Connections Section */}
                    <div className="mt-6">
                        <h2 className="text-xl font-semibold tracking-tight mb-4 text-foreground">Source Connections</h2>

                        <div className="flex flex-wrap gap-3">
                            {sourceConnections.map((connection) => (
                                <div
                                    key={connection.id}
                                    className={cn(
                                        "h-10 flex items-center gap-2 overflow-hidden flex-shrink-0 flex-grow-0 px-3 py-2 rounded-md cursor-pointer transition-colors",
                                        selectedConnection?.id === connection.id
                                            ? "border-2 border-primary"
                                            : isDark
                                                ? "border border-gray-700 bg-gray-800/50 hover:bg-gray-700/70"
                                                : "border border-gray-300 hover:bg-gray-100"
                                    )}
                                    onClick={() => handleSelectConnection(connection)}
                                >
                                    <div className={cn(
                                        "rounded-md flex items-center justify-center overflow-hidden flex-shrink-0",
                                    )}>
                                        <img
                                            src={getAppIconUrl(connection.short_name, resolvedTheme)}
                                            alt={connection.name}
                                            className="h-6 w-6 object-contain"
                                        />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <span className="text-[16px] font-medium truncate block text-foreground">{connection.name}</span>
                                    </div>
                                </div>
                            ))}
                        </div>

                        {sourceConnections.length === 0 && (
                            <div className={cn(
                                "text-center py-6 rounded-md border",
                                isDark ? "border-gray-700 bg-gray-800/20 text-gray-400" : "border-gray-200 bg-gray-50 text-muted-foreground"
                            )}>
                                <p className="mb-2">No source connections found.</p>
                                <Button
                                    variant="outline"
                                    className={cn(
                                        "mt-2",
                                        isDark ? "border-gray-700 hover:bg-gray-800" : ""
                                    )}
                                    onClick={() => setShowAddSourceDialog(true)}
                                >
                                    <Plus className="h-4 w-4 mr-2" />
                                    Add a source connection
                                </Button>
                            </div>
                        )}
                    </div>

                    {/* Render SourceConnectionDetailView when a connection is selected */}
                    {selectedConnection && (
                        <SourceConnectionDetailView
                            sourceConnectionId={selectedConnection.id}
                            shouldForceSubscribe={refreshingSourceIds.includes(selectedConnection.id)}
                            onSubscriptionComplete={handleSubscriptionComplete}
                        />
                    )}

                    {/* Delete Collection Dialog */}
                    <DeleteCollectionDialog
                        open={showDeleteDialog}
                        onOpenChange={setShowDeleteDialog}
                        onConfirm={handleDeleteCollection}
                        collectionReadableId={collection?.readable_id || ''}
                        confirmText={confirmText}
                        setConfirmText={setConfirmText}
                    />

                    {/* ConnectFlow for adding a source to an existing collection
                        Setting mode="add-source" with collectionId/Name ensures it will:
                        1. Start with SourceSelectorView (skipping CreateCollectionView)
                        2. Use the existing collection instead of creating a new one
                        3. Only create a source connection, not a new collection
                    */}
                    {collection && (
                        <ConnectFlow
                            isOpen={showAddSourceDialog}
                            onOpenChange={setShowAddSourceDialog}
                            mode="add-source"
                            collectionId={collection.readable_id}
                            collectionName={collection.name}
                            onComplete={() => {
                                setShowAddSourceDialog(false);
                                reloadData();
                            }}
                        />
                    )}
                </>
            )}
        </div>
    );
};

export default Collections;
