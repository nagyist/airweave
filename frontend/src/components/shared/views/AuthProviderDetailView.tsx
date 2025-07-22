import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { DialogViewProps } from "../DialogFlow";
import { useTheme } from "@/lib/theme-provider";
import { cn } from "@/lib/utils";
import { apiClient } from "@/lib/api";
import { Copy, Check, Trash, Pencil } from "lucide-react";
import { toast } from "sonner";
import { getAuthProviderIconUrl } from "@/lib/utils/icons";
import { format } from "date-fns";
import { useAuthProvidersStore } from "@/lib/stores/authProviders";
import { clearStoredErrorDetails } from "@/lib/error-utils";
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
import { Input } from "@/components/ui/input";

// Delete Connection Dialog component
interface DeleteConnectionDialogProps {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: () => void;
    connectionReadableId: string;
    confirmText: string;
    setConfirmText: (text: string) => void;
    isDeleting: boolean;
    isDark: boolean;
}

const DeleteConnectionDialog: React.FC<DeleteConnectionDialogProps> = ({
    open,
    onOpenChange,
    onConfirm,
    connectionReadableId,
    confirmText,
    setConfirmText,
    isDeleting,
    isDark
}) => (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
        <AlertDialogContent className={cn(
            "border-border",
            isDark ? "bg-card-solid text-foreground" : "bg-white"
        )}>
            <AlertDialogHeader>
                <AlertDialogTitle className="text-foreground">Delete Auth Provider Connection</AlertDialogTitle>
                <AlertDialogDescription className={isDark ? "text-gray-300" : "text-foreground"}>
                    <div className="mb-4">
                        This will permanently delete this auth provider connection.
                    </div>
                    <div className="mb-4 font-semibold text-red-500">
                        Warning: All source connections that were created using this auth provider will also be deleted,
                        as they will no longer work without this connection.
                    </div>
                    <div className="mb-4">
                        This action cannot be undone.
                    </div>

                    <div className="mt-4">
                        <label htmlFor="confirm-delete" className="text-sm font-medium block mb-2">
                            Type <span className="font-bold">{connectionReadableId}</span> to confirm deletion
                        </label>
                        <Input
                            id="confirm-delete"
                            value={confirmText}
                            onChange={(e) => setConfirmText(e.target.value)}
                            className="w-full"
                            placeholder={connectionReadableId}
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
                    disabled={confirmText !== connectionReadableId || isDeleting}
                    className="bg-red-600 text-white hover:bg-red-700 dark:bg-red-500 dark:text-white dark:hover:bg-red-600 disabled:opacity-50"
                >
                    {isDeleting ? "Deleting..." : "Delete Connection"}
                </AlertDialogAction>
            </AlertDialogFooter>
        </AlertDialogContent>
    </AlertDialog>
);

export interface AuthProviderDetailViewProps extends DialogViewProps {
    viewData?: {
        authProviderConnectionId?: string;
        authProviderName?: string;
        authProviderShortName?: string;
        dialogId?: string;
        [key: string]: any;
    };
}

export const AuthProviderDetailView: React.FC<AuthProviderDetailViewProps> = ({
    onCancel,
    onComplete,
    viewData = {},
    onError,
}) => {
    const { authProviderConnectionId, authProviderName, authProviderShortName } = viewData;
    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === 'dark';
    const { fetchAuthProviderConnections } = useAuthProvidersStore();

    const [loading, setLoading] = useState(true);
    const [connectionDetails, setConnectionDetails] = useState<any>(null);
    const [copiedField, setCopiedField] = useState<string | null>(null);

    // Delete dialog state
    const [showDeleteDialog, setShowDeleteDialog] = useState(false);
    const [confirmText, setConfirmText] = useState('');
    const [isDeleting, setIsDeleting] = useState(false);
    const [isClosing, setIsClosing] = useState(false);

    console.log('🔍 [AuthProviderDetailView] Component mounted with:', {
        authProviderConnectionId,
        authProviderName,
        authProviderShortName,
        viewData
    });

    // Log component lifecycle
    useEffect(() => {
        console.log('🌟 [AuthProviderDetailView] useEffect mount check');

        return () => {
            console.log('💥 [AuthProviderDetailView] Component unmounting');
            // Clear any stored errors when component unmounts
            clearStoredErrorDetails();
        };
    }, []);

    // Fetch connection details
    useEffect(() => {
        if (!authProviderConnectionId || isClosing) {
            if (!authProviderConnectionId) {
                console.warn('⚠️ [AuthProviderDetailView] No authProviderConnectionId provided');
            }
            return;
        }

        let isMounted = true;

        const fetchConnectionDetails = async () => {
            console.log('📡 [AuthProviderDetailView] Fetching connection details for:', authProviderConnectionId);
            setLoading(true);
            try {
                const response = await apiClient.get(`/auth-providers/connections/${authProviderConnectionId}`);
                if (response.ok && isMounted) {
                    const data = await response.json();
                    setConnectionDetails(data);
                } else if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`Failed to load connection details: ${errorText}`);
                }
            } catch (error) {
                if (isMounted && !isClosing) {
                    console.error("Error fetching connection details:", error);
                    if (onError) {
                        onError(error instanceof Error ? error : new Error(String(error)), authProviderName);
                    }
                }
            } finally {
                if (isMounted) {
                    setLoading(false);
                }
            }
        };

        fetchConnectionDetails();

        return () => {
            isMounted = false;
        };
    }, [authProviderConnectionId, authProviderName, onError, isClosing]);

    const handleCopy = async (value: string, fieldName: string) => {
        try {
            await navigator.clipboard.writeText(value);
            setCopiedField(fieldName);
            toast.success(`${fieldName} copied to clipboard`);

            // Reset copied state after 2 seconds
            setTimeout(() => {
                setCopiedField(null);
            }, 2000);
        } catch (error) {
            toast.error("Failed to copy to clipboard");
        }
    };

    const formatDate = (dateString: string) => {
        try {
            return format(new Date(dateString), "PPP 'at' p");
        } catch {
            return dateString;
        }
    };

    const handleDeleteConnection = async () => {
        if (!authProviderConnectionId || confirmText !== connectionDetails?.readable_id) return;

        setIsDeleting(true);
        setIsClosing(true);  // Prevent any further fetches

        try {
            const response = await apiClient.delete(`/auth-providers/${authProviderConnectionId}`);

            if (response.ok) {
                toast.success("Auth provider connection deleted successfully");

                // Clear any stored error details to prevent error dialog on navigation
                clearStoredErrorDetails();

                // Close the delete dialog immediately
                setShowDeleteDialog(false);
                setConfirmText('');

                // Refresh auth provider connections in the store
                await fetchAuthProviderConnections();

                // Close the main dialog using onComplete to properly cleanup
                if (onComplete) {
                    onComplete({ success: true, action: 'deleted' });
                }

                // Navigate will be handled by the parent component after dialog closes
            } else {
                const errorText = await response.text();
                throw new Error(`Failed to delete auth provider connection: ${errorText}`);
            }
        } catch (err) {
            console.error("Error deleting auth provider connection:", err);
            toast.error(err instanceof Error ? err.message : "Failed to delete auth provider connection");
            setIsDeleting(false);
            setShowDeleteDialog(false);
            setConfirmText(''); // Reset confirm text
        }
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center py-8">
                <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full"></div>
            </div>
        );
    }

    if (!connectionDetails) {
        return (
            <div className="text-center py-8 text-muted-foreground">
                <p>No connection details available</p>
            </div>
        );
    }

    const DeleteConnectionDialogComponent = (
        <DeleteConnectionDialog
            open={showDeleteDialog}
            onOpenChange={setShowDeleteDialog}
            onConfirm={handleDeleteConnection}
            connectionReadableId={connectionDetails?.readable_id}
            confirmText={confirmText}
            setConfirmText={setConfirmText}
            isDeleting={isDeleting}
            isDark={isDark}
        />
    );

    const CopyButton = ({ value, fieldName }: { value: string; fieldName: string }) => (
        <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={() => handleCopy(value, fieldName)}
        >
            {copiedField === fieldName ? (
                <Check className="h-4 w-4 text-green-500" />
            ) : (
                <Copy className="h-4 w-4" />
            )}
        </Button>
    );

    return (
        <div className="flex flex-col h-full">
            {/* Content area - scrollable */}
            <div className="flex-grow overflow-y-auto">
                <div className="p-8 h-full flex flex-col">
                    {/* Heading with delete button */}
                    <div className="flex items-start justify-between mb-4">
                        <div>
                            <DialogTitle className="text-4xl font-semibold text-left">
                                {authProviderName} Connection Details
                            </DialogTitle>
                            <DialogDescription className="text-sm text-muted-foreground mt-2">
                                View details for your {authProviderName} connection.
                            </DialogDescription>
                        </div>

                        {/* Action buttons */}
                        <div className="flex gap-2">
                            {/* Edit button */}
                            <div
                                onClick={() => {
                                    // Close current dialog and open edit dialog
                                    if (onComplete) {
                                        onComplete({
                                            action: 'edit',
                                            authProviderConnectionId,
                                            authProviderName,
                                            authProviderShortName
                                        });
                                    }
                                }}
                                className={cn(
                                    "h-10 w-10 rounded-full transition-all duration-200 flex items-center justify-center cursor-pointer border-2 border-transparent",
                                    isDark
                                        ? "hover:bg-gray-800 hover:border-gray-600"
                                        : "hover:bg-gray-50 hover:border-gray-300"
                                )}
                                title="Edit connection"
                                role="button"
                                tabIndex={0}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter' || e.key === ' ') {
                                        e.preventDefault();
                                        // Close current dialog and open edit dialog
                                        if (onComplete) {
                                            onComplete({
                                                action: 'edit',
                                                authProviderConnectionId,
                                                authProviderName,
                                                authProviderShortName
                                            });
                                        }
                                    }
                                }}
                            >
                                <Pencil className="h-5 w-5 text-gray-600 hover:text-blue-500" />
                            </div>

                            {/* Delete button */}
                            <div
                                onClick={() => setShowDeleteDialog(true)}
                                className={cn(
                                    "h-10 w-10 rounded-full transition-all duration-200 flex items-center justify-center cursor-pointer border-2 border-transparent",
                                    isDark
                                        ? "hover:bg-gray-800 hover:border-gray-600"
                                        : "hover:bg-gray-50 hover:border-gray-300"
                                )}
                                title="Delete connection"
                                role="button"
                                tabIndex={0}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter' || e.key === ' ') {
                                        e.preventDefault();
                                        setShowDeleteDialog(true);
                                    }
                                }}
                            >
                                <Trash className="h-5 w-5 text-gray-600 hover:text-red-500" />
                            </div>
                        </div>
                    </div>

                    {/* Small spacer to push emblem down 10% */}
                    <div style={{ height: "5%" }}></div>

                    {/* Auth Provider Icon - make much larger */}
                    {authProviderShortName && (
                        <div className="flex justify-center items-center mb-6" style={{ minHeight: "20%" }}>
                            <div className={cn(
                                "w-64 h-64 flex items-center justify-center border rounded-lg p-2",
                                isDark ? "border-gray-700" : "border-gray-300"
                            )}>
                                <img
                                    src={getAuthProviderIconUrl(authProviderShortName, resolvedTheme)}
                                    alt={`${authProviderName} icon`}
                                    className="w-full h-full object-contain"
                                    onError={(e) => {
                                        e.currentTarget.style.display = 'none';
                                        e.currentTarget.parentElement!.innerHTML = `
                                            <div class="w-full h-full rounded-lg flex items-center justify-center ${isDark ? 'bg-blue-900' : 'bg-blue-100'}">
                                                <span class="text-5xl font-bold ${isDark ? 'text-blue-400' : 'text-blue-600'}">
                                                    ${authProviderShortName.substring(0, 2).toUpperCase()}
                                                </span>
                                            </div>
                                        `;
                                    }}
                                />
                            </div>
                        </div>
                    )}

                    {/* Reduced spacer to bring details up closer to emblem */}
                    <div className="flex-grow" style={{ minHeight: "15%" }}></div>

                    {/* Connection Details - positioned closer to emblem */}
                    <div className="px-2 max-w-md mx-auto w-full">
                        <div className="space-y-6">
                            {/* Name */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Name</label>
                                <div className={cn(
                                    "w-full py-2 px-3 rounded-md border bg-transparent",
                                    isDark ? "border-gray-700" : "border-gray-300"
                                )}>
                                    <span className="text-sm">{connectionDetails.name}</span>
                                </div>
                            </div>

                            {/* Readable ID */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Readable ID</label>
                                <div className={cn(
                                    "w-full py-2 px-3 rounded-md border bg-transparent flex items-center justify-between",
                                    isDark ? "border-gray-700" : "border-gray-300"
                                )}>
                                    <span className="text-sm font-mono break-all">{connectionDetails.readable_id}</span>
                                    <CopyButton value={connectionDetails.readable_id} fieldName="Readable ID" />
                                </div>
                            </div>

                            {/* Created By */}
                            {connectionDetails.created_by_email && (
                                <div className="space-y-1">
                                    <label className="text-sm font-medium ml-1">Created By</label>
                                    <div className={cn(
                                        "w-full py-2 px-3 rounded-md border bg-transparent",
                                        isDark ? "border-gray-700" : "border-gray-300"
                                    )}>
                                        <span className="text-sm">{connectionDetails.created_by_email}</span>
                                    </div>
                                </div>
                            )}

                            {/* Created At */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Created At</label>
                                <div className={cn(
                                    "w-full py-2 px-3 rounded-md border bg-transparent",
                                    isDark ? "border-gray-700" : "border-gray-300"
                                )}>
                                    <span className="text-sm">{formatDate(connectionDetails.created_at)}</span>
                                </div>
                            </div>

                            {/* Modified By */}
                            {connectionDetails.modified_by_email && (
                                <div className="space-y-1">
                                    <label className="text-sm font-medium ml-1">Modified By</label>
                                    <div className={cn(
                                        "w-full py-2 px-3 rounded-md border bg-transparent",
                                        isDark ? "border-gray-700" : "border-gray-300"
                                    )}>
                                        <span className="text-sm">{connectionDetails.modified_by_email}</span>
                                    </div>
                                </div>
                            )}

                            {/* Modified At */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Modified At</label>
                                <div className={cn(
                                    "w-full py-2 px-3 rounded-md border bg-transparent",
                                    isDark ? "border-gray-700" : "border-gray-300"
                                )}>
                                    <span className="text-sm">{formatDate(connectionDetails.modified_at)}</span>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Small spacer at bottom */}
                    <div style={{ height: "5%" }}></div>
                </div>
            </div>

            {/* Footer - fixed at bottom */}
            <div className="flex-shrink-0 border-t">
                <DialogFooter className="flex justify-end gap-3 p-6">
                    <Button
                        type="button"
                        variant="outline"
                        onClick={onCancel}
                        className={cn(
                            "px-6",
                            isDark ? "border-gray-700 hover:bg-gray-800" : "border-gray-300 hover:bg-gray-100"
                        )}
                    >
                        Close
                    </Button>
                </DialogFooter>
            </div>

            {/* Delete Connection Dialog */}
            {DeleteConnectionDialogComponent}
        </div>
    );
};

export default AuthProviderDetailView;
