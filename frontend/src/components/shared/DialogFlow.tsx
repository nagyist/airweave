import React, { useState, useEffect, useRef, useMemo } from "react";
import { Dialog, DialogContent, DialogPortal, DialogOverlay } from "@/components/ui/dialog";
import { useTheme } from "@/lib/theme-provider";
import { cn } from "@/lib/utils";
import { CreateCollectionView } from "./views/CreateCollectionView";
import { SourceSelectorView } from "./views/SourceSelectorView";
import { ConfigureSourceView } from "./views/ConfigureSourceView";
import { ConfigureAuthProviderView } from "./views/ConfigureAuthProviderView";
import { AuthProviderDetailView } from "./views/AuthProviderDetailView";
import { EditAuthProviderView } from "./views/EditAuthProviderView";
import { useNavigate, useSearchParams } from "react-router-dom";
import { redirectWithError, getStoredErrorDetails, clearStoredErrorDetails, CONNECTION_ERROR_STORAGE_KEY } from "@/lib/error-utils";
import { ConnectionErrorView } from "./views/ConnectionErrorView";

// Flow types
export type DialogMode = 'source-button' | 'add-source' | 'create-collection' | 'auth-provider' | 'auth-provider-detail' | 'auth-provider-edit';

interface DialogFlowProps {
    isOpen: boolean;
    onOpenChange: (open: boolean) => void;
    mode: DialogMode;
    sourceId?: string;
    sourceName?: string;
    sourceShortName?: string;
    collectionId?: string;
    collectionName?: string;
    authProviderId?: string;
    authProviderName?: string;
    authProviderShortName?: string;
    authProviderAuthType?: string;
    authProviderConnectionId?: string;
    onComplete?: (result?: any) => void;
    dialogId?: string;
    errorData?: any;
}

// Add this interface after DialogFlowProps
export interface DialogViewProps {
    onNext?: (data?: any) => void;
    onBack?: () => void;
    onCancel?: () => void;
    onComplete?: (data?: any) => void;
    viewData?: Record<string, any>;
    onError?: (error: Error | string, errorSource?: string) => void;
}

export const DialogFlow: React.FC<DialogFlowProps> = ({
    isOpen,
    onOpenChange,
    mode,
    sourceId,
    sourceName,
    sourceShortName,
    collectionId,
    collectionName,
    authProviderId,
    authProviderName,
    authProviderShortName,
    authProviderAuthType,
    authProviderConnectionId,
    onComplete,
    dialogId = "default", // Default ID if none provided
    errorData,
}) => {
    // Track render count
    const renderCountRef = useRef(0);
    renderCountRef.current += 1;

    // Immediate logging of isOpen prop
    console.log('🎬 [DialogFlow] Render with isOpen:', isOpen, 'dialogId:', dialogId, 'renderCount:', renderCountRef.current);

    // Log component lifecycle
    useEffect(() => {
        console.log('🌟 [DialogFlow] Component mounted:', {
            dialogId,
            mode,
            renderCount: renderCountRef.current
        });

        return () => {
            console.log('💥 [DialogFlow] Component unmounting:', dialogId);
        };
    }, []);

    console.log('🖼️ [DialogFlow] Rendering:', {
        dialogId,
        mode,
        renderCount: renderCountRef.current,
        currentStep: 'will be set below'
    });

    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === "dark";
    const [currentStep, setCurrentStep] = useState(0);
    const [viewData, setViewData] = useState<Record<string, any>>({});

    console.log('🎯 [DialogFlow] State values:', {
        dialogId,
        currentStep,
        hasAuthProviderConnectionId: !!viewData.authProviderConnectionId,
        renderCount: renderCountRef.current
    });
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const [showErrorView, setShowErrorView] = useState(false);
    const [errorViewData, setErrorViewData] = useState<any>(null);
    const [hasBeenForced, setHasBeenForced] = useState(false);

    // Add a ref to track if we've already processed restore
    const hasProcessedRestoreRef = useRef(false);
    // Add a new ref to track if dialog is being restored without reset
    const isRestoringRef = useRef(false);
    // Track if dialog was previously open
    const wasOpenRef = useRef(false);

    // Monitor dialog open/close state
    useEffect(() => {
        console.log('🚪 [DialogFlow] Dialog open state changed:', {
            dialogId,
            isOpen,
            mode,
            currentStep,
            hasAuthProviderConnectionId: !!viewData.authProviderConnectionId
        });
    }, [isOpen]);

    // Track dependency changes for reset useEffect
    const prevDepsRef = useRef<any>({});

    useEffect(() => {
        const deps = {
            isOpen,
            mode,
            sourceId,
            sourceName,
            sourceShortName,
            collectionId,
            collectionName,
            authProviderId,
            authProviderName,
            authProviderShortName,
            authProviderAuthType,
            authProviderConnectionId,
            dialogId
        };

        const changes: string[] = [];
        Object.keys(deps).forEach(key => {
            if (prevDepsRef.current[key] !== deps[key]) {
                changes.push(`${key}: ${prevDepsRef.current[key]} → ${deps[key]}`);
            }
        });

        if (changes.length > 0) {
            console.log('🔄 [DialogFlow] Reset useEffect dependencies changed:', changes);
        }

        prevDepsRef.current = deps;
    }, [isOpen, mode, sourceId, sourceName, sourceShortName, collectionId, collectionName, authProviderId, authProviderName, authProviderShortName, authProviderAuthType, authProviderConnectionId, dialogId]);

    // Reset state when dialog opens or mode changes
    useEffect(() => {
        console.log('🔍 [DialogFlow] Reset useEffect triggered:', {
            dialogId,
            isOpen,
            wasOpen: wasOpenRef.current,
            hasProcessedRestore: hasProcessedRestoreRef.current,
            isRestoring: isRestoringRef.current,
            willReset: isOpen && !wasOpenRef.current && !hasProcessedRestoreRef.current && !isRestoringRef.current
        });

        // Only reset when transitioning from closed to open
        if (isOpen && !wasOpenRef.current && !hasProcessedRestoreRef.current && !isRestoringRef.current) {
            console.log('🔄 [DialogFlow] Initializing dialog state on open:', {
                dialogId,
                mode,
                currentStep,
                isOpen,
                hasProcessedRestore: hasProcessedRestoreRef.current,
                isRestoring: isRestoringRef.current
            });
            setCurrentStep(0);
            setViewData({
                sourceId,
                sourceName,
                sourceShortName,
                collectionId,
                collectionName,
                authProviderId,
                authProviderName,
                authProviderShortName,
                authProviderAuthType,
                authProviderConnectionId,
                dialogId
            });
        }

        // Update ref for next render
        console.log('📍 [DialogFlow] Updating wasOpenRef from', wasOpenRef.current, 'to', isOpen);
        wasOpenRef.current = isOpen;
    }, [isOpen, mode, sourceId, sourceName, sourceShortName, collectionId, collectionName, authProviderId, authProviderName, authProviderShortName, authProviderAuthType, authProviderConnectionId, dialogId]);

    // Handle direct error data passed via props
    useEffect(() => {
        if (errorData && isOpen) {
            console.log("🔔 [DialogFlow] Direct error data provided:", errorData);
            setErrorViewData({
                serviceName: errorData.serviceName || sourceName || "the service",
                sourceShortName: errorData.sourceShortName || sourceShortName || viewData.sourceShortName || "unknown",
                errorMessage: errorData.errorMessage || "Connection failed",
                errorDetails: errorData.errorDetails
            });
            setShowErrorView(true);
        }
    }, [errorData, isOpen, sourceName, sourceShortName, viewData]);

    // SINGLE error handling and dialog restoration effect
    useEffect(() => {
        const restoreDialog = searchParams.get('restore_dialog') === 'true';
        const oauthError = searchParams.get('oauth_error') === 'true';
        const connectionError = searchParams.get('connected') === 'error';

        // Handle dialog restoration
        if ((restoreDialog || oauthError) && !hasProcessedRestoreRef.current) {
            isRestoringRef.current = true;

            const newUrl = window.location.pathname;
            window.history.replaceState({}, '', newUrl);

            const savedStateJson = sessionStorage.getItem('oauth_dialog_state');
            if (savedStateJson) {
                try {
                    const savedState = JSON.parse(savedStateJson);
                    console.log("🔄 Restoring dialog state:", savedState);
                    console.log("⚠️ Important values:", {
                        credentialId: savedState.credentialId,
                        isAuthenticated: savedState.isAuthenticated,
                        currentStep: savedState.currentStep,
                        dialogFlowStep: savedState.dialogFlowStep,
                        dialogId: savedState.dialogId
                    });

                    if (savedState.dialogId && savedState.dialogId === dialogId) {
                        console.log(`🎯 Dialog ID match: ${dialogId}, restoring this dialog instance`);

                        const restoredStep = typeof savedState.dialogFlowStep === 'number' ? savedState.dialogFlowStep : 1;
                        setCurrentStep(restoredStep);
                        console.log(`🔢 Setting current step to: ${restoredStep}`);

                        setViewData({
                            ...savedState,
                            dialogId: savedState.dialogId,
                            dialogFlowStep: restoredStep,
                            dialogMode: savedState.dialogMode || mode
                        });

                        setTimeout(() => {
                            console.log(`🚨 Forcing dialog open with dialogId: ${dialogId}`);
                            setHasBeenForced(true);
                            hasProcessedRestoreRef.current = true;
                            onOpenChange(true);
                            sessionStorage.removeItem('oauth_dialog_state');

                            setTimeout(() => {
                                isRestoringRef.current = false;
                                console.log(`✅ Restoration complete for dialog: ${dialogId}`);
                            }, 50);
                        }, 50);
                    } else {
                        console.log(`⏭️ Dialog ID mismatch: saved=${savedState.dialogId}, current=${dialogId}, skipping restore`);
                        isRestoringRef.current = false;
                    }

                    console.log("📊 FULL SAVED STATE IN DIALOG FLOW:", JSON.stringify(savedState, null, 2));
                    console.log("📊 FULL VIEW DATA AFTER RESTORE:", JSON.stringify(viewData, null, 2));
                } catch (error) {
                    console.error("❌ Error restoring dialog state:", error);
                    isRestoringRef.current = false;

                    setErrorViewData({
                        serviceName: sourceName || "Dialog",
                        sourceShortName: sourceShortName || "dialog",
                        errorMessage: "Failed to restore dialog state",
                        errorDetails: error instanceof Error ? error.stack : String(error)
                    });
                    setShowErrorView(true);
                    onOpenChange(true);
                }
            } else {
                isRestoringRef.current = false;
            }
        }
        // Handle connection errors - ONLY for the matching dialogId
        else if (connectionError) {
            console.log("🔍 Checking for stored error details");
            const errorDetails = getStoredErrorDetails();

            if (errorDetails) {
                console.log("🔔 Found error details in localStorage:",
                    { type: typeof errorDetails, hasMessage: !!errorDetails.errorMessage });

                // ONLY open THIS dialog if the error was meant for it
                if (dialogId && errorDetails.dialogId === dialogId) {
                    const errorMessage = errorDetails.errorMessage || "Connection failed";
                    let errorDetailsText = errorDetails.errorDetails || "";

                    if (errorMessage && typeof errorMessage === 'string' && errorMessage.includes('{')) {
                        try {
                            const jsonStart = errorMessage.indexOf('{');
                            const jsonEnd = errorMessage.lastIndexOf('}') + 1;
                            if (jsonStart > -1 && jsonEnd > jsonStart) {
                                const jsonStr = errorMessage.substring(jsonStart, jsonEnd);
                                const parsed = JSON.parse(jsonStr);
                                if (parsed.detail) {
                                    errorDetailsText = parsed.detail;
                                }
                            }
                        } catch (e) {
                            console.error("Error parsing JSON from error message:", e);
                        }
                    }

                    setErrorViewData({
                        serviceName: errorDetails.serviceName || sourceName || "the service",
                        sourceShortName: errorDetails.sourceShortName ||
                            sourceShortName ||
                            viewData.sourceShortName ||
                            "unknown",
                        errorMessage: errorMessage,
                        errorDetails: errorDetailsText
                    });

                    setShowErrorView(true);
                    onOpenChange(true);

                    setTimeout(() => {
                        if (isOpen && showErrorView) {
                            const newUrl = window.location.pathname;
                            window.history.replaceState({}, '', newUrl);
                        }
                    }, 500);
                }
            }

            // Clean up URL regardless
            setTimeout(() => {
                const newUrl = window.location.pathname;
                window.history.replaceState({}, '', newUrl);
            }, 100);
        }
    }, [searchParams, onOpenChange, isOpen, dialogId, mode, sourceName, sourceShortName, showErrorView]);

    // Add a direct effect on isOpen to log when it changes
    useEffect(() => {
        console.log(`🔍 Dialog isOpen changed for dialogId=${dialogId}: ${isOpen}`);

        // If dialog was open and is now closing (not due to OAuth transition)
        if (!isOpen && hasBeenForced) {
            // Call handleCancel to properly clean up state
            handleCancel();
        }
    }, [isOpen, dialogId, hasBeenForced]);

    // Define flow sequences based on mode
    const flowSequences = {
        "source-button": ["createCollection", "connectSource"],
        "add-source": ["sourceSelector", "connectSource"],
        "create-collection": ["sourceSelector", "createCollection", "connectSource"],
        "auth-provider": ["configureAuthProvider", "authProviderDetail"],
        "auth-provider-detail": ["authProviderDetail"],
        "auth-provider-edit": ["editAuthProvider", "authProviderDetail"]
    };

    // Get current flow sequence
    const currentFlow = flowSequences[mode as keyof typeof flowSequences];

    // Get current view based on flow and step
    const currentView = useMemo(() => {
        if (showErrorView) {
            console.log('⚠️ [DialogFlow] Showing error view');
            return "error";
        }
        if (!currentFlow || currentStep >= currentFlow.length) {
            console.log('⚠️ [DialogFlow] No valid view: currentFlow =', currentFlow, ', currentStep =', currentStep);
            return null;
        }
        const view = currentFlow[currentStep];
        console.log('👁️ [DialogFlow] Current view calculated:', view, 'at step', currentStep, 'of', currentFlow);
        return view;
    }, [currentFlow, currentStep, showErrorView]);

    // Log flow calculation
    console.log('🎯 [DialogFlow] Flow calculation:', {
        dialogId,
        mode,
        currentStep,
        currentFlow,
        currentView,
        flowSequences: flowSequences[mode as keyof typeof flowSequences]
    });

    // Effect for opening dialog with populated credential info
    useEffect(() => {
        if (viewData.credentialId && !isOpen && hasProcessedRestoreRef.current && !viewData.isCompleted) {
            console.log(`🔔 Opening dialog ${dialogId} with populated viewData:`, JSON.stringify(viewData, null, 2));
            onOpenChange(true);
        }
    }, [viewData, isOpen, onOpenChange, dialogId]);

    // Effect for resetting authentication
    useEffect(() => {
        if (viewData.isAuthenticated && !viewData.credentialId) {
            setViewData(prevData => ({
                ...prevData,
                isAuthenticated: false
            }));
        }
    }, [viewData.isAuthenticated]);

    // Effect for handling dialog closing
    useEffect(() => {
        if (!isOpen && hasProcessedRestoreRef.current) {
            console.log('🚪 Dialog closed, cleaning up state');
            hasProcessedRestoreRef.current = false;
            isRestoringRef.current = false;

            if (showErrorView) {
                setShowErrorView(false);
                setErrorViewData(null);
            }
        }
    }, [isOpen, showErrorView]);

    // Log currentStep changes
    useEffect(() => {
        console.log('📊 [DialogFlow] currentStep changed:', {
            dialogId,
            currentStep,
            mode,
            currentFlow: flowSequences[mode as keyof typeof flowSequences],
            currentView: currentFlow ? currentFlow[currentStep] : null
        });
    }, [currentStep]);

    // Log viewData changes
    useEffect(() => {
        console.log('📦 [DialogFlow] viewData changed:', {
            dialogId,
            hasAuthProviderConnectionId: !!viewData.authProviderConnectionId,
            authProviderConnectionId: viewData.authProviderConnectionId,
            keys: Object.keys(viewData)
        });
    }, [viewData]);

    // Render view function
    const renderView = () => {
        console.log('🎨 [DialogFlow] renderView called:', {
            currentView,
            currentStep,
            mode,
            dialogId,
            showErrorView,
            currentFlow,
            viewData: {
                authProviderConnectionId: viewData.authProviderConnectionId,
                authProviderName: viewData.authProviderName,
                authProviderShortName: viewData.authProviderShortName
            }
        });

        if (showErrorView || currentView === "error") {
            return (
                <ConnectionErrorView
                    onCancel={handleCancel}
                    viewData={errorViewData}
                />
            );
        }

        if (currentView === null) {
            return (
                <ConnectionErrorView
                    onCancel={handleCancel}
                    viewData={{
                        serviceName: "Dialog Flow",
                        sourceShortName: "dialog",
                        errorMessage: `Configuration error in dialog flow`,
                        errorDetails: `Invalid mode: ${mode} or missing flow sequence`
                    }}
                />
            );
        }

        const commonProps = {
            onNext: handleNext,
            onCancel: handleCancel,
            onComplete: handleComplete,
            viewData: {
                sourceId,
                sourceName,
                sourceShortName,
                collectionId,
                collectionName,
                dialogId,
                dialogFlowStep: currentStep,
                dialogMode: mode,
                // Add auth provider specific data
                authProviderId,
                authProviderName,
                authProviderShortName,
                authProviderAuthType,
                authProviderConnectionId,
                // Spread viewData last so it can override any of the above
                ...viewData,
            } as any,
            onError: handleError,
        };

        console.log('🔧 [DialogFlow] Building commonProps for view:', {
            view: currentView,
            step: currentStep,
            viewDataKeys: Object.keys(commonProps.viewData),
            hasAuthProviderConnectionId: !!commonProps.viewData.authProviderConnectionId,
            authProviderConnectionIdValue: commonProps.viewData.authProviderConnectionId
        });

        if (currentStep > 0) {
            (commonProps as any).onBack = handleBack;
        }

        switch (currentView) {
            case "sourceSelector":
                return <SourceSelectorView {...commonProps} />;
            case "createCollection":
                return <CreateCollectionView {...commonProps} />;
            case "connectSource":
                return <ConfigureSourceView {...commonProps} />;
            case "configureAuthProvider":
                return <ConfigureAuthProviderView {...commonProps} />;
            case 'authProviderDetail':
                return <AuthProviderDetailView {...commonProps} />;
            case 'editAuthProvider':
                return <EditAuthProviderView {...commonProps} />;
            default:
                return (
                    <ConnectionErrorView
                        onCancel={handleCancel}
                        viewData={{
                            serviceName: "Dialog Flow",
                            sourceShortName: "dialog",
                            errorMessage: `Unknown view: ${currentView}`,
                            errorDetails: `Available views: sourceSelector, createCollection, connectSource, configureAuthProvider, authProviderDetail, editAuthProvider`
                        }}
                    />
                );
        }
    };

    // Navigation handlers
    const handleNext = (data?: any) => {
        console.log('➡️ [DialogFlow] handleNext called:', {
            currentStep,
            totalSteps: currentFlow?.length,
            data,
            dialogId,
            mode
        });

        if (currentFlow && currentStep < currentFlow.length - 1) {
            const nextStep = currentStep + 1;
            console.log('🔢 [DialogFlow] About to setCurrentStep to:', nextStep);

            // Use callback form to ensure we see the actual update
            setCurrentStep(prev => {
                console.log('🔄 [DialogFlow] setCurrentStep callback: prev =', prev, ', setting to =', nextStep);
                return nextStep;
            });

            console.log('📝 [DialogFlow] About to update viewData with:', data);
            setViewData(prev => {
                const newData = { ...prev, ...data };
                console.log('📝 [DialogFlow] setViewData callback: merging', data, 'into', prev);
                console.log('📝 [DialogFlow] viewData will be:', newData);
                console.log('📝 [DialogFlow] authProviderConnectionId will be:', newData.authProviderConnectionId);
                return newData;
            });

            console.log('✅ [DialogFlow] handleNext completed');
        } else if (onComplete && data?.isCompleted) {
            console.log('🏁 [DialogFlow] Flow completed, calling onComplete');
            onComplete(data);
        } else {
            console.log('⚠️ [DialogFlow] handleNext called but no next step available');
        }
    };

    // Handle back step
    const handleBack = () => {
        if (currentStep > 0) {
            setCurrentStep(currentStep - 1);
        }
    };

    // Handle cancel
    const handleCancel = () => {
        console.log('🚪 Cancelling dialog with ID:', dialogId);

        const isInErrorMode = showErrorView;

        // Always close the dialog immediately
        onOpenChange(false);

        // Reset the dialog state
        setCurrentStep(0);
        setShowErrorView(false);
        setErrorViewData(null);

        // Clear credential data to prevent auto-reopening
        setViewData(prevData => ({
            sourceId,
            sourceName,
            sourceShortName,
            collectionId,
            collectionName,
            authProviderId,
            authProviderName,
            authProviderShortName,
            authProviderAuthType,
            authProviderConnectionId,
            dialogId,
            credentialId: undefined,
            isAuthenticated: false
        }));

        // Clear error state from localStorage (if we were showing an error)
        if (isInErrorMode) {
            clearStoredErrorDetails();

            const currentUrl = new URL(window.location.href);
            if (currentUrl.searchParams.has('connected')) {
                currentUrl.searchParams.delete('connected');
                window.history.replaceState({}, '', currentUrl.toString());
            }
        }
    };

    // Handle completion
    const handleComplete = (result: any) => {
        console.log('🏁 [DialogFlow] handleComplete called:', {
            result,
            dialogId,
            mode
        });
        if (result && result.isCompleted) {
            setViewData(prevData => ({
                ...prevData,
                isCompleted: true
            }));
        }

        onOpenChange(false);
        if (onComplete) {
            onComplete(result);
        }
    };

    // Add enhanced error handling function that can be passed to views
    const handleError = (error: Error | string, errorSource?: string) => {
        console.error(`❌ [DialogFlow] Error with dialogId=${dialogId}:`, error);

        // First, close the dialog to prevent stale state
        onOpenChange(false);

        // Get the error message and stack
        const errorMsg = error instanceof Error ? error.message : error;
        const errorStack = error instanceof Error ? error.stack : undefined;

        // Create complete error details
        const completeError = {
            serviceName: errorSource || viewData.sourceName || "the service",
            sourceShortName: viewData.sourceShortName || sourceShortName || "unknown",
            errorMessage: errorMsg,
            errorDetails: errorStack,
            // Store current dialog state
            dialogState: {
                ...viewData,
                dialogFlowStep: currentStep
            },
            dialogId,
            timestamp: Date.now()
        };

        // Use the common error utility to redirect with full details
        redirectWithError(navigate, completeError, errorSource || viewData.sourceName);
    };

    return (
        <Dialog
            open={isOpen}
            onOpenChange={(open) => {
                if (!open) {
                    handleCancel();
                } else {
                    onOpenChange(open);
                }
            }}
        >
            <DialogPortal>
                <DialogOverlay className="bg-black/75" />
                <DialogContent
                    className={cn(
                        "p-0 rounded-xl border overflow-hidden",
                        isDark ? "bg-background border-gray-800" : "bg-background border-gray-200"
                    )}
                    style={{
                        width: "800px",
                        height: "80vh",
                        maxWidth: "95vw",
                        maxHeight: "95vh"
                    }}
                >
                    <div className="h-full w-full overflow-hidden">
                        {(() => {
                            console.log('🏗️ [DialogFlow] About to renderView, isOpen:', isOpen, 'currentView:', currentView);
                            return renderView();
                        })()}
                    </div>
                </DialogContent>
            </DialogPortal>
        </Dialog>
    );
};

export default DialogFlow;
