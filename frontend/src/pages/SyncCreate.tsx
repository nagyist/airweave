import { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import { toast, useToast } from "@/components/ui/use-toast";
import { SyncDataSourceGrid } from "@/components/sync/SyncDataSourceGrid";
import { VectorDBSelector } from "@/components/VectorDBSelector";
import { SyncProgress } from "@/components/sync/SyncProgress";
import { Button } from "@/components/ui/button";
import { ChevronRight } from "lucide-react";
import { apiClient } from "@/lib/api";
import { useSyncSubscription } from "@/hooks/useSyncSubscription";
import { SyncPipelineVisual } from "@/components/sync/SyncPipelineVisual";
import { SyncDagEditor } from "@/components/sync/SyncDagEditor";
import { SyncUIMetadata } from "@/components/sync/types";
import { Dag } from "@/components/sync/dag";

/**
 * This component coordinates all user actions (source selection,
 * vector DB selection, sync creation, and sync job triggering).
 * It uses local React state, but you can integrate any data-fetching
 * or global state libraries (e.g., React Query, Redux, Zustand).
 */

interface ConnectionSelection {
  connectionId: string;
  isNative?: boolean;
  destinationShortName?: string;
}

// Utility to ensure we have a valid destination type
function ensureDestinationType(selection: ConnectionSelection): string {
  if (selection.isNative) {
    return selection.destinationShortName || 'weaviate_native'; // Default to weaviate_native
  }
  return 'connection'; // For non-native connections
}

const Sync = () => {
  // Which setup step are we on?
  const [step, setStep] = useState<number>(1);

  // Chosen data source (step 1 -> 2)
  const [selectedSource, setSelectedSource] = useState<ConnectionSelection | null>(null);

  // Chosen vector DB or native indexing (step 2 -> 3)
  const [selectedDB, setSelectedDB] = useState<ConnectionSelection | ConnectionSelection[] | null>(null);

  // Created sync ID and job ID once we make calls
  const [syncId, setSyncId] = useState<string | null>(null);
  const [syncJobId, setSyncJobId] = useState<string | null>(null);

  // Hook for showing user feedback toasts
  const { toast } = useToast();
  const location = useLocation();

  // Subscribe to SSE updates whenever syncJobId is set
  // 'updates' returns an array of progress updates
  const updates = useSyncSubscription(syncJobId);

  // Add UI metadata state for the pipeline visual
  const [pipelineMetadata, setPipelineMetadata] = useState<SyncUIMetadata | null>(null);

  // Add user info state
  const [userInfo, setUserInfo] = useState<{
    userId: string;
    organizationId: string;
    userEmail: string;
  } | null>(null);

  // Replace the initialDag state with:
  const [dag, setDag] = useState<Dag | null>(null);

  /**
   * Notify the user if they've just returned from an oauth2 flow.
   */
  useEffect(() => {
    const query = new URLSearchParams(location.search);
    const connectedStatus = query.get("connected");
    if (connectedStatus === "success") {
      toast({
        title: "Connection successful",
        description: "Your data source is now connected."
      });
    } else if (connectedStatus === "error") {
      toast({
        variant: "destructive",
        title: "Connection failed",
        description: "There was an error connecting to your data source."
      });
    }
  }, [location.search, toast]);


  /**
   * handleSourceSelect is triggered by SyncDataSourceGrid when the user
   * chooses a data source. We move from step 1 -> 2 to pick vector DB.
   */
  const handleSourceSelect = async (connectionId: string, metadata: { name: string; shortName: string }) => {
    setSelectedSource({ connectionId });
    if (userInfo) {
      setPipelineMetadata({
        source: {
          ...metadata,
          type: "source",
        },
        destination: {
          name: "Native Weaviate",
          shortName: "weaviate_native",
          type: "destination",
        },
        ...userInfo,
      });
    }
    setStep(2);
  };

  /**
   * handleVectorDBSelected is triggered after the user chooses a vector DB.
   * We move from step 2 -> 3 to confirm the pipeline.
   */
  const handleVectorDBSelected = async (
    dbDetails: ConnectionSelection | ConnectionSelection[], 
    metadata: { name: string; shortName: string }[]
  ) => {
    try {
      setSelectedDB(Array.isArray(dbDetails) ? dbDetails : [dbDetails]);
      
      // Update pipeline metadata with all destinations
      if (userInfo) {
        setPipelineMetadata(prev => prev ? {
          ...prev,
          destination: Array.isArray(dbDetails) 
            ? metadata.map(m => ({
                ...m,
                type: "destination"
              }))
            : dbDetails.isNative 
              ? {
                  name: "Native Weaviate",
                  shortName: "weaviate_native",
                  type: "destination",
                }
              : {
                  ...metadata[0],
                  type: "destination",
                }
        } : null);
      }

      if (!selectedSource) {
        throw new Error("No source selected");
      }

      // Create sync with source connection only
      const syncPayload: any = {
        name: "Sync from UI",
        source_connection_id: selectedSource.connectionId,
        run_immediately: false,
        sync_metadata: {}
      };
      
      // Create the base sync (source only)
      const syncResp = await apiClient.post("/sync/", syncPayload);

      if (!syncResp.ok) {
        throw new Error("Failed to create sync");
      }

      const syncData = await syncResp.json();
      const newSyncId = syncData.id;
      setSyncId(newSyncId);
      
      // Add destinations to the sync
      const destinationsToAdd = Array.isArray(dbDetails) ? dbDetails : [dbDetails];
      const destinationPayload = destinationsToAdd.map(db => {
        return {
          sync_id: newSyncId,
          connection_id: db.isNative ? null : db.connectionId,
          is_native: Boolean(db.isNative), // Ensure it's a boolean
          destination_type: ensureDestinationType(db)
        };
      });
      
      // Create destinations for the sync
      const destResp = await apiClient.post(`/sync/${newSyncId}/destinations`, destinationPayload);
      
      if (!destResp.ok) {
        throw new Error("Failed to add destinations to sync");
      }

      // Initialize the DAG
      const dagResp = await apiClient.get(`/sync/${newSyncId}/dag`);
      if (!dagResp.ok) {
        throw new Error("Failed to initialize DAG");
      }

      const dagData: Dag = await dagResp.json();
      setDag(dagData);
      setStep(3);
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Failed to setup pipeline",
        description: err.message || String(err),
      });
    }
  };

  /**
   * handleStartSync is called when the user clicks the Start Sync button
   * or when the DAG is saved.
   */
  const handleStartSync = async () => {
    try {
      const resp = await apiClient.post(`/sync/${syncId}/run`);
      if (!resp.ok) {
        throw new Error("Failed to run sync job");
      }
      const data = await resp.json();
      setSyncJobId(data.id);
      setStep(4);
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Sync job start failed",
        description: err.message || String(err)
      });
    }
  };

  return (
    <div className="container mx-auto py-8">
      <div className="mx-auto">
        {/* Step + progress bar */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <h1 className="text-3xl font-bold">
              Set up your pipeline
            </h1>
            <div className="text-sm text-muted-foreground">
              Step {step} of 4
            </div>
          </div>
          <div className="mt-2 h-2 w-full rounded-full bg-secondary/20">
            <div
              className="h-2 rounded-full bg-primary transition-all duration-300"
              style={{ width: `${(step / 4) * 100}%` }}
            />
          </div>
        </div>

        {/* Add pipeline visual for steps 3 and 4 */}
        {pipelineMetadata?.source.name && (step === 3 || step === 4) && (
          <div className="mb-8">
            <SyncPipelineVisual
              sync={{
                uiMetadata: pipelineMetadata
              }}
            />
          </div>
        )}

        {/* Step 1: Pick data source */}
        {step === 1 && (
          <div className="space-y-6">
            <div className="flex items-center space-x-2">
              <h2 className="text-2xl font-semibold">Choose your data source</h2>
              <ChevronRight className="h-5 w-5 text-muted-foreground" />
            </div>
            <SyncDataSourceGrid onSelect={handleSourceSelect} />
          </div>
        )}

        {/* Step 2: Pick vector DB */}
        {step === 2 && (
          <div className="space-y-6">
            <div className="flex items-center space-x-2">
              <h2 className="text-2xl font-semibold">Choose your vector database</h2>
              <ChevronRight className="h-5 w-5 text-muted-foreground" />
            </div>
            <VectorDBSelector onComplete={handleVectorDBSelected} />
          </div>
        )}

        {/* Step 3: Configure and start sync */}
        {step === 3 && (
          <div className="space-y-8">
            <div>
              <h2 className="text-2xl font-semibold">Configure your pipeline</h2>
              <p className="text-muted-foreground mt-2">
                Add transformers and entities to your pipeline.
              </p>
            </div>
            {dag && syncId && (
              <div className="space-y-8">
                <SyncDagEditor
                  syncId={syncId}
                  initialDag={dag}
                  onSave={handleStartSync}
                />
                <div className="flex flex-col items-center gap-2">
                  <Button size="lg" onClick={handleStartSync}>
                    Start Sync
                  </Button>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Step 4: Show progress updates with pipeline visual */}
        {step === 4 && (
          <div className="space-y-6">
            <SyncProgress syncId={syncId} syncJobId={syncJobId} />
          </div>
        )}
      </div>
    </div>
  );
};

export default Sync;
