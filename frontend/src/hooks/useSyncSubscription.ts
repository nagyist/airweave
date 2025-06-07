import { useEffect, useRef, useState, useCallback } from "react";
import { env } from "../config/env";
import { SSEClient, createSSEConnection } from "../lib/sseClient";

interface SyncUpdate {
  updated?: number;
  inserted?: number;
  deleted?: number;
  [key: string]: any;
}

export function useSyncSubscription(jobId?: string | null) {
  const [updates, setUpdates] = useState<SyncUpdate[]>([]);
  const sseClientRef = useRef<SSEClient | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const currentJobIdRef = useRef<string | null>(null);
  const connectionInProgressRef = useRef(false);

  // Stable cleanup function
  const cleanup = useCallback(() => {
    if (sseClientRef.current) {
      console.log("Closing sync subscription SSE client");
      sseClientRef.current.disconnect();
      sseClientRef.current = null;
      setIsConnected(false);
    }
    connectionInProgressRef.current = false;
  }, []);

  useEffect(() => {
    // If no jobId, clean up and return
    if (!jobId) {
      cleanup();
      currentJobIdRef.current = null;
      return;
    }

    // If jobId hasn't changed and we already have a connection, don't reconnect
    if (jobId === currentJobIdRef.current && sseClientRef.current && isConnected) {
      console.log(`[SSE] Skipping reconnection - already connected to job ${jobId}`);
      return;
    }

    // If connection is already in progress for this job, don't start another
    if (connectionInProgressRef.current && jobId === currentJobIdRef.current) {
      console.log(`[SSE] Connection already in progress for job ${jobId}`);
      return;
    }

    // Clean up any existing connection
    cleanup();

    // Update tracking refs
    currentJobIdRef.current = jobId;
    connectionInProgressRef.current = true;

    // Create cleanup function for this effect
    let isMounted = true;

    // Setup SSE connection
    const setupSSEConnection = async () => {
      try {
        const url = `${env.VITE_API_URL}/sync/job/${jobId}/subscribe`;

        console.log(`[SSE] Creating sync subscription to job: ${jobId}`);

        const sseClient = createSSEConnection({
          url,
          onMessage: (data: SyncUpdate) => {
            console.log('[PubSub] Raw event received:', data);
            if (!isMounted) return;

            setUpdates((prev) => [...prev, data]);
          },
          onOpen: () => {
            if (!isMounted) return;
            setIsConnected(true);
            connectionInProgressRef.current = false;
            console.log(`[SSE] Connection established for job: ${jobId}`);
          },
          onClose: () => {
            if (!isMounted) return;
            setIsConnected(false);
            connectionInProgressRef.current = false;
            console.log(`[SSE] Connection closed for job: ${jobId}`);
          },
          onError: (error) => {
            console.error(`[SSE] Subscription failed for job ${jobId}:`, error);
            if (!isMounted) return;
            setIsConnected(false);
            connectionInProgressRef.current = false;
          }
        });

        sseClientRef.current = sseClient;
        await sseClient.connect();

      } catch (error) {
        console.error(`[SSE] Error setting up subscription for job ${jobId}:`, error);
        if (!isMounted) return;
        setIsConnected(false);
        connectionInProgressRef.current = false;
      }
    };

    // Start the connection
    void setupSSEConnection();

    // Return cleanup function
    return () => {
      isMounted = false;
      // Only clean up if this effect is for the current job
      if (currentJobIdRef.current === jobId) {
        cleanup();
        currentJobIdRef.current = null;
      }
    };
  }, [jobId, cleanup]); // Only depend on jobId and stable cleanup function

  // Reset updates when jobId changes
  useEffect(() => {
    if (jobId !== currentJobIdRef.current) {
      setUpdates([]);
    }
  }, [jobId]);

  return {
    updates,
    latestUpdate: updates.length > 0 ? updates[updates.length - 1] : null,
    isConnected
  };
}
