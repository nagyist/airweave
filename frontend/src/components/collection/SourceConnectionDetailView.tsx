import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Alert } from "@/components/ui/alert";
import { AlertCircle, RefreshCw, Pencil, Play, Clock, Loader2 } from "lucide-react";
import { apiClient } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { useTheme } from "@/lib/theme-provider";
import ReactFlow, {
    useNodesState,
    useEdgesState,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { SourceNode } from '@/components/sync/nodes/SourceNode';
import { TransformerNode } from '@/components/sync/nodes/TransformerNode';
import { DestinationNode } from '@/components/sync/nodes/DestinationNode';
import { EntityNode } from '@/components/sync/nodes/EntityNode';
import { Dag } from '@/components/sync/dag';
import {
    cleanEntityName,
    convertDagToFlowGraph
} from '@/components/collection/DagToFlow';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { SyncSchedule, SyncScheduleConfig, buildCronExpression, isValidCronExpression } from '@/components/sync/SyncSchedule';
import { QueryTool } from '@/components/collection/QueryTool';
import { LiveApiDoc } from '@/components/collection/LiveApiDoc';
import { useSyncSubscription } from "@/hooks/useSyncSubscription";
import { cn } from "@/lib/utils";
import { SyncErrorCard } from './SyncErrorCard';
import { SyncDagCard, SourceConnection } from './SyncDagCard';

const nodeTypes = {
    sourceNode: SourceNode,
    transformerNode: TransformerNode,
    destinationNode: DestinationNode,
    entityNode: EntityNode
};

interface SourceConnectionJob {
    source_connection_id: string;
    id: string;
    sync_id?: string; // This may be needed for DAG fetching
    organization_id: string;
    created_by_email: string;
    modified_by_email: string;
    created_at: string;
    modified_at: string;
    status: 'pending' | 'in_progress' | 'completed' | 'failed';
    entities_inserted: number;
    entities_updated: number;
    entities_deleted: number;
    entities_kept: number;
    entities_skipped: number;
    entities_encountered: Record<string, number>;
    started_at: string | null;
    completed_at: string | null;
    failed_at: string | null;
    error: string | null;
}

interface SourceConnectionDetailViewProps {
    sourceConnectionId: string;
    shouldForceSubscribe?: boolean;
    onSubscriptionComplete?: () => void;
}

const SourceConnectionDetailView = ({
    sourceConnectionId,
    shouldForceSubscribe = false,
    onSubscriptionComplete
}: SourceConnectionDetailViewProps) => {
    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === 'dark';

    /********************************************
     * STATE DECLARATIONS - GROUPED BY PURPOSE
     ********************************************/

    // 1. Core connection and job state
    const [selectedConnection, setSelectedConnection] = useState<SourceConnection | null>(null);
    const [lastSyncJob, setLastSyncJob] = useState<SourceConnectionJob | null>(null);
    const [isReloading, setIsReloading] = useState(false);
    const [isInitiatingSyncJob, setIsInitiatingSyncJob] = useState(false);
    const [finalPubSubData, setFinalPubSubData] = useState<{
        inserted: number;
        updated: number;
        deleted: number;
        kept: number;
        skipped: number;
        encountered: Record<string, number>;
    } | null>(null);
    const [isLoading, setIsLoading] = useState(true); // Add loading state

    // Cache previous connection data to prevent flashing during transitions
    const prevConnectionRef = useRef<{
        connection: SourceConnection | null;
        syncJob: SourceConnectionJob | null;
        entities: number;
        runtime: number | null;
        entityDict: Record<string, number>;
        status: string;
    }>({
        connection: null,
        syncJob: null,
        entities: 0,
        runtime: null,
        entityDict: {},
        status: ""
    });

    // 2. Real-time updates state
    const [internalShouldForceSubscribe, setInternalShouldForceSubscribe] = useState(false);

    // 3. Entity processing and visualization state
    const [totalEntities, setTotalEntities] = useState<number>(0);
    const [totalRuntime, setTotalRuntime] = useState<number | null>(null);
    const [entityDict, setEntityDict] = useState<Record<string, number>>({});
    const [selectedEntity, setSelectedEntity] = useState<string>('');
    const prevEntityDictRef = useRef<Record<string, number>>({});

    // 4. Graph visualization state
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);
    const [reactFlowInstance, setReactFlowInstance] = useState(null);
    const [entityDags, setEntityDags] = useState<Dag[]>([]);
    const [selectedDag, setSelectedDag] = useState<Dag | null>(null);

    // 5. Scheduling state
    const [showScheduleDialog, setShowScheduleDialog] = useState(false);
    const [scheduleConfig, setScheduleConfig] = useState<SyncScheduleConfig>({
        type: "one-time",
        frequency: "custom"
    });
    const [nextRunTime, setNextRunTime] = useState<string | null>(null);

    // Add this near other useRef declarations:
    const flowContainerRef = useRef<HTMLDivElement>(null);

    /********************************************
     * COMPUTED VALUES & DERIVED STATE
     ********************************************/

    // 1. Subscription control logic
    const shouldSubscribe = useMemo(() => {
        return shouldForceSubscribe || internalShouldForceSubscribe ||
            (lastSyncJob?.status === 'pending' || lastSyncJob?.status === 'in_progress');
    }, [lastSyncJob?.status, internalShouldForceSubscribe, shouldForceSubscribe]);

    // Subscribe to real-time updates when necessary
    const { updates, latestUpdate, isConnected: isPubSubConnected } = useSyncSubscription(
        shouldSubscribe ? lastSyncJob?.id || null : null
    );

    // 2. Status derived from most up-to-date source
    const status = useMemo(() => {
        // If we have real-time updates and are connected, use those first
        if (isPubSubConnected && latestUpdate) {
            if (latestUpdate.is_complete) return "completed";
            if (latestUpdate.is_failed) return "failed";
            return "in_progress";
        }
        // Otherwise fall back to the DB state
        return lastSyncJob?.status || "";
    }, [isPubSubConnected, latestUpdate, lastSyncJob?.status]);

    // 3. Active job status for conditional rendering
    const isSyncJobRunning = status === "in_progress" || status === "pending";

    // 4. Unified entity data from multiple sources
    const entityData = useMemo(() => {
        // If we have real-time updates and are connected, use those
        if (isPubSubConnected && latestUpdate && isSyncJobRunning) {
            return {
                inserted: latestUpdate.inserted || 0,
                updated: latestUpdate.updated || 0,
                deleted: latestUpdate.deleted || 0,
                kept: latestUpdate.kept || 0,
                skipped: latestUpdate.skipped || 0,
                encountered: latestUpdate.entities_encountered || {},
                isRealtimeData: true
            };
        }

        // If we have saved final PubSub data and the job just finished, use that
        if (finalPubSubData && (status === "completed" || status === "failed") && !isSyncJobRunning) {
            return {
                ...finalPubSubData,
                isRealtimeData: false
            };
        }

        // Otherwise use database state
        return {
            inserted: lastSyncJob?.entities_inserted || 0,
            updated: lastSyncJob?.entities_updated || 0,
            deleted: lastSyncJob?.entities_deleted || 0,
            kept: lastSyncJob?.entities_kept || 0,
            skipped: lastSyncJob?.entities_skipped || 0,
            encountered: lastSyncJob?.entities_encountered || {},
            isRealtimeData: false
        };
    }, [isPubSubConnected, latestUpdate, lastSyncJob, isSyncJobRunning, finalPubSubData, status]);

    // 5. Diagnostics for data source
    const isShowingRealtimeUpdates = shouldSubscribe && updates.length > 0 && isPubSubConnected;

    // 6. Display metrics for UI
    const total = entityData.inserted + entityData.updated + entityData.kept + entityData.deleted + entityData.skipped;

    // Add this to prevent entity dictionary from completely emptying during transitions
    // This preserves the previous entity dictionary if new data doesn't have entities yet
    const stableEntityDict = useMemo(() => {
        if (Object.keys(entityDict).length > 0) {
            return entityDict;
        }
        return prevEntityDictRef.current;
    }, [entityDict]);

    /********************************************
     * API AND DATA FETCHING FUNCTIONS
     ********************************************/

    // 1. Main connection data fetching
    const fetchSourceConnectionDetails = async () => {
        setIsLoading(true);
        try {
            const response = await apiClient.get(`/source-connections/${sourceConnectionId}`);

            if (response.ok) {
                const detailedData = await response.json();
                console.log("Source connection details received:", detailedData);
                console.log("Cron schedule from API:", detailedData.cron_schedule);
                setSelectedConnection(detailedData);
                await fetchSourceConnectionJob(detailedData);
            } else {
                console.error("Failed to load source connection details:", await response.text());
            }
        } catch (err) {
            console.error("Error fetching source connection details:", err);
        } finally {
            setIsLoading(false);
        }
    };

    // 2. Sync job data fetching
    const fetchSourceConnectionJob = async (connection: SourceConnection) => {
        if (!connection.sync_id || !connection.latest_sync_job_id) {
            // Reset states if no sync ID or job ID available
            setLastSyncJob(null);
            setTotalEntities(0);
            setTotalRuntime(null);
            return;
        }

        try {
            const response = await apiClient.get(`/source-connections/${connection.id}/jobs/${connection.latest_sync_job_id}`);

            if (!response.ok) {
                console.error(`Failed to fetch job with ID ${connection.latest_sync_job_id}, status: ${response.status}`);
                setLastSyncJob(null);
                setTotalEntities(0);
                setTotalRuntime(null);
                return;
            }

            const sourceConnectionJob = await response.json();
            console.log("Source connection job:", sourceConnectionJob);
            setLastSyncJob(sourceConnectionJob);

            // Calculate runtime for this job
            if (sourceConnectionJob.started_at && (sourceConnectionJob.completed_at || sourceConnectionJob.failed_at)) {
                const endTime = sourceConnectionJob.completed_at || sourceConnectionJob.failed_at;
                const runtime = new Date(endTime!).getTime() - new Date(sourceConnectionJob.started_at).getTime();
                setTotalRuntime(runtime);
            }

            // After we have the job data with entities_encountered, fetch entity DAGs
            if (connection.sync_id && sourceConnectionJob.entities_encountered) {
                await fetchEntityDags(connection.sync_id);
            }
        } catch (err) {
            console.error("Error fetching source connection job:", err);
            setLastSyncJob(null);
            setTotalEntities(0);
            setTotalRuntime(null);
        }
    };

    // 3. Entity DAG fetching
    const fetchEntityDags = async (syncId: string) => {
        try {
            const response = await apiClient.get(`/dag/sync/${syncId}/entity_dags`);

            if (!response.ok) {
                throw new Error('Failed to load entity DAGs');
            }

            const data = await response.json();
            setEntityDags(data);
            console.log('DAG data loaded:', data);

            // After getting the DAGs, process them with the actual short_name
            if (selectedConnection && data.length > 0) {
                // This will be used when rendering the DAG
                data.forEach(dag => {
                    dag.sourceShortName = selectedConnection.short_name;
                    dag.nodes.find(node => node.type === 'source').connection_id = selectedConnection.connection_id;
                });
            }
        } catch (error) {
            console.error('Error fetching entity DAGs:', error);
        }
    };

    // 4. Schedule data refreshing
    const refreshScheduleData = async () => {
        if (!selectedConnection?.id) return;

        try {
            console.log("Starting schedule refresh for source connection:", selectedConnection.id);
            setIsReloading(true);

            // Get source connection details
            const response = await apiClient.get(`/source-connections/${selectedConnection.id}`);
            if (!response.ok) throw new Error("Failed to refresh source connection data");

            const sourceData = await response.json();
            console.log("Got source connection data:", sourceData);
            console.log("Cron schedule after update:", sourceData.cron_schedule);

            // Update the source connection state with the new schedule information
            setSelectedConnection(prev => {
                if (!prev) return null;
                console.log("Updating connection details with cron_schedule:", sourceData.cron_schedule);
                return sourceData;
            });

            // Update the config state as well
            setScheduleConfig({
                type: sourceData.cron_schedule ? "scheduled" : "one-time",
                frequency: "custom",
                cronExpression: sourceData.cron_schedule || undefined
            });

            // Update the next run time
            const nextRun = calculateNextRunTime(sourceData.cron_schedule);
            setNextRunTime(nextRun);
            console.log("Next run time calculated:", nextRun);

            toast({
                title: "Success",
                description: "Schedule updated successfully"
            });
        } catch (error) {
            console.error("Error refreshing schedule data:", error);
            toast({
                title: "Error",
                description: "Failed to refresh schedule data",
                variant: "destructive"
            });
        } finally {
            setIsReloading(false);
            console.log("Schedule refresh complete");
        }
    };

    // 5. General data reload
    const reloadData = async () => {
        setIsReloading(true);
        try {
            await fetchSourceConnectionDetails();
        } finally {
            setIsReloading(false);
        }
    };

    /********************************************
     * UI EVENT HANDLERS
     ********************************************/

    // 1. Run sync job
    const handleRunSync = async () => {
        if (!selectedConnection?.id) {
            toast({
                title: "Error",
                description: "No source connection selected",
                variant: "destructive"
            });
            return;
        }

        try {
            setIsInitiatingSyncJob(true);
            const response = await apiClient.post(`/source-connections/${selectedConnection.id}/run`);

            if (!response.ok) {
                throw new Error("Failed to start sync job");
            }

            const newJob = await response.json();

            // Set the new job as current
            setLastSyncJob(newJob);

            // Force subscription for this new job
            setInternalShouldForceSubscribe(true);
            // PubSub will handle updates - no need for manual reloading

        } catch (error) {
            console.error("Error running sync:", error);
            toast({
                title: "Error",
                description: "Failed to start sync job",
                variant: "destructive"
            });
        } finally {
            setIsInitiatingSyncJob(false);
        }
    };

    // 2. Schedule dialog handling
    const handleScheduleDialogClose = () => {
        console.log("Closing schedule dialog without submitting");
        setShowScheduleDialog(false);
    };

    // Simple function to handle the Done button click
    const handleScheduleDone = async () => {
        if (!selectedConnection?.id) {
            toast({
                title: "Error",
                description: "No source connection selected",
                variant: "destructive"
            });
            return;
        }

        try {
            console.log("Saving schedule config:", scheduleConfig);

            // Build cron expression
            const cronExpression = scheduleConfig.type === "scheduled"
                ? buildCronExpression(scheduleConfig)
                : null;

            console.log("Generated cron expression:", cronExpression);

            // Validate if needed
            if (scheduleConfig.type === "scheduled" &&
                scheduleConfig.frequency === "custom" &&
                scheduleConfig.cronExpression &&
                !isValidCronExpression(scheduleConfig.cronExpression)) {

                toast({
                    title: "Validation Error",
                    description: "Invalid cron expression. Please check the format.",
                    variant: "destructive"
                });
                return;
            }

            // Update data to send
            const updateData = {
                cron_schedule: cronExpression
            };

            console.log("Updating source connection schedule:", updateData);

            // Make direct API call
            const response = await apiClient.put(
                `/source-connections/${selectedConnection.id}`,
                null, // No query params
                updateData // Data as third parameter
            );

            if (!response.ok) {
                throw new Error("Failed to update schedule");
            }

            // Refresh data and close dialog
            await refreshScheduleData();
            setShowScheduleDialog(false);

        } catch (error) {
            console.error("Error updating schedule:", error);
            toast({
                title: "Error",
                description: "Failed to update schedule",
                variant: "destructive"
            });
        }
    };

    /********************************************
     * UTILITY FUNCTIONS
     ********************************************/

    // 1. Entity dictionary processing
    const updateEntityDictionary = useCallback(() => {
        // Use the unified entityData.encountered
        const entitiesEncountered = entityData.encountered;

        // Store current dictionary in ref
        if (Object.keys(entityDict).length > 0) {
            prevEntityDictRef.current = { ...entityDict };
        }

        // If we have no encountered entities but had them before,
        // don't empty the dictionary during state transitions
        if ((!entitiesEncountered || Object.keys(entitiesEncountered).length === 0) &&
            Object.keys(prevEntityDictRef.current).length > 0) {
            console.log('Preserving previous entity dictionary during transition');
            setEntityDict(prevEntityDictRef.current);
            return;
        }

        if (!entitiesEncountered || !entityDags.length) {
            return; // Need both entities and DAGs
        }

        // Get source name from entityDags
        const sourceName = entityDags[0].nodes
            .filter(node => node.type === 'source')
            .map(node => node.name)[0] || '';

        // Process the entities_encountered data with source name
        const cleanedDict = Object.entries(entitiesEncountered).reduce((acc, [key, value]) => {
            const cleanedName = cleanEntityName(key, sourceName);
            acc[cleanedName] = value as number;
            return acc;
        }, {} as Record<string, number>);

        console.log('Created cleaned entity dictionary:', cleanedDict, 'with source name:', sourceName);

        // Only update if we have actual entities
        if (Object.keys(cleanedDict).length > 0) {
            setEntityDict(cleanedDict);
        }

        // Select first entity if none selected
        if (Object.keys(cleanedDict).length > 0 && !selectedEntity) {
            setSelectedEntity(Object.keys(cleanedDict)[0]);
        }
    }, [entityData.encountered, entityDags, selectedEntity]);

    // 2. Schedule time calculation
    const calculateNextRunTime = useCallback((cronExpression: string | null) => {
        if (!cronExpression) {
            return null;
        }

        try {
            // Parse the cron expression
            const parts = cronExpression.split(' ');
            if (parts.length !== 5) {
                console.error("Invalid cron expression format");
                return null;
            }

            const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

            // Get current date in UTC
            const now = new Date();
            const nowUtc = new Date(Date.UTC(
                now.getUTCFullYear(),
                now.getUTCMonth(),
                now.getUTCDate(),
                now.getUTCHours(),
                now.getUTCMinutes(),
                now.getUTCSeconds()
            ));

            let nextRun = new Date(Date.UTC(
                nowUtc.getUTCFullYear(),
                nowUtc.getUTCMonth(),
                nowUtc.getUTCDate(),
                nowUtc.getUTCHours(),
                nowUtc.getUTCMinutes(),
                nowUtc.getUTCSeconds()
            ));

            // For weekly schedules (specific day of week)
            if (dayOfWeek !== '*' && dayOfMonth === '*') {
                const targetDay = parseInt(dayOfWeek) % 7; // 0-6, where 0 is Sunday
                const currentDay = nowUtc.getUTCDay(); // 0-6, where 0 is Sunday

                // Calculate days to add to get to the target day
                let daysToAdd = (targetDay - currentDay + 7) % 7;
                if (daysToAdd === 0) {
                    // If today is the target day, check if the time has already passed
                    const targetHour = parseInt(hour);
                    const targetMinute = parseInt(minute);

                    if (hour !== '*' && minute !== '*') {
                        const currentHour = nowUtc.getUTCHours();
                        const currentMinute = nowUtc.getUTCMinutes();

                        if (currentHour > targetHour || (currentHour === targetHour && currentMinute >= targetMinute)) {
                            // Time already passed today, go to next week
                            daysToAdd = 7;
                        }
                    }
                }

                // Set the date to the next occurrence
                nextRun.setUTCDate(nowUtc.getUTCDate() + daysToAdd);

                // Set the time
                if (hour !== '*') {
                    nextRun.setUTCHours(parseInt(hour), parseInt(minute) || 0, 0, 0);
                } else {
                    nextRun.setUTCHours(nowUtc.getUTCHours(), parseInt(minute) || 0, 0, 0);
                }
            }
            // For monthly schedules (specific day of month)
            else if (dayOfMonth !== '*') {
                const targetDay = parseInt(dayOfMonth);

                // Create a date for the target day in current month (in UTC)
                const targetDate = new Date(Date.UTC(
                    nowUtc.getUTCFullYear(),
                    nowUtc.getUTCMonth(),
                    targetDay,
                    hour !== '*' ? parseInt(hour) : nowUtc.getUTCHours(),
                    minute !== '*' ? parseInt(minute) : 0,
                    0, 0
                ));

                // If the target day already passed this month, go to next month
                if (targetDate <= nowUtc) {
                    targetDate.setUTCMonth(targetDate.getUTCMonth() + 1);
                }

                nextRun = targetDate;
            }
            // For daily schedules
            else if (hour !== '*' && dayOfMonth === '*' && dayOfWeek === '*') {
                const targetHour = parseInt(hour);
                const targetMinute = parseInt(minute) || 0;

                nextRun.setUTCHours(targetHour, targetMinute, 0, 0);

                // If the time already passed today, go to tomorrow
                if (nextRun <= nowUtc) {
                    nextRun.setUTCDate(nowUtc.getUTCDate() + 1);
                }
            }
            // For hourly schedules
            else if (hour === '*' && minute !== '*') {
                const targetMinute = parseInt(minute);
                const currentMinute = nowUtc.getUTCMinutes();

                nextRun.setUTCMinutes(targetMinute, 0, 0);

                // If the minute already passed this hour, go to next hour
                if (currentMinute >= targetMinute) {
                    nextRun.setUTCHours(nowUtc.getUTCHours() + 1);
                }
            }

            // Calculate time difference in UTC
            const diffMs = nextRun.getTime() - nowUtc.getTime();
            const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
            const diffHrs = Math.floor((diffMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            const diffMins = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));

            // Format the time difference
            let result = '';
            if (diffDays > 0) {
                result += `${diffDays}d `;
            }
            if (diffHrs > 0) {
                result += `${diffHrs}h `;
            }
            if (diffMins > 0 || (diffDays === 0 && diffHrs === 0)) {
                result += `${diffMins}m`;
            }

            return result.trim();
        } catch (error) {
            console.error("Error calculating next run time:", error);
            return null;
        }
    }, []);

    // 3. Time formatting utilities
    const formatTotalRuntime = (ms: number) => {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);

        if (hours > 0) {
            return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
        } else if (minutes > 0) {
            return `${minutes}m ${seconds % 60}s`;
        } else {
            return `${seconds}s`;
        }
    };

    const formatTimeSince = (dateStr: string) => {
        const now = new Date();
        const date = new Date(dateStr);
        const diffMs = now.getTime() - date.getTime();
        const diffHrs = Math.floor(diffMs / (1000 * 60 * 60));
        const diffMins = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));

        if (diffHrs > 24) {
            const days = Math.floor(diffHrs / 24);
            return `${days}d ago`;
        } else if (diffHrs > 0) {
            return `${diffHrs}h ${diffMins}m ago`;
        } else {
            return `${diffMins}m ago`;
        }
    };

    const formatCronTime = (cronExpression: string): string => {
        const parts = cronExpression.split(' ');
        const minute = parts[0];
        const hour = parts[1];

        if (hour === '*') return `${minute} minutes past each hour`;
        return `${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
    };

    // Replace formatCronTimeToLocal with a UTC-only version
    const formatCronTimeUTC = (cronExpression: string): string => {
        const parts = cronExpression.split(' ');
        const minute = parts[0];
        const hour = parts[1];
        const dayOfMonth = parts[2];
        const month = parts[3];
        const dayOfWeek = parts[4];

        // Base time format
        let timeInfo = `${hour !== '*' ? hour.padStart(2, '0') : '*'}:${minute.padStart(2, '0')} UTC`;

        // Format the date part if specific day
        if (dayOfWeek !== '*' || dayOfMonth !== '*') {
            const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

            // Build date context based on schedule type
            if (dayOfMonth !== '*') {
                // Monthly schedule
                timeInfo += ` on day ${dayOfMonth}`;
            } else if (dayOfWeek !== '*') {
                // Weekly schedule (0 = Sunday, 6 = Saturday)
                const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
                const dayIndex = parseInt(dayOfWeek) % 7;
                timeInfo += ` on ${days[dayIndex]}`;
            }
        }

        return timeInfo;
    }

    /********************************************
     * SIDE EFFECTS - ORDERED BY PRIORITY
     ********************************************/

    // 1. Initial data loading
    useEffect(() => {
        fetchSourceConnectionDetails();
    }, [sourceConnectionId]);

    // 2. PubSub subscription control
    useEffect(() => {
        if (latestUpdate?.is_complete || latestUpdate?.is_failed ||
            lastSyncJob?.status === 'completed' || lastSyncJob?.status === 'failed') {
            setInternalShouldForceSubscribe(false);

            // Call the completion callback if provided
            if (onSubscriptionComplete) {
                onSubscriptionComplete();
            }
        }
    }, [latestUpdate, lastSyncJob?.status, onSubscriptionComplete]);

    // 3. PubSub completion handling
    useEffect(() => {
        if ((latestUpdate?.is_complete || latestUpdate?.is_failed) && lastSyncJob?.id) {
            // Save the final state from PubSub
            setFinalPubSubData({
                inserted: latestUpdate.inserted || 0,
                updated: latestUpdate.updated || 0,
                deleted: latestUpdate.deleted || 0,
                kept: latestUpdate.kept || 0,
                skipped: latestUpdate.skipped || 0,
                encountered: latestUpdate.entities_encountered || {},
            });

            // Mark the job as done in state
            setLastSyncJob(prev => prev ? {
                ...prev,
                status: latestUpdate.is_complete ? 'completed' : 'failed'
            } : prev);

            // Fetch the latest job data including timestamps
            if (selectedConnection?.id && lastSyncJob?.id) {
                apiClient.get(`/source-connections/${selectedConnection.id}/jobs/${lastSyncJob.id}`)
                    .then(async response => {
                        if (response.ok) {
                            const updatedJob = await response.json();
                            setLastSyncJob(updatedJob);

                            // Recalculate runtime with fresh timestamps
                            if (updatedJob.started_at && (updatedJob.completed_at || updatedJob.failed_at)) {
                                const endTime = updatedJob.completed_at || updatedJob.failed_at;
                                const runtime = new Date(endTime).getTime() - new Date(updatedJob.started_at).getTime();
                                setTotalRuntime(runtime);
                            }
                        }
                    })
                    .catch(err => console.error("Error fetching updated job data:", err));
            }
        }
    }, [latestUpdate?.is_complete, latestUpdate?.is_failed, selectedConnection?.id, lastSyncJob?.id]);

    // 4. Entity data processing
    useEffect(() => {
        const totalEntitiesCount =
            entityData.inserted +
            entityData.updated +
            entityData.kept +
            entityData.skipped -
            entityData.deleted;
        setTotalEntities(totalEntitiesCount);
    }, [entityData]);

    // 5. Entity dictionary maintenance
    useEffect(() => {
        updateEntityDictionary();
    }, [updateEntityDictionary]);

    // 6. Entity selection
    useEffect(() => {
        if (!selectedEntity || entityDags.length === 0) {
            setSelectedDag(null);
            return;
        }

        // Find DAG that has name exactly matching the selected entity
        const exactMatch = entityDags.find(dag =>
            dag.name && dag.name.includes(selectedEntity + "Entity")
        );
        // Fall back to partial match if exact match not found
        const matchingDag = exactMatch || entityDags.find(dag =>
            dag.name && dag.name.includes(selectedEntity)
        );

        if (matchingDag) {
            setSelectedDag(matchingDag);
            console.log(`Selected DAG for entity "${selectedEntity}":`, matchingDag);
        } else {
            console.warn(`No DAG found for entity "${selectedEntity}"`);
            setSelectedDag(null);
        }
    }, [selectedEntity, entityDags]);

    // 7. DAG visualization
    useEffect(() => {
        // Convert the DAG to basic flow graph
        convertDagToFlowGraph(selectedDag, setNodes, setEdges);
    }, [selectedDag, setNodes, setEdges]);

    // 8. ReactFlow visualization adjustments
    useEffect(() => {
        if (reactFlowInstance) {
            reactFlowInstance.fitView({
                padding: 0.2,
                duration: 0
            });
        }
    }, [nodes, edges, reactFlowInstance]);

    // 9. Schedule configuration
    useEffect(() => {
        if (selectedConnection?.sync_id && selectedConnection.cron_schedule) {
            // Parse cron expression in UTC (no conversion)
            const cronParts = selectedConnection.cron_schedule.split(' ');
            const utcMinute = parseInt(cronParts[0]);
            const utcHour = cronParts[1] !== '*' ? parseInt(cronParts[1]) : undefined;

            // Set config with UTC time values
            setScheduleConfig({
                type: "scheduled",
                frequency: "custom",
                hour: utcHour,
                minute: utcMinute,
                cronExpression: selectedConnection.cron_schedule
            });
        }
    }, [selectedConnection]);

    // 10. Clean up on source change
    useEffect(() => {
        setFinalPubSubData(null);
    }, [sourceConnectionId]);

    // 11. Logging for diagnostic purposes
    useEffect(() => {
        if (updates.length > 0 && latestUpdate) {
            console.log(`[PubSub] Received update for job ${lastSyncJob?.id}:`, {
                isComplete: latestUpdate.is_complete,
                isFailed: latestUpdate.is_failed,
                counts: {
                    inserted: latestUpdate.inserted,
                    updated: latestUpdate.updated,
                    deleted: latestUpdate.deleted,
                    kept: latestUpdate.kept
                }
            });
        }
    }, [updates, latestUpdate, lastSyncJob?.id]);

    // Add this effect to handle resizing
    useEffect(() => {
        if (!reactFlowInstance || !flowContainerRef.current) return;

        const resizeObserver = new ResizeObserver(() => {
            setTimeout(() => {
                reactFlowInstance.fitView({
                    padding: 0.2,
                    duration: 0
                });
            }, 0);
        });

        resizeObserver.observe(flowContainerRef.current);

        return () => {
            if (flowContainerRef.current) {
                resizeObserver.unobserve(flowContainerRef.current);
            }
        };
    }, [reactFlowInstance]);

    // Store previous connection data before changes
    useEffect(() => {
        if (!isLoading && selectedConnection) {
            prevConnectionRef.current = {
                connection: selectedConnection,
                syncJob: lastSyncJob,
                entities: totalEntities,
                runtime: totalRuntime,
                entityDict: { ...entityDict },
                status: status
            };
        }
    }, [isLoading, selectedConnection, lastSyncJob, totalEntities, totalRuntime, entityDict, status]);

    // Replace the existing cleanup effect with this improved version
    useEffect(() => {
        // First set loading state when connection changes
        if (sourceConnectionId) {
            setIsLoading(true);
        }

        return () => {
            // Only clean up when component unmounts, not on every sourceConnectionId change
            if (!sourceConnectionId) {
                setNodes([]);
                setEdges([]);
                setSelectedEntity('');
                setEntityDict({});
                setSelectedDag(null);
                setEntityDags([]);
                prevEntityDictRef.current = {};
                setFinalPubSubData(null);
                setLastSyncJob(null);
                setTotalEntities(0);
                setTotalRuntime(null);

                if (reactFlowInstance) {
                    reactFlowInstance.setNodes([]);
                    reactFlowInstance.setEdges([]);
                }
            }
        };
    }, [sourceConnectionId, setNodes, setEdges, reactFlowInstance]);

    // Add this effect to initialize nextRunTime on component mount or when cron_schedule changes
    useEffect(() => {
        if (selectedConnection?.cron_schedule) {
            const nextRun = calculateNextRunTime(selectedConnection.cron_schedule);
            setNextRunTime(nextRun);
        }
    }, [selectedConnection?.cron_schedule, calculateNextRunTime]);

    console.log(`[PubSub] Data source for job ${lastSyncJob?.id}: ${isShowingRealtimeUpdates ? 'LIVE UPDATES' : 'DATABASE'}`);

    // Render based on loading state
    const connectionToDisplay = isLoading ? prevConnectionRef.current.connection : selectedConnection;
    const jobToDisplay = isLoading ? prevConnectionRef.current.syncJob : lastSyncJob;
    const entitiesToDisplay = isLoading ? prevConnectionRef.current.entities : totalEntities;
    const runtimeToDisplay = isLoading ? prevConnectionRef.current.runtime : totalRuntime;
    const statusToDisplay = isLoading ? prevConnectionRef.current.status : status;
    const entityDictToDisplay = Object.keys(stableEntityDict).length > 0 ? stableEntityDict :
        isLoading ? prevConnectionRef.current.entityDict : {};

    console.log(`[Loading] State for ${sourceConnectionId}: ${isLoading ? 'LOADING' : 'LOADED'}`);

    /********************************************
     * RENDER
     ********************************************/

    if (!connectionToDisplay) {
        return (
            <div className="w-full py-6">
                <div className="flex items-center justify-center">
                    <Loader2 className="h-8 w-8 animate-spin text-primary" />
                    <span className="ml-2">Loading connection details...</span>
                </div>
            </div>
        );
    }

    console.log("Render state:", {
        hasSchedule: !!selectedConnection?.cron_schedule,
        cronSchedule: selectedConnection?.cron_schedule,
        nextRunTime
    });

    return (
        <div className={cn(isDark ? "text-foreground" : "")}>
            {/* Visualization Section */}
            <div className="py-2 space-y-3 mt-4">
                {/* Status Dashboard */}
                <div className="grid grid-cols-12 gap-3">
                    {/* Status Stats Cards - Add stable min-height to prevent layout shifts */}
                    <div className="col-span-12 md:col-span-8 grid grid-cols-3 gap-3">
                        {/* Entities Card */}
                        <div className={cn(
                            "col-span-1 rounded-lg p-3 flex flex-col shadow-sm transition-all duration-200 min-h-[5.5rem]",
                            isDark
                                ? "bg-gray-800/60 border border-gray-700/50"
                                : "bg-white border border-gray-100"
                        )}>
                            <div className="text-xs uppercase tracking-wider mb-1 font-medium opacity-60">
                                Entities
                            </div>
                            <div className="text-2xl font-semibold">
                                {isLoading ? (
                                    <span>{entitiesToDisplay.toLocaleString()}</span>
                                ) : (
                                    <span>{totalEntities.toLocaleString()}</span>
                                )}
                            </div>
                        </div>

                        {/* Status Card */}
                        <div className={cn(
                            "col-span-1 rounded-lg p-3 flex flex-col shadow-sm transition-all duration-200 min-h-[5.5rem]",
                            isDark
                                ? "bg-gray-800/60 border border-gray-700/50"
                                : "bg-white border border-gray-100"
                        )}>
                            <div className="text-xs uppercase tracking-wider mb-1 font-medium opacity-60">
                                Status
                            </div>
                            <div className="text-lg font-medium flex items-center">
                                <span className={`inline-flex h-3 w-3 rounded-full mr-2
                                    ${statusToDisplay === 'completed' ? 'bg-green-500' :
                                        statusToDisplay === 'failed' ? 'bg-red-500' :
                                            statusToDisplay === 'in_progress' ? 'bg-blue-500 animate-pulse' :
                                                'bg-amber-500'}`}
                                />
                                <span className="capitalize">
                                    {statusToDisplay === 'in_progress' ? 'Running' : statusToDisplay || 'Not run'}
                                    {(statusToDisplay === 'in_progress' || statusToDisplay === 'pending') &&
                                        <span className="animate-pulse ml-1">•••</span>
                                    }
                                </span>
                            </div>
                        </div>

                        {/* Runtime Card */}
                        <div className={cn(
                            "col-span-1 rounded-lg p-3 flex flex-col shadow-sm transition-all duration-200 min-h-[5.5rem]",
                            isDark
                                ? "bg-gray-800/60 border border-gray-700/50"
                                : "bg-white border border-gray-100"
                        )}>
                            <div className="text-xs uppercase tracking-wider mb-1 font-medium opacity-60">
                                Runtime
                            </div>
                            <div className="text-lg font-medium">
                                {runtimeToDisplay ? formatTotalRuntime(runtimeToDisplay) : 'Not available'}
                            </div>
                            {lastSyncJob?.completed_at && <div className="text-xs opacity-70 mt-1">
                                Completed {formatTimeSince(lastSyncJob.completed_at)}
                            </div>}
                        </div>
                    </div>

                    {/* Schedule Card */}
                    <div className={cn(
                        "col-span-12 md:col-span-4 rounded-lg p-3 flex flex-col justify-between shadow-sm transition-all duration-200 min-h-[5.5rem]",
                        isDark
                            ? "bg-gray-800/60 border border-gray-700/50"
                            : "bg-white border border-gray-100"
                    )}>
                        <div className="flex items-center justify-between mb-1">
                            <div className="text-xs uppercase tracking-wider font-medium opacity-60">
                                Schedule
                            </div>
                            <Button
                                variant="ghost"
                                size="sm"
                                className="h-1 px-1 rounded-md"
                                onClick={() => {
                                    setScheduleConfig({
                                        type: connectionToDisplay.cron_schedule ? "scheduled" : "one-time",
                                        frequency: "custom",
                                        cronExpression: connectionToDisplay.cron_schedule || undefined
                                    });
                                    setShowScheduleDialog(true);
                                }}
                            >
                                <Pencil className="h-3 w-3" />
                            </Button>
                        </div>
                        <div className="flex items-center">
                            <Clock className={cn(
                                "w-5 h-5 mr-2",
                                isDark ? "text-gray-400" : "text-gray-500"
                            )} />
                            <div>
                                <div className="text-lg font-medium">
                                    {connectionToDisplay.cron_schedule
                                        ? (nextRunTime ? `Due in ${nextRunTime}` : 'Scheduled')
                                        : 'Manual only'}
                                </div>
                                <div className="text-xs opacity-70 mt-0.5">
                                    {connectionToDisplay.cron_schedule && (
                                        <span>Runs at {formatCronTimeUTC(connectionToDisplay.cron_schedule)}</span>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Display appropriate card based on error status */}
                {jobToDisplay?.error ? (
                    <SyncErrorCard
                        error={jobToDisplay.error}
                        onRunSync={handleRunSync}
                        isInitiatingSyncJob={isInitiatingSyncJob}
                        isSyncJobRunning={isSyncJobRunning}
                        isDark={isDark}
                    />
                ) : (
                    <SyncDagCard
                        selectedConnection={connectionToDisplay}
                        stableEntityDict={entityDictToDisplay}
                        selectedEntity={selectedEntity}
                        setSelectedEntity={setSelectedEntity}
                        nodes={nodes}
                        edges={edges}
                        onNodesChange={onNodesChange}
                        onEdgesChange={onEdgesChange}
                        reactFlowInstance={reactFlowInstance}
                        setReactFlowInstance={setReactFlowInstance}
                        flowContainerRef={flowContainerRef}
                        entityData={entityData}
                        total={total}
                        onRunSync={handleRunSync}
                        isInitiatingSyncJob={isInitiatingSyncJob}
                        isSyncJobRunning={isSyncJobRunning}
                        isDark={isDark}
                    />
                )}
            </div>

            {/* Schedule Edit Dialog */}
            {showScheduleDialog && (
                <Dialog
                    open={showScheduleDialog}
                    onOpenChange={(open) => !open && handleScheduleDialogClose()}
                >
                    <DialogContent className={cn("max-w-3xl", isDark ? "bg-card-solid border-border" : "")}>
                        <DialogHeader>
                            <DialogTitle className={isDark ? "text-foreground" : ""}>Edit Sync Schedule</DialogTitle>
                        </DialogHeader>

                        <div className="py-4">
                            {connectionToDisplay?.id && (
                                <SyncSchedule
                                    value={scheduleConfig}
                                    onChange={(newConfig) => {
                                        console.log("Schedule config changed:", newConfig);
                                        setScheduleConfig(newConfig);
                                    }}
                                />
                            )}
                        </div>

                        <DialogFooter>
                            <Button
                                variant="outline"
                                className={isDark ? "bg-gray-800 text-white hover:bg-gray-700" : ""}
                                onClick={handleScheduleDone}
                            >
                                Save
                            </Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>
            )}
        </div>
    );
};

export default SourceConnectionDetailView;
