import { create } from 'zustand';
import { apiClient } from '@/lib/api';
import { onCollectionEvent, COLLECTION_DELETED, COLLECTION_CREATED, COLLECTION_UPDATED } from "@/lib/events";

// Interface for Collection type
export interface Collection {
  id: string;
  name: string;
  readable_id: string;
  status: string;
}

// Interface for SourceConnection type
export interface SourceConnection {
  id: string;
  name: string;
  short_name: string;
  collection: string;
  status?: string;
}

interface CollectionsState {
  collections: Collection[];
  isLoading: boolean;
  error: string | null;
  // Cache for source connections by collection ID
  sourceConnections: Record<string, SourceConnection[]>;
  sourceConnectionsLoading: Record<string, boolean>;

  fetchCollections: (forceRefresh?: boolean) => Promise<Collection[]>;
  fetchSourceConnections: (collectionId: string, forceRefresh?: boolean) => Promise<SourceConnection[]>;
  subscribeToEvents: () => () => void;
}

export const useCollectionsStore = create<CollectionsState>((set, get) => ({
  collections: [],
  isLoading: false,
  error: null,
  sourceConnections: {},
  sourceConnectionsLoading: {},

  fetchCollections: async (forceRefresh = false) => {
    // If collections are already loaded and no force refresh requested, return existing data
    const { collections, isLoading } = get();
    if (collections.length > 0 && !forceRefresh) {
      console.log("🔍 [CollectionsStore] Using cached collections, skipping API call");
      return collections;
    }

    // If we're already loading, don't start another request
    if (isLoading && !forceRefresh) {
      console.log("🔍 [CollectionsStore] Collections already loading, skipping duplicate request");
      return collections;
    }

    console.log("🔍 [CollectionsStore] Fetching collections from API");
    set({ isLoading: true, error: null });

    try {
      const response = await apiClient.get('/collections');

      if (response.ok) {
        const data = await response.json();
        set({ collections: data, isLoading: false });
        return data;
      } else {
        const errorText = await response.text();
        const errorMessage = `Failed to load collections: ${errorText}`;
        set({ error: errorMessage, isLoading: false });
        console.error("❌ [CollectionsStore]", errorMessage);
        return get().collections;
      }
    } catch (err) {
      const errorMessage = `An error occurred: ${err instanceof Error ? err.message : String(err)}`;
      set({ error: errorMessage, isLoading: false });
      console.error("❌ [CollectionsStore]", errorMessage);
      return get().collections;
    }
  },

  fetchSourceConnections: async (collectionId: string, forceRefresh = false) => {
    // Check if we already have this data and aren't currently loading it
    if (get().sourceConnections[collectionId] && !forceRefresh) {
      console.log(`🔍 [CollectionsStore] Using cached source connections for ${collectionId}`);
      return get().sourceConnections[collectionId];
    }

    // Check if already loading this collection's sources
    if (get().sourceConnectionsLoading[collectionId]) {
      console.log(`🔍 [CollectionsStore] Source connections for ${collectionId} already loading`);
      return get().sourceConnections[collectionId] || [];
    }

    // Mark as loading
    console.log(`🔍 [CollectionsStore] Fetching source connections for ${collectionId}`);
    set(state => ({
      sourceConnectionsLoading: {
        ...state.sourceConnectionsLoading,
        [collectionId]: true
      }
    }));

    try {
      const response = await apiClient.get(`/source-connections/?collection=${collectionId}`);

      if (response.ok) {
        const data = await response.json();
        set(state => ({
          sourceConnections: {
            ...state.sourceConnections,
            [collectionId]: data
          },
          sourceConnectionsLoading: {
            ...state.sourceConnectionsLoading,
            [collectionId]: false
          }
        }));
        return data;
      } else {
        console.error(`Failed to load source connections for ${collectionId}:`, await response.text());
        set(state => ({
          sourceConnections: {
            ...state.sourceConnections,
            [collectionId]: []
          },
          sourceConnectionsLoading: {
            ...state.sourceConnectionsLoading,
            [collectionId]: false
          }
        }));
        return [];
      }
    } catch (err) {
      console.error(`Error fetching source connections for ${collectionId}:`, err);
      set(state => ({
        sourceConnections: {
          ...state.sourceConnections,
          [collectionId]: []
        },
        sourceConnectionsLoading: {
          ...state.sourceConnectionsLoading,
          [collectionId]: false
        }
      }));
      return [];
    }
  },

  subscribeToEvents: () => {
    // Subscribe to collection events
    console.log("🔄 [CollectionsStore] Setting up event subscriptions");

    const unsubscribeDeleted = onCollectionEvent(COLLECTION_DELETED, () => {
      console.log("🔄 [CollectionsStore] Collection deleted event - refreshing data");
      get().fetchCollections(true); // Force refresh on delete
    });

    const unsubscribeCreated = onCollectionEvent(COLLECTION_CREATED, () => {
      console.log("🔄 [CollectionsStore] Collection created event - refreshing data");
      get().fetchCollections(true); // Force refresh on create
    });

    const unsubscribeUpdated = onCollectionEvent(COLLECTION_UPDATED, () => {
      console.log("🔄 [CollectionsStore] Collection updated event - refreshing data");
      get().fetchCollections(true); // Force refresh on update
    });

    // Return function to unsubscribe from all events
    return () => {
      console.log("🔄 [CollectionsStore] Cleaning up event subscriptions");
      unsubscribeDeleted();
      unsubscribeCreated();
      unsubscribeUpdated();
    };
  }
}));
