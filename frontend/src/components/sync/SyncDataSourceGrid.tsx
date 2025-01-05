import { useEffect, useState } from "react";
import { Input } from "@/components/ui/input";
import { Search } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";
import { SyncDataSourceCard } from "./SyncDataSourceCard";
import { TooltipProvider } from "@/components/ui/tooltip";

interface SyncDataSourceGridProps {
  onSelect: (sourceId: string, skipCredentials?: boolean) => void;
}

/**
 * Represents a Source object from the backend.
 */
interface Source {
  id: string;
  name: string;
  description?: string | null;
  short_name: string;
  auth_type?: string | null;
}

/**
 * Represents a Connection object from the backend (for a source).
 */
interface SourceConnection {
  id: string;
  name: string;
  organization_id: string;
  created_by_email: string;
  modified_by_email: string;
  status: "active" | "inactive" | "error";
  integration_type: string;
  integration_credential_id: string;
  source_id: string;
  modified_at: string;
}

interface Connection {
  id: string;
  name: string;
  isSelected?: boolean;
}

/**
 * Get connections for a specific source by matching source_id
 */
const getConnectionsForSource = (sourceId: string, connections: SourceConnection[]): Connection[] => {
  return connections
    .filter(conn => conn.source_id === sourceId)
    .map(conn => ({
      id: conn.id,
      name: conn.name,
      organization_id: conn.organization_id,
      created_by_email: conn.created_by_email,
      modified_by_email: conn.modified_by_email,
      status: conn.status,
      integration_type: conn.integration_type,
      integration_credential_id: conn.integration_credential_id,
      source_id: conn.source_id,
      modified_at: conn.modified_at
    }));
};

export const SyncDataSourceGrid = ({ onSelect }: SyncDataSourceGridProps) => {
  const [search, setSearch] = useState("");
  const [sources, setSources] = useState<Source[]>([]);
  const [connections, setConnections] = useState<SourceConnection[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const { toast } = useToast();

  /**
   * Fetch sources from the backend.
   */
  const fetchSources = async () => {
    try {
      const resp = await fetch("http://localhost:8001/sources/", {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          // "x-api-key": "someKeyValue" // Add if needed
        },
      });
      if (!resp.ok) {
        throw new Error("Failed to fetch sources");
      }
      const data = await resp.json();
      setSources(data);
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Fetch sources failed",
        description: err.message || String(err),
      });
    }
  };

  /**
   * Fetch source connections from the backend.
   * This endpoint would ideally return all "source" type connections
   * so we can identify which sources are already connected.
   */
  const fetchConnections = async () => {
    try {
      const resp = await fetch("http://localhost:8001/connections/list/source", {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      });
      if (!resp.ok) {
        // It's possible the user doesn't have any connections yet,
        // so handle a 404 or an empty array gracefully if needed
        if (resp.status === 404) {
          setConnections([]);
          return;
        }
        throw new Error("Failed to fetch source connections");
      }
      const data = await resp.json();
      setConnections(data);
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Fetch connections failed",
        description: err.message || String(err),
      });
    }
  };

  useEffect(() => {
    (async () => {
      setIsLoading(true);
      await fetchSources();
      await fetchConnections();
      setIsLoading(false);
    })();
  }, []);

  /**
   * Generate a quick map to see if a short_name is connected
   * to any SourceConnection.
   */
  const shortNameIsConnected = (shortName: string) => {
    // For each connection, we want to see if the associated source
    // has that short_name and is active
    // However, we only have an ID (source_id) in the connections.
    // So we'd need to match connection.source_id to the Source's ID.
    const matchedConnection = connections.find((conn) => {
      const matchedSource = sources.find((s) => s.id === conn.source_id);
      return matchedSource?.short_name === shortName && conn.status === "active";
    });
    return Boolean(matchedConnection);
  };

  /**
   * handleSelect is triggered when the user clicks "Choose Source" or "Connect."
   * If needed, you could determine whether to skip credentials (e.g. if the
   * source is already connected). For now, we'll keep it the same as before.
   */
  const handleSelect = async (sourceId: string) => {
    // Check if there's a connection for this source
    const isConnected = shortNameIsConnected(sourceId);
    // If connected, we might skip credentials and go straight to step 3
    onSelect(sourceId, isConnected);
  };

  // Filter and sort sources similarly to the original approach
  const filteredSources = sources
    .filter((source) =>
      source.name.toLowerCase().includes(search.toLowerCase())
    )
    .sort((a, b) => {
      const aConnected = shortNameIsConnected(a.short_name);
      const bConnected = shortNameIsConnected(b.short_name);
      if (aConnected && !bConnected) return -1;
      if (!aConnected && bConnected) return 1;
      return 0;
    });

  return (
    <div className="space-y-6">
      <div className="relative">
        <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Search apps..."
          value={search}
          disabled={isLoading}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-9"
        />
      </div>
      <TooltipProvider>
        {!filteredSources.length && !isLoading && (
          <div className="text-sm text-muted-foreground">
            No sources found.
          </div>
        )}
        <div className="grid gap-4 grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {filteredSources.map((source) => {
            // Find the source's connections
            const sourceConnections = getConnectionsForSource(
              source.id,
              connections
            );
            
            return (
              <SyncDataSourceCard
                key={source.short_name}
                shortName={source.short_name}
                name={source.name}
                description={source.description || ""}
                status={shortNameIsConnected(source.short_name) ? "connected" : "disconnected"}
                onSelect={() => handleSelect(source.short_name)}
                connections={sourceConnections}
              />
            );
          })}
        </div>
      </TooltipProvider>
    </div>
  );
};