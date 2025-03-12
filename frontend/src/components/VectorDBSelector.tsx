import { useEffect, useState } from "react";
import {
  Card,
  CardHeader,
  CardTitle,
  CardDescription,
  CardContent,
  CardFooter,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Loader2, Check, CirclePlus } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";

import { useToast } from "@/components/ui/use-toast";
import { getDestinationIconUrl } from "@/lib/utils/icons";
import { apiClient } from "@/lib/api";
import { cn } from "@/lib/utils";

interface Destination {
  id: string;
  name: string;
  description: string | null;
  short_name: string;
  auth_type: string | null;
}

interface ConfigField {
  name: string;
  title: string;
  description: string | null;
  type: string;
}

interface DestinationWithConfig extends Destination {
  config_fields?: {
    fields: ConfigField[];
  };
}

interface Connection {
  id: string;
  name: string;
  status: "active" | "inactive" | "error";
  modified_at: string;
  short_name: string;
}

interface VectorDBSelectorProps {
  onComplete: (details: ConnectionSelection | ConnectionSelection[], metadata: { name: string; shortName: string }[]) => void;
}

interface DestinationDetails {
  name: string;
  description: string;
  short_name: string;
  class_name: string;
  auth_type: string;
  auth_config_class: string;
  id: string;
  created_at: string;
  modified_at: string;
  config_fields: {
    fields: ConfigField[];
  };
}

interface ConnectionSelection {
  connectionId: string;
  isNative: boolean;
  destinationName: string;
  destinationShortName: string;
}

/**
 * Example endpoint for listing existing "destination" connections:
 *   GET /connections/list/destination
 * Returns an array of objects matching the Connection interface above.
 */

export const VectorDBSelector = ({ onComplete }: VectorDBSelectorProps) => {
  const [destinations, setDestinations] = useState<DestinationDetails[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [selectedDestination, setSelectedDestination] = useState<DestinationDetails | null>(null);
  const [selectedConnections, setSelectedConnections] = useState<ConnectionSelection[]>([]);
  const [showConfig, setShowConfig] = useState(false);
  const [configValues, setConfigValues] = useState<Record<string, string>>({});
  const [configFields, setConfigFields] = useState<ConfigField[]>([]);
  const [isConnecting, setIsConnecting] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();

  // Simplified data fetching - just like in Destinations.tsx
  useEffect(() => {
    const fetchData = async () => {
      try {
        // 1. Get connections
        const connResp = await apiClient.get("/connections/list/destination");
        if (connResp.ok) {
          const connData = await connResp.json();
          setConnections(connData);
        }

        // 2. Get destinations
        const destResp = await apiClient.get("/destinations/list");
        if (destResp.ok) {
          const destData = await destResp.json();
          setDestinations(destData);
        }
      } catch (err) {
        console.error("Error fetching data:", err);
        toast({
          variant: "destructive",
          title: "Failed to load vector databases",
          description: "Please try again later",
        });
      }
    };

    fetchData();
  }, []);

  /**
   * When user clicks "Add new connection" or chooses to configure a new one,
   * we fetch config fields for that destination's short_name.
   */
  const handleAddNewConnection = async (dest: DestinationDetails) => {
    try {
      const response = await apiClient.get(`/destinations/detail/${dest.short_name}`);
      if (!response.ok) throw new Error("Failed to fetch destination details");
      const data: DestinationWithConfig = await response.json();

      setSelectedDestination(dest);
      setConfigFields(data.config_fields?.fields || []);
      setConfigValues({});
      setShowConfig(true);
    } catch (err) {
      console.error("Error fetching destination details:", err);
      toast({
        variant: "destructive",
        title: "Failed to load configuration",
        description: "Please try again later",
      });
    }
  };

  /**
   * Called when user selects an existing connection or native instance
   */
  const handleUseExistingConnection = (connId: string, isNative?: boolean, dest?: DestinationDetails) => {
    const selection: ConnectionSelection = {
      connectionId: isNative ? "" : connId,
      isNative: isNative,
      destinationName: dest?.name || "Native Weaviate",
      destinationShortName: dest?.short_name || "weaviate_native",
    };
    
    // Check if the connection is already selected
    const alreadySelectedIndex = selectedConnections.findIndex(
      conn => conn.connectionId === selection.connectionId && 
              conn.isNative === selection.isNative && 
              conn.destinationShortName === selection.destinationShortName
    );
    
    // Toggle selection
    if (alreadySelectedIndex >= 0) {
      const updatedSelections = [...selectedConnections];
      updatedSelections.splice(alreadySelectedIndex, 1);
      setSelectedConnections(updatedSelections);
    } else {
      setSelectedConnections([...selectedConnections, selection]);
    }
  };

  // Add a Continue button to proceed with multiple selections
  const handleContinueWithSelections = () => {
    if (selectedConnections.length === 0) {
      toast({
        title: "No destinations selected",
        description: "Please select at least one destination",
        variant: "destructive",
      });
      return;
    }
    
    // Create metadata array from selections
    const metadata = selectedConnections.map(selection => ({
      name: selection.destinationName || "",
      shortName: selection.destinationShortName || "",
    }));

    // If only one selection, call onComplete with single item for backward compatibility
    if (selectedConnections.length === 1) {
      onComplete(selectedConnections[0], metadata);
    } else {
      onComplete(selectedConnections, metadata);
    }
  };

  /**
   * Actually connect a new instance for the currently selected destination.
   */
  const handleConnect = async () => {
    if (!selectedDestination) return;

    const missingFields = configFields.filter((field) => !configValues[field.name]);
    if (missingFields.length > 0) {
      toast({
        variant: "destructive",
        title: "Missing required fields",
        description: `Please fill in: ${missingFields.map((f) => f.title).join(", ")}`,
      });
      return;
    }

    setIsConnecting(true);
    try {
      const response = await apiClient.post(
        `/connections/connect/destination/${selectedDestination.short_name}`,
        configValues
      );

      if (!response.ok) throw new Error("Failed to connect");

      const data = await response.json();
      const newConnection: ConnectionSelection = {
        connectionId: data.id,
        destinationName: selectedDestination.name,
        destinationShortName: selectedDestination.short_name,
        isNative: false
      };
      
      // Add the new connection to selected connections
      setSelectedConnections([...selectedConnections, newConnection]);
      setShowConfig(false);
    } catch (err) {
      toast({
        variant: "destructive",
        title: "Connection failed",
        description: "Please check your credentials and try again",
      });
    } finally {
      setIsConnecting(false);
    }
  };

  /**
   * Render the native Weaviate card separately
   */
  const renderNativeWeaviate = () => {
    const isSelected = selectedConnections.some(
      conn => conn.isNative && conn.destinationShortName === "weaviate_native"
    );

    return (
      <Card className={cn(
        "flex flex-col justify-between hover:border-primary/50 transition-colors bg-gradient-to-br from-background to-muted/50",
        isSelected ? "border-primary bg-primary/5" : ""
      )}>
        <CardHeader>
          <div className="flex items-center space-x-3">
            <img
              src={getDestinationIconUrl("weaviate_native")}
              alt="Weaviate icon"
              className="w-8 h-8"
            />
            <div>
              <CardTitle>Native Weaviate</CardTitle>
              <CardDescription>Built-in vector database</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex-grow">
          <p className="text-sm text-muted-foreground">
            Use the built-in Weaviate instance for optimal performance and seamless integration.
          </p>
        </CardContent>
        <CardFooter>
          <Button 
            className="w-full" 
            variant={isSelected ? "default" : "outline"}
            onClick={() => handleUseExistingConnection("native", true, {
              name: "Native Weaviate",
              short_name: "weaviate_native",
              description: "Built-in vector database",
              auth_type: null,
              id: "",
              created_at: "",
              modified_at: "",
              class_name: "WeaviateDestination",
              auth_config_class: "",
              config_fields: {
                fields: []
              }
            })}
          >
            {isSelected ? "Selected" : "Use Native Instance"}
          </Button>
        </CardFooter>
      </Card>
    );
  };

  /**
   * Render the native Neo4j card separately
   */
  const renderNativeNeo4j = () => {
    const isSelected = selectedConnections.some(
      conn => conn.isNative && conn.destinationShortName === "neo4j_native"
    );

    return (
      <Card className={cn(
        "flex flex-col justify-between hover:border-primary/50 transition-colors bg-gradient-to-br from-background to-muted/50",
        isSelected ? "border-primary bg-primary/5" : ""
      )}>
        <CardHeader>
          <div className="flex items-center space-x-3">
            <img
              src={getDestinationIconUrl("neo4j")}
              alt="Neo4j icon"
              className="w-8 h-8"
            />
            <div>
              <CardTitle>Native Neo4j</CardTitle>
              <CardDescription>Built-in graph database</CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex-grow">
          <p className="text-sm text-muted-foreground">
            Use the built-in Neo4j instance for graph database capabilities and relationship queries.
          </p>
        </CardContent>
        <CardFooter>
          <Button 
            className="w-full" 
            variant={isSelected ? "default" : "outline"}
            onClick={() => handleUseExistingConnection("neo4j_native", true, {
              name: "Native Neo4j",
              short_name: "neo4j_native",
              description: "Built-in graph database",
              auth_type: null,
              id: "",
              created_at: "",
              modified_at: "",
              class_name: "Neo4jDestination",
              auth_config_class: "Neo4jAuthConfig",
              config_fields: {
                fields: []
              }
            })}
          >
            {isSelected ? "Selected" : "Use Native Instance"}
          </Button>
        </CardFooter>
      </Card>
    );
  };

  /**
   * Group connections by destination type and render them as separate cards
   */
  const renderDestinationGroup = (dest: DestinationDetails) => {
    // Skip native Weaviate as it's rendered separately
    if (dest.short_name === "weaviate_native") return null;

    const destConnections = connections
      .filter((c) => c.short_name === dest.short_name)
      .filter((c) => c.status === "active")
      .sort((a, b) => new Date(b.modified_at).getTime() - new Date(a.modified_at).getTime());

    return (
      <div key={dest.short_name} className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <img
              src={getDestinationIconUrl(dest.short_name)}
              alt={`${dest.name} icon`}
              className="w-6 h-6"
            />
            <h3 className="font-semibold text-lg">{dest.name}</h3>
          </div>
        </div>

        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {/* Existing connections */}
          {destConnections.map((conn) => (
            <Card 
              key={conn.id} 
              className={cn(
                "flex flex-col justify-between hover:border-primary/50 transition-colors",
                selectedConnections.some(selected => selected.connectionId === conn.id && !selected.isNative) ? "border-primary bg-primary/5" : ""
              )}
            >
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle>{conn.name}</CardTitle>
                  {conn.status === "active" && (
                    <Check className="h-4 w-4 text-primary" />
                  )}
                </div>
                <CardDescription>
                  {conn.status === "active" ? "Connected" : "Connection Error"}
                </CardDescription>
              </CardHeader>
              <CardContent className="flex-grow">
                <p className="text-sm text-muted-foreground">
                  Last modified: {new Date(conn.modified_at).toLocaleDateString()}
                </p>
              </CardContent>
              <CardFooter>
                <Button 
                  className="w-full" 
                  variant={selectedConnections.some(selected => selected.connectionId === conn.id && !selected.isNative) ? "default" : "outline"}
                  onClick={() => handleUseExistingConnection(conn.id, false, dest)}
                >
                  {selectedConnections.some(selected => selected.connectionId === conn.id && !selected.isNative) ? "Selected" : "Select"}
                </Button>
              </CardFooter>
            </Card>
          ))}
          
          {/* Modified "Add New Instance" card */}
          <Card className="flex flex-col justify-between border-dashed hover:border-primary/50 transition-colors bg-gradient-to-br from-background to-muted/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <CirclePlus className="h-7 w-7" />
                Add New Connection
              </CardTitle>
              <CardDescription>Set up a new {dest.name} instance</CardDescription>
            </CardHeader>
            <CardContent className="flex-grow">
              <p className="text-sm text-muted-foreground">
                Configure a new connection in the destinations page to add it to your vector database collection.
              </p>
            </CardContent>
            <CardFooter>
              <Button 
                variant="outline"
                className="w-full" 
                onClick={() => navigate("/destinations")}
              >
                Go to Destinations
              </Button>
            </CardFooter>
          </Card>
        </div>
      </div>
    );
  };

  // If we are showing config form, show that in a single card
  if (showConfig && selectedDestination) {
    return (
      <div className="max-w-md mx-auto">
        <Card>
          <CardHeader>
            <div className="flex items-center space-x-4">
              <img
                src={getDestinationIconUrl(selectedDestination.short_name)}
                alt={`${selectedDestination.name} icon`}
                className="w-8 h-8"
              />
              <div>
                <CardTitle>{selectedDestination.name}</CardTitle>
                <CardDescription>Configure your connection</CardDescription>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {configFields.map((field) => (
              <div key={field.name} className="space-y-2">
                <label className="text-sm font-medium">
                  {field.title}
                  {field.description && (
                    <span className="text-xs text-muted-foreground ml-2">
                      ({field.description})
                    </span>
                  )}
                </label>
                <Input
                  type={field.type === "string" ? "text" : field.type}
                  value={configValues[field.name] || ""}
                  onChange={(e) =>
                    setConfigValues((prev) => ({
                      ...prev,
                      [field.name]: e.target.value,
                    }))
                  }
                  placeholder={`Enter ${field.title.toLowerCase()}`}
                />
              </div>
            ))}
          </CardContent>
          <CardFooter className="flex justify-between">
            <Button
              variant="outline"
              onClick={() => {
                setShowConfig(false);
                setSelectedDestination(null);
              }}
            >
              Back
            </Button>
            <Button onClick={handleConnect} disabled={isConnecting}>
              {isConnecting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Connecting
                </>
              ) : (
                "Connect"
              )}
            </Button>
          </CardFooter>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Native instances on top */}
      <div className="grid gap-4 sm:grid-cols-2">
        {renderNativeWeaviate()}
        {renderNativeNeo4j()}
      </div>

      {/* Other destinations */}
      <div className="space-y-8">
        {destinations
          .filter(dest => dest.short_name !== "weaviate_native")
          .sort((a, b) => a.name.localeCompare(b.name))
          .map(renderDestinationGroup)}
      </div>

      {/* Add this at the bottom */}
      {selectedConnections.length > 0 && (
        <div className="flex flex-col space-y-4 mt-8">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Selected Destinations ({selectedConnections.length})</h3>
            <Button onClick={handleContinueWithSelections}>
              Continue
            </Button>
          </div>
          <div className="flex flex-wrap gap-2">
            {selectedConnections.map((selection, index) => (
              <Badge key={index} variant="outline" className="px-3 py-1">
                {selection.destinationName}
              </Badge>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};