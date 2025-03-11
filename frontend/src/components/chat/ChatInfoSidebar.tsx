import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Slider } from "@/components/ui/slider";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ChevronRight, Settings, Link as LinkIcon, Settings2, Trash2, Database } from "lucide-react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Separator } from "@/components/ui/separator";
import { useToast } from "@/hooks/use-toast";
import { ModelSettings, Chat } from "./types";
import { Badge } from "@/components/ui/badge";
import { getAppIconUrl, getDestinationIconUrl } from "@/lib/utils/icons";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ChatInfoSidebarProps } from "./types";
import { cn } from "@/lib/utils";
import { useNavigate } from "react-router-dom";
import { apiClient } from "@/lib/api";
import { useTheme } from "@/lib/theme-provider";
import { SearchTypeSelector } from "./SearchTypeSelector";

const AVAILABLE_MODELS = [
  { id: "gpt-4o", name: "GPT-4o" },
  { id: "gpt-4o-mini", name: "GPT-4o Mini" },
  { id: "o1-preview", name: "O1 Preview" },
];

// Add this custom badge style variant
const badgeVariants = {
  active: "bg-emerald-50 text-emerald-700 border-emerald-200",
  inactive: "bg-gray-50 text-gray-600 border-gray-200",
};

export function ChatInfoSidebar({ chatInfo, onUpdateSettings }: ChatInfoSidebarProps) {
  const [modelSettings, setModelSettings] = useState<ModelSettings>(() => {
    // Get search_type from various sources with priority
    // 1. localStorage (for persistence across refreshes)
    // 2. chatInfo.model_settings.search_type 
    // 3. Default to "vector" if nothing else is available
    
    const savedSearchType = localStorage.getItem(`chat_search_type_${chatInfo.id}`);
    const currentSearchType = chatInfo.model_settings?.search_type;
    
    const searchType = savedSearchType && ["vector", "graph", "hybrid"].includes(savedSearchType) 
      ? savedSearchType as "vector" | "graph" | "hybrid"
      : currentSearchType || "vector";
    
    // Store this search type for persistence
    if (searchType) {
      localStorage.setItem(`chat_search_type_${chatInfo.id}`, searchType);
    }
    
    // Ensure we have all required ModelSettings properties
    return {
      temperature: chatInfo.model_settings?.temperature || 0.7,
      max_tokens: chatInfo.model_settings?.max_tokens || 1000,
      top_p: chatInfo.model_settings?.top_p || 1,
      frequency_penalty: chatInfo.model_settings?.frequency_penalty || 0,
      presence_penalty: chatInfo.model_settings?.presence_penalty || 0,
      search_type: searchType
    };
  });
  const [modelName, setModelName] = useState<string>(chatInfo.model_name);
  const { toast } = useToast();
  const [saveTimeout, setSaveTimeout] = useState<NodeJS.Timeout | null>(null);
  const navigate = useNavigate();
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const { resolvedTheme } = useTheme();
  
  // Track if we're currently updating search type to prevent feedback loops
  const [isUpdatingSearchType, setIsUpdatingSearchType] = useState(false);

  useEffect(() => {
    // Only update if we're not currently in the middle of a search type update
    if (!isUpdatingSearchType) {
      // Preserve search type from local state if it differs from server
      const currentSearchType = modelSettings.search_type;
      
      setModelSettings(prev => ({
        ...chatInfo.model_settings,
        // Preserve our locally selected search type if it exists
        search_type: currentSearchType || chatInfo.model_settings.search_type || "vector"
      }));
      
      setModelName(chatInfo.model_name);
    }
  }, [chatInfo.model_settings, chatInfo.model_name, isUpdatingSearchType]);

  const handleModelChange = async (newModelName: string) => {
    setModelName(newModelName);
    try {
      // Update model_name at root level
      await onUpdateSettings({ model_name: newModelName } as any);
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to update model",
        variant: "destructive",
      });
    }
  };

  const handleSettingChange = (key: keyof ModelSettings, value: number | string) => {
    // Create new settings object with the updated value
    const newSettings = { ...modelSettings, [key]: value };
    setModelSettings(newSettings);
    
    // Clear existing timeout
    if (saveTimeout) {
      clearTimeout(saveTimeout);
    }
    
    // For search_type changes, apply immediately without debounce
    if (key === "search_type") {
      (async () => {
        try {
          // Update model_settings separately
          console.log(`Updating search type to: ${value}`);
          await onUpdateSettings({ model_settings: newSettings } as any);
        } catch (error) {
          // If update fails, revert the local state
          setModelSettings(modelSettings);
          toast({
            title: "Error",
            description: "Failed to update search type",
            variant: "destructive",
          });
        }
      })();
      return;
    }
    
    // For other settings, use debounce
    const timeout = setTimeout(async () => {
      try {
        // Update model_settings separately
        await onUpdateSettings({ model_settings: newSettings } as any);
      } catch (error) {
        toast({
          title: "Error",
          description: "Failed to save settings",
          variant: "destructive",
        });
      }
    }, 500);
    
    setSaveTimeout(timeout);
  };

  const handleDelete = async () => {
    try {
      await apiClient.delete(`/chat/${chatInfo.id}`);
      toast({
        title: "Chat deleted",
        description: "The chat has been deleted successfully.",
      });
      navigate('/chat');
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to delete chat",
        variant: "destructive",
      });
    }
  };

  // Enhanced version of handleSearchTypeChange
  const handleSearchTypeChange = async (searchType: "vector" | "graph" | "hybrid") => {
    // Save to localStorage for persistence across refreshes
    localStorage.setItem(`chat_search_type_${chatInfo.id}`, searchType);
    
    // Update local state
    setModelSettings((prev) => ({
      ...prev,
      search_type: searchType,
    }));
    
    // Notify parent component to update settings remotely
    try {
      // Pass settings directly in the format expected by the onUpdateSettings prop
      await onUpdateSettings({
        temperature: modelSettings.temperature,
        max_tokens: modelSettings.max_tokens, 
        top_p: modelSettings.top_p,
        frequency_penalty: modelSettings.frequency_penalty,
        presence_penalty: modelSettings.presence_penalty,
        search_type: searchType
      });
      
      toast({
        title: "Search type updated",
        description: `Using ${searchType} search for this chat`,
        variant: "default",
      });
    } catch (error) {
      console.error("Failed to update search type:", error);
      toast({
        title: "Failed to update search type",
        description: "An error occurred while updating search settings",
        variant: "destructive",
      });
    }
  };

  return (
    <>
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Chat</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this chat? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              Cancel
            </Button>
            <Button 
              variant="destructive" 
              onClick={() => {
                setShowDeleteDialog(false);
                void handleDelete();
              }}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <div className="w-[350px] border-l bg-background">
        <div className="p-4 border-b flex items-center justify-between">
          <h2 className="font-semibold">Chat Settings</h2>
          <div className="flex items-center gap-2">
            <Button 
              variant="ghost" 
              size="icon"
              onClick={() => setShowDeleteDialog(true)}
              className="h-8 w-8 text-muted-foreground hover:text-destructive"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
            <Settings2 className="h-4 w-4 text-muted-foreground" />
          </div>
        </div>
        
        <ScrollArea className="h-[calc(100vh-65px)]">
          <div className="p-4 space-y-6">
            {/* Chat Details Section */}
            <div className="space-y-4">
              <div className="space-y-2">
                <h3 className="text-sm font-medium text-muted-foreground">CHAT DETAILS</h3>
                <div className="rounded-lg border p-3 space-y-3">
                  <div className="space-y-1">
                    <Label className="text-xs text-muted-foreground">Name</Label>
                    <p className="text-sm font-medium">{chatInfo.name}</p>
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs text-muted-foreground">Chat ID</Label>
                    <p className="text-xs font-mono text-muted-foreground">{chatInfo.id}</p>
                  </div>
                  {chatInfo.description && (
                    <div className="space-y-1">
                      <Label className="text-xs text-muted-foreground">Description</Label>
                      <p className="text-sm">{chatInfo.description}</p>
                    </div>
                  )}
                </div>
              </div>

              {/* Data Connection Section */}
              {chatInfo.sync && (
                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-muted-foreground">DATA CONNECTION</h3>
                  <div className="rounded-lg border p-3 space-y-4">
                    {/* Sync Details */}
                    <div className="space-y-3">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium">{chatInfo.sync.name}</span>
                        <Badge 
                          className={cn(
                            "text-[10px] font-medium px-2 py-0.5 border", 
                            chatInfo.sync.status === 'active' ? badgeVariants.active : badgeVariants.inactive
                          )}
                        >
                          {chatInfo.sync.status}
                        </Badge>
                      </div>
                      <div className="space-y-1">
                        <Label className="text-xs text-muted-foreground">Sync ID</Label>
                        <p className="text-xs font-mono text-muted-foreground">{chatInfo.sync.id}</p>
                      </div>
                    </div>

                    <Separator />

                    {/* Source Connection */}
                    {chatInfo.sync.source_connection && (
                      <div className="space-y-2">
                        <Label className="text-xs text-muted-foreground">Source</Label>
                        <div className="space-y-2">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              <img 
                                src={getAppIconUrl(chatInfo.sync.source_connection.short_name, resolvedTheme)} 
                                alt={chatInfo.sync.source_connection.source?.name}
                                className="h-5 w-5"
                              />
                              <span className="text-sm">{chatInfo.sync.source_connection.source?.name}</span>
                            </div>
                            <div className="flex items-center gap-2">
                              {chatInfo.sync.source_connection.source?.app_url && (
                                <a 
                                  href={chatInfo.sync.source_connection.source.app_url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs text-muted-foreground hover:text-primary"
                                >
                                  <LinkIcon className="h-3 w-3" />
                                </a>
                              )}
                              <Badge 
                                className={cn(
                                  "text-[10px] font-medium px-2 py-0.5 border", 
                                  chatInfo.sync.source_connection.status === 'active' ? badgeVariants.active : badgeVariants.inactive
                                )}
                              >
                                {chatInfo.sync.source_connection.status}
                              </Badge>
                            </div>
                          </div>
                          <div className="space-y-1">
                            <Label className="text-xs text-muted-foreground">Connection ID</Label>
                            <p className="text-xs font-mono text-muted-foreground">{chatInfo.sync.source_connection.id}</p>
                          </div>
                        </div>
                      </div>
                    )}

                    <Separator />

                    {/* Destination */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground">Destination</Label>
                      <div className="space-y-2">
                        <div className="flex items-center gap-2">
                          <img 
                            src={getDestinationIconUrl(
                              chatInfo.sync.destination_connection?.short_name || "weaviate_native"
                            )} 
                            alt={chatInfo.sync.destination_connection?.destination?.name || "Native Weaviate"}
                            className="h-5 w-5"
                          />
                          <span className="text-sm">
                            {chatInfo.sync.destination_connection?.destination?.name || "Native Weaviate"}
                          </span>
                        </div>
                        {chatInfo.sync.destination_connection && (
                          <>
                            <div className="space-y-1">
                              <Label className="text-xs text-muted-foreground">Connection ID</Label>
                              <p className="text-xs font-mono text-muted-foreground">{chatInfo.sync.destination_connection.id}</p>
                            </div>
                            <div className="space-y-1">
                              <Label className="text-xs text-muted-foreground">Status</Label>
                              <Badge 
                                className={cn(
                                  "text-[10px] font-medium px-2 py-0.5 border",
                                  chatInfo.sync.destination_connection.status === 'active' ? badgeVariants.active : badgeVariants.inactive
                                )}
                              >
                                {chatInfo.sync.destination_connection.status}
                              </Badge>
                            </div>
                          </>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Model Settings Section */}
              <div className="space-y-2">
                <h3 className="text-sm font-medium text-muted-foreground">MODEL SETTINGS</h3>
                <div className="rounded-lg border p-3 space-y-4">
                  {/* Model Selection */}
                  <div className="space-y-2">
                    <Label className="text-xs text-muted-foreground">Model</Label>
                    <Select
                      value={modelName}
                      onValueChange={handleModelChange}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="Select a model" />
                      </SelectTrigger>
                      <SelectContent>
                        {AVAILABLE_MODELS.map((model) => (
                          <SelectItem key={model.id} value={model.id}>
                            {model.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Temperature */}
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <Label className="text-xs text-muted-foreground">Temperature</Label>
                      <span className="text-xs">{modelSettings.temperature?.toFixed(1)}</span>
                    </div>
                    <Slider
                      value={[modelSettings.temperature ?? 0.7]}
                      min={0}
                      max={1}
                      step={0.1}
                      onValueChange={([value]) => handleSettingChange("temperature", value)}
                    />
                  </div>

                  {/* Max Tokens */}
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <Label className="text-xs text-muted-foreground">Max Tokens</Label>
                      <span className="text-xs">{modelSettings.max_tokens}</span>
                    </div>
                    <Slider
                      value={[modelSettings.max_tokens ?? 1000]}
                      min={100}
                      max={4000}
                      step={100}
                      onValueChange={([value]) => handleSettingChange("max_tokens", value)}
                    />
                  </div>

                  {/* Top P */}
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <Label className="text-xs text-muted-foreground">Top P</Label>
                      <span className="text-xs">{modelSettings.top_p?.toFixed(1)}</span>
                    </div>
                    <Slider
                      value={[modelSettings.top_p ?? 1]}
                      min={0}
                      max={1}
                      step={0.1}
                      onValueChange={([value]) => handleSettingChange("top_p", value)}
                    />
                  </div>

                  {/* Frequency Penalty */}
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <Label className="text-xs text-muted-foreground">Frequency Penalty</Label>
                      <span className="text-xs">{modelSettings.frequency_penalty?.toFixed(1)}</span>
                    </div>
                    <Slider
                      value={[modelSettings.frequency_penalty ?? 0]}
                      min={0}
                      max={2}
                      step={0.1}
                      onValueChange={([value]) => handleSettingChange("frequency_penalty", value)}
                    />
                  </div>

                  {/* Presence Penalty */}
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <Label className="text-xs text-muted-foreground">Presence Penalty</Label>
                      <span className="text-xs">{modelSettings.presence_penalty?.toFixed(1)}</span>
                    </div>
                    <Slider
                      value={[modelSettings.presence_penalty ?? 0]}
                      min={0}
                      max={2}
                      step={0.1}
                      onValueChange={([value]) => handleSettingChange("presence_penalty", value)}
                    />
                  </div>
                </div>
              </div>

              {/* Search Settings */}
              <div className="space-y-6">
                <div>
                  <div className="flex items-center">
                    <Database className="mr-2 h-4 w-4" />
                    <h3 className="text-sm font-medium">Search Settings</h3>
                  </div>
                  <Separator className="my-2" />
                  
                  <div className="space-y-4 mt-4">
                    {/* Custom search type selector using radio buttons to avoid resets */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground">Search Type</Label>
                      
                      <div className="space-y-2 mt-2">
                        <div 
                          className={`border p-2 rounded-md cursor-pointer hover:bg-accent hover:text-accent-foreground ${modelSettings.search_type === 'vector' ? 'bg-primary text-primary-foreground' : ''}`}
                          onClick={() => handleSearchTypeChange('vector')}
                        >
                          <div className="font-medium">Vector Search</div>
                          <div className="text-xs">Search using embeddings for semantic similarity</div>
                        </div>
                        
                        <div 
                          className={`border p-2 rounded-md cursor-pointer hover:bg-accent hover:text-accent-foreground ${modelSettings.search_type === 'graph' ? 'bg-primary text-primary-foreground' : ''}`}
                          onClick={() => handleSearchTypeChange('graph')}
                        >
                          <div className="font-medium">Graph Search</div>
                          <div className="text-xs">Search using graph relationships between entities</div>
                        </div>
                        
                        <div 
                          className={`border p-2 rounded-md cursor-pointer hover:bg-accent hover:text-accent-foreground ${modelSettings.search_type === 'hybrid' ? 'bg-primary text-primary-foreground' : ''}`}
                          onClick={() => handleSearchTypeChange('hybrid')}
                        >
                          <div className="font-medium">Hybrid Search</div>
                          <div className="text-xs">Combine vector and graph search for comprehensive results</div>
                        </div>
                      </div>
                    </div>
                    
                    {/* Show info for graph search if selected but not available */}
                    {modelSettings.search_type === "graph" && (
                      <div className="bg-yellow-50 rounded p-2 text-xs text-yellow-800">
                        <p>
                          <strong>Note:</strong> Graph search requires a properly configured Neo4j instance.
                          If unavailable, results may fall back to vector search.
                        </p>
                      </div>
                    )}
                    
                    {/* Show info for hybrid search if selected */}
                    {modelSettings.search_type === "hybrid" && (
                      <div className="bg-blue-50 rounded p-2 text-xs text-blue-800">
                        <p>
                          <strong>Note:</strong> Hybrid search combines results from both vector and graph search.
                          Both Weaviate and Neo4j must be configured for optimal results.
                        </p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </ScrollArea>
      </div>
    </>
  );
} 