import { memo, useState } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { SplitSquareHorizontal, Wand2, FileText, X, Eye } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface TransformerConfig {
  chunkSize?: number;
  overlap?: number;
  maxChunkSize?: number;
  splitMethod?: 'size' | 'delimiter' | 'visual';
  delimiter?: string;
}

interface TransformerNodeData {
  name: string;
  transformer_id: string;
  config?: TransformerConfig;
  isConfigured?: boolean;
}

const getTransformerIcon = (transformerId: string) => {
  switch (transformerId) {
    case 'text-splitter':
      return <SplitSquareHorizontal className="w-12 h-12" />;
    case 'visual-pdf-chunker':
      return <FileText className="w-12 h-12" />;
    default:
      return <Wand2 className="w-12 h-12" />;
  }
};

const getDefaultConfig = (transformerId: string): TransformerConfig => {
  switch (transformerId) {
    case 'text-splitter':
      return { chunkSize: 1000, overlap: 200, splitMethod: 'size' };
    case 'visual-pdf-chunker':
      return { chunkSize: 1, overlap: 50, maxChunkSize: 2048, splitMethod: 'visual' };
    default:
      return {};
  }
};

export const TransformerNode = memo(({ data, selected, ...props }: NodeProps<TransformerNodeData>) => {
  const [showConfig, setShowConfig] = useState(!data.isConfigured);
  const [config, setConfig] = useState<TransformerConfig>(
    data.config || getDefaultConfig(data.transformer_id)
  );

  const handleSave = () => {
    // Here you would typically update the node data through React Flow's API
    // For now, we'll just close the config
    setShowConfig(false);
    data.isConfigured = true;
    data.config = config;
  };

  return (
    <div className="relative w-20 h-20">
      <div
        className={cn(
          "w-full h-full flex items-center justify-center bg-background/80 backdrop-blur-sm",
          "border-2 transition-colors duration-200 cursor-pointer",
          "border-muted-foreground/50 hover:border-primary rounded-lg",
          selected ? "border-primary shadow-sm shadow-primary/20" : ""
        )}
      >
        <Handle
          type="target"
          position={Position.Left}
          className="w-3 h-3 -ml-[3px] border border-background bg-muted-foreground"
        />
        <div className="w-16 h-16 flex items-center justify-center">
          {getTransformerIcon(data.transformer_id)}
        </div>
        <Handle
          type="source"
          position={Position.Right}
          className="w-3 h-3 -mr-[3px] border border-background bg-muted-foreground"
        />
      </div>
      <div className="absolute -bottom-7 left-1/2 -translate-x-1/2 whitespace-nowrap">
        <span className="text-sm font-semibold text-foreground text-center">{data.name}</span>
      </div>

      {/* Configuration Form */}
      {showConfig && (
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50">
          <Card className="w-80">
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Configure {data.name}</CardTitle>
                <Button 
                  variant="ghost" 
                  size="sm"
                  onClick={() => setShowConfig(false)}
                >
                  <X className="w-4 h-4" />
                </Button>
              </div>
              <CardDescription>
                Set up your transformer parameters
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {data.transformer_id === 'text-splitter' && (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="splitMethod">Split Method</Label>
                    <Select
                      value={config.splitMethod}
                      onValueChange={(value: 'size' | 'delimiter') => 
                        setConfig(prev => ({ ...prev, splitMethod: value }))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select split method" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="size">By Size</SelectItem>
                        <SelectItem value="delimiter">By Delimiter</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  
                  {config.splitMethod === 'size' && (
                    <>
                      <div className="space-y-2">
                        <Label htmlFor="chunkSize">Chunk Size (characters)</Label>
                        <Input
                          id="chunkSize"
                          type="number"
                          value={config.chunkSize}
                          onChange={(e) => 
                            setConfig(prev => ({ ...prev, chunkSize: parseInt(e.target.value) }))
                          }
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="overlap">Overlap Size</Label>
                        <Input
                          id="overlap"
                          type="number"
                          value={config.overlap}
                          onChange={(e) => 
                            setConfig(prev => ({ ...prev, overlap: parseInt(e.target.value) }))
                          }
                        />
                      </div>
                    </>
                  )}

                  {config.splitMethod === 'delimiter' && (
                    <div className="space-y-2">
                      <Label htmlFor="delimiter">Delimiter</Label>
                      <Input
                        id="delimiter"
                        value={config.delimiter}
                        onChange={(e) => 
                          setConfig(prev => ({ ...prev, delimiter: e.target.value }))
                        }
                        placeholder="\n\n"
                      />
                    </div>
                  )}
                </>
              )}

              {data.transformer_id === 'visual-pdf-chunker' && (
                <>
                  <div className="space-y-2">
                    <Label htmlFor="maxChunkSize">Max Chunk Size (tokens)</Label>
                    <Input
                      id="maxChunkSize"
                      type="number"
                      min="128"
                      max="8192"
                      step="128"
                      value={config.maxChunkSize}
                      onChange={(e) => 
                        setConfig(prev => ({ ...prev, maxChunkSize: parseInt(e.target.value) }))
                      }
                    />
                    <p className="text-sm text-muted-foreground">
                      Recommended: 2048 tokens for optimal context
                    </p>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="overlap">Overlap (%)</Label>
                    <Input
                      id="overlap"
                      type="number"
                      min="0"
                      max="100"
                      value={config.overlap}
                      onChange={(e) => 
                        setConfig(prev => ({ ...prev, overlap: parseInt(e.target.value) }))
                      }
                    />
                  </div>
                </>
              )}
            </CardContent>
            <CardFooter className="flex justify-between">
              <Button variant="outline" onClick={() => window.open('#', '_blank')}>
                <Eye className="w-4 h-4 mr-2" />
                Preview Source
              </Button>
              <Button onClick={handleSave}>
                Apply
              </Button>
            </CardFooter>
          </Card>
        </div>
      )}
    </div>
  );
});

TransformerNode.displayName = "TransformerNode"; 