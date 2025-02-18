import { memo } from "react";
import { Handle, NodeProps, Position, useEdges, useNodes } from "reactflow";
import { Box, FileText } from "lucide-react";
import { cn } from "@/lib/utils";

interface EntityNodeData {
  name: string;
  entityDefinitionId?: string;
  config?: Record<string, any>;
}

interface EntityNodeProps {
  data: EntityNodeData;
}

export const EntityNode = memo(({ data, selected, id, ...props }: NodeProps) => {
  const edges = useEdges();
  const nodes = useNodes();
  const isFile = data.config?.type === 'file';
  
  // Check if this file node has any outgoing edges to transformers
  const hasTransformer = isFile && edges.some(edge => {
    if (edge.source !== id) return false;
    const targetNode = nodes.find(n => n.id === edge.target);
    return targetNode?.type === 'transformer';
  });

  // Only show file styling if it's a file and hasn't been transformed
  const showFileStyle = isFile && !hasTransformer;
  
  return (
    <div className="w-20 h-20 flex items-center justify-center">
      <div className="w-10 h-10">
        <div
          className={cn(
            "w-full h-full flex items-center justify-center rounded-2xl",
            "transition-colors duration-200",
            showFileStyle 
              ? "bg-blue-500/20 backdrop-blur-sm" 
              : "bg-muted/40 backdrop-blur-sm",
            selected 
              ? showFileStyle ? "bg-blue-500/40" : "bg-muted/90"
              : showFileStyle ? "bg-blue-500/30" : "bg-muted/80"
          )}
        >
          <Handle
            type="target"
            position={Position.Left}
            className="opacity-0 w-0 h-0"
            style={{ left: -6 }}
          />
          {isFile ? (
            <FileText className={cn(
              "w-6 h-6",
              showFileStyle ? "text-blue-500" : "text-muted-foreground"
            )} />
          ) : (
            <Box className="w-6 h-6 text-muted-foreground" />
          )}
          <Handle
            type="source"
            position={Position.Right}
            className="opacity-0 w-0 h-0"
            style={{ right: -6 }}
          />
        </div>
        <div className="absolute -bottom-3 left-1/2 -translate-x-1/2 whitespace-nowrap">
          <span className={cn(
            "text-xs font-light text-center",
            showFileStyle ? "text-blue-500" : "text-foreground"
          )}>
            {data.name}
          </span>
        </div>
      </div>
    </div>
  );
});

EntityNode.displayName = "EntityNode"; 