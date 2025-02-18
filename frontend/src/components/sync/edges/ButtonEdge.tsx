import { memo } from "react";
import { BaseEdge, EdgeProps, getBezierPath, EdgeLabelRenderer, useNodes, Node } from 'reactflow';
import { PlusCircle, SplitSquareHorizontal, Wand2, FileText } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuLabel,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

// Transformers with enhanced metadata
const TRANSFORMERS = [
  { 
    id: "text-splitter", 
    name: "Text Splitter", 
    description: "Split text into chunks based on size or delimiter",
    icon: SplitSquareHorizontal,
    requiresConfig: true
  },
  { 
    id: "visual-pdf-chunker", 
    name: "Visual PDF Chunker", 
    description: "Intelligently split PDFs while preserving visual context",
    icon: FileText,
    requiresConfig: true
  },
  { 
    id: "summarizer", 
    name: "Summarizer", 
    description: "Generate concise text summaries",
    icon: Wand2,
    requiresConfig: true
  },
];

interface NodeData {
  name: string;
  config?: {
    type?: string;
  };
}

interface ButtonEdgeProps extends EdgeProps {
  data?: {
    onTransformerAdd?: (
      transformerId: string,
      transformerName: string,
      sourceNodeId: string,
      targetNodeId: string,
      sourceEdge: Pick<EdgeProps, 'id' | 'source' | 'target'>
    ) => void;
  };
}

export const ButtonEdge = memo(({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  source,
  target,
  data,
}: ButtonEdgeProps) => {
  const nodes = useNodes();
  const sourceNode = nodes.find(n => n.id === source) as Node<NodeData>;
  const targetNode = nodes.find(n => n.id === target) as Node<NodeData>;
  
  // Check if source is a file entity and target is not a transformer
  const isFileEdge = sourceNode?.data?.config?.type === 'file' && targetNode?.type !== 'transformer';

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // For file edges without transformers, only show the first half of the path
  const halfwayPoint = isFileEdge ? labelX : undefined;
  const displayPath = isFileEdge 
    ? `M ${sourceX} ${sourceY} C ${sourceX + 50} ${sourceY} ${halfwayPoint - 50} ${labelY} ${halfwayPoint} ${labelY}`
    : edgePath;

  return (
    <>
      <TooltipProvider>
        <Tooltip delayDuration={0}>
          <TooltipTrigger asChild>
            <g style={{ pointerEvents: 'all' }}>
              <path
                d={displayPath}
                fill="none"
                strokeWidth="20"
                stroke="transparent"
                style={{ pointerEvents: 'all', cursor: 'pointer' }}
              />
              <BaseEdge
                path={displayPath}
                markerEnd={isFileEdge ? undefined : markerEnd}
                style={{
                  ...style,
                  strokeWidth: 2,
                  stroke: isFileEdge ? '#3b82f6' : '#94a3b8', // blue-500 for file edges
                  strokeDasharray: '6 6',
                  animation: 'flowMove 0.7s linear infinite',
                  pointerEvents: 'none',
                }}
              />
            </g>
          </TooltipTrigger>
          {isFileEdge && (
            <TooltipContent side="top" sideOffset={5} className="max-w-[200px] bg-background/80 backdrop-blur-sm">
              Native Weaviate requires simple text chunks or embeddings.
            </TooltipContent>
          )}
        </Tooltip>
      </TooltipProvider>
      <style>
        {`
          @keyframes flowMove {
            from {
              stroke-dashoffset: 12;
            }
            to {
              stroke-dashoffset: 0;
            }
          }
        `}
      </style>
      {data?.onTransformerAdd && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan"
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: 'all',
            }}
          >
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button 
                  className={cn(
                    "flex items-center justify-center w-6 h-6 rounded-full bg-background border-2 transition-colors",
                    isFileEdge 
                      ? "border-blue-500 hover:border-blue-600" 
                      : "border-border hover:border-primary"
                  )}
                >
                  <PlusCircle className={cn(
                    "w-4 h-4",
                    isFileEdge ? "text-blue-500" : "text-muted-foreground"
                  )} />
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="center" className="w-72">
                <DropdownMenuLabel className="text-center font-semibold">
                  Add Transformer
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                {TRANSFORMERS.map((transformer) => {
                  const Icon = transformer.icon;
                  return (
                    <DropdownMenuItem
                      key={transformer.id}
                      onClick={() => {
                        if (data?.onTransformerAdd) {
                          data.onTransformerAdd(
                            transformer.id,
                            transformer.name,
                            source,
                            target,
                            { id, source, target }
                          );
                        }
                      }}
                      className="py-2"
                    >
                      <div className="flex items-start gap-3">
                        <div className="mt-1">
                          <Icon className="w-5 h-5 text-muted-foreground" />
                        </div>
                        <div className="flex flex-col">
                          <span className="font-medium">{transformer.name}</span>
                          <span className="text-xs text-muted-foreground">
                            {transformer.description}
                          </span>
                        </div>
                      </div>
                    </DropdownMenuItem>
                  );
                })}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
});

ButtonEdge.displayName = "ButtonEdge"; 