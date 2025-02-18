import { memo } from "react";
import { BaseEdge, EdgeProps, getBezierPath, EdgeLabelRenderer } from 'reactflow';
import { PlusCircle, SplitSquareHorizontal, Wand2, FileText } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuLabel,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";

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
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  return (
    <>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          ...style,
          strokeWidth: 2,
          stroke: '#94a3b8',
          strokeDasharray: '6 6',
          animation: 'flowMove 0.7s linear infinite',
        }}
      />
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
                <button className="flex items-center justify-center w-6 h-6 rounded-full bg-background border-2 border-border hover:border-primary transition-colors">
                  <PlusCircle className="w-4 h-4 text-muted-foreground" />
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