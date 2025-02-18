import { memo } from "react";
import { BaseEdge, EdgeProps, getBezierPath, useNodes, Node } from 'reactflow';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface NodeData {
  name: string;
  config?: {
    type?: string;
  };
}

export const BlankEdge = memo(({
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
}: EdgeProps) => {
  const nodes = useNodes();
  const sourceNode = nodes.find(n => n.id === source) as Node<NodeData>;
  const targetNode = nodes.find(n => n.id === target) as Node<NodeData>;
  
  // Check if source is a file entity and target is not a transformer
  const isFileEdge = sourceNode?.data?.config?.type === 'file' && targetNode?.type !== 'transformer';

  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // For file edges without transformers, only show the first half of the path
  const midX = (sourceX + targetX) / 2;
  const midY = (sourceY + targetY) / 2;
  const displayPath = isFileEdge 
    ? `M ${sourceX} ${sourceY} C ${sourceX + 50} ${sourceY} ${midX - 50} ${midY} ${midX} ${midY}`
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
    </>
  );
});

BlankEdge.displayName = "BlankEdge"; 