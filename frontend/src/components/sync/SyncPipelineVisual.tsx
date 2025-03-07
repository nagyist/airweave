import { SyncDestination, SyncUIMetadata } from "./types";

interface SyncPipelineVisualProps {
  sync: {
    uiMetadata: SyncUIMetadata;
    id?: string;
    status?: string;
    organizationId?: string;
    createdAt?: string;
  };
}

export const SyncPipelineVisual = ({ sync }: SyncPipelineVisualProps) => {
  // Convert destination to array for consistent handling
  const destinations = Array.isArray(sync.uiMetadata.destination) 
    ? sync.uiMetadata.destination 
    : [sync.uiMetadata.destination];

  return (
    <div className="flex flex-col space-y-4">
      <div className="flex items-center justify-center space-x-4 p-4 bg-background rounded-lg border">
        {/* Source */}
        <div className="flex items-center space-x-2 p-2 bg-primary/10 rounded">
          <div className="text-sm font-medium">{sync.uiMetadata.source.name}</div>
        </div>

        {/* Arrow */}
        <div className="text-muted-foreground">→</div>

        {/* Multiple Destinations */}
        <div className="flex flex-col space-y-2">
          {destinations.map((destination: SyncDestination, index: number) => (
            <div key={index} className="flex items-center space-x-2 p-2 bg-primary/10 rounded">
              <div className="text-sm font-medium">{destination.name}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};