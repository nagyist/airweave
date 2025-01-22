import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { getAppIconUrl } from "@/lib/utils/icons";
import { ManageSourceDialog } from "./ManageSourceDialog";
import { ApiKeyAuthDialog } from "./ApiKeyAuthDialog";
import { useState } from "react";
import { Connection, DataSourceCardProps } from "@/types";

interface SourcesDataSourceCardProps extends Omit<DataSourceCardProps, 'onSelect'> {
  onConnect: () => void;
  existingConnections?: Connection[];
  authType?: string;
}

export function SourcesDataSourceCard({ 
  shortName, 
  name, 
  description, 
  status,
  onConnect,
  existingConnections = [],
  authType,
}: SourcesDataSourceCardProps) {
  const [showManageDialog, setShowManageDialog] = useState(false);
  const [showApiKeyDialog, setShowApiKeyDialog] = useState(false);

  if (authType === "api_key" && status === "disconnected"){
    setShowApiKeyDialog(true);
  } else {
    // Fallback to existing “ManageSourceDialog” or OAuth logic, etc.
    onConnect();
    setShowManageDialog(true);
  }
  return (
    <>
      <Card className="w-full min-h-[240px] flex flex-col justify-between overflow-hidden">
        <CardHeader className="p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="flex items-start space-x-3 flex-1 min-w-0">
              <div className="w-8 h-8 shrink-0 flex items-center justify-center">
                <img 
                  src={getAppIconUrl(shortName)} 
                  alt={`${name} icon`}
                  className="w-6 h-6"
                />
              </div>
              <div className="min-w-0 flex-1">
                <CardTitle className="text-lg mb-1 line-clamp-1">{name}</CardTitle>
                <CardDescription className="line-clamp-2">{description}</CardDescription>
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-4 pt-0 flex-grow">
          <p className="text-sm text-muted-foreground line-clamp-3">
            Extract and sync your {name} data to your vector database of choice.
          </p>
        </CardContent>
        <CardFooter className="p-4 pt-0">
          <Button 
            onClick={() => setShowManageDialog(true)} 
            variant={status === "connected" ? "secondary" : "default"} 
            className="w-full"
          >
            {status === "connected" ? "Manage Source" : "Connect"}
          </Button>
        </CardFooter>
      </Card>

      <ManageSourceDialog
        open={showManageDialog}
        onOpenChange={setShowManageDialog}
        name={name}
        shortName={shortName}
        description={description}
        onConnect={onConnect}
        existingConnections={existingConnections}
      />

      <ApiKeyAuthDialog
        open={showApiKeyDialog}
        onOpenChange={setShowApiKeyDialog}
        sourceName={name}
        sourceShortName={shortName}
        onConnected={() => {
          setShowApiKeyDialog(false);
          onConnect();
        }}
      />
    </>
  );
}