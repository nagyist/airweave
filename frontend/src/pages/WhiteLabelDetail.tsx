import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Edit, Trash2, Plus, ExternalLink, Shield, AlertCircle, Loader2 } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { TestIntegrationCard } from "@/components/white-label/TestIntegrationCard";
import { CodeSnippet } from "@/components/white-label/CodeSnippet";
import { apiClient } from "@/lib/api";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import { useTheme } from "@/lib/theme-provider";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from "@/components/ui/table";

interface WhiteLabel {
  id: string;
  name: string;
  source_short_name: string;
  redirect_url: string;
  client_id: string;
  client_secret: string;
  allowed_origins: string;
  created_at: string;
  modified_at: string;
}

interface SourceConnection {
  id: string;
  name: string;
  status: string;
  created_at: string;
  collection: string;
}

const WhiteLabelDetail = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const [whiteLabel, setWhiteLabel] = useState<WhiteLabel | null>(null);
  const [sourceConnections, setSourceConnections] = useState<SourceConnection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === 'dark';

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        // Fetch white label details
        const response = await apiClient.get(`/white-labels/${id}`);
        if (!response.ok) {
          throw new Error(`Failed to fetch white label. Status: ${response.status}`);
        }
        const data = await response.json();
        setWhiteLabel(data);

        // Fetch source connections for this white label
        const connectionsResponse = await apiClient.get(`/white-labels/${id}/source-connections`);
        if (connectionsResponse.ok) {
          const connectionsData = await connectionsResponse.json();
          setSourceConnections(connectionsData);
        }
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    if (id) {
      fetchData();
    }
  }, [id]);

  const handleDelete = async () => {
    try {
      setDeleting(true);
      const response = await apiClient.delete(`/white-labels/${id}`);
      if (!response.ok) {
        throw new Error(`Failed to delete white label. Status: ${response.status}`);
      }
      toast.success("White label integration deleted successfully");
      navigate("/white-label");
    } catch (err: any) {
      setError(err.message);
      toast.error(`Failed to delete: ${err.message}`);
    } finally {
      setDeleting(false);
      setDeleteDialogOpen(false);
    }
  };

  if (loading) {
    return (
      <div className="container mx-auto mt-8 flex justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto mt-8">
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>Error: {error}</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={() => navigate("/white-label")}>Back to White Labels</Button>
      </div>
    );
  }

  if (!whiteLabel) {
    return (
      <div className="container mx-auto mt-8">
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>White label not found</AlertDescription>
        </Alert>
        <Button className="mt-4" onClick={() => navigate("/white-label")}>Back to White Labels</Button>
      </div>
    );
  }

  return (
    <div className="container mx-auto pb-8 space-y-8">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate("/white-label")}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <div>
            <h1 className="text-3xl font-bold">{whiteLabel.name}</h1>
            <p className="text-muted-foreground">
              Created on {new Date(whiteLabel.created_at).toLocaleDateString()}
            </p>
          </div>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => navigate(`/white-label/${id}/edit`)}>
            <Edit className="mr-2 h-4 w-4" />
            Edit
          </Button>
          <Button
            variant="outline"
            className="text-red-500"
            onClick={() => setDeleteDialogOpen(true)}
          >
            <Trash2 className="mr-2 h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>

      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="connections">
            Source Connections ({sourceConnections.length})
          </TabsTrigger>
          <TabsTrigger value="code">Integration Code</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-6 pt-4">
          <Card>
            <CardHeader>
              <CardTitle>White Label Details</CardTitle>
              <CardDescription>Details about your white label integration</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-sm font-medium">Integration ID</p>
                  <p className="text-sm text-muted-foreground">{whiteLabel.id}</p>
                </div>
                <div>
                  <p className="text-sm font-medium">Data Source</p>
                  <p className="text-sm text-muted-foreground">{whiteLabel.source_short_name}</p>
                </div>
                <div>
                  <p className="text-sm font-medium">Redirect URL</p>
                  <p className="text-sm text-muted-foreground">{whiteLabel.redirect_url}</p>
                </div>
                <div>
                  <p className="text-sm font-medium">Client ID</p>
                  <p className="text-sm text-muted-foreground">{whiteLabel.client_id}</p>
                </div>
                <div>
                  <p className="text-sm font-medium">Allowed Origins</p>
                  <p className="text-sm text-muted-foreground">
                    {whiteLabel.allowed_origins || "All origins allowed"}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <TestIntegrationCard whitelabelId={whiteLabel.id} />
        </TabsContent>

        <TabsContent value="connections" className="space-y-6 pt-4">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold">Source Connections</h2>
          </div>

          {sourceConnections.length === 0 ? (
            <Card>
              <CardContent className="pt-6">
                <div className="text-center p-4">
                  <p className="text-muted-foreground mb-4">
                    No source connections have been created for this white label yet.
                  </p>
                  <p className="text-sm text-muted-foreground mb-4">
                    Source connections will be automatically created when users authenticate with this integration.
                  </p>
                </div>
              </CardContent>
            </Card>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Collection</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sourceConnections.map((connection) => (
                  <TableRow key={connection.id}>
                    <TableCell className="font-medium">{connection.name}</TableCell>
                    <TableCell>
                      <span className={`px-2 py-1 rounded-full text-xs ${
                        connection.status === 'ACTIVE'
                          ? 'bg-green-100 text-green-800'
                          : 'bg-amber-100 text-amber-800'
                      }`}>
                        {connection.status}
                      </span>
                    </TableCell>
                    <TableCell>{connection.collection}</TableCell>
                    <TableCell>
                      {new Date(connection.created_at).toLocaleDateString()}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => window.open(`/collections/${connection.collection}`, "_blank")}
                      >
                        <ExternalLink className="h-4 w-4" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </TabsContent>

        <TabsContent value="code" className="space-y-6 pt-4">
          <Card>
            <CardHeader>
              <CardTitle>Integration Code</CardTitle>
              <CardDescription>
                Copy this code to implement the integration in your application.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <CodeSnippet
                whitelabelGuid={whiteLabel.id}
                frontendUrl={whiteLabel.redirect_url}
                clientId={whiteLabel.client_id}
                source={whiteLabel.source_short_name}
              />
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Delete confirmation dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Shield className={cn("h-5 w-5", isDark ? "text-amber-400" : "text-amber-500")} />
              Confirm Delete
            </DialogTitle>
            <DialogDescription>
              This will permanently delete the white label integration and all associated source connections.
              Any applications currently using this integration will no longer be able to authenticate.
            </DialogDescription>
          </DialogHeader>

          {whiteLabel && (
            <div className={cn(
              "my-4 p-3 rounded-md",
              isDark ? "bg-slate-900 border border-slate-800" : "bg-slate-50 border border-slate-200"
            )}>
              <p className="font-medium mb-1">{whiteLabel.name}</p>
              <div className="text-sm text-muted-foreground">{whiteLabel.source_short_name}</div>
              <div className="text-sm text-muted-foreground">{whiteLabel.id}</div>
            </div>
          )}

          <DialogFooter className="sm:justify-between gap-3 mt-2">
            <Button
              variant="ghost"
              onClick={() => setDeleteDialogOpen(false)}
              disabled={deleting}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleting}
              className="sm:w-auto w-full"
            >
              {deleting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Deleting...
                </>
              ) : (
                <>
                  <Trash2 className="mr-2 h-4 w-4" />
                  Delete Integration
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default WhiteLabelDetail;
