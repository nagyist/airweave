import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { ArrowLeft, AlertCircle, Loader2 } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { apiClient } from "@/lib/api";
import { toast } from "sonner";
import { WhiteLabelForm } from "@/components/white-label/WhiteLabelForm";

interface WhiteLabelData {
  id: string;
  name: string;
  source_id: string;
  source_short_name: string;
  redirect_url: string;
  client_id: string;
  client_secret: string;
  allowed_origins: string;
  created_at: string;
  modified_at: string;
}

const WhiteLabelEdit = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const [whiteLabel, setWhiteLabel] = useState<WhiteLabelData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await apiClient.get(`/white-labels/${id}`);
        if (!response.ok) {
          throw new Error(`Failed to fetch white label. Status: ${response.status}`);
        }
        const data = await response.json();
        setWhiteLabel(data);
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

  const handleSuccess = () => {
    toast.success("White label updated successfully");
    navigate(`/white-label/${id}`);
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
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="icon" onClick={() => navigate(`/white-label/${id}`)}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div>
          <h1 className="text-3xl font-bold">Edit White Label</h1>
          <p className="text-muted-foreground mt-2">
            Update your OAuth2 integration for {whiteLabel.name}
          </p>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Edit Integration</CardTitle>
          <CardDescription>
            Update your white label integration details.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <WhiteLabelForm
            onSuccess={handleSuccess}
            initialData={whiteLabel}
            isEditing={true}
          />
        </CardContent>
      </Card>
    </div>
  );
};

export default WhiteLabelEdit;
