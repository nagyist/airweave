import { useState } from "react";
import {
Dialog,
DialogContent,
DialogHeader,
DialogTitle,
DialogDescription,
DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/use-toast";
import { apiClient } from "@/config/api";
interface ApiKeyAuthDialogProps {
open: boolean;
onOpenChange: (open: boolean) => void;
sourceName: string; // For dynamic display, e.g. "Stripe"
sourceShortName: string; // E.g. "stripe"
onConnected?: () => void; // Callback if the connection is successfully added
}
/**
* A dialog that prompts the user for an API key to connect
* a source (e.g., Stripe).
*/
export function ApiKeyAuthDialog({
open,
onOpenChange,
sourceName,
sourceShortName,
onConnected,
}: ApiKeyAuthDialogProps) {
const [apiKey, setApiKey] = useState("");
const [isLoading, setIsLoading] = useState(false);
const { toast } = useToast();
// Called when the user submits their API key
const handleSave = async () => {
if (!apiKey.trim()) {
toast({
variant: "destructive",
title: "Invalid API Key",
description: "Please enter a valid API key.",
});
return;
}
setIsLoading(true);
try {
// An example endpoint. Adjust the URL or body payload as needed
// to match your backend’s FastAPI route for storing API keys.
const resp = await apiClient.post("/connections/create/api_key", {
sourceShortName,
apiKey,
});
if (!resp.ok) {
throw new Error("Failed to connect with provided API Key.");
}
// If needed, parse JSON if your endpoint returns something
// const data = await resp.json();
toast({
variant: "default",
title: `${sourceName} connected`,
description: "Your API key was saved successfully.",
});
// Optionally call a callback
if (onConnected) onConnected();
// Close the dialog
onOpenChange(false);
} catch (err: any) {
toast({
variant: "destructive",
title: "Connection Failed",
description: err.message || "Could not connect with this API Key.",
});
} finally {
setIsLoading(false);
}
};
return (
<Dialog open={open} onOpenChange={onOpenChange}>
<DialogContent className="max-w-md">
<DialogHeader>
<DialogTitle>Add your API key</DialogTitle>
<DialogDescription>
Get your key for the {sourceName} API and fill it in below.
</DialogDescription>
</DialogHeader>
<div className="mt-4 mb-2 space-y-3">
<Input
placeholder="Paste your API Key here"
type="text"
value={apiKey}
onChange={(e) => setApiKey(e.target.value)}
disabled={isLoading}
/>
</div>
<DialogFooter>
<Button variant="secondary" onClick={() => onOpenChange(false)} disabled={isLoading}>
Cancel
</Button>
<Button onClick={handleSave} disabled={isLoading}>
{isLoading ? "Saving..." : "Save"}
</Button>
</DialogFooter>
</DialogContent>
</Dialog>
);
}
