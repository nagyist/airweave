import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Check, Copy, Key } from "lucide-react";
import { toast } from "sonner";

interface ApiKeyCardProps {
  apiKey: {
    decrypted_key?: string;
  } | null;
  onRequestNewKey?: () => void;
}

export const ApiKeyCard = ({ apiKey, onRequestNewKey }: ApiKeyCardProps) => {
  const [copySuccess, setCopySuccess] = useState(false);

  const handleCopyApiKey = () => {
    if (apiKey?.decrypted_key) {
      navigator.clipboard.writeText(apiKey.decrypted_key);
      setCopySuccess(true);
      toast.success("API key copied to clipboard");
      setTimeout(() => setCopySuccess(false), 2000);
    }
  };

  const maskApiKey = (key: string) => {
    if (!key) return "";
    const firstFour = key.substring(0, 4);
    const masked = Array(key.length - 4).fill("*").join("");
    return `${firstFour}${masked}`;
  };

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden shadow-sm">
      <div className="p-5">
        <div className="flex items-center mb-1">
          <Key className="h-4 w-4 mr-1.5 text-muted-foreground" />
          <h3 className="text-sm font-medium">API Key</h3>
        </div>
        <p className="text-xs text-muted-foreground mb-4">Store your API keys securely</p>
        <div className="flex items-center">
          <Input
            value={apiKey?.decrypted_key ? maskApiKey(apiKey.decrypted_key) : `tc-${Array(30).fill("*").join("")}0007`}
            className="text-xs font-mono h-9 bg-background border-border"
            readOnly
          />
          <Button
            variant="ghost"
            size="icon"
            className="ml-1 h-9 w-9 relative"
            onClick={handleCopyApiKey}
          >
            <div className="relative">
              <Copy className={`h-3.5 w-3.5 transition-all duration-200 ${copySuccess ? 'opacity-0 scale-75' : 'opacity-100 scale-100'}`} />
              <Check className={`h-3.5 w-3.5 absolute inset-0 transition-all duration-200 ${copySuccess ? 'opacity-100 scale-100' : 'opacity-0 scale-75'}`} />
            </div>
          </Button>
        </div>
        <div className="mt-2 text-right">
          <button
            className="text-xs text-primary hover:underline"
            onClick={onRequestNewKey}
          >
            Need another API key?
          </button>
        </div>
      </div>
    </div>
  );
};

export default ApiKeyCard;
