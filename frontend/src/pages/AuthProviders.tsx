import { Lightbulb } from "lucide-react";
import { Button } from "@/components/ui/button";
import { AuthProviderTable } from "@/components/auth-providers/AuthProviderTable";

const AuthProviders = () => {
    return (
        <div className="container mx-auto pb-8 space-y-8">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold">Auth Providers</h1>
                    <p className="text-muted-foreground mt-2">
                        Authenticate data sources through third-party applications.
                    </p>
                </div>
                <div className="flex space-x-2">
                    <Button
                        variant="outline"
                        onClick={() => window.open("https://docs.airweave.ai/auth-providers", "_blank")}
                    >
                        <Lightbulb className="h-4 w-4" />
                        Learn More
                    </Button>
                </div>
            </div>

            <AuthProviderTable />
        </div>
    );
};

export default AuthProviders;
