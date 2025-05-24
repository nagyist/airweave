/**
 * AuthCallback.tsx
 *
 * This component handles the OAuth callback process after a user authorizes
 * an external service. It's the final leg of the OAuth flow, where we:
 *
 * 1. Receive the authorization code from the OAuth provider
 * 2. Exchange the code for a connection via the backend
 * 3. Create the collection (using previously stored details)
 * 4. Create the source connection with the OAuth credentials
 * 5. Redirect the user to the appropriate page
 *
 * Flow context:
 * - This page is loaded when returning from an OAuth provider (like Google, GitHub, etc.)
 * - The user previously started in ConnectFlow, which stored collection details
 * - After processing, the user is redirected to the collection detail page
 */

import React, { useEffect, useState, useRef } from "react";
import { useSearchParams, useParams } from "react-router-dom";
import { apiClient } from "@/lib/api";
import { CONNECTION_ERROR_STORAGE_KEY } from "@/lib/error-utils";

/**
 * AuthCallback Component
 *
 * This component handles the final leg of the OAuth2 flow:
 * 1) Receives code from OAuth provider in URL
 * 2) Exchanges code for connection via /connections/oauth2/source/code
 * 3) Creates source connection using our custom endpoint
 * 4) Redirects to collection page with success/error status
 */
export function AuthCallback() {
  const [searchParams] = useSearchParams();
  const { short_name } = useParams();
  const [isProcessing, setIsProcessing] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add this ref to track if we've already processed the code
  const hasProcessedRef = useRef(false);

  useEffect(() => {
    const processOAuthCallback = async () => {
      // Skip if we've already processed this code
      if (hasProcessedRef.current) return;

      try {
        // Set the ref immediately to prevent duplicate processing
        hasProcessedRef.current = true;

        // Get code from URL
        const code = searchParams.get("code");
        const errorParam = searchParams.get("error");

        // Check for OAuth provider errors
        if (errorParam) {
          console.error(`OAuth provider returned error: ${errorParam}`);
          const errorDesc = searchParams.get("error_description") || "Authorization denied";

          // Set error state for UI
          setError(`OAuth error: ${errorParam} - ${errorDesc}`);
          setIsProcessing(false);

          // Create error data and store it
          const errorData = {
            serviceName: short_name,
            errorMessage: `OAuth error: ${errorParam} - ${errorDesc}`,
            errorDetails: `The OAuth provider rejected the authorization request with error: ${errorParam}`,
            timestamp: Date.now()
          };

          // Store in localStorage without risk of exception
          localStorage.setItem(CONNECTION_ERROR_STORAGE_KEY, JSON.stringify(errorData));

          // Immediate redirect to dashboard with error flag
          window.location.href = "/dashboard?connected=error";
          return;
        }

        if (!code || !short_name) {
          throw new Error("Missing required parameters (code or source)");
        }

        // Retrieve saved dialog state
        const savedStateJson = sessionStorage.getItem('oauth_dialog_state');
        if (!savedStateJson) {
          throw new Error("Missing dialog state - cannot restore context");
        }

        const savedState = JSON.parse(savedStateJson);
        console.log("📋 Retrieved saved state:", savedState);
        console.log("📊 FULL SAVED STATE IN AUTH CALLBACK:", JSON.stringify(savedState, null, 2));

        // Exchange code for credentials using the new endpoint
        console.log(`🔄 Exchanging code for credentials for ${short_name}`);

        // Define interface for type safety
        interface CredentialRequestData {
          credential_name: string;
          credential_description: string;
          client_id?: string;
          client_secret?: string;
        }

        const requestData: CredentialRequestData = {
          credential_name: `${savedState.sourceDetails?.name || short_name} OAuth Credential`,
          credential_description: `OAuth credential for ${savedState.sourceDetails?.name || short_name}`
        };

        // Add client_id and client_secret from authValues if they exist
        if (savedState.authValues?.client_id) {
          requestData.client_id = savedState.authValues.client_id;
        }
        if (savedState.authValues?.client_secret) {
          requestData.client_secret = savedState.authValues.client_secret;
        }

        const response = await apiClient.post(
          `/source-connections/${short_name}/code_to_token_credentials?code=${encodeURIComponent(code)}`,
          requestData
        );

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Failed to exchange code: ${errorText}`);
        }

        // Get credential data
        const credential = await response.json();
        console.log("✅ Credentials created:", credential.id);

        // Update saved state with credential info
        const updatedState = {
          ...savedState,
          credentialId: credential.id,
          isAuthenticated: true
        };
        console.log("📊 UPDATED STATE WITH CREDENTIALS:", JSON.stringify(updatedState, null, 2));
        sessionStorage.setItem('oauth_dialog_state', JSON.stringify(updatedState));

        // Redirect back to original page with flag to restore dialog
        const returnPath = savedState.originPath || "/dashboard";
        window.location.href = `${returnPath}?restore_dialog=true`;

      } catch (error) {
        console.error("❌ Error processing OAuth callback:", error);
        setError(error instanceof Error ? error.message : String(error));
        setIsProcessing(false);

        // Store error details using error-utils before redirecting
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorDetails = error instanceof Error ? error.stack : undefined;

        // Create error details object
        const errorData = {
          serviceName: short_name,
          errorMessage,
          errorDetails,
          timestamp: Date.now()
        };

        // Store in localStorage
        localStorage.setItem(CONNECTION_ERROR_STORAGE_KEY, JSON.stringify(errorData));

        // Redirect with error flag - this will trigger the error UI
        const savedState = sessionStorage.getItem('oauth_dialog_state');
        const parsedState = savedState ? JSON.parse(savedState) : {};
        const returnPath = parsedState.originPath || "/dashboard";
        window.location.href = `${returnPath}?connected=error`;

        console.error("OAuth error detected:", {
          error: errorParam,
          description: searchParams.get("error_description") || "Not provided",
          state: searchParams.get("state"),
          savedState: !!savedStateJson
        });
      }
    };

    processOAuthCallback();
  }, [searchParams, short_name]);

  // Simple loading/error UI
  if (error) {
    return (
      <div className="flex items-center justify-center h-screen flex-col">
        <div className="rounded-lg border bg-card p-8 max-w-md text-center">
          <h2 className="text-xl font-semibold mb-4">Authentication Error</h2>
          <p className="text-muted-foreground mb-6">{error}</p>
          <a
            href="/dashboard?connected=error"
            className="inline-block px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 transition-colors"
          >
            Return to Dashboard
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-center h-screen">
      <div className="text-center">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500 mx-auto mb-4"></div>
        <p className="text-muted-foreground text-lg font-medium mb-2">Completing authentication...</p>
        <p className="text-sm text-muted-foreground">
          Processing OAuth response from {short_name}...
        </p>
      </div>
    </div>
  );
}
