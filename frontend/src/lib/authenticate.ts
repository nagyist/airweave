import { apiClient } from "@/lib/api";
import { redirectWithError } from "@/lib/error-utils";
import { useNavigate } from "react-router-dom";

// Function for OAuth2 authentication flow
const create_credentials_oauth = async (
    dialogState: Record<string, any>,
    navigate?: any  // Accept navigate as a parameter
): Promise<boolean> => {
    try {
        const { sourceDetails, sourceShortName } = dialogState;
        // Don't destructure authValues - access it directly to ensure we get the correct object
        const authValues = dialogState.authValues;

        console.log(`🔄 Starting OAuth2 authentication for ${sourceDetails.name}`);
        console.log(`📦 Auth values:`, authValues); // Debug log
        console.log(`📊 FULL DIALOG STATE IN AUTHENTICATE:`, JSON.stringify(dialogState, null, 2));

        // Make sure we're including dialogId in the stored state
        const stateToStore = {
            ...dialogState,
            // If dialogId isn't already set, use a default one
            dialogId: dialogState.dialogId || "default"
        };

        // Save state to sessionStorage (persists only for this tab, cleared on tab close)
        sessionStorage.setItem('oauth_dialog_state', JSON.stringify(stateToStore));

        // Check if we have a client_id in auth fields
        let url = `/source-connections/${sourceShortName}/oauth2_url`;

        // If client_id is present in auth values, add it as a query parameter
        if (authValues && authValues.client_id) {
            console.log(`🔑 Using provided client_id: ${authValues.client_id}`);
            url += `?client_id=${encodeURIComponent(authValues.client_id)}`;
        }

        // Get OAuth URL from backend
        console.log(`🔄 Fetching OAuth URL from: ${url}`);
        const response = await apiClient.get(url);

        if (!response.ok) {
            const errorData = await response.json().catch(() => response.text());
            throw new Error(typeof errorData === 'string' ? errorData : JSON.stringify(errorData));
        }

        const data = await response.json();

        // Backend returns the URL in the 'url' field
        const authorizationUrl = data.url;

        if (!authorizationUrl) {
            console.error("No authorization URL returned:", data);
            throw new Error("No authorization URL returned from server");
        }

        console.log(`✅ Redirecting to OAuth provider: ${authorizationUrl}`);

        // Redirect to OAuth provider
        window.location.href = authorizationUrl;

        // Return value doesn't matter as we're redirecting away
        return true;
    } catch (error) {
        console.error("❌ OAuth authorization error:", error);

        // Replace the alert with redirectWithError
        if (navigate) {
            // If navigate is available, use it for redirection
            redirectWithError(navigate, error, dialogState.sourceDetails?.name || dialogState.sourceShortName);
        } else {
            // Fallback to window.location if navigate isn't available
            redirectWithError(window.location, error, dialogState.sourceDetails?.name || dialogState.sourceShortName);
        }
        return false;
    }
};

// Function for non-OAuth authentication flow that calls the backend API
const create_credential_non_oauth = async (
    authValues: Record<string, any>,
    sourceDetails: any,
    sourceShortName: string,
    navigate?: any
): Promise<{ success: boolean, credentialId?: string }> => {
    try {
        // Prepare the credential data according to IntegrationCredentialRawCreate schema
        const credentialData = {
            name: `${sourceDetails.name} Credential`,
            integration_short_name: sourceShortName,
            description: `Credential for ${sourceDetails.name}`,
            integration_type: "source", // Must be uppercase to match the enum
            auth_type: sourceDetails.auth_type,
            auth_config_class: sourceDetails.auth_config_class,
            auth_fields: authValues // The auth values from the form
        };

        console.log("Sending credential data:", JSON.stringify(credentialData));

        // Use /connections/credentials/ as the base path
        const response = await apiClient.post(
            `/connections/credentials/source/${sourceShortName}`,
            credentialData
        );

        if (!response.ok) {
            const errorData = await response.json().catch(() => response.text());
            console.error("Error response:", errorData);
            throw new Error(typeof errorData === 'string' ? errorData : JSON.stringify(errorData));
        }

        // Get the created credential with its ID
        const credential = await response.json();

        // Return success with credential ID instead of showing alert
        return { success: true, credentialId: credential.id };
    } catch (error) {
        // Replace the alert with redirectWithError
        if (navigate) {
            redirectWithError(navigate, error, sourceDetails?.name || sourceShortName);
        } else {
            redirectWithError(window.location, error, sourceDetails?.name || sourceShortName);
        }
        return { success: false };
    }
};

// Update authenticateSource to handle the credential ID
export const authenticateSource = async (
    dialogState: Record<string, any>,
    navigate?: any
): Promise<{ success: boolean, credentialId?: string }> => {
    const { authValues, sourceDetails, sourceShortName } = dialogState;

    // Format the data for display
    const authFieldsInfo = Object.entries(authValues)
        .map(([key, value]) => `${key}: ${value === null ? 'null' : value}`)
        .join('\n');

    // Create a console log with formatted information
    const message = `
Source: ${sourceDetails.name} (${sourceShortName})
Auth Config Class: ${sourceDetails.auth_config_class}
Config Class: ${sourceDetails.config_class}
Auth Type: ${sourceDetails.auth_type || 'not specified'}

Authentication Values:
${authFieldsInfo}
    `;

    console.log(message);

    try {
        // Check auth_type and call appropriate function
        if (sourceDetails.auth_type && sourceDetails.auth_type.startsWith('oauth2')) {
            return { success: await create_credentials_oauth(dialogState, navigate) };
        } else {
            return await create_credential_non_oauth(authValues, sourceDetails, sourceShortName, navigate);
        }
    } catch (error) {
        console.error("Authentication failed:", error);

        // Add redirect here too for any uncaught errors
        if (navigate) {
            redirectWithError(navigate, error, sourceDetails?.name || sourceShortName);
        } else {
            redirectWithError(window.location, error, sourceDetails?.name || sourceShortName);
        }
        return { success: false };
    }
};

export default authenticateSource;
