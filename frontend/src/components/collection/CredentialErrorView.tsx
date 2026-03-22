import React, { useState, useCallback } from 'react';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { DESIGN_SYSTEM } from '@/lib/design-system';
import { AlertCircle, KeyRound, RefreshCw, ExternalLink, ShieldAlert, Loader2 } from 'lucide-react';
import { apiClient } from '@/lib/api';
import { toast } from '@/hooks/use-toast';
import { useTheme } from '@/lib/theme-provider';

interface CredentialErrorViewProps {
  errorCategory: string;
  errorMessage?: string | null;
  providerSettingsUrl?: string | null;
  sourceConnectionId: string;
  shortName: string;
  isDark: boolean;
  onRefreshAuthUrl: () => Promise<void>;
  onCredentialUpdated: () => void;
}

interface CategoryConfig {
  title: string;
  description: string;
  icon: React.ReactNode;
  actionType: 'reauth' | 'credential_form' | 'external_link';
  actionLabel: string;
  color: string;
  bgColor: string;
  borderColor: string;
}

function getCategoryConfig(category: string, isDark: boolean): CategoryConfig {
  switch (category) {
    case 'oauth_credentials_expired':
      return {
        title: 'OAuth Token Expired',
        description: 'Your OAuth authorization has expired. Re-authenticate to restore the connection.',
        icon: <RefreshCw className="h-5 w-5" />,
        actionType: 'reauth',
        actionLabel: 'Re-authenticate',
        color: isDark ? 'text-amber-400' : 'text-amber-600',
        bgColor: isDark ? 'bg-amber-900/10' : 'bg-amber-50',
        borderColor: isDark ? 'border-amber-800/30' : 'border-amber-200',
      };
    case 'api_key_invalid':
      return {
        title: 'API Key Invalid',
        description: 'The API key is no longer valid. Please enter a new one.',
        icon: <KeyRound className="h-5 w-5" />,
        actionType: 'credential_form',
        actionLabel: 'Update API Key',
        color: isDark ? 'text-red-400' : 'text-red-600',
        bgColor: isDark ? 'bg-red-900/10' : 'bg-red-50',
        borderColor: isDark ? 'border-red-800/30' : 'border-red-200',
      };
    case 'client_credentials_invalid':
      return {
        title: 'Client Credentials Invalid',
        description: 'The OAuth client credentials (client ID/secret) are invalid. Please update them.',
        icon: <ShieldAlert className="h-5 w-5" />,
        actionType: 'credential_form',
        actionLabel: 'Update Credentials',
        color: isDark ? 'text-red-400' : 'text-red-600',
        bgColor: isDark ? 'bg-red-900/10' : 'bg-red-50',
        borderColor: isDark ? 'border-red-800/30' : 'border-red-200',
      };
    case 'auth_provider_account_gone':
      return {
        title: 'Auth Provider Account Removed',
        description: 'The connected account on the auth provider has been deleted or deactivated.',
        icon: <AlertCircle className="h-5 w-5" />,
        actionType: 'external_link',
        actionLabel: 'Open Provider Dashboard',
        color: isDark ? 'text-orange-400' : 'text-orange-600',
        bgColor: isDark ? 'bg-orange-900/10' : 'bg-orange-50',
        borderColor: isDark ? 'border-orange-800/30' : 'border-orange-200',
      };
    case 'auth_provider_credentials_invalid':
      return {
        title: 'Auth Provider Credentials Invalid',
        description: 'The credentials on the auth provider need to be refreshed or re-configured.',
        icon: <ShieldAlert className="h-5 w-5" />,
        actionType: 'external_link',
        actionLabel: 'Open Provider Dashboard',
        color: isDark ? 'text-orange-400' : 'text-orange-600',
        bgColor: isDark ? 'bg-orange-900/10' : 'bg-orange-50',
        borderColor: isDark ? 'border-orange-800/30' : 'border-orange-200',
      };
    default:
      return {
        title: 'Authentication Error',
        description: 'There was a problem with your credentials.',
        icon: <AlertCircle className="h-5 w-5" />,
        actionType: 'reauth',
        actionLabel: 'Fix Authentication',
        color: isDark ? 'text-red-400' : 'text-red-600',
        bgColor: isDark ? 'bg-red-900/10' : 'bg-red-50',
        borderColor: isDark ? 'border-red-800/30' : 'border-red-200',
      };
  }
}

interface AuthField {
  name: string;
  title: string;
  type: string;
}

const InlineCredentialForm: React.FC<{
  sourceConnectionId: string;
  shortName: string;
  isDark: boolean;
  onSuccess: () => void;
}> = ({ sourceConnectionId, shortName, isDark, onSuccess }) => {
  const [credentials, setCredentials] = useState<Record<string, string>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [authFields, setAuthFields] = useState<AuthField[]>([]);
  const [isLoadingFields, setIsLoadingFields] = useState(true);

  // Fetch auth fields from source metadata
  React.useEffect(() => {
    (async () => {
      try {
        const response = await apiClient.get(`/sources/${shortName}`);
        if (response.ok) {
          const data = await response.json();
          const fields = data.auth_fields?.fields || [];
          setAuthFields(
            fields.map((f: any) => ({
              name: f.name,
              title: f.title || f.name.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase()),
              type: f.is_secret ? 'password' : 'text',
            }))
          );
        }
      } catch {
        // Fallback to a generic field
        setAuthFields([{ name: 'api_key', title: 'API Key', type: 'password' }]);
      } finally {
        setIsLoadingFields(false);
      }
    })();
  }, [shortName]);

  const allFieldsFilled = authFields.length > 0 && authFields.every(f => credentials[f.name]);

  const handleSubmit = useCallback(async () => {
    if (!allFieldsFilled) return;

    setIsSubmitting(true);
    try {
      const response = await apiClient.patch(
        `/source-connections/${sourceConnectionId}`,
        {
          authentication: {
            credentials: { ...credentials },
          },
        }
      );

      if (response.ok) {
        toast({
          title: 'Credentials Updated',
          description: 'Your credentials have been updated. The next sync will use the new credentials.',
        });
        onSuccess();
      } else {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || 'Failed to update credentials');
      }
    } catch (error) {
      toast({
        title: 'Error',
        description: error instanceof Error ? error.message : 'Failed to update credentials',
        variant: 'destructive',
      });
    } finally {
      setIsSubmitting(false);
    }
  }, [sourceConnectionId, credentials, allFieldsFilled, onSuccess]);

  if (isLoadingFields) {
    return (
      <div className="mt-3 flex items-center gap-2 text-sm text-gray-400">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading fields...
      </div>
    );
  }

  return (
    <div className="mt-3 space-y-3">
      {authFields.map((field) => (
        <div key={field.name}>
          <label
            htmlFor={`cred-${field.name}`}
            className={cn(
              'block text-xs font-medium mb-1',
              isDark ? 'text-gray-400' : 'text-gray-600'
            )}
          >
            {field.title}
          </label>
          <input
            id={`cred-${field.name}`}
            type={field.type}
            value={credentials[field.name] || ''}
            onChange={(e) => setCredentials({ ...credentials, [field.name]: e.target.value })}
            placeholder={`Enter ${field.title.toLowerCase()}`}
            className={cn(
              'w-full px-3 py-2 rounded-md border text-sm',
              isDark
                ? 'bg-gray-800 border-gray-700 text-gray-200 placeholder:text-gray-500'
                : 'bg-white border-gray-300 text-gray-900 placeholder:text-gray-400'
            )}
          />
        </div>
      ))}
      <Button
        onClick={handleSubmit}
        disabled={!allFieldsFilled || isSubmitting}
        size="sm"
        className="w-full"
      >
        {isSubmitting ? (
          <>
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            Updating...
          </>
        ) : (
          'Update Credentials'
        )}
      </Button>
    </div>
  );
};

export const CredentialErrorView: React.FC<CredentialErrorViewProps> = ({
  errorCategory,
  errorMessage,
  providerSettingsUrl,
  sourceConnectionId,
  shortName,
  isDark,
  onRefreshAuthUrl,
  onCredentialUpdated,
}) => {
  const [isReauthing, setIsReauthing] = useState(false);
  const config = getCategoryConfig(errorCategory, isDark);

  const handleReauth = useCallback(async () => {
    setIsReauthing(true);
    try {
      await onRefreshAuthUrl();
    } finally {
      setIsReauthing(false);
    }
  }, [onRefreshAuthUrl]);

  return (
    <Card className={cn(
      'overflow-hidden border p-0',
      DESIGN_SYSTEM.radius.card,
      config.borderColor,
      config.bgColor,
    )}>
      <CardHeader className={cn(DESIGN_SYSTEM.spacing.padding.compact, 'pb-0')}>
        <h3 className={cn(
          DESIGN_SYSTEM.typography.sizes.title,
          DESIGN_SYSTEM.typography.weights.medium,
          'flex items-center',
          config.color,
        )}>
          <span className="mr-2">{config.icon}</span>
          {config.title}
        </h3>
      </CardHeader>
      <CardContent className={DESIGN_SYSTEM.spacing.padding.compact}>
        <p className={cn(
          DESIGN_SYSTEM.typography.sizes.body,
          isDark ? 'text-gray-300' : 'text-gray-600',
        )}>
          {config.description}
        </p>

        {errorMessage && (
          <div className={cn(
            'mt-3 p-3 rounded-md font-mono text-xs overflow-auto max-h-32',
            isDark
              ? 'bg-gray-800/50 text-gray-300 border border-gray-700'
              : 'bg-white/80 text-gray-600 border border-gray-200',
          )}>
            {errorMessage}
          </div>
        )}

        {/* Action area */}
        <div className="mt-4">
          {config.actionType === 'reauth' && (
            <Button
              onClick={handleReauth}
              disabled={isReauthing}
              size="sm"
              variant="default"
            >
              {isReauthing ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Reconnecting...
                </>
              ) : (
                <>
                  <RefreshCw className="h-4 w-4 mr-2" />
                  {config.actionLabel}
                </>
              )}
            </Button>
          )}

          {config.actionType === 'credential_form' && (
            <InlineCredentialForm
              sourceConnectionId={sourceConnectionId}
              shortName={shortName}
              isDark={isDark}
              onSuccess={onCredentialUpdated}
            />
          )}

          {config.actionType === 'external_link' && providerSettingsUrl && (
            <Button
              onClick={() => window.open(providerSettingsUrl, '_blank')}
              size="sm"
              variant="outline"
            >
              <ExternalLink className="h-4 w-4 mr-2" />
              {config.actionLabel}
            </Button>
          )}

          {config.actionType === 'external_link' && !providerSettingsUrl && (
            <p className={cn(
              'text-xs',
              isDark ? 'text-gray-400' : 'text-gray-500',
            )}>
              Please check your auth provider dashboard to resolve this issue.
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
};
