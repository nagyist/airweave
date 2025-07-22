import React, { useState, useRef, useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import * as z from "zod";
import { Button } from "@/components/ui/button";
import { DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { DialogViewProps } from "../DialogFlow";
import { useTheme } from "@/lib/theme-provider";
import { cn } from "@/lib/utils";
import { apiClient } from "@/lib/api";
import { Loader2, Pencil, Info } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { toast } from "sonner";
import { useAuthProvidersStore } from "@/lib/stores/authProviders";
import { getAuthProviderIconUrl } from "@/lib/utils/icons";

/**
 * Generates a random suffix for the readable ID
 * This ensures uniqueness for similar connection names
 *
 * @returns Random alphanumeric string of length 6
 */
const generateRandomSuffix = () => {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < 6; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
};

/**
 * Helper to generate the base readable ID from a name
 * Transforms name to lowercase, replaces spaces with hyphens, and removes special characters
 *
 * @param name Connection name to transform
 * @returns Sanitized base readable ID (without suffix)
 */
const generateReadableIdBase = (name: string): string => {
    if (!name || name.trim() === "") return "";

    // Convert to lowercase and replace spaces with hyphens
    let readable_id = name.toLowerCase().trim();

    // Replace any character that's not a letter, number, or space with nothing
    readable_id = readable_id.replace(/[^a-z0-9\s]/g, "");

    // Replace spaces with hyphens
    readable_id = readable_id.replace(/\s+/g, "-");

    // Ensure no consecutive hyphens
    readable_id = readable_id.replace(/-+/g, "-");

    // Trim hyphens from start and end
    readable_id = readable_id.replace(/^-|-$/g, "");

    return readable_id;
};

export interface ConfigureAuthProviderViewProps extends DialogViewProps {
    viewData?: {
        authProviderId?: string;
        authProviderName?: string;
        authProviderShortName?: string;
        authProviderAuthType?: string;
        dialogId?: string;
        [key: string]: any;
    };
}

export const ConfigureAuthProviderView: React.FC<ConfigureAuthProviderViewProps> = ({
    onNext,
    onCancel,
    onComplete,
    viewData = {},
    onError,
}) => {
    const { authProviderId, authProviderName, authProviderShortName, authProviderAuthType } = viewData;
    const { resolvedTheme } = useTheme();
    const isDark = resolvedTheme === 'dark';
    const navigate = useNavigate();
    const { fetchAuthProviderConnections } = useAuthProvidersStore();

    // Log component lifecycle
    useEffect(() => {
        console.log('🌟 [ConfigureAuthProviderView] Component mounted:', {
            authProviderName,
            authProviderShortName,
            viewData
        });

        return () => {
            console.log('💥 [ConfigureAuthProviderView] Component unmounting');
        };
    }, []);

    const [isSubmitting, setIsSubmitting] = useState(false);
    const [loading, setLoading] = useState(true);
    const [authProviderDetails, setAuthProviderDetails] = useState<any>(null);
    const [authFieldValues, setAuthFieldValues] = useState<Record<string, any>>({});
    const [errors, setErrors] = useState<Record<string, string>>({});

    // Log loading state changes
    useEffect(() => {
        console.log('⏳ [ConfigureAuthProviderView] Loading state:', loading);
    }, [loading]);

    // Default name for the connection
    const defaultConnectionName = authProviderName ? `My ${authProviderName} Connection` : "My Connection";

    // Random suffix for readable ID (stored in ref to persist across renders)
    const randomSuffixRef = useRef(generateRandomSuffix());
    const previousNameRef = useRef(defaultConnectionName);
    const [userEditedId, setUserEditedId] = useState(false);

    // Direct input values to avoid one-keypress delay
    const [nameValue, setNameValue] = useState(defaultConnectionName);
    const [readableIdValue, setReadableIdValue] = useState("");

    // Form validation schema
    const formSchema = z.object({
        name: z.string().min(1, "Name is required").max(255, "Name must be less than 255 characters"),
        readable_id: z.string().optional().refine(
            (val) => !val || /^[a-z0-9-]+$/.test(val),
            { message: "Readable ID must contain only lowercase letters, numbers, and hyphens" }
        ),
    });

    const form = useForm<z.infer<typeof formSchema>>({
        resolver: zodResolver(formSchema),
        defaultValues: {
            name: defaultConnectionName,
            readable_id: getReadableId(defaultConnectionName),
        },
    });

    // Reset form when dialog opens
    useEffect(() => {
        randomSuffixRef.current = generateRandomSuffix();
        previousNameRef.current = defaultConnectionName;
        setNameValue(defaultConnectionName);
        const generatedId = getReadableId(defaultConnectionName);
        setReadableIdValue(generatedId);

        form.reset({
            name: defaultConnectionName,
            readable_id: generatedId,
        });

        setUserEditedId(false);
    }, [defaultConnectionName, form]);

    // Fetch auth provider details
    useEffect(() => {
        console.log('🔍 [ConfigureAuthProviderView] Auth provider details effect triggered:', {
            authProviderShortName,
            currentLoading: loading
        });

        if (!authProviderShortName) {
            console.log('⚠️ [ConfigureAuthProviderView] No authProviderShortName, skipping fetch');
            setLoading(false);
            return;
        }

        const fetchDetails = async () => {
            console.log('🚀 [ConfigureAuthProviderView] Starting to fetch auth provider details');
            setLoading(true);
            try {
                const response = await apiClient.get(`/auth-providers/detail/${authProviderShortName}`);
                console.log('📡 [ConfigureAuthProviderView] Auth provider details response:', response.ok);

                if (response.ok) {
                    const data = await response.json();
                    console.log('✅ [ConfigureAuthProviderView] Auth provider details loaded:', {
                        hasAuthFields: !!data.auth_fields,
                        fieldsCount: data.auth_fields?.fields?.length || 0
                    });
                    setAuthProviderDetails(data);

                    // Initialize auth field values
                    if (data.auth_fields && data.auth_fields.fields) {
                        const initialValues: Record<string, any> = {};
                        data.auth_fields.fields.forEach((field: any) => {
                            if (field.name) {
                                initialValues[field.name] = '';
                            }
                        });
                        setAuthFieldValues(initialValues);
                    }
                } else {
                    const errorText = await response.text();
                    console.error('❌ [ConfigureAuthProviderView] Failed to load auth provider details:', errorText);
                    throw new Error(`Failed to load auth provider details: ${errorText}`);
                }
            } catch (error) {
                console.error("Error fetching auth provider details:", error);
                if (onError) {
                    onError(error instanceof Error ? error : new Error(String(error)), authProviderName);
                }
            } finally {
                console.log('🏁 [ConfigureAuthProviderView] Setting loading to false');
                setLoading(false);
            }
        };

        fetchDetails();
    }, [authProviderShortName, authProviderName, onError]);

    function getReadableId(name: string) {
        if (!name || name.trim() === "") {
            return "";
        }

        const base = generateReadableIdBase(name);
        return base ? `${base}-${randomSuffixRef.current}` : "";
    }

    const handleNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const newName = e.target.value;

        setNameValue(newName);
        form.setValue("name", newName);

        // Clear name validation error if it exists
        if (errors.name) {
            setErrors(prev => {
                const updated = { ...prev };
                delete updated.name;
                return updated;
            });
        }

        if (previousNameRef.current && newName === "" && !userEditedId) {
            randomSuffixRef.current = generateRandomSuffix();
        }

        if (!userEditedId) {
            if (!newName || newName.trim() === "") {
                const emptyId = "";
                setReadableIdValue(emptyId);
                form.setValue("readable_id", emptyId);
            } else {
                const newId = getReadableId(newName);
                setReadableIdValue(newId);
                form.setValue("readable_id", newId);
            }
        }

        previousNameRef.current = newName;
    };

    const handleReadableIdChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const newId = e.target.value;

        setReadableIdValue(newId);
        form.setValue("readable_id", newId);

        // Clear readable_id validation error if it exists
        if (errors.readable_id || form.formState.errors.readable_id) {
            setErrors(prev => {
                const updated = { ...prev };
                delete updated.readable_id;
                return updated;
            });
        }

        if (!nameValue || nameValue.trim() === "") {
            setUserEditedId(true);
            return;
        }

        const currentNameBase = generateReadableIdBase(nameValue);
        const currentFullId = `${currentNameBase}-${randomSuffixRef.current}`;

        if (newId !== currentFullId) {
            setUserEditedId(true);
        } else {
            setUserEditedId(false);
        }
    };

    const validateAuthFields = (): boolean => {
        const newErrors: Record<string, string> = {};
        let isValid = true;

        // Validate all auth fields are filled
        if (authProviderDetails?.auth_fields?.fields) {
            authProviderDetails.auth_fields.fields.forEach((field: any) => {
                if (!authFieldValues[field.name] || authFieldValues[field.name].trim() === '') {
                    newErrors[field.name] = `${field.title || field.name} is required`;
                    isValid = false;
                }
            });
        }

        setErrors(newErrors);
        return isValid;
    };

    // Helper function to check if any required fields are empty (similar to ConfigureSourceView)
    const hasEmptyRequiredFields = (): boolean => {
        // Check if name is empty (backend requires min_length=1)
        if (!nameValue || nameValue.trim() === '') {
            return true;
        }

        // Check if any auth fields are empty
        if (authProviderDetails?.auth_fields?.fields) {
            return authProviderDetails.auth_fields.fields.some((field: any) =>
                !authFieldValues[field.name] || authFieldValues[field.name].trim() === ''
            );
        }

        return false;
    };

    const handleAuthFieldChange = (key: string, value: string) => {
        setAuthFieldValues(prev => ({
            ...prev,
            [key]: value
        }));

        // Clear error for this field if any
        if (errors[key]) {
            setErrors(prev => {
                const updated = { ...prev };
                delete updated[key];
                return updated;
            });
        }
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();

        // Only validate if the button is somehow clicked while disabled
        if (hasEmptyRequiredFields()) {
            return;
        }

        // Validate form
        const formValid = await form.trigger();
        if (!formValid) return;

        // Validate auth fields (this will set errors but we shouldn't reach here if button is properly disabled)
        if (!validateAuthFields()) return;

        setIsSubmitting(true);

        try {
            // Create auth provider connection
            const connectionData = {
                name: nameValue,
                readable_id: readableIdValue,
                short_name: authProviderShortName,
                auth_fields: authFieldValues,
            };

            const response = await apiClient.post('/auth-providers/', connectionData);

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Failed to create auth provider connection: ${errorText}`);
            }

            const connection = await response.json();

            // Show success message first
            toast.success(`Successfully connected to ${authProviderName}`);

            // Navigate to detail view BEFORE refreshing connections
            console.log('🎯 [ConfigureAuthProviderView] Connection created successfully:', {
                connectionId: connection.id,
                readableId: connection.readable_id,
                name: connection.name,
                shortName: connection.short_name
            });

            if (onNext) {
                console.log('🚀 [ConfigureAuthProviderView] Calling onNext to navigate to detail view');
                onNext({
                    authProviderConnectionId: connection.readable_id,
                    authProviderName: authProviderName,  // Use the original auth provider name, not connection name
                    authProviderShortName: connection.short_name,
                    isNewConnection: true  // Flag to indicate this is a new connection
                });

                // Refresh connections after navigation - testing without delay
                console.log('📡 [ConfigureAuthProviderView] Refreshing auth provider connections after navigation');
                fetchAuthProviderConnections();
            } else {
                console.warn('⚠️ [ConfigureAuthProviderView] onNext is not defined!');
                // If no onNext, refresh immediately
                await fetchAuthProviderConnections();
            }
        } catch (error) {
            console.error("Error creating auth provider connection:", error);
            if (onError) {
                onError(error instanceof Error ? error : new Error(String(error)), authProviderName);
            }
        } finally {
            setIsSubmitting(false);
        }
    };

    if (loading) {
        return (
            <div className="flex justify-center items-center py-8">
                <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full"></div>
            </div>
        );
    }

    return (
        <div className="flex flex-col h-full">
            {/* Content area - scrollable */}
            <div className="flex-grow overflow-y-auto">
                <div className="p-8 h-full flex flex-col">
                    {/* Heading */}
                    <DialogTitle className="text-4xl font-semibold text-left mb-4">
                        Connect to {authProviderName}
                    </DialogTitle>
                    <DialogDescription className="text-sm text-muted-foreground mb-8">
                        Create a connection to {authProviderName} that can be used to authenticate to data sources.
                    </DialogDescription>

                    {/* Small spacer to push emblem down 10% */}
                    <div style={{ height: "5%" }}></div>

                    {/* Auth Provider Icon - make much larger */}
                    {authProviderShortName && (
                        <div className="flex justify-center items-center mb-6" style={{ minHeight: "20%" }}>
                            <div className={cn(
                                "w-64 h-64 flex items-center justify-center border rounded-lg p-2",
                                isDark ? "border-gray-700" : "border-gray-300"
                            )}>
                                <img
                                    src={getAuthProviderIconUrl(authProviderShortName, resolvedTheme)}
                                    alt={`${authProviderName} icon`}
                                    className="w-full h-full object-contain"
                                    onError={(e) => {
                                        e.currentTarget.style.display = 'none';
                                        e.currentTarget.parentElement!.innerHTML = `
                                            <div class="w-full h-full rounded-lg flex items-center justify-center ${isDark ? 'bg-blue-900' : 'bg-blue-100'}">
                                                <span class="text-5xl font-bold ${isDark ? 'text-blue-400' : 'text-blue-600'}">
                                                    ${authProviderShortName.substring(0, 2).toUpperCase()}
                                                </span>
                                            </div>
                                        `;
                                    }}
                                />
                            </div>
                        </div>
                    )}

                    {/* Reduced spacer to bring form up closer to emblem */}
                    <div className="flex-grow" style={{ minHeight: "15%" }}></div>

                    {/* Form - positioned closer to emblem */}
                    <div className="px-2 max-w-md mx-auto w-full">
                        <form onSubmit={handleSubmit} className="space-y-6">
                            {/* Name field */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Name</label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        className={cn(
                                            "w-full py-2 px-3 rounded-md border bg-transparent pr-10",
                                            isDark
                                                ? "border-gray-700 focus:border-blue-500"
                                                : "border-gray-300 focus:border-blue-500",
                                            "focus:outline-none",
                                            form.formState.errors.name ? "border-red-500" : ""
                                        )}
                                        value={nameValue}
                                        onChange={handleNameChange}
                                        placeholder="My Connection"
                                    />
                                    <Pencil className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground opacity-70" />
                                </div>
                                {form.formState.errors.name && (
                                    <p className="text-sm text-red-500 mt-1">
                                        {form.formState.errors.name.message}
                                    </p>
                                )}
                            </div>

                            {/* Readable ID field */}
                            <div className="space-y-1">
                                <label className="text-sm font-medium ml-1">Readable ID</label>
                                <div className="relative">
                                    <input
                                        type="text"
                                        className={cn(
                                            "w-full py-2 px-3 rounded-md border bg-transparent pr-10",
                                            isDark
                                                ? "border-gray-700 focus:border-blue-500"
                                                : "border-gray-300 focus:border-blue-500",
                                            "focus:outline-none",
                                            errors.readable_id || form.formState.errors.readable_id ? "border-red-500" : ""
                                        )}
                                        value={readableIdValue}
                                        onChange={handleReadableIdChange}
                                        placeholder="Auto-generated"
                                    />
                                    <Pencil className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground opacity-70" />
                                </div>
                                {form.formState.errors.readable_id && (
                                    <p className="text-sm text-red-500 mt-1">
                                        {form.formState.errors.readable_id.message}
                                    </p>
                                )}
                            </div>

                            {/* Auth fields */}
                            {authProviderDetails?.auth_fields?.fields && authProviderDetails.auth_fields.fields.length > 0 && (
                                <div className="space-y-4 mt-6">
                                    {authProviderDetails.auth_fields.fields.map((field: any) => (
                                        <div key={field.name} className="space-y-1">
                                            <label className="text-sm font-medium ml-1">
                                                {field.title || field.name}
                                            </label>
                                            {field.description && (
                                                <p className="text-xs text-muted-foreground mb-1">{field.description}</p>
                                            )}
                                            <input
                                                type={field.secret ? 'password' : 'text'}
                                                className={cn(
                                                    "w-full py-2 px-3 rounded-md border bg-transparent",
                                                    isDark
                                                        ? "border-gray-700 focus:border-blue-500"
                                                        : "border-gray-300 focus:border-blue-500",
                                                    "focus:outline-none",
                                                    errors[field.name] ? "border-red-500" : ""
                                                )}
                                                value={authFieldValues[field.name] || ''}
                                                onChange={(e) => handleAuthFieldChange(field.name, e.target.value)}
                                                placeholder={field.secret ? '••••••••' : ''}
                                            />
                                            {errors[field.name] && (
                                                <p className="text-xs text-red-500">{errors[field.name]}</p>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            )}
                        </form>
                    </div>

                    {/* Small spacer at bottom */}
                    <div style={{ height: "5%" }}></div>
                </div>
            </div>

            {/* Footer - fixed at bottom */}
            <div className="flex-shrink-0 border-t">
                <DialogFooter className="flex justify-end gap-3 p-6">
                    <Button
                        type="button"
                        variant="outline"
                        onClick={onCancel}
                        className={cn(
                            "px-6",
                            isDark ? "border-gray-700 hover:bg-gray-800" : "border-gray-300 hover:bg-gray-100"
                        )}
                    >
                        Cancel
                    </Button>
                    <Button
                        type="button"
                        onClick={handleSubmit}
                        disabled={isSubmitting || hasEmptyRequiredFields()}
                        className="px-6 bg-blue-600 hover:bg-blue-700 text-white"
                    >
                        {isSubmitting ? (
                            <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Connecting...
                            </>
                        ) : (
                            "Connect"
                        )}
                    </Button>
                </DialogFooter>
            </div>
        </div>
    );
};

export default ConfigureAuthProviderView;
