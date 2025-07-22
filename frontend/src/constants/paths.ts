export const protectedPaths = {
    dashboard: "/",
    collections: "/collections",
    collectionDetail: "/collections/:readable_id",
    apiKeys: "/api-keys",
    authProviders: "/auth-providers",
    whiteLabel: "/white-label",
    whiteLabelTab: "/white-label/:id",
    whiteLabelCreate: "/white-label/create",
    whiteLabelDetail: "/white-label/:id",
    whiteLabelEdit: "/white-label/:id/edit",
    authCallback: "/auth/callback/:short_name",
}

export const publicPaths = {
    login: "/login",
    callback: "/callback",
    semanticMcp: "/semantic-mcp",
    noOrganization: "/no-organization",
}
