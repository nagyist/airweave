export interface SyncDestination {
  id: string;
  sync_id: string;
  connection_id: string | null;
  is_native: boolean;
  destination_type: string;
}

export interface Sync {
  id: string;
  name: string;
  description?: string;
  source_connection_id: string;
  destination_connection_id?: string;
  embedding_model_connection_id?: string;
  cron_schedule?: string;
  white_label_id?: string;
  white_label_user_identifier?: string;
  sync_metadata?: {
    use_native_weaviate?: boolean;
    use_native_neo4j?: boolean;
    [key: string]: any;
  };
  status: string;
  organization_id: string;
  created_at: string;
  modified_at: string;
  created_by_email: string;
  modified_by_email: string;
  destinations?: SyncDestination[];
} 