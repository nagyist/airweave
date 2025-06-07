import { fetchEventSource } from '@microsoft/fetch-event-source';
import { apiClient } from './api';

interface SSEOptions {
  url: string;
  onMessage: (data: any) => void;
  onError?: (error: any) => void;
  onOpen?: () => void;
  onClose?: () => void;
}

export class SSEClient {
  private controller: AbortController | null = null;
  private options: SSEOptions;
  private isConnected = false;

  constructor(options: SSEOptions) {
    this.options = options;
  }

  async connect(): Promise<void> {
    if (this.controller) {
      this.disconnect();
    }

    this.controller = new AbortController();

    try {
      // Get token using existing system before connection
      const token = await apiClient.getToken();
      const headers: Record<string, string> = token
        ? { 'Authorization': `Bearer ${token}` }
        : {};

      await fetchEventSource(this.options.url, {
        signal: this.controller.signal,
        headers,

        onopen: async (response) => {
          if (response.ok) {
            console.log('SSE connection opened');
            this.isConnected = true;
            this.options.onOpen?.();
          } else if (response.status === 401 || response.status === 403) {
            // Use existing token refresh mechanism
            console.log('SSE auth failed, clearing token for refresh');
            apiClient.clearToken();
            throw new Error(`Authentication failed: ${response.status}`);
          } else {
            throw new Error(`SSE connection failed: ${response.status} ${response.statusText}`);
          }
        },

        onmessage: (event) => {
          try {
            const data = JSON.parse(event.data);
            this.options.onMessage(data);
          } catch (error) {
            console.error('Failed to parse SSE message:', error);
          }
        },

        onerror: (error) => {
          console.error('SSE error:', error);
          this.isConnected = false;
          this.options.onError?.(error);

          // If it's an auth error, the token refresh will be handled by onopen
          // The library will automatically retry with the new token
        },

        onclose: () => {
          console.log('SSE connection closed');
          this.isConnected = false;
          this.options.onClose?.();
        },
      });
    } catch (error) {
      this.isConnected = false;
      console.error('SSE connection error:', error);
      this.options.onError?.(error);
    }
  }

  disconnect(): void {
    if (this.controller) {
      this.controller.abort();
      this.controller = null;
    }
    this.isConnected = false;
  }

  get connected(): boolean {
    return this.isConnected;
  }
}

// Convenience function that integrates with your existing token system
export const createSSEConnection = (options: SSEOptions): SSEClient => {
  return new SSEClient(options);
};
