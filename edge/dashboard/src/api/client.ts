// API client for the lark-edge admin REST surface. Same-origin in
// production (the SPA and the API are both served by lark-edge under
// /admin/* and /admin/api/* respectively); Vite's dev proxy routes
// /admin/api/* to localhost:8080 in dev.

const API_BASE = '/admin/api';

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...options.headers,
    },
  });

  if (!res.ok) {
    let message = `Request failed (${res.status})`;
    try {
      const body = await res.json();
      message = body.error || body.message || message;
    } catch {
      // body wasn't JSON; keep the generic message
    }
    throw new ApiError(res.status, message);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Types — mirrors of the Go db.* models actually returned by /admin/api/*.
// ---------------------------------------------------------------------------

export interface Account {
  id: string;
  email: string;
  role: string;
  must_change_password: boolean;
  created_at: number;
}

export interface Project {
  id: string;
  name: string;
  secret_key: string;
  admin_secret_key: string;
  rules_json: string;
  ephemeral: boolean;
  auto_create: boolean;
  firebase_compat_enabled: boolean;
  firebase_project_id: string;
  use_first_path_segment_as_database: boolean;
  config_version: number;
  created_at: number;
  updated_at: number;
}

export interface Database {
  project_id: string;
  id: string;
  server_id: string;
  ephemeral: boolean;
  status: 'inactive' | 'active' | 'evicting';
  last_activity: number;
  created_at: number;
}

export interface DatabaseEvent {
  id: number;
  ts: string;
  project_id: string;
  database_id: string;
  event_type: string;
  message: string;
  details?: string;
}

export interface DashboardSummary {
  peak_ccu: number;
  total_bytes_in: number;
  total_bytes_out: number;
  total_writes: number;
  total_reads: number;
  total_events: number;
  avg_latency_us: number;
}

export interface TimeseriesPoint {
  ts: string;
  ccu: number;
  bytes_in: number;
  bytes_out: number;
  writes: number;
  reads: number;
  events_sent: number;
  p50_latency_us: number;
  p99_latency_us: number;
}

export interface DashboardData {
  project: { id: string; name: string };
  summary?: DashboardSummary;
  time_range: { start: string; end: string };
  timeseries: TimeseriesPoint[];
  recent_events: DatabaseEvent[];
}

export interface Stats {
  accounts: number;
  projects: number;
  databases: number;
  healthy_servers: number;
}

// ---------------------------------------------------------------------------
// Client.
// ---------------------------------------------------------------------------

export const api = {
  // Auth.
  login: (email: string, password: string) =>
    request<{ account: Account }>('/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    }),

  logout: () => request<{ status: string }>('/logout', { method: 'POST' }),

  me: () =>
    request<{ account: Account; larkdb_domain: string }>('/me'),

  changePassword: (currentPassword: string | undefined, newPassword: string) =>
    request<{ status: string }>('/change-password', {
      method: 'POST',
      body: JSON.stringify({
        current_password: currentPassword ?? '',
        new_password: newPassword,
      }),
    }),

  // Users.
  listUsers: () => request<{ users: Account[] }>('/users'),

  createUser: (email: string) =>
    request<{ account: Account; temporary_password: string }>('/users', {
      method: 'POST',
      body: JSON.stringify({ email }),
    }),

  deleteUser: (id: string) =>
    request<{ status: string }>(`/users/${id}`, { method: 'DELETE' }),

  resetUserPassword: (id: string) =>
    request<{ temporary_password: string }>(`/users/${id}/reset-password`, {
      method: 'POST',
    }),

  // Projects.
  listProjects: () => request<{ projects: Project[] }>('/projects'),

  getProject: (id: string) => request<Project>(`/projects/${id}`),

  createProject: (body: {
    id: string;
    name: string;
    ephemeral?: boolean;
    auto_create?: boolean;
    firebase_compat_enabled?: boolean;
    firebase_project_id?: string;
    use_first_path_segment_as_database?: boolean;
    rules_json?: string;
  }) =>
    request<Project>('/projects', {
      method: 'POST',
      body: JSON.stringify(body),
    }),

  updateProject: (id: string, body: Partial<Project>) =>
    request<Project>(`/projects/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),

  deleteProject: (id: string) =>
    request<{ status: string }>(`/projects/${id}`, { method: 'DELETE' }),

  regenerateProjectSecret: (id: string) =>
    request<{ secret_key: string; config_version: number }>(
      `/projects/${id}/regenerate-secret`,
      { method: 'POST' },
    ),

  // Short-lived admin JWT for the database editor's SDK auth.
  getAdminToken: (projectId: string) =>
    request<{ token: string }>(`/projects/${projectId}/admin-token`, {
      method: 'POST',
    }),

  // Databases.
  listDatabases: (projectId: string) =>
    request<{ databases: Database[] }>(`/projects/${projectId}/databases`),

  createDatabase: (projectId: string, id: string) =>
    request<Database>(`/projects/${projectId}/databases`, {
      method: 'POST',
      body: JSON.stringify({ id }),
    }),

  deleteDatabase: (projectId: string, dbId: string) =>
    request<{ status: string }>(
      `/projects/${projectId}/databases/${dbId}`,
      { method: 'DELETE' },
    ),

  // Metrics + events.
  getDashboard: (
    projectId: string,
    options?: { start?: string; end?: string },
  ) => {
    const params = new URLSearchParams();
    if (options?.start) params.set('start', options.start);
    if (options?.end) params.set('end', options.end);
    const query = params.toString();
    return request<DashboardData>(
      `/projects/${projectId}/dashboard${query ? `?${query}` : ''}`,
    );
  },

  getEvents: (
    projectId: string,
    options?: { limit?: number; offset?: number },
  ) => {
    const params = new URLSearchParams();
    if (options?.limit) params.set('limit', options.limit.toString());
    if (options?.offset !== undefined)
      params.set('offset', options.offset.toString());
    const query = params.toString();
    return request<{
      events: DatabaseEvent[];
      limit: number;
      offset: number;
      total: number;
    }>(`/projects/${projectId}/events${query ? `?${query}` : ''}`);
  },

  // Stats (high-level counts).
  getStats: () => request<Stats>('/stats'),
};
