import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { LarkDatabase, type DataSnapshot } from '@lark-sh/client';
import { JsonEditor, githubLightTheme, isCollection } from 'json-edit-react';
import type { NodeData } from 'json-edit-react';
import { api, ApiError, type Project } from '../api/client';
import { useAuth } from '../hooks/useAuth';

// wireProtocolURLs derives the wire-protocol base URL for a project from
// LARKDB_DOMAIN (per-deployment config from /admin/api/me) and the page's
// current origin. lark-edge always serves the wire protocol on the same
// port + protocol as the dashboard, so we mirror window.location for
// scheme + port and substitute the project subdomain in the host.
function wireProtocolURLs(projectId: string, larkdbDomain: string) {
  const isSecure = window.location.protocol === 'https:';
  const httpScheme = isSecure ? 'https' : 'http';
  const portSuffix = window.location.port ? `:${window.location.port}` : '';
  const domainWithPort = `${larkdbDomain}${portSuffix}`;
  const sub = projectId.toLowerCase();
  return {
    restBase: `${httpScheme}://${sub}.${domainWithPort}`,
    sdkDomain: domainWithPort,
    sdkSecure: isSecure,
  };
}

interface CustomButtonDefinition {
  Element: React.FC<{ nodeData: NodeData }>;
  onClick: (nodeData: NodeData, e: React.MouseEvent) => void;
}

// Walks two trees in lockstep and returns every path-string that changed.
// Used to flash a highlight on rows that just updated.
function findChangedPaths(
  oldData: unknown,
  newData: unknown,
  path: (string | number)[] = [],
): string[] {
  const changes: string[] = [];
  const pathStr = path.join('.');

  if (oldData === newData) return changes;

  if (
    typeof oldData !== typeof newData ||
    oldData === null ||
    newData === null ||
    typeof oldData !== 'object'
  ) {
    if (pathStr) changes.push(pathStr);
    return changes;
  }

  const oldObj = oldData as Record<string, unknown>;
  const newObj = newData as Record<string, unknown>;
  const allKeys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);

  for (const key of allKeys) {
    const childPath = [...path, key];
    if (!(key in oldObj) || !(key in newObj)) {
      changes.push(childPath.join('.'));
    } else {
      changes.push(...findChangedPaths(oldObj[key], newObj[key], childPath));
    }
  }
  if (changes.length > 0 && pathStr) changes.push(pathStr);
  return changes;
}

// Fetches the shallow key list (and total bytes of object-valued children)
// at the given path via the REST API. Used to bail out into the
// "shallow keys" view when the subtree is too big to render in one go.
async function fetchShallowKeys(
  databaseId: string,
  path: string,
  authToken: string,
  restBase: string,
): Promise<{ keys: string[]; totalSize: number } | null> {
  try {
    const url = `${restBase}/${databaseId}${path}/.json?v=2&shallow=true&auth=${encodeURIComponent(authToken)}`;
    const response = await fetch(url);
    if (!response.ok) return null;
    const data = await response.json();
    if (data === null) return { keys: [], totalSize: 0 };
    if (typeof data === 'object' && !Array.isArray(data)) {
      const keys = Object.keys(data);
      let totalSize = 0;
      for (const key of keys) {
        const v = (data as Record<string, unknown>)[key];
        if (v && typeof v === 'object' && typeof (v as { '.sz'?: unknown })['.sz'] === 'number') {
          totalSize += (v as { '.sz': number })['.sz'];
        }
      }
      return { keys, totalSize };
    }
    return null;
  } catch {
    return null;
  }
}

export function DatabaseEditor() {
  const { projectId, databaseId, '*': splatPath } = useParams<{
    projectId: string;
    databaseId: string;
    '*': string;
  }>();
  const navigate = useNavigate();
  const { larkdbDomain } = useAuth();
  const urls = useMemo(
    () => (projectId ? wireProtocolURLs(projectId, larkdbDomain) : null),
    [projectId, larkdbDomain],
  );

  const currentPath = (splatPath || '').replace(/^\/+|\/+$/g, '');
  const currentRefPath = currentPath ? `/${currentPath}` : '/';
  const pathSegments = currentPath ? currentPath.split('/') : [];

  const [project, setProject] = useState<Project | null>(null);
  const [db, setDb] = useState<LarkDatabase | null>(null);
  const [data, setData] = useState<unknown>(null);
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [changedPaths, setChangedPaths] = useState<Set<string>>(new Set());
  const [authToken, setAuthToken] = useState<string | null>(null);
  const unsubscribeRef = useRef<(() => void) | null>(null);
  const prevDataRef = useRef<unknown>(null);
  const shallowCheckDoneRef = useRef(false);

  const [viewMode, setViewMode] = useState<'loading' | 'full' | 'shallow'>('loading');
  const [shallowKeys, setShallowKeys] = useState<string[]>([]);
  const [shallowHasData, setShallowHasData] = useState(false);
  const [keyFilter, setKeyFilter] = useState('');

  const theme = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const base = githubLightTheme as any;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const highlight = (props: any) => {
      const path = props.path as (string | number)[];
      const pathStr = path.join('.');
      if (changedPaths.has(pathStr)) {
        return {
          backgroundColor: 'rgba(251, 191, 36, 0.3)',
          transition: 'background-color 0.3s ease-out',
        };
      }
      return {};
    };

    return {
      ...base,
      styles: { ...base.styles, property: highlight },
    };
  }, [changedPaths]);

  const customButtons: CustomButtonDefinition[] = useMemo(
    () => [
      {
        Element: ({ nodeData }) => {
          if (nodeData.path.length === 0) return null;
          if (!isCollection(nodeData.value)) return null;
          return (
            <span
              style={{ cursor: 'pointer', fontSize: '26px', opacity: 0.6, padding: '0 4px' }}
              title="Navigate into"
            >
              →
            </span>
          );
        },
        onClick: (nodeData) => {
          const relativePath = nodeData.path.join('/');
          const basePath = `/projects/${projectId}/databases/${databaseId}`;
          const newPath = currentPath
            ? `${basePath}/${currentPath}/${relativePath}`
            : `${basePath}/${relativePath}`;
          navigate(newPath);
        },
      },
    ],
    [projectId, databaseId, currentPath, navigate],
  );

  // Project lookup (used to surface the "ephemeral" banner).
  useEffect(() => {
    if (!projectId) return;
    api.getProject(projectId).then(setProject).catch(() => {});
  }, [projectId]);

  // Connect to the database. Pulls a fresh admin token and hands it to the SDK.
  useEffect(() => {
    if (!projectId || !databaseId || !urls) return;

    let database: LarkDatabase | null = null;
    let unsubConnect: (() => void) | null = null;
    let unsubDisconnect: (() => void) | null = null;
    let unsubError: (() => void) | null = null;

    (async () => {
      try {
        const { token } = await api.getAdminToken(projectId);
        setAuthToken(token);

        database = new LarkDatabase(`${projectId}/${databaseId}`, {
          token,
          domain: urls.sdkDomain,
          secure: urls.sdkSecure,
        });
        setDb(database);

        unsubConnect = database.onConnect(() => setConnected(true));
        unsubDisconnect = database.onDisconnect(() => setConnected(false));
        unsubError = database.onError((e) => setError(e.message));

        await database.connect();
        setConnected(true);
        setConnecting(false);
      } catch (err) {
        setError(err instanceof ApiError ? err.message : 'Failed to connect');
        setConnecting(false);
      }
    })();

    return () => {
      unsubConnect?.();
      unsubDisconnect?.();
      unsubError?.();
      if (unsubscribeRef.current) unsubscribeRef.current();
      database?.disconnect();
    };
  }, [projectId, databaseId]);

  // Shallow prefetch — if a subtree is huge, render a key list instead of
  // pulling the whole thing into a JSON editor.
  useEffect(() => {
    if (!connected || !authToken || !projectId || !databaseId || !urls) return;

    let cancelled = false;
    if (unsubscribeRef.current) {
      unsubscribeRef.current();
      unsubscribeRef.current = null;
    }
    shallowCheckDoneRef.current = false;
    setViewMode('loading');
    setData(null);
    prevDataRef.current = null;
    setShallowHasData(false);
    setKeyFilter('');

    (async () => {
      const result = await fetchShallowKeys(
        databaseId,
        currentRefPath,
        authToken,
        urls.restBase,
      );
      if (cancelled) return;

      if (result && result.keys.length > 0) setShallowHasData(true);
      shallowCheckDoneRef.current = true;

      if (result && (result.totalSize > 20 * 1024 * 1024 || result.keys.length > 500)) {
        setShallowKeys(result.keys);
        setViewMode('shallow');
      } else {
        setViewMode('full');
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [connected, authToken, projectId, databaseId, currentRefPath, urls]);

  // Live subscription — only in full-view mode.
  useEffect(() => {
    if (!db || !connected || viewMode !== 'full' || !shallowCheckDoneRef.current) return;

    const unsubscribe = db.ref(currentRefPath).on('value', (snapshot: DataSnapshot) => {
      const newData = snapshot.val();
      if (prevDataRef.current !== null && newData !== null) {
        const paths = findChangedPaths(prevDataRef.current, newData);
        if (paths.length > 0) {
          setChangedPaths(new Set(paths));
          setTimeout(() => setChangedPaths(new Set()), 1500);
        }
      }
      prevDataRef.current = newData;
      setData(newData);
    });

    unsubscribeRef.current = unsubscribe;
    return () => {
      unsubscribe();
      unsubscribeRef.current = null;
    };
  }, [db, connected, viewMode, currentRefPath]);

  const handleUpdate = useCallback(
    async ({ newValue, path }: { newValue: unknown; path: (string | number)[] }) => {
      if (!db) return;
      try {
        const relativePath = path.join('/');
        const fullPath =
          currentRefPath === '/'
            ? '/' + relativePath
            : currentRefPath + (relativePath ? '/' + relativePath : '');
        await db.ref(fullPath).set(newValue);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to update data');
      }
    },
    [db, currentRefPath],
  );

  // json-edit-react treats `false` as cancel; on user-cancelled confirm we
  // must return `false` so the editor reverts its optimistic mutation.
  const handleDelete = useCallback(
    async ({ path }: { path: (string | number)[] }) => {
      if (!db) return false;

      const relativePath = path.join('/');
      const displayPath = relativePath || '/';
      if (!confirm(`Delete "${displayPath}"? This cannot be undone.`)) return false;

      try {
        const fullPath =
          currentRefPath === '/'
            ? '/' + relativePath
            : currentRefPath + (relativePath ? '/' + relativePath : '');
        await db.ref(fullPath).remove();
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to delete data');
        return false;
      }
    },
    [db, currentRefPath],
  );

  const handleExport = async () => {
    if (!projectId || !databaseId || !authToken || !urls) return;
    try {
      const pathSuffix = currentPath ? `/${currentPath}` : '';
      const url = `${urls.restBase}/${databaseId}${pathSuffix}/.json?v=2&auth=${encodeURIComponent(authToken)}`;
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Export failed: ${response.statusText}`);
      const exported = await response.json();

      const blob = new Blob([JSON.stringify(exported, null, 2)], { type: 'application/json' });
      const blobUrl = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = blobUrl;
      a.download = [projectId, databaseId, ...pathSegments].join('-') + '.json';
      a.click();
      URL.revokeObjectURL(blobUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to export data');
    }
  };

  const handleImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !projectId || !databaseId || !authToken || !urls) return;

    try {
      const text = await file.text();
      const imported = JSON.parse(text);

      const scope = currentPath
        ? `This will replace all data at /${currentPath}. Continue?`
        : 'This will replace all data in the database. Continue?';
      if (!confirm(scope)) {
        e.target.value = '';
        return;
      }

      const pathSuffix = currentPath ? `/${currentPath}` : '';
      const url = `${urls.restBase}/${databaseId}${pathSuffix}/.json?v=2&auth=${encodeURIComponent(authToken)}`;
      const response = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(imported),
      });
      if (!response.ok) throw new Error(`Import failed: ${response.statusText}`);

      if (viewMode === 'shallow') {
        const result = await fetchShallowKeys(
          databaseId,
          currentRefPath,
          authToken,
          urls.restBase,
        );
        if (result) setShallowKeys(result.keys);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to import data');
    }
    e.target.value = '';
  };

  const breadcrumbs = useMemo(() => {
    const crumbs: { label: string; to: string }[] = [
      { label: projectId || '', to: `/projects/${projectId}` },
      {
        label: databaseId || '',
        to: `/projects/${projectId}/databases/${databaseId}`,
      },
    ];
    let acc = `/projects/${projectId}/databases/${databaseId}`;
    for (const seg of pathSegments) {
      acc += `/${seg}`;
      crumbs.push({ label: seg, to: acc });
    }
    return crumbs;
  }, [projectId, databaseId, pathSegments]);

  const rootName =
    pathSegments.length > 0 ? pathSegments[pathSegments.length - 1] : databaseId || '';

  const filteredKeys = useMemo(() => {
    if (!keyFilter) return shallowKeys;
    const f = keyFilter.toLowerCase();
    return shallowKeys.filter((k) => k.toLowerCase().includes(f));
  }, [shallowKeys, keyFilter]);

  if (connecting) {
    return (
      <div className="flex items-center justify-center py-12 text-sm text-gray-500">
        Connecting to database…
      </div>
    );
  }

  if (error && !connected) {
    return (
      <div>
        <div className="mb-6">
          <Link to={`/projects/${projectId}`} className="text-sm text-gray-600 hover:text-gray-900">
            ← Back to project
          </Link>
        </div>
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div>
      {/* Breadcrumbs + status pill */}
      <div className="mb-6 flex items-center justify-between">
        <nav className="flex items-center gap-1 text-sm font-mono">
          {breadcrumbs.map((c, i) => {
            const isLast = i === breadcrumbs.length - 1;
            return (
              <span key={c.to} className="flex items-center gap-1">
                {i > 0 && <span className="text-gray-400">/</span>}
                {isLast ? (
                  <span className="font-semibold text-gray-900">{c.label}</span>
                ) : (
                  <Link to={c.to} className="text-blue-600 hover:text-blue-800">
                    {c.label}
                  </Link>
                )}
              </span>
            );
          })}
        </nav>
        <span
          className={
            'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ' +
            (connected ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800')
          }
        >
          {connected ? 'Connected' : 'Disconnected'}
        </span>
      </div>

      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-semibold text-gray-900 font-mono">{rootName}</h1>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={handleExport}
            disabled={!authToken}
            className="px-3 py-1.5 text-sm border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50"
          >
            Export JSON
          </button>
          <label className="px-3 py-1.5 text-sm border border-gray-300 rounded-md hover:bg-gray-50 cursor-pointer">
            Import JSON
            <input type="file" accept=".json" onChange={handleImport} className="hidden" />
          </label>
        </div>
      </div>

      {project?.ephemeral && (
        <div className="mb-6 bg-amber-50 border border-amber-200 text-amber-800 px-4 py-3 rounded-md text-sm">
          <strong>Ephemeral database:</strong> will be deleted when no active connections remain.
        </div>
      )}

      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm flex items-center justify-between">
          <span>{error}</span>
          <button
            type="button"
            onClick={() => setError(null)}
            className="text-red-800 hover:text-red-900"
          >
            ×
          </button>
        </div>
      )}

      {viewMode === 'shallow' ? (
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="mb-4">
            <p className="text-sm text-gray-600">{shallowKeys.length} keys at this path</p>
          </div>

          <input
            type="text"
            placeholder="Filter keys…"
            value={keyFilter}
            onChange={(e) => setKeyFilter(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md mb-4 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />

          <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
            {filteredKeys
              .slice()
              .sort((a, b) => a.localeCompare(b))
              .map((key) => {
                const basePath = `/projects/${projectId}/databases/${databaseId}`;
                const linkPath = currentPath
                  ? `${basePath}/${currentPath}/${key}`
                  : `${basePath}/${key}`;
                return (
                  <Link
                    key={key}
                    to={linkPath}
                    className="flex items-center justify-between px-3 py-2 hover:bg-gray-50 font-mono text-sm group"
                  >
                    <span className="text-gray-900">{key}</span>
                    <span className="text-gray-400 group-hover:text-blue-600">→</span>
                  </Link>
                );
              })}
            {filteredKeys.length === 0 && keyFilter && (
              <p className="text-sm text-gray-500 py-4 text-center">
                No keys matching "{keyFilter}"
              </p>
            )}
          </div>
        </div>
      ) : viewMode === 'loading' || data === undefined ? (
        <div className="text-center py-12 bg-white rounded-lg border border-gray-200">
          <p className="text-sm text-gray-500">Loading data…</p>
        </div>
      ) : data === null ? (
        shallowHasData ? (
          <div className="text-center py-12 bg-white rounded-lg border border-gray-200">
            <p className="text-sm text-gray-500">Loading data…</p>
          </div>
        ) : (
          <div className="text-center py-12 bg-white rounded-lg border border-gray-200">
            <p className="text-sm text-gray-500 mb-4">
              {currentPath ? `No data at /${currentPath}` : 'Database is empty'}
            </p>
            <button
              type="button"
              onClick={() => handleUpdate({ newValue: {}, path: [] })}
              className="text-sm text-blue-600 hover:text-blue-800"
            >
              Initialize with empty object
            </button>
          </div>
        )
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 p-4 overflow-auto">
          <JsonEditor
            data={data as object}
            onUpdate={handleUpdate}
            onDelete={handleDelete}
            rootName={rootName}
            theme={theme}
            collapse={2}
            enableClipboard
            restrictEdit={false}
            restrictDelete={false}
            restrictAdd={false}
            restrictTypeSelection={false}
            defaultValue=""
            maxWidth="100%"
            customButtons={customButtons}
          />
        </div>
      )}
    </div>
  );
}
