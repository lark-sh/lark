import { useEffect, useMemo, useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import { api, ApiError, type Database } from '../api/client';

interface DatabaseListProps {
  projectId: string;
}

export function DatabaseList({ projectId }: DatabaseListProps) {
  const [databases, setDatabases] = useState<Database[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const [showCreate, setShowCreate] = useState(false);
  const [newId, setNewId] = useState('');
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    if (!projectId) return;
    let cancelled = false;
    (async () => {
      try {
        const { databases: list } = await api.listDatabases(projectId);
        if (!cancelled) setDatabases(list ?? []);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof ApiError ? err.message : 'Failed to load databases');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [projectId]);

  const filtered = useMemo(() => {
    if (!filter) return databases;
    const f = filter.toLowerCase();
    return databases.filter((d) => d.id.toLowerCase().includes(f));
  }, [databases, filter]);

  async function handleCreate(e: FormEvent) {
    e.preventDefault();
    setError(null);
    const id = newId.trim();
    if (!id) return;
    setCreating(true);
    try {
      const created = await api.createDatabase(projectId, id);
      setDatabases((prev) => [created, ...prev]);
      setNewId('');
      setShowCreate(false);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to create database');
    } finally {
      setCreating(false);
    }
  }

  async function handleDelete(dbId: string) {
    if (
      !confirm(
        `Delete database "${dbId}"? This permanently removes the database and all of its data.`,
      )
    )
      return;

    setDeletingId(dbId);
    setError(null);
    try {
      await api.deleteDatabase(projectId, dbId);
      setDatabases((prev) => prev.filter((d) => d.id !== dbId));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to delete database');
    } finally {
      setDeletingId(null);
    }
  }

  if (loading) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="text-sm text-gray-500">Loading databases…</div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200">
      <div className="p-4 border-b border-gray-200 space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium text-gray-700">Databases</h3>
          <button
            type="button"
            onClick={() => setShowCreate((v) => !v)}
            className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700"
          >
            {showCreate ? 'Cancel' : 'New database'}
          </button>
        </div>

        {showCreate && (
          <form onSubmit={handleCreate} className="flex items-end gap-3 pt-1">
            <div className="flex-1">
              <label htmlFor="newDbId" className="block text-xs font-medium text-gray-700 mb-1">
                Database ID
              </label>
              <input
                id="newDbId"
                type="text"
                autoFocus
                value={newId}
                onChange={(e) => setNewId(e.target.value)}
                placeholder="room-abc123"
                maxLength={40}
                className="w-full px-3 py-2 text-sm font-mono border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
              <p className="mt-1 text-xs text-gray-500">
                Lowercase letters, digits, hyphens. 1–40 chars. No leading/trailing or double hyphens.
              </p>
            </div>
            <button
              type="submit"
              disabled={creating || !newId.trim()}
              className="px-3 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-60"
            >
              {creating ? 'Creating…' : 'Create'}
            </button>
          </form>
        )}

        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter databases…"
          className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        />
      </div>

      {error && (
        <div className="mx-4 mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {error}
        </div>
      )}

      {filtered.length === 0 ? (
        <div className="text-center py-8 px-4">
          <p className="text-sm text-gray-500">
            {filter
              ? `No databases matching "${filter}".`
              : 'No databases yet. Databases are created when a client first connects to one.'}
          </p>
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  ID
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Server
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last activity
                </th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filtered.map((db) => (
                <tr key={db.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 whitespace-nowrap">
                    <Link
                      to={`/projects/${projectId}/databases/${db.id}`}
                      className="text-blue-600 hover:text-blue-800 font-mono text-sm"
                    >
                      {db.id}
                    </Link>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap">
                    <span
                      className={
                        'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ' +
                        (db.status === 'active'
                          ? 'bg-green-100 text-green-800'
                          : 'bg-gray-100 text-gray-800')
                      }
                    >
                      {db.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500 font-mono">
                    {db.server_id || '—'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                    {db.last_activity
                      ? new Date(db.last_activity).toLocaleString()
                      : '—'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-right text-sm">
                    <button
                      type="button"
                      onClick={() => handleDelete(db.id)}
                      disabled={deletingId === db.id}
                      className="text-red-600 hover:text-red-800 disabled:opacity-50"
                    >
                      {deletingId === db.id ? 'Deleting…' : 'Delete'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="px-4 py-3 border-t border-gray-200 text-xs text-gray-500">
            {filtered.length} of {databases.length} database
            {databases.length === 1 ? '' : 's'}
            {filter && ` matching "${filter}"`}
          </div>
        </div>
      )}
    </div>
  );
}
