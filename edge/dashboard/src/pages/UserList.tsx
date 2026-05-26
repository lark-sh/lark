import { useEffect, useState, type FormEvent } from 'react';
import { api, ApiError, type Account } from '../api/client';
import { useAuth } from '../hooks/useAuth';

// Emitted after create or admin reset-password, since both produce a
// one-time temp password the operator must communicate to the user.
interface TempCredential {
  email: string;
  password: string;
  context: 'created' | 'reset';
}

export function UserList() {
  const { account: self } = useAuth();

  const [users, setUsers] = useState<Account[]>([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const [showCreate, setShowCreate] = useState(false);
  const [newEmail, setNewEmail] = useState('');
  const [creating, setCreating] = useState(false);

  const [pendingId, setPendingId] = useState<string | null>(null);
  const [tempCred, setTempCred] = useState<TempCredential | null>(null);

  async function refresh() {
    try {
      const { users } = await api.listUsers();
      setUsers(users ?? []);
      // (above already defensive; keep parity with other list pages)
      setListError(null);
    } catch (err) {
      setListError(err instanceof ApiError ? err.message : 'Failed to load users');
    }
  }

  useEffect(() => {
    (async () => {
      await refresh();
      setLoading(false);
    })();
  }, []);

  async function handleCreate(e: FormEvent) {
    e.preventDefault();
    setActionError(null);
    if (!newEmail.trim()) return;

    setCreating(true);
    try {
      const { account, temporary_password } = await api.createUser(newEmail.trim());
      setUsers((prev) => [...prev, account]);
      setNewEmail('');
      setShowCreate(false);
      setTempCred({
        email: account.email,
        password: temporary_password,
        context: 'created',
      });
    } catch (err) {
      setActionError(err instanceof ApiError ? err.message : 'Failed to create user');
    } finally {
      setCreating(false);
    }
  }

  async function handleReset(user: Account) {
    if (
      !confirm(
        `Reset password for ${user.email}? Their current sessions stay valid until they sign out.`,
      )
    )
      return;
    setPendingId(user.id);
    setActionError(null);
    try {
      const { temporary_password } = await api.resetUserPassword(user.id);
      setTempCred({
        email: user.email,
        password: temporary_password,
        context: 'reset',
      });
    } catch (err) {
      setActionError(err instanceof ApiError ? err.message : 'Failed to reset password');
    } finally {
      setPendingId(null);
    }
  }

  async function handleDelete(user: Account) {
    if (!confirm(`Delete ${user.email}? This signs them out everywhere.`)) return;
    setPendingId(user.id);
    setActionError(null);
    try {
      await api.deleteUser(user.id);
      setUsers((prev) => prev.filter((u) => u.id !== user.id));
    } catch (err) {
      setActionError(err instanceof ApiError ? err.message : 'Failed to delete user');
    } finally {
      setPendingId(null);
    }
  }

  if (loading) return <div className="text-sm text-gray-500">Loading users…</div>;

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-semibold text-gray-900">Users</h1>
        <button
          type="button"
          onClick={() => setShowCreate((v) => !v)}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          {showCreate ? 'Cancel' : 'Add user'}
        </button>
      </div>

      {listError && (
        <div className="mb-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {listError}
        </div>
      )}
      {actionError && (
        <div className="mb-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {actionError}
        </div>
      )}

      {showCreate && (
        <form
          onSubmit={handleCreate}
          className="mb-6 bg-white rounded-lg border border-gray-200 p-4 flex items-end gap-3"
        >
          <div className="flex-1">
            <label htmlFor="newEmail" className="block text-sm font-medium text-gray-700 mb-1">
              Email
            </label>
            <input
              id="newEmail"
              type="email"
              autoFocus
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              placeholder="alice@example.com"
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">
              A one-time temporary password will be generated. The new user must change it on
              first login.
            </p>
          </div>
          <button
            type="submit"
            disabled={creating || !newEmail.trim()}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-60"
          >
            {creating ? 'Creating…' : 'Create'}
          </button>
        </form>
      )}

      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Email
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Role
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Created
              </th>
              <th className="px-6 py-3" />
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {users.map((u) => {
              const isSelf = self?.id === u.id;
              return (
                <tr key={u.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className="text-sm text-gray-900">{u.email}</span>
                    {isSelf && (
                      <span className="ml-2 text-xs text-gray-500">(you)</span>
                    )}
                    {u.must_change_password && (
                      <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-800">
                        password reset pending
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{u.role}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {new Date(u.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm space-x-3">
                    <button
                      type="button"
                      onClick={() => handleReset(u)}
                      disabled={pendingId === u.id}
                      className="text-gray-600 hover:text-gray-900 disabled:opacity-50"
                    >
                      Reset password
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDelete(u)}
                      disabled={pendingId === u.id || isSelf}
                      title={isSelf ? "You can't delete your own account" : undefined}
                      className="text-red-600 hover:text-red-800 disabled:opacity-30 disabled:cursor-not-allowed"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {tempCred && (
        <TempPasswordModal
          cred={tempCred}
          onClose={() => setTempCred(null)}
        />
      )}
    </div>
  );
}

function TempPasswordModal({
  cred,
  onClose,
}: {
  cred: TempCredential;
  onClose: () => void;
}) {
  const verb = cred.context === 'created' ? 'created' : 'reset';
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4">
        <h3 className="text-lg font-medium text-gray-900 mb-2">
          Password {verb}
        </h3>
        <p className="text-sm text-gray-500 mb-4">
          Share this temporary password with <strong>{cred.email}</strong>. They'll be prompted to
          change it on first login. This password won't be shown again.
        </p>

        <div className="bg-gray-50 border border-gray-200 rounded-md p-3 mb-4 flex items-center justify-between">
          <code className="text-sm font-mono text-gray-900 break-all">{cred.password}</code>
          <button
            type="button"
            onClick={() => navigator.clipboard.writeText(cred.password)}
            className="ml-3 px-3 py-1.5 text-xs text-gray-700 border border-gray-300 rounded-md hover:bg-gray-100"
          >
            Copy
          </button>
        </div>

        <div className="flex justify-end">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
