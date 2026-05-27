import { useEffect, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import Editor from 'react-simple-code-editor';
import Prism from 'prismjs';
import 'prismjs/components/prism-json';
import 'prismjs/themes/prism.css';
import JSON5 from 'json5';
import { api, ApiError, type Project } from '../api/client';

export function ProjectSettings() {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();

  const [project, setProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  // Form state.
  const [name, setName] = useState('');
  const [ephemeral, setEphemeral] = useState(true);
  const [autoCreate, setAutoCreate] = useState(true);
  const [rulesJson, setRulesJson] = useState('');
  const [firebaseProjectId, setFirebaseProjectId] = useState('');

  // Status.
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);

  // Secret reveal + regen.
  const [showSecret, setShowSecret] = useState(false);
  const [regenerating, setRegenerating] = useState(false);

  // Delete modal.
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState('');
  const [deleting, setDeleting] = useState(false);

  useEffect(() => {
    if (!projectId) return;
    let cancelled = false;
    (async () => {
      try {
        const data = await api.getProject(projectId);
        if (cancelled) return;
        setProject(data);
        setName(data.name);
        setEphemeral(data.ephemeral);
        setAutoCreate(data.auto_create);
        setRulesJson(data.rules_json || '');
        setFirebaseProjectId(data.firebase_project_id || '');
      } catch (err) {
        if (!cancelled) {
          setLoadError(err instanceof ApiError ? err.message : 'Failed to load project');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [projectId]);

  async function handleSave() {
    if (!projectId) return;

    if (rulesJson.trim()) {
      try {
        JSON5.parse(rulesJson);
      } catch (e) {
        const err = e as { lineNumber?: number; columnNumber?: number };
        setSaveError(
          `Invalid JSON in rules on line ${err.lineNumber ?? '?'}, column ${err.columnNumber ?? '?'}`,
        );
        return;
      }
    }

    setSaving(true);
    setSaveError(null);
    setSaveMessage(null);
    try {
      const updated = await api.updateProject(projectId, {
        name,
        ephemeral,
        auto_create: autoCreate,
        rules_json: rulesJson || '',
        // firebase_compat_enabled is intentionally omitted: it defaults to true on
        // project creation and is no longer user-facing, so we leave it untouched here.
        firebase_project_id: firebaseProjectId || '',
      });
      setProject(updated);
      setSaveMessage('Settings saved');
      setTimeout(() => setSaveMessage(null), 3000);
    } catch (err) {
      setSaveError(err instanceof ApiError ? err.message : 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  }

  async function handleRegenerateSecret() {
    if (!projectId || !project) return;
    if (!confirm('Regenerate the secret key? This will invalidate any tokens signed with the current key.')) return;

    setRegenerating(true);
    try {
      const { secret_key, config_version } = await api.regenerateProjectSecret(projectId);
      setProject({ ...project, secret_key, config_version });
      setShowSecret(true);
    } catch (err) {
      setSaveError(err instanceof ApiError ? err.message : 'Failed to regenerate secret');
    } finally {
      setRegenerating(false);
    }
  }

  async function handleDelete() {
    if (!projectId || deleteConfirm !== projectId) return;
    setDeleting(true);
    try {
      await api.deleteProject(projectId);
      navigate('/projects', { replace: true });
    } catch (err) {
      setSaveError(err instanceof ApiError ? err.message : 'Failed to delete project');
      setDeleting(false);
    }
  }

  const copy = (text: string) => navigator.clipboard.writeText(text);

  if (loading) return <div className="text-sm text-gray-500">Loading project…</div>;
  if (loadError && !project) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
        {loadError}
      </div>
    );
  }
  if (!project) return <div className="text-sm text-gray-500">Project not found</div>;

  return (
    <div>
      <div className="mb-6">
        <Link to={`/projects/${projectId}`} className="text-sm text-gray-600 hover:text-gray-900">
          ← Back to project
        </Link>
      </div>

      <h1 className="text-2xl font-semibold text-gray-900 mb-6">
        {project.name} <span className="text-gray-400 font-normal">/ Settings</span>
      </h1>

      {saveError && (
        <div className="mb-6 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {saveError}
        </div>
      )}
      {saveMessage && (
        <div className="mb-6 bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded-md text-sm">
          {saveMessage}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 items-start">
        {/* Left column */}
        <div className="space-y-8">
          {/* Basic settings */}
          <section className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-medium text-gray-900 mb-4">Settings</h2>

            <div className="space-y-4">
              <div>
                <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-1">
                  Project name
                </label>
                <input
                  id="name"
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Project ID</label>
                <div className="flex items-center gap-2">
                  <code className="flex-1 px-3 py-2 bg-gray-100 border border-gray-200 rounded-md text-sm font-mono">
                    {project.id}
                  </code>
                  <button
                    type="button"
                    onClick={() => copy(project.id)}
                    className="px-3 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
                  >
                    Copy
                  </button>
                </div>
              </div>

              <Toggle
                label="Ephemeral"
                hint="Databases disappear after inactivity"
                value={ephemeral}
                onChange={setEphemeral}
              />

              <Toggle
                label="Auto-create"
                hint="Create databases on first connect"
                value={autoCreate}
                onChange={setAutoCreate}
              />

              <div className="pt-3 border-t border-gray-100">
                <label
                  htmlFor="firebase_project_id"
                  className="block text-sm font-medium text-gray-700 mb-1"
                >
                  Firebase Auth project ID
                </label>
                <input
                  id="firebase_project_id"
                  type="text"
                  value={firebaseProjectId}
                  onChange={(e) => setFirebaseProjectId(e.target.value)}
                  placeholder="my-firebase-project"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
                <p className="mt-1 text-xs text-gray-500">
                  Used to validate Firebase ID tokens on incoming connections.
                </p>
              </div>
            </div>

            <div className="mt-6 pt-4 border-t border-gray-100">
              <button
                type="button"
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-60"
              >
                {saving ? 'Saving…' : 'Save settings'}
              </button>
            </div>
          </section>

          {/* Secret key */}
          <section className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-medium text-gray-900 mb-4">Secret key</h2>
            <p className="text-sm text-gray-500 mb-4">
              Sign JWTs for your users with this key. Keep it secret.
            </p>

            <div className="flex items-center gap-2">
              <code className="flex-1 px-3 py-2 bg-gray-100 border border-gray-200 rounded-md text-sm font-mono overflow-hidden">
                {showSecret ? project.secret_key : '••••••••••••••••••••••••••••••••'}
              </code>
              <button
                type="button"
                onClick={() => setShowSecret((v) => !v)}
                className="px-3 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
              >
                {showSecret ? 'Hide' : 'Show'}
              </button>
              <button
                type="button"
                onClick={() => copy(project.secret_key)}
                className="px-3 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
              >
                Copy
              </button>
            </div>

            <div className="mt-4">
              <button
                type="button"
                onClick={handleRegenerateSecret}
                disabled={regenerating}
                className="text-sm text-red-600 hover:text-red-800 disabled:opacity-50"
              >
                {regenerating ? 'Regenerating…' : 'Regenerate secret key'}
              </button>
            </div>
          </section>

          {/* Danger zone */}
          <section className="bg-white rounded-lg border border-red-200 p-6">
            <h2 className="text-lg font-medium text-red-600 mb-4">Danger zone</h2>

            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-medium text-gray-700">Delete project</div>
                <div className="text-sm text-gray-500">
                  Permanently deletes the project and all of its databases.
                </div>
              </div>
              <button
                type="button"
                onClick={() => setShowDeleteModal(true)}
                className="px-4 py-2 text-sm text-red-600 border border-red-300 rounded-md hover:bg-red-50"
              >
                Delete project
              </button>
            </div>
          </section>
        </div>

        {/* Right column — Security rules editor */}
        <div>
          <section className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-medium text-gray-900 mb-4">Security rules</h2>
            <p className="text-sm text-gray-500 mb-4">
              Define read/write permissions for paths in your data tree.
            </p>

            <div
              className="border border-gray-300 rounded-md overflow-y-auto focus-within:ring-2 focus-within:ring-blue-500 focus-within:border-blue-500"
              style={{ maxHeight: 700 }}
            >
              <Editor
                value={rulesJson}
                onValueChange={setRulesJson}
                highlight={(code) => Prism.highlight(code || '', Prism.languages.json, 'json')}
                placeholder='{"rules": {".read": true, ".write": "auth != null"}}'
                padding={12}
                style={{
                  fontFamily:
                    'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
                  fontSize: 13,
                  lineHeight: 1.5,
                  minHeight: 192,
                }}
              />
            </div>

            <div className="mt-4">
              <button
                type="button"
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-60"
              >
                {saving ? 'Saving…' : 'Save rules'}
              </button>
            </div>
          </section>
        </div>
      </div>

      {showDeleteModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Delete project</h3>
            <p className="text-sm text-gray-500 mb-4">
              This can't be undone. All databases under this project are deleted.
            </p>
            <p className="text-sm text-gray-700 mb-4">
              Type <code className="bg-gray-100 px-1 rounded">{project.id}</code> to confirm:
            </p>
            <input
              type="text"
              value={deleteConfirm}
              onChange={(e) => setDeleteConfirm(e.target.value)}
              placeholder={project.id}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-red-500 mb-4"
            />
            <div className="flex gap-3 justify-end">
              <button
                type="button"
                onClick={() => {
                  setShowDeleteModal(false);
                  setDeleteConfirm('');
                }}
                className="px-4 py-2 border border-gray-300 text-gray-700 text-sm rounded-md hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleDelete}
                disabled={deleteConfirm !== project.id || deleting}
                className="px-4 py-2 bg-red-600 text-white text-sm rounded-md hover:bg-red-700 disabled:opacity-60"
              >
                {deleting ? 'Deleting…' : 'Delete project'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function Toggle({
  label,
  hint,
  value,
  onChange,
}: {
  label: string;
  hint: string;
  value: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between py-3 border-t border-gray-100 first:border-t-0 first:pt-0">
      <div>
        <div className="text-sm font-medium text-gray-700">{label}</div>
        <div className="text-xs text-gray-500">{hint}</div>
      </div>
      <button
        type="button"
        onClick={() => onChange(!value)}
        className={
          'relative inline-flex h-6 w-11 items-center rounded-full transition-colors ' +
          (value ? 'bg-blue-600' : 'bg-gray-200')
        }
      >
        <span
          className={
            'inline-block h-4 w-4 transform rounded-full bg-white transition-transform ' +
            (value ? 'translate-x-6' : 'translate-x-1')
          }
        />
      </button>
    </div>
  );
}
