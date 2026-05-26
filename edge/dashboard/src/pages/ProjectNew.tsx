import { useState, type FormEvent } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api, ApiError } from '../api/client';

// Project IDs need to be DNS-label-compatible: lowercase letters, digits,
// and hyphens; can't start or end with a hyphen; no double-hyphens. The
// server enforces the same rules; this regex is just for friendlier
// client-side feedback.
const ID_RE = /^[a-z0-9](?:[a-z0-9]|-(?!-))*[a-z0-9]$/;

function suggestIDFromName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 40);
}

export function ProjectNew() {
  const navigate = useNavigate();
  const [id, setId] = useState('');
  const [name, setName] = useState('');
  const [idTouched, setIdTouched] = useState(false);
  const [ephemeral, setEphemeral] = useState(true);
  const [autoCreate, setAutoCreate] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleNameChange = (v: string) => {
    setName(v);
    if (!idTouched) {
      setId(suggestIDFromName(v));
    }
  };

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);

    if (!name.trim()) {
      setError('Project name is required.');
      return;
    }
    if (!id || !ID_RE.test(id) || id.length > 40) {
      setError(
        'Project ID must be 1-40 chars, lowercase letters / digits / hyphens, no leading or trailing hyphen, no double hyphens.',
      );
      return;
    }

    setSubmitting(true);
    try {
      const project = await api.createProject({
        id,
        name: name.trim(),
        ephemeral,
        auto_create: autoCreate,
      });
      navigate(`/projects/${project.id}`);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Failed to create project');
      setSubmitting(false);
    }
  }

  return (
    <div className="max-w-lg">
      <div className="mb-6">
        <Link to="/projects" className="text-sm text-gray-600 hover:text-gray-900">
          ← Back to projects
        </Link>
      </div>

      <h1 className="text-2xl font-semibold text-gray-900 mb-6">New project</h1>

      <form onSubmit={onSubmit} className="space-y-6">
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
            {error}
          </div>
        )}

        <div>
          <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-1">
            Name
          </label>
          <input
            id="name"
            type="text"
            value={name}
            onChange={(e) => handleNameChange(e.target.value)}
            placeholder="My App"
            autoFocus
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>

        <div>
          <label htmlFor="id" className="block text-sm font-medium text-gray-700 mb-1">
            ID
          </label>
          <input
            id="id"
            type="text"
            value={id}
            onChange={(e) => {
              setId(e.target.value);
              setIdTouched(true);
            }}
            placeholder="my-app"
            className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          <p className="mt-1 text-xs text-gray-500">
            Used in subdomains and the wire protocol. Cannot be changed later.
          </p>
        </div>

        <div className="space-y-3 pt-2 border-t border-gray-100">
          <Toggle label="Ephemeral" hint="Databases disappear after inactivity" value={ephemeral} onChange={setEphemeral} />
          <Toggle label="Auto-create" hint="Create databases on first connect" value={autoCreate} onChange={setAutoCreate} />
        </div>

        <div className="flex gap-3 pt-2">
          <button
            type="submit"
            disabled={submitting}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 disabled:opacity-60"
          >
            {submitting ? 'Creating…' : 'Create project'}
          </button>
          <Link
            to="/projects"
            className="px-4 py-2 border border-gray-300 text-gray-700 text-sm font-medium rounded-md hover:bg-gray-50"
          >
            Cancel
          </Link>
        </div>
      </form>
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
    <div className="flex items-center justify-between">
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
