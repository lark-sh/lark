import { useState } from 'react';
import { useAuth } from '../hooks/useAuth';
import { ChangePasswordForm } from '../components/ChangePasswordForm';

export function AccountSettings() {
  const { account } = useAuth();
  const [savedAt, setSavedAt] = useState<number | null>(null);

  if (!account) return null;

  return (
    <div className="space-y-6 max-w-2xl">
      <h1 className="text-2xl font-semibold text-gray-900">Account</h1>

      <section className="bg-white rounded-lg border border-gray-200 p-6">
        <h2 className="text-lg font-medium text-gray-900 mb-4">Profile</h2>
        <dl className="grid grid-cols-3 gap-y-3 text-sm">
          <dt className="text-gray-500">Email</dt>
          <dd className="col-span-2 text-gray-900">{account.email}</dd>

          <dt className="text-gray-500">Role</dt>
          <dd className="col-span-2 text-gray-900">{account.role}</dd>

          <dt className="text-gray-500">Member since</dt>
          <dd className="col-span-2 text-gray-900">
            {new Date(account.created_at).toLocaleDateString()}
          </dd>
        </dl>
      </section>

      <section className="bg-white rounded-lg border border-gray-200 p-6">
        <h2 className="text-lg font-medium text-gray-900 mb-1">Change password</h2>
        <p className="text-sm text-gray-500 mb-4">
          You'll stay signed in on this device.
        </p>
        {savedAt && (
          <div className="mb-4 text-sm text-green-700 bg-green-50 border border-green-200 rounded-md px-3 py-2">
            Password updated.
          </div>
        )}
        <ChangePasswordForm
          forced={false}
          onSuccess={() => setSavedAt(Date.now())}
        />
      </section>
    </div>
  );
}
