import { useNavigate } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';
import { ChangePasswordForm } from '../components/ChangePasswordForm';

// Standalone /change-password route, used primarily for the forced-reset
// flow after first-boot bootstrap or admin password reset. Proactive
// password changes go through /account (AccountSettings) instead.
export function ChangePassword() {
  const navigate = useNavigate();
  const { account, refresh } = useAuth();
  const forced = account?.must_change_password === true;

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="w-full max-w-sm bg-white rounded-lg border border-gray-200 p-8 shadow-sm">
        <h1 className="text-xl font-semibold text-gray-900 mb-1">
          {forced ? 'Set a new password' : 'Change password'}
        </h1>
        <p className="text-sm text-gray-500 mb-6">
          {forced
            ? 'Your account is using a temporary password. Choose a new one to continue.'
            : 'Pick a new password for your account.'}
        </p>

        <ChangePasswordForm
          forced={forced}
          onSuccess={async () => {
            await refresh();
            navigate('/projects', { replace: true });
          }}
        />
      </div>
    </div>
  );
}
