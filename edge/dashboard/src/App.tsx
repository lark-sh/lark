import {
  BrowserRouter,
  Navigate,
  Outlet,
  Route,
  Routes,
} from 'react-router-dom';
import { AuthProvider, useAuth } from './hooks/useAuth';
import { Layout } from './components/Layout';
import { Login } from './pages/Login';
import { ChangePassword } from './pages/ChangePassword';
import { ProjectList } from './pages/ProjectList';
import { ProjectNew } from './pages/ProjectNew';
import { ProjectSettings } from './pages/ProjectSettings';
import { ProjectDashboard } from './pages/ProjectDashboard';
import { DatabaseEditor } from './pages/DatabaseEditor';
import { UserList } from './pages/UserList';
import { AccountSettings } from './pages/AccountSettings';

function LoadingScreen() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 text-sm text-gray-500">
      Loading…
    </div>
  );
}

// Requires a logged-in account. Forces the password-change flow when the
// account is flagged for it.
function RequireAuth() {
  const { account, loading } = useAuth();
  if (loading) return <LoadingScreen />;
  if (!account) return <Navigate to="/login" replace />;
  if (account.must_change_password) {
    return <Navigate to="/change-password" replace />;
  }
  return <Outlet />;
}

// Hides /login from already-authenticated users.
function NoAuthRedirect() {
  const { account, loading } = useAuth();
  if (loading) return <LoadingScreen />;
  if (account && !account.must_change_password) {
    return <Navigate to="/projects" replace />;
  }
  return <Outlet />;
}

// Allows the forced-change page only when actually flagged for it (or when
// the user is logged in and chose to change password proactively).
function RequireAuthFlexible() {
  const { account, loading } = useAuth();
  if (loading) return <LoadingScreen />;
  if (!account) return <Navigate to="/login" replace />;
  return <Outlet />;
}

function AppRoutes() {
  return (
    <Routes>
      <Route element={<NoAuthRedirect />}>
        <Route path="/login" element={<Login />} />
      </Route>

      <Route element={<RequireAuthFlexible />}>
        <Route path="/change-password" element={<ChangePassword />} />
      </Route>

      <Route path="/" element={<Navigate to="/projects" replace />} />

      <Route element={<RequireAuth />}>
        <Route element={<Layout />}>
          <Route path="/projects" element={<ProjectList />} />
          <Route path="/projects/new" element={<ProjectNew />} />
          <Route path="/projects/:projectId" element={<ProjectDashboard />} />
          <Route path="/projects/:projectId/settings" element={<ProjectSettings />} />
          <Route
            path="/projects/:projectId/databases/:databaseId/*"
            element={<DatabaseEditor />}
          />
          <Route path="/users" element={<UserList />} />
          <Route path="/account" element={<AccountSettings />} />
        </Route>
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

export default function App() {
  return (
    <BrowserRouter basename="/admin">
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </BrowserRouter>
  );
}
