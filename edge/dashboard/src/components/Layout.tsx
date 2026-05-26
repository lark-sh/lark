import { Link, NavLink, Outlet, useLocation } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';

export function Layout() {
  const { account, logout } = useAuth();
  const location = useLocation();

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-8">
              <Link to="/projects" className="text-xl font-semibold text-gray-900">
                Lark
              </Link>
              <nav className="flex gap-2">
                <NavTab to="/projects" active={location.pathname.startsWith('/projects')}>
                  Projects
                </NavTab>
                <NavTab to="/users" active={location.pathname.startsWith('/users')}>
                  Users
                </NavTab>
              </nav>
            </div>

            <div className="flex items-center gap-4">
              {account && (
                <>
                  <Link
                    to="/account"
                    className="text-sm text-gray-700 hover:text-gray-900"
                  >
                    {account.email}
                  </Link>
                  <button
                    type="button"
                    onClick={() => logout()}
                    className="text-sm text-gray-600 hover:text-gray-900"
                  >
                    Sign out
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Outlet />
      </main>
    </div>
  );
}

function NavTab({
  to,
  active,
  children,
}: {
  to: string;
  active: boolean;
  children: React.ReactNode;
}) {
  return (
    <NavLink
      to={to}
      className={
        'px-3 py-2 text-sm font-medium rounded-md ' +
        (active ? 'bg-gray-100 text-gray-900' : 'text-gray-600 hover:text-gray-900')
      }
    >
      {children}
    </NavLink>
  );
}
