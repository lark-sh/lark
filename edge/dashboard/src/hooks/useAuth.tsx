import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from 'react';
import { api, ApiError, type Account } from '../api/client';

interface AuthContextValue {
  account: Account | null;
  // Per-deployment wire-protocol domain (LARKDB_DOMAIN). The database
  // editor uses it to construct subdomain URLs without hardcoding any
  // host. Empty string until the first me() resolves.
  larkdbDomain: string;
  loading: boolean;
  error: string | null;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [account, setAccount] = useState<Account | null>(null);
  const [larkdbDomain, setLarkdbDomain] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const { account: a, larkdb_domain } = await api.me();
      setAccount(a);
      setLarkdbDomain(larkdb_domain || '');
      setError(null);
    } catch (err) {
      setAccount(null);
      // 401 is the expected "not logged in" state; only surface other errors.
      if (err instanceof ApiError && err.status !== 401) {
        setError(err.message);
      }
    }
  }, []);

  useEffect(() => {
    (async () => {
      await refresh();
      setLoading(false);
    })();
  }, [refresh]);

  const login = useCallback(
    async (email: string, password: string) => {
      const { account: a } = await api.login(email, password);
      setAccount(a);
      setError(null);
      // The login response doesn't include larkdb_domain, so refresh me()
      // to pull it in (and any other deploy-config we add later).
      await refresh();
    },
    [refresh],
  );

  const logout = useCallback(async () => {
    try {
      await api.logout();
    } finally {
      setAccount(null);
    }
  }, []);

  return (
    <AuthContext.Provider
      value={{ account, larkdbDomain, loading, error, login, logout, refresh }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error('useAuth must be used inside an AuthProvider');
  }
  return ctx;
}
