import { createContext, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import {
  clearStoredSimpleAuthSession,
  isSimpleAuthEnabled,
  loadStoredSimpleAuthSession,
  persistSimpleAuthSession,
  type SimpleAuthSession,
  validateSimpleCredentials,
} from '../auth';
import { getSupabaseBrowserClient } from '../services/supabaseClient';

type AuthMode = 'supabase' | 'simple' | 'none';

interface AuthContextValue {
  isAuthenticated: boolean;
  username: string | null;
  loading: boolean;
  authMode: AuthMode;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

interface AuthState {
  loading: boolean;
  session: SimpleAuthSession | null;
  mode: AuthMode;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

function buildSimpleState(session: SimpleAuthSession | null): AuthState {
  return {
    loading: false,
    session,
    mode: session ? 'simple' : 'none',
  };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>(() => ({
    loading: true,
    session: null,
    mode: 'none',
  }));

  useEffect(() => {
    let active = true;
    const supabase = getSupabaseBrowserClient();

    if (!supabase) {
      setState(buildSimpleState(loadStoredSimpleAuthSession()));
      return undefined;
    }

    const applySupabaseSession = (session: any) => {
      if (!active) {
        return;
      }
      const username = session?.user?.email || session?.user?.id || null;
      setState({
        loading: false,
        session: username
          ? {
              authenticated: true,
              username,
            }
          : null,
        mode: username ? 'supabase' : 'none',
      });
    };

    supabase.auth.getSession()
      .then(({ data }) => applySupabaseSession(data.session))
      .catch(() => {
        if (!active) {
          return;
        }
        setState(buildSimpleState(loadStoredSimpleAuthSession()));
      });

    const { data } = supabase.auth.onAuthStateChange((_event, session) => {
      applySupabaseSession(session);
    });

    return () => {
      active = false;
      data.subscription.unsubscribe();
    };
  }, []);

  const login = async (username: string, password: string): Promise<void> => {
    const normalizedUsername = username.trim();
    const supabase = getSupabaseBrowserClient();
    if (supabase) {
      const { data, error } = await supabase.auth.signInWithPassword({
        email: normalizedUsername,
        password,
      });
      if (error) {
        throw new Error(error.message || 'Unable to sign in.');
      }
      const nextSession: SimpleAuthSession = {
        authenticated: true,
        username: data.user?.email || normalizedUsername,
      };
      setState({
        loading: false,
        session: nextSession,
        mode: 'supabase',
      });
      return;
    }

    if (!isSimpleAuthEnabled()) {
      throw new Error('Supabase browser auth is not configured in the frontend environment.');
    }

    if (!validateSimpleCredentials(normalizedUsername, password)) {
      throw new Error('Invalid login or password. Please try again.');
    }

    const nextSession: SimpleAuthSession = {
      authenticated: true,
      username: normalizedUsername,
    };
    setState({
      loading: false,
      session: nextSession,
      mode: 'simple',
    });
    persistSimpleAuthSession(nextSession);
  };

  const logout = async (): Promise<void> => {
    const supabase = getSupabaseBrowserClient();
    if (supabase) {
      try {
        await supabase.auth.signOut();
      } catch {
        // Ignore sign-out transport issues and clear local state anyway.
      }
    }

    clearStoredSimpleAuthSession();
    setState({
      loading: false,
      session: null,
      mode: 'none',
    });
  };

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated: Boolean(state.session),
        username: state.session?.username ?? null,
        loading: state.loading,
        authMode: state.mode,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);

  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }

  return context;
}
