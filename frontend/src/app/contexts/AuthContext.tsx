import { createContext, useContext, useState } from 'react';
import type { ReactNode } from 'react';
import {
  clearStoredSimpleAuthSession,
  loadStoredSimpleAuthSession,
  persistSimpleAuthSession,
  type SimpleAuthSession,
  validateSimpleCredentials,
} from '../auth';

interface AuthContextValue {
  isAuthenticated: boolean;
  username: string | null;
  login: (username: string, password: string) => boolean;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<SimpleAuthSession | null>(() => loadStoredSimpleAuthSession());

  const login = (username: string, password: string): boolean => {
    if (!validateSimpleCredentials(username, password)) {
      return false;
    }

    const nextSession: SimpleAuthSession = {
      authenticated: true,
      username,
    };

    setSession(nextSession);
    persistSimpleAuthSession(nextSession);
    return true;
  };

  const logout = (): void => {
    setSession(null);
    clearStoredSimpleAuthSession();
  };

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated: Boolean(session),
        username: session?.username ?? null,
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
