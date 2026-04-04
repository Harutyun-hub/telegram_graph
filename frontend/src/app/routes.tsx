import { Navigate, Outlet, createBrowserRouter, useLocation } from "react-router";
import { AdminLayout } from "./layouts/AdminLayout";
import { DataProvider } from "./contexts/DataContext";
import { AdminConfigProvider } from "./contexts/AdminConfigContext";
import { resolveAuthRedirectTarget } from "./auth";
import { useAuth } from "./contexts/AuthContext";

function AuthBootstrapScreen() {
  return (
    <div className="flex min-h-[100dvh] items-center justify-center bg-slate-950 text-sm text-slate-300">
      Checking access...
    </div>
  );
}

function DashboardShell() {
  return (
    <DataProvider>
      <AdminConfigProvider>
        <AdminLayout />
      </AdminConfigProvider>
    </DataProvider>
  );
}

function AdminShell() {
  return (
    <AdminConfigProvider>
      <AdminLayout />
    </AdminConfigProvider>
  );
}

function PlainShell() {
  return <AdminLayout />;
}

function ProtectedRoutes() {
  const { isAuthenticated, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return <AuthBootstrapScreen />;
  }

  if (!isAuthenticated) {
    return (
      <Navigate
        replace
        to="/login"
        state={{
          from: {
            pathname: location.pathname,
            search: location.search,
            hash: location.hash,
          },
        }}
      />
    );
  }

  return <Outlet />;
}

function LoginRoute() {
  const { isAuthenticated, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return <AuthBootstrapScreen />;
  }

  if (isAuthenticated) {
    return <Navigate replace to={resolveAuthRedirectTarget(location.state)} />;
  }

  return <Outlet />;
}

function SocialLegacyRedirect() {
  return <Navigate replace to="/social" />;
}

export const router = createBrowserRouter([
  {
    path: "/login",
    Component: LoginRoute,
    children: [
      {
        index: true,
        lazy: async () => {
          const { LoginPage } = await import("./pages/LoginPage");
          return { Component: LoginPage };
        },
      },
    ],
  },
  {
    Component: ProtectedRoutes,
    children: [
      {
        path: "/",
        Component: DashboardShell,
        children: [
          {
            index: true,
            lazy: async () => {
              const { DashboardPage } = await import("./pages/DashboardPage");
              return { Component: DashboardPage };
            },
          },
        ],
      },
      {
        path: "/",
        Component: AdminShell,
        children: [
          {
            path: "admin",
            lazy: async () => {
              const { AdminPage } = await import("./pages/AdminPage");
              return { Component: AdminPage };
            },
          },
        ],
      },
      {
        path: "/",
        Component: PlainShell,
        children: [
          {
            path: "topics",
            lazy: async () => {
              const { TopicsPage } = await import("./pages/TopicsPage");
              return { Component: TopicsPage };
            },
          },
          {
            path: "channels",
            lazy: async () => {
              const { ChannelsPage } = await import("./pages/ChannelsPage");
              return { Component: ChannelsPage };
            },
          },
          {
            path: "audience",
            lazy: async () => {
              const { AudiencePage } = await import("./pages/AudiencePage");
              return { Component: AudiencePage };
            },
          },
          {
            path: "graph",
            lazy: async () => {
              const { GraphPage } = await import("./pages/GraphPage");
              return { Component: GraphPage };
            },
          },
          {
            path: "sources",
            lazy: async () => {
              const { SourcesPage } = await import("./pages/SourcesPage");
              return { Component: SourcesPage };
            },
          },
          {
            path: "social",
            lazy: async () => {
              const { SocialPage } = await import("./pages/SocialPage");
              return { Component: SocialPage };
            },
          },
          {
            path: "social/ops",
            lazy: async () => {
              const { SocialOpsPage } = await import("./pages/SocialOpsPage");
              return { Component: SocialOpsPage };
            },
          },
          {
            path: "social-activities",
            Component: SocialLegacyRedirect,
          },
          {
            path: "settings",
            lazy: async () => {
              const { SettingsPage } = await import("./pages/SettingsPage");
              return { Component: SettingsPage };
            },
          },
          {
            path: "agent",
            lazy: async () => {
              const { AgentPage } = await import("./pages/AgentPage");
              return { Component: AgentPage };
            },
          },
        ],
      },
    ],
  },
]);
