import { createBrowserRouter } from "react-router";
import { AdminLayout } from "./layouts/AdminLayout";
import { DataProvider } from "./contexts/DataContext";
import { AdminConfigProvider } from "./contexts/AdminConfigContext";

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

export const router = createBrowserRouter([
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
        path: "settings",
        lazy: async () => {
          const { SettingsPage } = await import("./pages/SettingsPage");
          return { Component: SettingsPage };
        },
      },
    ],
  },
]);
