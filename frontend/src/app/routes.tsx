import { createBrowserRouter } from "react-router";
import { AdminLayout } from "./layouts/AdminLayout";
import { DashboardPage } from "./pages/DashboardPage";
import { TopicsPage } from "./pages/TopicsPage";
import { ChannelsPage } from "./pages/ChannelsPage";
import { AudiencePage } from "./pages/AudiencePage";
import { GraphPage } from "./pages/GraphPage";
import { SettingsPage } from "./pages/SettingsPage";
import { SourcesPage } from "./pages/SourcesPage";
import { AdminPage } from "./pages/AdminPage";

export const router = createBrowserRouter([
  {
    path: "/",
    Component: AdminLayout,
    children: [
      { index: true, Component: DashboardPage },
      { path: "topics", Component: TopicsPage },
      { path: "channels", Component: ChannelsPage },
      { path: "audience", Component: AudiencePage },
      { path: "graph", Component: GraphPage },
      { path: "sources", Component: SourcesPage },
      { path: "admin", Component: AdminPage },
      { path: "settings", Component: SettingsPage },
    ],
  },
]);
