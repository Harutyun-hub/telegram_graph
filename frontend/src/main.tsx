
  import { createRoot } from "react-dom/client";
  import App from "./app/App.tsx";
  import { Sentry, initFrontendSentry } from "./monitoring/sentry";
  import "./styles/index.css";

  initFrontendSentry();

  createRoot(document.getElementById("root")!).render(
    <Sentry.ErrorBoundary fallback={<div className="p-6 text-sm text-slate-600">The app hit an unexpected error.</div>}>
      <App />
    </Sentry.ErrorBoundary>,
  );
  
