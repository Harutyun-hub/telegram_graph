import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DashboardDateRangeProvider } from './contexts/DashboardDateRangeContext';

function App() {
  return (
    <LanguageProvider>
      <DashboardDateRangeProvider>
        <RouterProvider router={router} />
      </DashboardDateRangeProvider>
    </LanguageProvider>
  );
}

export default App;
