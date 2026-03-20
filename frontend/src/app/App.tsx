import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DashboardDateRangeProvider } from './contexts/DashboardDateRangeContext';
import { DataProvider } from './contexts/DataContext';
import { AdminConfigProvider } from './contexts/AdminConfigContext';

function App() {
  return (
    <LanguageProvider>
      <DashboardDateRangeProvider>
        <DataProvider>
          <AdminConfigProvider>
            <RouterProvider router={router} />
          </AdminConfigProvider>
        </DataProvider>
      </DashboardDateRangeProvider>
    </LanguageProvider>
  );
}

export default App;
