import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DashboardDateRangeProvider } from './contexts/DashboardDateRangeContext';
import { AuthProvider } from './contexts/AuthContext';

function App() {
  return (
    <LanguageProvider>
      <AuthProvider>
        <DashboardDateRangeProvider>
          <RouterProvider router={router} />
        </DashboardDateRangeProvider>
      </AuthProvider>
    </LanguageProvider>
  );
}

export default App;
