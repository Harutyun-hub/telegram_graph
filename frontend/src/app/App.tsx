import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DashboardDateRangeProvider } from './contexts/DashboardDateRangeContext';
import { SocialDateRangeProvider } from './contexts/SocialDateRangeContext';
import { AuthProvider } from './contexts/AuthContext';

function App() {
  return (
    <LanguageProvider>
      <AuthProvider>
        <DashboardDateRangeProvider>
          <SocialDateRangeProvider>
            <RouterProvider router={router} />
          </SocialDateRangeProvider>
        </DashboardDateRangeProvider>
      </AuthProvider>
    </LanguageProvider>
  );
}

export default App;
