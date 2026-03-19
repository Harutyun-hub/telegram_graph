import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DataProvider } from './contexts/DataContext';
import { AdminConfigProvider } from './contexts/AdminConfigContext';

function App() {
  return (
    <LanguageProvider>
      <DataProvider>
        <AdminConfigProvider>
          <RouterProvider router={router} />
        </AdminConfigProvider>
      </DataProvider>
    </LanguageProvider>
  );
}

export default App;
