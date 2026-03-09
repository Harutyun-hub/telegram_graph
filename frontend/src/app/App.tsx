import { RouterProvider } from 'react-router';
import { router } from './routes';
import { LanguageProvider } from './contexts/LanguageContext';
import { DataProvider } from './contexts/DataContext';

function App() {
  return (
    <LanguageProvider>
      <DataProvider>
        <RouterProvider router={router} />
      </DataProvider>
    </LanguageProvider>
  );
}

export default App;
