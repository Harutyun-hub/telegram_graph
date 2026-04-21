import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { useLanguage } from '../contexts/LanguageContext';

export function SocialOpsPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  return (
    <main className="min-h-[calc(100dvh-5rem)] bg-background px-4 py-6 md:px-6">
      <Card className="mx-auto max-w-4xl">
        <CardHeader>
          <CardTitle>{ru ? 'Social Ops недоступен' : 'Social Ops unavailable'}</CardTitle>
          <CardDescription>
            {ru
              ? 'Эта ветка staging сфокусирована на original dashboard и backend performance lab.'
              : 'This staging branch is focused on the original dashboard and backend performance lab.'}
          </CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          {ru
            ? 'Для тестов используйте dashboard и связанные original-path маршруты.'
            : 'Use the dashboard and related original-path routes for testing on this deployment.'}
        </CardContent>
      </Card>
    </main>
  );
}
