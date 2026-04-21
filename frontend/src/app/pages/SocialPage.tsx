import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { useLanguage } from '../contexts/LanguageContext';

export function SocialPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';

  return (
    <main className="min-h-[calc(100dvh-5rem)] bg-background px-4 py-6 md:px-6">
      <Card className="mx-auto max-w-4xl">
        <CardHeader>
          <CardTitle>{ru ? 'Social-поверхность недоступна' : 'Social surface unavailable'}</CardTitle>
          <CardDescription>
            {ru
              ? 'Эта staging-ветка предназначена для тестирования original dashboard пути. Social-страницы здесь временно не включены.'
              : 'This staging branch is reserved for testing the original dashboard path. Social pages are temporarily not enabled here.'}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm text-muted-foreground">
          <p>
            {ru
              ? 'Для проверки производительности и регрессий используйте основной dashboard на маршруте /.'
              : 'Use the main dashboard on / for performance and regression testing.'}
          </p>
          <p>
            {ru
              ? 'Когда social-поверхность понадобится в этой ветке, её можно вернуть отдельно без влияния на original dashboard flow.'
              : 'If the social surface is needed on this branch later, we can restore it separately without affecting the original dashboard flow.'}
          </p>
        </CardContent>
      </Card>
    </main>
  );
}
