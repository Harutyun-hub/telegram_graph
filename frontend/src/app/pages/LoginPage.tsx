import { useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { AlertCircle, ArrowRight, LockKeyhole, User } from 'lucide-react';
import { resolveAuthRedirectTarget } from '../auth';
import { useAuth } from '../contexts/AuthContext';
import { useLanguage, type Lang } from '../contexts/LanguageContext';

const COPY: Record<Lang, {
  title: string;
  subtitle: string;
  usernameLabel: string;
  usernamePlaceholder: string;
  passwordLabel: string;
  passwordPlaceholder: string;
  submit: string;
  error: string;
  panelTitle: string;
}> = {
  en: {
    title: 'Protected Access',
    subtitle: 'Sign in before entering the community intelligence workspace.',
    usernameLabel: 'Login',
    usernamePlaceholder: 'Enter your login',
    passwordLabel: 'Password',
    passwordPlaceholder: 'Enter your password',
    submit: 'Enter System',
    error: 'Invalid login or password. Please try again.',
    panelTitle: 'Sign In',
  },
  ru: {
    title: 'Защищённый вход',
    subtitle: 'Введите логин и пароль, чтобы продолжить.',
    usernameLabel: 'Логин',
    usernamePlaceholder: 'Введите логин',
    passwordLabel: 'Пароль',
    passwordPlaceholder: 'Введите пароль',
    submit: 'Войти в систему',
    error: 'Неверный логин или пароль. Попробуйте ещё раз.',
    panelTitle: 'Авторизация',
  },
};

export function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { login } = useAuth();
  const { lang, setLang } = useLanguage();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const copy = COPY[lang];

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const didLogin = login(username, password);
    if (!didLogin) {
      setError(copy.error);
      return;
    }

    setError('');
    navigate(resolveAuthRedirectTarget(location.state), { replace: true });
  }

  return (
    <div className="min-h-[100dvh] bg-slate-950 text-white">
      <div
        className="flex min-h-[100dvh] items-center justify-center px-4 py-10 sm:px-6 lg:px-8"
        style={{
          background:
            'radial-gradient(circle at top left, rgba(30, 64, 175, 0.36), transparent 28%), radial-gradient(circle at bottom right, rgba(14, 116, 144, 0.24), transparent 30%), linear-gradient(160deg, #020617 0%, #0f172a 55%, #111827 100%)',
        }}
      >
        <div className="w-full max-w-md">
          <section className="rounded-[32px] border border-slate-800 bg-white p-6 text-slate-900 shadow-2xl sm:p-8">
            <div className="flex items-center justify-end">
              <div className="flex items-center rounded-full border border-slate-200 bg-slate-50 p-1">
                {(['en', 'ru'] as Lang[]).map((language) => (
                  <button
                    key={language}
                    type="button"
                    onClick={() => setLang(language)}
                    className={`rounded-full px-3 py-1.5 text-xs transition-colors ${
                      lang === language ? 'bg-slate-900 text-white' : 'text-slate-500 hover:text-slate-900'
                    }`}
                    style={{ fontWeight: 700 }}
                  >
                    {language === 'en' ? 'EN' : 'РУ'}
                  </button>
                ))}
              </div>
            </div>

            <div className="mt-8 text-center">
              <h1 className="text-3xl text-slate-950 sm:text-[2.1rem]" style={{ fontWeight: 700 }}>
                {copy.panelTitle}
              </h1>
              <p className="mt-3 text-sm leading-6 text-slate-500 sm:text-base">
                {copy.subtitle}
              </p>
            </div>

            <form className="mt-8 space-y-5" onSubmit={handleSubmit}>
              <label className="block">
                <span className="mb-2 flex items-center gap-2 text-sm text-slate-600" style={{ fontWeight: 600 }}>
                  <User className="h-4 w-4" />
                  {copy.usernameLabel}
                </span>
                <input
                  value={username}
                  onChange={(event) => {
                    setUsername(event.target.value);
                    if (error) {
                      setError('');
                    }
                  }}
                  autoComplete="username"
                  className="w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-base text-slate-950 outline-none transition focus:border-blue-500 focus:bg-white focus:ring-4 focus:ring-blue-500/10"
                  placeholder={copy.usernamePlaceholder}
                />
              </label>

              <label className="block">
                <span className="mb-2 flex items-center gap-2 text-sm text-slate-600" style={{ fontWeight: 600 }}>
                  <LockKeyhole className="h-4 w-4" />
                  {copy.passwordLabel}
                </span>
                <input
                  type="password"
                  value={password}
                  onChange={(event) => {
                    setPassword(event.target.value);
                    if (error) {
                      setError('');
                    }
                  }}
                  autoComplete="current-password"
                  className="w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-base text-slate-950 outline-none transition focus:border-blue-500 focus:bg-white focus:ring-4 focus:ring-blue-500/10"
                  placeholder={copy.passwordPlaceholder}
                />
              </label>

              {error ? (
                <div className="flex items-start gap-2 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                  <AlertCircle className="mt-0.5 h-4 w-4 flex-shrink-0" />
                  <span>{error}</span>
                </div>
              ) : null}

              <button
                type="submit"
                className="flex w-full items-center justify-center gap-2 rounded-2xl px-4 py-3 text-base text-white transition hover:opacity-95 focus:outline-none focus:ring-4 focus:ring-blue-500/20"
                style={{ fontWeight: 700, background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}
              >
                <span>{copy.submit}</span>
                <ArrowRight className="h-4 w-4" />
              </button>
            </form>
          </section>
        </div>
      </div>
    </div>
  );
}
