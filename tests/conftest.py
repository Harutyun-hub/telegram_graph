from __future__ import annotations

import os


_DEFAULT_ENV = {
    "APP_ROLE": "all",
    "TELEGRAM_API_ID": "1",
    "TELEGRAM_API_HASH": "test-hash",
    "SUPABASE_URL": "https://example.supabase.co",
    "SUPABASE_SERVICE_ROLE_KEY": "test-service-role-key",
    "SOCIAL_SUPABASE_URL": "https://example.social.supabase.co",
    "SOCIAL_SUPABASE_SERVICE_ROLE_KEY": "test-social-service-role-key",
    "NEO4J_URI": "bolt://localhost:7687",
    "NEO4J_PASSWORD": "test-password",
    "SOCIAL_NEO4J_URI": "bolt://localhost:9687",
    "SOCIAL_NEO4J_PASSWORD": "test-social-password",
    "OPENAI_API_KEY": "test-openai-key",
    "OPENAI_MODEL": "gpt-5.4-mini",
    "ADMIN_API_KEY": "test-admin-key",
    "ANALYTICS_API_REQUIRE_AUTH": "false",
    "SOCIAL_RUNTIME_ENABLED": "true",
}


for key, value in _DEFAULT_ENV.items():
    os.environ.setdefault(key, value)
