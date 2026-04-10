# Supabase Migrations

This directory is now the source-of-truth location for executable Supabase schema changes.

## Current Status
- Historical SQL files have been copied here from `docs/migrations/` so future database work has a canonical home.
- A true baseline migration zero still needs to be generated from the live production schema before the first managed apply flow is used against staging or production.

## Policy
- Forward-only migrations.
- Fix-forward on failure.
- Apply order: local -> staging -> production.
- Do not ship destructive schema changes without a rehearsed restore plan.
