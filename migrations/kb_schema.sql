-- ============================================================
-- Knowledge Base Metadata Tables
-- Run this migration in Supabase SQL Editor
--
-- NOTE: ChromaDB (Docker volume) is the actual vector store.
-- These tables track document metadata and ingestion status
-- for the web dashboard (document list, status indicators).
-- ============================================================

-- Document collections (mirrors ChromaDB collection names)
create table if not exists kb_collections (
  id          uuid primary key default gen_random_uuid(),
  name        text not null unique,
  description text not null default '',
  created_at  timestamptz not null default now()
);

-- Documents ingested per collection
create table if not exists kb_documents (
  id            uuid primary key default gen_random_uuid(),
  collection_id uuid references kb_collections(id) on delete cascade,
  doc_id        text not null unique,        -- ChromaDB doc_id (UUID string)
  doc_title     text not null,
  source        text not null default '',     -- original file path or URL
  source_type   text not null default '',     -- 'pdf', 'docx', 'txt', 'url'
  chunk_count   integer not null default 0,
  status        text not null default 'indexed', -- 'pending', 'indexed', 'failed'
  error_message text,
  ingested_at   timestamptz not null default now()
);

-- Indexes for common queries
create index if not exists kb_documents_collection_id_idx on kb_documents(collection_id);
create index if not exists kb_documents_doc_id_idx on kb_documents(doc_id);
create index if not exists kb_documents_ingested_at_idx on kb_documents(ingested_at desc);

-- ============================================================
-- Optional: RLS (Row Level Security)
-- Uncomment and adapt if you enable Supabase Auth
-- ============================================================
-- alter table kb_collections enable row level security;
-- alter table kb_documents enable row level security;
-- create policy "Allow service role full access" on kb_collections
--   using (true) with check (true);
-- create policy "Allow service role full access" on kb_documents
--   using (true) with check (true);
