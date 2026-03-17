-- Migrate questions.embedding from vector(768) to vector(1536)
-- gemini-embedding-001 supports Matryoshka truncation; we use output_dimensionality=1536
-- which gives near-full quality while staying within pgvector's 2000-dim HNSW index limit

-- Ensure pgvector extension is available in search path
set search_path = public, extensions;

-- Drop existing HNSW index first (required before altering column type)
drop index if exists questions_embedding_idx;

-- Alter column to new dimension
alter table questions
  alter column embedding type extensions.vector(1536);

-- Recreate HNSW cosine index
create index on questions using hnsw (embedding extensions.vector_cosine_ops);
