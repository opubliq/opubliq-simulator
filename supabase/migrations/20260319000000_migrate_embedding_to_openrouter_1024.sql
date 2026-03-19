-- Migrate questions.embedding from vector(1536) to vector(1024)
-- Switch from Google Gemini gemini-embedding-001 (Matryoshka 1536 dims)
-- to OpenRouter intfloat/multilingual-e5-large-instruct (native 1024 dims).
--
-- All existing embeddings are nulled out so generate_embeddings.py will
-- re-embed every row on the next run (idempotent via --force or null check).

set search_path = public, extensions;

-- 1. Drop HNSW index (required before altering column type)
drop index if exists questions_embedding_idx;

-- Also drop any auto-named HNSW indexes on questions.embedding
do $$
declare
  idx_name text;
begin
  for idx_name in
    select indexname
    from pg_indexes
    where tablename = 'questions'
      and indexdef ilike '%hnsw%'
      and indexdef ilike '%embedding%'
  loop
    execute 'drop index if exists ' || quote_ident(idx_name);
  end loop;
end;
$$;

-- 2. Clear existing embeddings (they were generated with a different model and are incompatible)
update questions set embedding = null;

-- 3. Alter column type to vector(1024)
alter table questions
  alter column embedding type extensions.vector(1024);

-- 4. Drop old match_questions function (signature uses vector(1536), must be dropped before redefining)
drop function if exists match_questions(extensions.vector(1536), int);

-- 5. Recreate match_questions with vector(1024) signature
create or replace function match_questions(
  query_embedding extensions.vector(1024),
  match_count     int default 15
)
returns table (
  id               bigint,
  text             text,
  scale_type       text,
  var_name         text,
  prefix           text,
  survey_id        bigint,
  choices          jsonb,
  cosine_similarity float
)
language sql stable
set search_path = public, extensions
as $$
  select
    q.id,
    q.text,
    q.scale_type,
    q.var_name,
    q.prefix,
    q.survey_id,
    q.choices,
    1 - (q.embedding <=> query_embedding) as cosine_similarity
  from questions q
  where
    q.embedding is not null
    and (q.prefix is null or q.prefix != 'meta')
  order by q.embedding <=> query_embedding
  limit match_count;
$$;

-- 6. Recreate HNSW index for vector(1024) cosine similarity search
create index on questions using hnsw (embedding extensions.vector_cosine_ops);
