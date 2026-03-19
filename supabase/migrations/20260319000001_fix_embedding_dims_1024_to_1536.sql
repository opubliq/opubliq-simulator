-- Fix: revert questions.embedding from vector(1024) back to vector(1536).
-- intfloat/multilingual-e5-large-instruct is not available on OpenRouter;
-- using openai/text-embedding-3-small (1536 dims) instead.
-- The previous migration already nulled all embeddings, so no data to migrate.

set search_path = public, extensions;

-- 1. Drop HNSW index created for vector(1024)
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

-- 2. Alter column back to vector(1536)
alter table questions
  alter column embedding type extensions.vector(1536);

-- 3. Drop match_questions(vector(1024)) and recreate with vector(1536)
drop function if exists match_questions(extensions.vector(1024), int);

create or replace function match_questions(
  query_embedding extensions.vector(1536),
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

-- 4. Recreate HNSW index for vector(1536)
create index on questions using hnsw (embedding extensions.vector_cosine_ops);
