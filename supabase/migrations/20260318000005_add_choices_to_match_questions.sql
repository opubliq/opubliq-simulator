-- Add choices to match_questions RPC return type
-- choices is already in the questions table; this updates the function definition to expose it.
-- Must DROP first because PostgreSQL forbids changing the return type via CREATE OR REPLACE.

drop function if exists match_questions(extensions.vector(1536), int);

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
