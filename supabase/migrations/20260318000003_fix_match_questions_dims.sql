-- Fix match_questions function to use vector(1536) matching questions.embedding column.
-- The previous migration (20260318000002) incorrectly used vector(768);
-- questions.embedding was migrated to vector(1536) in 20260317000002.

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
    1 - (q.embedding <=> query_embedding) as cosine_similarity
  from questions q
  where
    q.embedding is not null
    and (q.prefix is null or q.prefix != 'meta')
  order by q.embedding <=> query_embedding
  limit match_count;
$$;
