-- RPC function used by the semantic-search Edge Function (Step 1).
-- Performs cosine similarity search on questions.embedding via pgvector,
-- excluding meta-prefixed rows (sampling weights, identifiers, pre-computed strata).
--
-- Parameters:
--   query_embedding  vector(1536)  — embedding of the user's fictional question (gemini-embedding-001 @ 1536 dims)
--   match_count      int           — number of top candidates to return (default 15)
--
-- Returns rows ordered by cosine similarity descending.

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
