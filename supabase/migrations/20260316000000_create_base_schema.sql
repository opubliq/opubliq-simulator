-- Enable pgvector extension
create extension if not exists vector with schema extensions;

-- Make vector type accessible
set search_path to public, extensions;

-- Create surveys table
create table surveys (
  id bigint primary key generated always as identity,
  title text not null,
  year integer not null,
  source text not null,
  n_respondents integer not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- Create questions table with embedding vector
create table questions (
  id bigint primary key generated always as identity,
  survey_id bigint not null references surveys(id) on delete cascade,
  text text not null,
  scale_type text,
  embedding vector(768),
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- Create HNSW index on questions.embedding for vector similarity search
create index on questions using hnsw (embedding vector_cosine_ops);

-- Create respondents table
create table respondents (
  id bigint primary key generated always as identity,
  survey_id bigint not null references surveys(id) on delete cascade,
  strate_canonical jsonb,
  responses jsonb,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- Create strate_predictions table
create table strate_predictions (
  question_id bigint not null references questions(id) on delete cascade,
  age_group text not null,
  language text not null,
  region text not null,
  genre text not null,
  distribution jsonb not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now(),
  primary key (question_id, age_group, language, region, genre)
);

-- Create strate_weights table
create table strate_weights (
  age_group text not null,
  language text not null,
  region text not null,
  genre text not null,
  weight_pct numeric(5, 2) not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now(),
  primary key (age_group, language, region, genre)
);

-- Create indexes for common queries
create index idx_questions_survey_id on questions(survey_id);
create index idx_respondents_survey_id on respondents(survey_id);
create index idx_strate_predictions_question_id on strate_predictions(question_id);
