-- Enable pgvector extension
create extension if not exists vector;

-- Create surveys table
create table surveys (
  id bigint primary key generated always as identity,
  titre text not null,
  année integer not null,
  source text not null,
  n_répondants integer not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- Create questions table with embedding vector
create table questions (
  id bigint primary key generated always as identity,
  survey_id bigint not null references surveys(id) on delete cascade,
  texte text not null,
  type_échelle text,
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
  strate_canonique jsonb,
  réponses jsonb,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- Create strate_predictions table
create table strate_predictions (
  question_id bigint not null references questions(id) on delete cascade,
  age_group text not null,
  langue text not null,
  region text not null,
  genre text not null,
  distribution jsonb not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now(),
  primary key (question_id, age_group, langue, region, genre)
);

-- Create strate_weights table
create table strate_weights (
  age_group text not null,
  langue text not null,
  region text not null,
  genre text not null,
  weight_pct numeric(5, 2) not null,
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now(),
  primary key (age_group, langue, region, genre)
);

-- Create indexes for common queries
create index idx_questions_survey_id on questions(survey_id);
create index idx_respondents_survey_id on respondents(survey_id);
create index idx_strate_predictions_question_id on strate_predictions(question_id);
