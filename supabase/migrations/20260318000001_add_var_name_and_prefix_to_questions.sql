-- Add var_name and prefix columns to questions table
-- var_name: canonical variable name from clean.py codebooks (e.g. op_health_importance, ses_age, meta_weight_calibrated)
-- prefix: everything before the first underscore (e.g. op, ses, meta, behav, cps)
-- The prefix 'meta' allows filtering out sampling weights, identifiers, and pre-computed strata
-- from compute_strate_predictions.py and anywhere we want to work only with real opinion/behaviour variables.

ALTER TABLE questions ADD COLUMN var_name text;
ALTER TABLE questions ADD COLUMN prefix text;

-- Backfill prefix from var_name where var_name is already set
-- (split on first underscore; vars without underscore get the full name as prefix)
UPDATE questions
SET prefix = split_part(var_name, '_', 1)
WHERE var_name IS NOT NULL;

-- Index on prefix for fast filtering (e.g. WHERE prefix != 'meta')
CREATE INDEX idx_questions_prefix ON questions(prefix);
-- Index on var_name for lookups by variable name
CREATE INDEX idx_questions_var_name ON questions(var_name);
