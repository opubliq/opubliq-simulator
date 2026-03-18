-- Add choices JSONB column to questions table
-- Format: {"value": "label"}, e.g. {"1": "Très satisfait", "2": "Plutôt satisfait"}
-- Populated from CODEBOOK_VARIABLES value_labels in each clean.py
-- NULL for continuous/numeric variables (empty value_labels)
ALTER TABLE questions ADD COLUMN choices jsonb;
