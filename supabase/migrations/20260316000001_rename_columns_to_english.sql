-- Rename French/accented columns to English in surveys
alter table surveys rename column titre to title;
alter table surveys rename column année to year;
alter table surveys rename column n_répondants to n_respondents;

-- Rename French/accented columns to English in questions
alter table questions rename column texte to text;
alter table questions rename column type_échelle to scale_type;

-- Rename French/accented columns to English in respondents
alter table respondents rename column strate_canonique to strate_canonical;
alter table respondents rename column réponses to responses;

-- Rename French/accented columns to English in strate_predictions
alter table strate_predictions rename column langue to language;

-- Rename French/accented columns to English in strate_weights
alter table strate_weights rename column langue to language;
