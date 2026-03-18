-- Normalize strate_age_group values from underscore to hyphen format
-- DB was populated with '18_34', '35_54', '55_plus' but canonical format is '18-34', '35-54', '55+'

UPDATE strate_predictions SET strate_age_group = '18-34' WHERE strate_age_group = '18_34';
UPDATE strate_predictions SET strate_age_group = '35-54' WHERE strate_age_group = '35_54';
UPDATE strate_predictions SET strate_age_group = '55+'   WHERE strate_age_group = '55_plus';

UPDATE strate_weights SET strate_age_group = '18-34' WHERE strate_age_group = '18_34';
UPDATE strate_weights SET strate_age_group = '35-54' WHERE strate_age_group = '35_54';
UPDATE strate_weights SET strate_age_group = '55+'   WHERE strate_age_group = '55_plus';
