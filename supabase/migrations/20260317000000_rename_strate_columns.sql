-- Renommer les colonnes de strate_weights pour aligner avec les strates canoniques
alter table strate_weights rename column age_group to strate_age_group;
alter table strate_weights rename column language  to strate_langue;
alter table strate_weights rename column region    to strate_region;
alter table strate_weights rename column genre     to strate_genre;

-- Renommer les colonnes de strate_predictions pour aligner avec les strates canoniques
alter table strate_predictions rename column age_group to strate_age_group;
alter table strate_predictions rename column language  to strate_langue;
alter table strate_predictions rename column region    to strate_region;
alter table strate_predictions rename column genre     to strate_genre;

-- Mettre à jour les valeurs de strate_weights (old english/french/male/female → nouveaux noms)
update strate_weights set strate_langue = 'francophone' where strate_langue = 'french';
update strate_weights set strate_langue = 'anglo_autre' where strate_langue = 'english';
update strate_weights set strate_genre  = 'homme'       where strate_genre  = 'male';
update strate_weights set strate_genre  = 'femme'       where strate_genre  = 'female';
