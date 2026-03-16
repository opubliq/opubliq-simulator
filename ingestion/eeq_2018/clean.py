#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2018

Ce script est exécuté par lambda_raffineur_nettoyage dans pipeline_sondages.
La lambda appelle:
  - clean_data(df) → retourne DataFrame nettoyé
  - get_metadata() → retourne dictionnaire avec métadonnées du sondage et variables

Les métadonnées enrichies sont sauvegardées comme codebook.json dans S3 et utilisées
par les marts downstream pour l'interprétation sémantique des données.
"""

import numpy as np
import pandas as pd

# ============================================================================
# SURVEY METADATA (à remplir au début du script)
# ============================================================================
SURVEY_METADATA = {
    'survey_id': 'eeq_2018',
    'title': 'Quebec Election Study 2018',
    'year': 2018,
    'description': 'Quebec Election Study 2018',
    'organization': '',
    'sample_size': None,
    'language': 'fr',
    'methodology': ''
}

# ============================================================================
# CODEBOOK VARIABLES (construit progressivement variable par variable)
# ============================================================================
CODEBOOK_VARIABLES = {}

def clean_data(df):
    """Nettoie et standardise les données

    Appelé par lambda_raffineur_nettoyage dans pipeline_sondages.

    Args:
        df (pd.DataFrame): Données brutes chargées depuis Parquet

    Returns:
        pd.DataFrame: Données nettoyées

    Approche: Créer une nouvelle dataframe propre avec seulement les variables
    nettoyées. Les données raw (df) restent intactes.
    """
    df_clean = pd.DataFrame(index=df.index)

    # ========================================================================
    # DEMOGRAPHIC / STRATIFICATION (ses_*)
    # ========================================================================

    # --- Q0QC ---
    # ses_region_qc — Region administrative du Québec (17 régions)
    # Source: Q0QC
    df_clean['ses_region_qc'] = df['q0qc'].map({
        1.0: 'bas_saint_laurent',
        2.0: 'saguenay_lac_saint_jean',
        3.0: 'capitale_nationale',
        4.0: 'mauricie',
        5.0: 'estrie',
        6.0: 'montreal',
        7.0: 'outaouais',
        8.0: 'abitibi_temiscamingue',
        9.0: 'cote_nord',
        10.0: 'nord_du_quebec',
        11.0: 'gaspesie_iles_de_la_madeleine',
        12.0: 'chaudiere_appalaches',
        13.0: 'laval',
        14.0: 'lanaudiere',
        15.0: 'laurentides',
        16.0: 'monteregie',
        17.0: 'centre_du_quebec',
        96.0: np.nan,  # Other
        99.0: np.nan   # Refused
    })
    CODEBOOK_VARIABLES['ses_region_qc'] = {
        'original_variable': 'q0qc',
        'question_label': "Dans quelle région du Québec demeurez-vous ?",
        'type': 'categorical',
        'value_labels': {
            'bas_saint_laurent': "Bas-Saint-Laurent",
            'saguenay_lac_saint_jean': "Saguenay-Lac-Saint-Jean",
            'capitale_nationale': "Capitale-Nationale",
            'mauricie': "Mauricie",
            'estrie': "Estrie",
            'montreal': "Montréal",
            'outaouais': "Outaouais",
            'abitibi_temiscamingue': "Abitibi-Témiscamingue",
            'cote_nord': "Côte-Nord",
            'nord_du_quebec': "Nord-du-Québec",
            'gaspesie_iles_de_la_madeleine': "Gaspésie-Îles-de-la-Madeleine",
            'chaudiere_appalaches': "Chaudière-Appalaches",
            'laval': "Laval",
            'lanaudiere': "Lanaudière",
            'laurentides': "Laurentides",
            'monteregie': "Montérégie",
            'centre_du_quebec': "Centre-du-Québec"
        }
    }

    # --- REGIO ---
    # ses_region_rmr — Région métropolitaine de recensement
    # Source: REGIO
    df_clean['ses_region_rmr'] = df['regio'].map({
        1.0: 'mtl_rmr',
        2.0: 'qc_rmr',
        3.0: 'autres_regions',
    })
    CODEBOOK_VARIABLES['ses_region_rmr'] = {
        'original_variable': 'regio',
        'question_label': "Région métropolitaine de recensement",
        'type': 'categorical',
        'value_labels': {
            'mtl_rmr': "Montréal RMR",
            'qc_rmr': "Québec RMR",
            'autres_regions': "Autres régions"
        }
    }

    # --- QSEXE ---
    # ses_gender — Genre de la personne
    # Source: QSEXE
    df_clean['ses_gender'] = df['qsexe'].map({
        1.0: 'male',
        2.0: 'female'
    })
    CODEBOOK_VARIABLES['ses_gender'] = {
        'original_variable': 'qsexe',
        'question_label': "Quel est votre sexe? Note : comme indiqué par Statistique Canada, les Canadiens transgenres, transsexuels et intersexués doivent indiquer le sexe (masculin ou féminin) auquel ils s'identifient le plus.",
        'type': 'categorical',
        'value_labels': {
            'male': "Masculin",
            'female': "Féminin"
        }
    }

    # --- AGENUM ---
    # ses_age — Âge en années
    # Source: AGENUM
    df_clean['ses_age'] = pd.to_numeric(df['agenum'], errors='coerce')
    CODEBOOK_VARIABLES['ses_age'] = {
        'original_variable': 'agenum',
        'question_label': "Quel âge avez-vous ?",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- QPARENTS ---
    # ses_live_with_parents — Demeure chez ses parents
    # Source: QPARENTS
    df_clean['ses_live_with_parents'] = df['qparents'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_live_with_parents'] = {
        'original_variable': 'qparents',
        'question_label': "Demeurez-vous toujours chez vos parents?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- QLANGUE ---
    # ses_language — Langue maternelle
    # Source: QLANGUE
    df_clean['ses_language'] = df['qlangue'].map({
        1.0: 'french',
        2.0: 'english',
        96.0: 'other',
        98.0: np.nan,  # Ne sais pas
        99.0: np.nan   # Refused
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'qlangue',
        'question_label': "Quelle est la langue principale que vous avez apprise en premier lieu à la maison dans votre enfance et que vous comprenez toujours?",
        'type': 'categorical',
        'value_labels': {
            'french': "Français",
            'english': "Anglais",
            'other': "Autre"
        }
    }

    # --- QSCOL ---
    # ses_education — Niveau d'éducation
    # Source: QSCOL
    df_clean['ses_education'] = df['qscol'].map({
        1.0: 'no_schooling',
        2.0: 'primary_incomplete',
        3.0: 'primary_complete',
        4.0: 'sec1',
        5.0: 'sec2',
        6.0: 'sec3',
        7.0: 'sec4',
        8.0: 'des',
        9.0: 'dep',
        10.0: 'cegep_incomplete',
        11.0: 'dec',
        12.0: 'cegep_tech',
        13.0: 'univ_incomplete',
        14.0: 'bachelor',
        15.0: 'graduate',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_education'] = {
        'original_variable': 'qscol',
        'question_label': "À quel niveau se situe la dernière année de scolarité que vous avez complétée?",
        'type': 'categorical',
        'value_labels': {
            'no_schooling': "Aucune scolarité",
            'primary_incomplete': "Cours primaire (pas fini)",
            'primary_complete': "Cours primaire (complété)",
            'sec1': "Secondaire 1",
            'sec2': "Secondaire 2",
            'sec3': "Secondaire 3",
            'sec4': "Secondaire 4",
            'des': "Secondaire 5 (DES)",
            'dep': "Secondaire 5 (DEP)",
            'cegep_incomplete': "CÉGEP (pas fini)",
            'dec': "CÉGEP (DEC)",
            'cegep_tech': "CÉGEP (technique)",
            'univ_incomplete': "Université non complétée",
            'bachelor': "Baccalauréat",
            'graduate': "Maîtrise ou doctorat"
        }
    }

    # --- QOCCUP ---
    # ses_occupation — Occupation principale
    # Source: QOCCUP
    df_clean['ses_occupation'] = df['qoccup'].map({
        1.0: 'self_employed',
        2.0: 'on_leave',
        3.0: 'retired',
        4.0: 'unemployed',
        5.0: 'student',
        6.0: 'homemaker',
        7.0: 'disabled',
        8.0: 'multiple_jobs',
        9.0: 'student_worker',
        10.0: 'homemaker_worker',
        11.0: 'retired_worker',
        96.0: 'other',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_occupation'] = {
        'original_variable': 'qoccup',
        'question_label': "Quelle description ci-dessous correspond le mieux à votre occupation?",
        'type': 'categorical',
        'value_labels': {
            'self_employed': "Travaille à son compte (avec ou sans employés)",
            'on_leave': "En congé payé",
            'retired': "Retraité(e)",
            'unemployed': "Au chômage / cherche du travail",
            'student': "Étudiant(e)",
            'homemaker': "Homme/femme au foyer",
            'disabled': "Handicapé(e)",
            'multiple_jobs': "Occupe deux emplois ou plus",
            'student_worker': "Étudiant(e) et salarié(e)",
            'homemaker_worker': "Homme/femme au foyer et salarié(e)",
            'retired_worker': "Retraité(e) et salarié(e)",
            'other': "Autre"
        }
    }

    # --- QSTAT ---
    # ses_marital_status — Statut civil
    # Source: QSTAT
    df_clean['ses_marital_status'] = df['qstat'].map({
        1.0: 'married',
        2.0: 'married_separated',
        3.0: 'single',
        4.0: 'divorced',
        5.0: 'widowed',
        6.0: 'common_law'
    })
    CODEBOOK_VARIABLES['ses_marital_status'] = {
        'original_variable': 'qstat',
        'question_label': "Quel est votre statut civil officiel?",
        'type': 'categorical',
        'value_labels': {
            'married': "Marié(e) ou union civile",
            'married_separated': "Marié(e) mais séparé(e)",
            'single': "Célibataire",
            'divorced': "Divorcé(e)",
            'widowed': "Veuf/veuve",
            'common_law': "Conjoint(e) de fait"
        }
    }

    # --- QENFAN ---
    # ses_has_children — Enfants à la maison
    # Source: QENFAN
    df_clean['ses_has_children'] = df['qenfan'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_has_children'] = {
        'original_variable': 'qenfan',
        'question_label': "Y a-t-il des enfants de moins de 18 ans vivant dans votre famille?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- POND ---
    # ses_weight — Poids d'échantillonnage
    # Source: POND
    df_clean['ses_weight'] = df['pond']
    CODEBOOK_VARIABLES['ses_weight'] = {
        'original_variable': 'pond',
        'question_label': "Pondération de l'échantillon",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q69 ---
    # ses_birth_place — Lieu de naissance
    # Source: Q69
    df_clean['ses_birth_place'] = df['q69'].map({
        1.0: 'quebec',
        2.0: 'other_canada',
        3.0: 'outside_canada',
        98.0: np.nan,  # Ne sais pas
        99.0: np.nan   # Refused
    })
    CODEBOOK_VARIABLES['ses_birth_place'] = {
        'original_variable': 'q69',
        'question_label': "Où êtes-vous né(e)?",
        'type': 'categorical',
        'value_labels': {
            'quebec': "Québec",
            'other_canada': "Autre province canadienne",
            'outside_canada': "Hors Canada"
        }
    }

    # --- Q66 ---
    # ses_religious_practice — Appartenance religieuse
    # Source: Q66
    df_clean['ses_religious_practice'] = df['q66'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_religious_practice'] = {
        'original_variable': 'q66',
        'question_label': "Considérez-vous appartenir à une religion ou à une dénomination particulière?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q67 ---
    # ses_religious_affiliation — Appartenance religieuse détaillée
    # Source: Q67
    df_clean['ses_religious_affiliation'] = df['q67'].map({
        1.0: 'catholic',
        2.0: 'protestant',
        3.0: 'other_christian',
        4.0: 'jewish',
        5.0: 'muslim',
        96.0: 'other',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_religious_affiliation'] = {
        'original_variable': 'q67',
        'question_label': "À quelle religion appartenez-vous?",
        'type': 'categorical',
        'value_labels': {
            'catholic': "Christianisme (Catholicisme)",
            'protestant': "Christianisme (Protestant)",
            'other_christian': "Christianisme (autre)",
            'jewish': "Judaïsme",
            'muslim': "Islam",
            'other': "Autre"
        }
    }

    # --- Q61 ---
    # ses_income — Revenu ménage
    # Source: Q61
    df_clean['ses_income'] = df['q61'].map({
        1.0: 'under_8000',
        2.0: '8000_15999',
        3.0: '16000_23999',
        4.0: '24000_39999',
        5.0: '40000_55999',
        6.0: '56000_71999',
        7.0: '72000_87999',
        8.0: '88000_103999',
        9.0: '104000_plus',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_income'] = {
        'original_variable': 'q61',
        'question_label': "Parmi les catégories suivantes, laquelle reflète le mieux le revenu total avant impôt de tous les membres de votre foyer pour l'année 2017?",
        'type': 'categorical',
        'value_labels': {
            'under_8000': "Moins de 8 000$",
            '8000_15999': "8 000$ - 15 999$",
            '16000_23999': "16 000$ - 23 999$",
            '24000_39999': "24 000$ - 39 999$",
            '40000_55999': "40 000$ - 55 999$",
            '56000_71999': "56 000$ - 71 999$",
            '72000_87999': "72 000$ - 87 999$",
            '88000_103999': "88 000$ - 103 999$",
            '104000_plus': "104 000$ ou plus"
        }
    }

    # --- Q62A ---
    # ses_mother_education — Éducation de la mère
    # Source: Q62A
    df_clean['ses_mother_education'] = df['q62a'].map({
        1.0: 'no_schooling',
        2.0: 'primary_incomplete',
        3.0: 'primary_complete',
        4.0: 'secondary_incomplete',
        5.0: 'secondary_complete',
        6.0: 'cegep_incomplete',
        7.0: 'cegep_complete',
        8.0: 'technical',
        9.0: 'univ_incomplete',
        10.0: 'bachelor',
        11.0: 'graduate',
        98.0: np.nan,  # Ne sais pas
        99.0: np.nan   # Refused
    })
    CODEBOOK_VARIABLES['ses_mother_education'] = {
        'original_variable': 'q62a',
        'question_label': "À quel niveau se situe la dernière année de scolarité que votre mère a complétée?",
        'type': 'categorical',
        'value_labels': {
            'no_schooling': "Aucune scolarité",
            'primary_incomplete': "Cours primaire (sans diplôme)",
            'primary_complete': "Cours primaire (avec diplôme)",
            'secondary_incomplete': "Secondaire (sans diplôme)",
            'secondary_complete': "Secondaire (avec diplôme)",
            'cegep_incomplete': "CÉGEP (sans diplôme)",
            'cegep_complete': "CÉGEP (avec diplôme)",
            'technical': "Cours technique",
            'univ_incomplete': "Université non complétée",
            'bachelor': "Baccalauréat",
            'graduate': "Maîtrise ou doctorat"
        }
    }

    # --- Q62B ---
    # ses_father_education — Éducation du père
    # Source: Q62B
    df_clean['ses_father_education'] = df['q62b'].map({
        1.0: 'no_schooling',
        2.0: 'primary_incomplete',
        3.0: 'primary_complete',
        4.0: 'secondary_incomplete',
        5.0: 'secondary_complete',
        6.0: 'cegep_incomplete',
        7.0: 'cegep_complete',
        8.0: 'technical',
        9.0: 'univ_incomplete',
        10.0: 'bachelor',
        11.0: 'graduate',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_father_education'] = {
        'original_variable': 'q62b',
        'question_label': "À quel niveau se situe la dernière année de scolarité que votre père a complétée?",
        'type': 'categorical',
        'value_labels': {
            'no_schooling': "Aucune scolarité",
            'primary_incomplete': "Cours primaire (sans diplôme)",
            'primary_complete': "Cours primaire (avec diplôme)",
            'secondary_incomplete': "Secondaire (sans diplôme)",
            'secondary_complete': "Secondaire (avec diplôme)",
            'cegep_incomplete': "CÉGEP (sans diplôme)",
            'cegep_complete': "CÉGEP (avec diplôme)",
            'technical': "Cours technique",
            'univ_incomplete': "Université non complétée",
            'bachelor': "Baccalauréat",
            'graduate': "Maîtrise ou doctorat"
        }
    }

    # --- Q70 ---
    # ses_home_language — Langue parlée à la maison
    # Source: Q70
    df_clean['ses_home_language'] = df['q70'].map({
        1.0: 'english',
        2.0: 'french',
        3.0: 'chinese',
        4.0: 'italian',
        5.0: 'portuguese',
        6.0: 'spanish',
        7.0: 'german',
        8.0: 'polish',
        9.0: 'punjabi',
        10.0: 'greek',
        11.0: 'vietnamese',
        12.0: 'arabic',
        13.0: 'inuktitut',
        14.0: 'cree',
        15.0: 'tagalog',
        16.0: 'ukrainian_russian',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['ses_home_language'] = {
        'original_variable': 'q70',
        'question_label': "Quelle langue parlez-vous le plus souvent à la maison?",
        'type': 'categorical',
        'value_labels': {
            'english': "Anglais",
            'french': "Français",
            'chinese': "Chinois",
            'italian': "Italien",
            'portuguese': "Portugais",
            'spanish': "Espagnol",
            'german': "Allemand",
            'polish': "Polonais",
            'punjabi': "Punjabi",
            'greek': "Grec",
            'vietnamese': "Vietnamien",
            'arabic': "Arabe",
            'inuktitut': "Inuktitut",
            'cree': "Cri",
            'tagalog': "Tagal (Philippin)",
            'ukrainian_russian': "Ukrainien / Russe",
            'other': "Autre"
        }
    }

    # ========================================================================
    # POLITICAL OPINIONS / ATTITUDES (op_*)
    # ========================================================================

    # --- Q1 ---
    # op_dem_satisfaction — Satisfaction envers la démocratie
    # Source: Q1
    df_clean['op_dem_satisfaction'] = df['q1'].map({
        1.0: 'very_satisfied',
        2.0: 'somewhat_satisfied',
        3.0: 'not_very_satisfied',
        4.0: 'not_at_all_satisfied',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_dem_satisfaction'] = {
        'original_variable': 'q1',
        'question_label': "Dans l'ensemble, à quel point êtes-vous satisfait(e) de la façon dont la démocratie fonctionne au Québec?",
        'type': 'ordinal',
        'value_labels': {
            'very_satisfied': "Très satisfait(e)",
            'somewhat_satisfied': "Assez satisfait(e)",
            'not_very_satisfied': "Peu satisfait(e)",
            'not_at_all_satisfied': "Pas du tout satisfait(e)"
        }
    }

    # --- Q2 ---
    # op_most_important_issue — Enjeu le plus important
    # Source: Q2
    df_clean['op_most_important_issue'] = df['q2'].map({
        1.0: 'economy',
        2.0: 'health',
        3.0: 'environment',
        4.0: 'education',
        5.0: 'family_aid',
        6.0: 'poverty',
        7.0: 'corruption',
        8.0: 'taxes',
        9.0: 'sovereignty',
        10.0: 'immigration',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_most_important_issue'] = {
        'original_variable': 'q2',
        'question_label': "Parmi les enjeux suivants, lequel était, pour vous personnellement, le plus important lors de l'élection provinciale du 1er octobre dernier?",
        'type': 'categorical',
        'value_labels': {
            'economy': "L'économie",
            'health': "La santé",
            'environment': "L'environnement",
            'education': "L'éducation",
            'family_aid': "L'aide aux familles",
            'poverty': "La pauvreté",
            'corruption': "L'intégrité des politiciens et la corruption",
            'taxes': "Les taxes et les finances publiques",
            'sovereignty': "La souveraineté du Québec",
            'immigration': "L'immigration",
            'other': "Autre"
        }
    }

    # --- Q3 ---
    # op_vote_if_16_17 — Vote si 16-17 ans
    # Source: Q3
    df_clean['op_vote_if_16_17'] = df['q3'].map({
        1.0: 'certainly_would_vote',
        2.0: 'probably_would_vote',
        3.0: 'probably_would_not_vote',
        4.0: 'certainly_would_not_vote',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_vote_if_16_17'] = {
        'original_variable': 'q3',
        'question_label': "Si, lors de cette election, les citoyens ages de 16 et 17 ans avaient eu le droit de vote, est-ce que vous seriez alle voter?",
        'type': 'ordinal',
        'value_labels': {
            'certainly_would_vote': "J'aurais certainement vote",
            'probably_would_vote': "J'aurais probablement vote",
            'probably_would_not_vote': "Je n'aurais probablement pas vote",
            'certainly_would_not_vote': "Je n'aurais certainement pas vote"
        }
    }

    # --- Q4 ---
    # op_party_if_16_17 — Choix de parti si 16-17 ans
    # Source: Q4
    df_clean['op_party_if_16_17'] = df['q4'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'would_not_vote',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_party_if_16_17'] = {
        'original_variable': 'q4',
        'question_label': "Si, lors de cette election, les citoyens ages de 16 et 17 ans avaient eu le droit de vote, pour quel parti auriez-vous vote?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti liberal du Quebec",
            'pq': "Parti quebecois",
            'caq': "Coalition avenir Quebec",
            'qs': "Quebec solidaire",
            'other_party': "Un autre parti",
            'would_not_vote': "Je n'aurais pas vote"
        }
    }

    # --- Q5 ---
    # behav_voted_2018 — Participation électorale 2018
    # Source: Q5
    df_clean['behav_voted_2018'] = df['q5'].map({
        1.0: 'did_not_vote',
        2.0: 'wanted_but_didnt',
        3.0: 'usually_votes_but_not_this_time',
        4.0: 'certain_voted',
        5.0: 'not_eligible',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_voted_2018'] = {
        'original_variable': 'q5',
        'question_label': "Laquelle des situations suivantes correspond le mieux à votre cas lors de l'élection du 1er octobre?",
        'type': 'categorical',
        'value_labels': {
            'did_not_vote': "N'a pas voté",
            'wanted_but_didnt': "A voulu voter mais n'a pas pu",
            'usually_votes_but_not_this_time': "Vote habituellement mais pas cette fois",
            'certain_voted': "Certain d'avoir voté",
            'not_eligible': "Non éligible"
        }
    }

    # --- Q6 ---
    # behav_vote_choice_2018 — Choix de vote 2018
    # Source: Q6
    df_clean['behav_vote_choice_2018'] = df['q6'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        95.0: 'spoiled_ballot',
        96.0: 'other_party',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_choice_2018'] = {
        'original_variable': 'q6',
        'question_label': "Pour quel parti avez-vous voté?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'spoiled_ballot': "A annulé son vote",
            'other_party': "Autre parti"
        }
    }

    # --- Q6A ---
    # behav_vote_timing — Moment du choix de vote
    # Source: Q6A
    df_clean['behav_vote_timing'] = df['q6a'].map({
        1.0: 'months_before',
        2.0: 'weeks_before',
        3.0: 'days_before',
        4.0: 'election_day',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_timing'] = {
        'original_variable': 'q6a',
        'question_label': "Quand avez-vous arrêté votre choix de vote?",
        'type': 'categorical',
        'value_labels': {
            'months_before': "Plusieurs mois avant la campagne",
            'weeks_before': "Quelques semaines avant",
            'days_before': "Quelques jours avant",
            'election_day': "Le jour même"
        }
    }

    # --- Q5A ---
    # behav_vote_hypothetical_2018 — Choix de vote hypothétique 2018 (si n'a pas voted)
    # Source: Q5A
    df_clean['behav_vote_hypothetical_2018'] = df['q5a'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        95.0: 'spoiled_ballot',
        96.0: 'other_party',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_hypothetical_2018'] = {
        'original_variable': 'q5a',
        'question_label': "Si vous aviez été voter le jour de cette election, pour quel parti auriez-vous vote?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti liberal du Quebec",
            'pq': "Parti quebecois",
            'caq': "Coalition avenir Quebec",
            'qs': "Quebec solidaire",
            'spoiled_ballot': "J'aurais annule mon vote",
            'other_party': "Un autre parti"
        }
    }

    # --- Q7 ---
    # behav_first_choice — Le parti vote etait-il le premier choix?
    # Source: Q7
    df_clean['behav_first_choice'] = df['q7'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_first_choice'] = {
        'original_variable': 'q7',
        'question_label': "Est-ce que ce parti etait votre premier choix?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q8 ---
    # behav_party_preference — Parti préféré (même si pas voté)
    # Source: Q8
    df_clean['behav_party_preference'] = df['q8'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_preference'] = {
        'original_variable': 'q8',
        'question_label': "Quel parti était votre premier choix?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire"
        }
    }

    # --- Q9 ---
    # behav_vote_2014 — Choix de vote 2014
    # Source: Q9
    df_clean['behav_vote_2014'] = df['q9'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        95.0: 'spoiled_ballot',
        96.0: 'other',
        97.0: 'did_not_vote',
        98.0: 'dont_remember',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_2014'] = {
        'original_variable': 'q9',
        'question_label': "Pour quel parti aviez-vous voté lors de l'élection provinciale du 7 avril 2014?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'spoiled_ballot': "A annulé son vote",
            'other': "Autre parti",
             'did_not_vote': "N'a pas voté",
             'dont_remember': "Ne se rappelle plus"
         }
    }

    # --- Q47 ---
    # op_gov_employment_role — Rôle du gouvernement dans l'emploi et la qualité de vie
    # Source: Q47
    # 5-point scale: 1 = government should ensure employment/quality of life, 5 = people should fend for themselves
    df_clean['op_gov_employment_role'] = df['q47'].map({
        1.0: 'gov_should_ensure',
        2.0: 'gov_lean_ensure',
        3.0: 'neutral',
        4.0: 'people_lean_self_reliant',
        5.0: 'people_should_fend_for_themselves',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_gov_employment_role'] = {
        'original_variable': 'q47',
        'question_label': "Certaines personnes disent que les gouvernements devraient s'assurer que chaque personne ait un emploi et une bonne qualité de vie. D'autres personnes disent que les gouvernements devraient plutôt laisser chaque personne se débrouiller par elle-même. Où vous situez-vous sur l'échelle ci-dessous?",
        'type': 'ordinal',
        'value_labels': {
             'gov_should_ensure': "Les gouvernements devraient s'assurer que chaque personne a un emploi et une bonne qualité de vie",
             'gov_lean_ensure': "Penche vers le gouvernement devrait assurer",
             'neutral': "Position neutre",
             'people_lean_self_reliant': "Penche vers les gens devraient se débrouiller",
             'people_should_fend_for_themselves': "Les gens devraient se débrouiller par eux-mêmes"
        }
    }

     # --- Q50x1 ---
     # behav_vote_at_16_control — Vote à 16 ans (control)
     # Source: Q50x1
    df_clean['behav_vote_at_16_control'] = df['q50x1'].map({
        1.0: 'strongly_agree',
        2.0: 'somewhat_agree',
        3.0: 'somewhat_disagree',
        4.0: 'strongly_disagree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_at_16_control'] = {
        'original_variable': 'q50x1',
        'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (contrôle)",
        'type': 'ordinal',
        'value_labels': {
            'strongly_agree': "Fortement d'accord",
            'somewhat_agree': "Plutôt d'accord",
            'somewhat_disagree': "Plutôt en désaccord",
            'strongly_disagree': "Fortement en désaccord"
        }
    }

    # --- Q50x2 ---
    # behav_vote_at_16_argument_participation — Vote à 16 ans (argument participation)
    # Source: Q50x2
    df_clean['behav_vote_at_16_argument_participation'] = df['q50x2'].map({
        1.0: 'strongly_agree',
        2.0: 'somewhat_agree',
        3.0: 'somewhat_disagree',
        4.0: 'strongly_disagree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_at_16_argument_participation'] = {
        'original_variable': 'q50x2',
        'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (argument: participation)",
        'type': 'ordinal',
        'value_labels': {
            'strongly_agree': "Fortement d'accord",
            'somewhat_agree': "Plutôt d'accord",
            'somewhat_disagree': "Plutôt en désaccord",
            'strongly_disagree': "Fortement en désaccord"
        }
    }

    # --- Q50x3 ---
    # behav_vote_at_16_argument_rights — Vote à 16 ans (argument rights)
    # Source: Q50x3
    df_clean['behav_vote_at_16_argument_rights'] = df['q50x3'].map({
        1.0: 'strongly_agree',
        2.0: 'somewhat_agree',
        3.0: 'somewhat_disagree',
        4.0: 'strongly_disagree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_vote_at_16_argument_rights'] = {
        'original_variable': 'q50x3',
        'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (argument: droits)",
        'type': 'ordinal',
        'value_labels': {
            'strongly_agree': "Fortement d'accord",
            'somewhat_agree': "Plutôt d'accord",
            'somewhat_disagree': "Plutôt en désaccord",
            'strongly_disagree': "Fortement en désaccord"
        }
    }

    # --- Q13 ---
    # op_knowledge_mnas — Connaissance du nombre de députés
    # Source: Q13
    df_clean['op_knowledge_mnas'] = pd.to_numeric(df['q13'], errors='coerce')
    CODEBOOK_VARIABLES['op_knowledge_mnas'] = {
        'original_variable': 'q13',
        'question_label': "Combien y a-t-il de député(e)s à l'Assemblée nationale du Québec?",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q14_1 ---
    # op_campaign_slogan_now — Association slogan de campagne: Maintenant
    # Source: Q14_1
    df_clean['op_campaign_slogan_now'] = df['q14_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_campaign_slogan_now'] = {
        'original_variable': 'q14_1',
        'question_label': "Pouvez-vous associer le slogan de campagne suivant à son parti: Maintenant?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q14_2 ---
    # op_campaign_slogan_seriously — Association slogan de campagne: Sérieusement
    # Source: Q14_2
    df_clean['op_campaign_slogan_seriously'] = df['q14_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_campaign_slogan_seriously'] = {
        'original_variable': 'q14_2',
        'question_label': "Pouvez-vous associer le slogan de campagne suivant à son parti: Sérieusement?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q14_3 ---
    # op_campaign_slogan_popular — Association slogan de campagne: Populaire
    # Source: Q14_3
    df_clean['op_campaign_slogan_popular'] = df['q14_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_campaign_slogan_popular'] = {
        'original_variable': 'q14_3',
        'question_label': "Pouvez-vous associer le slogan de campagne suivant à son parti: Populaire?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q14_4 ---
    # op_campaign_slogan_ease_life — Association slogan de campagne: Pour faciliter la vie des Québécois
    # Source: Q14_4
    df_clean['op_campaign_slogan_ease_life'] = df['q14_4'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_campaign_slogan_ease_life'] = {
        'original_variable': 'q14_4',
        'question_label': "Pouvez-vous associer le slogan de campagne suivant à son parti: Pour faciliter la vie des Québécois?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q14_5 ---
    # op_campaign_slogan_do_differently — Association slogan de campagne: Faire autrement maintenant
    # Source: Q14_5
    df_clean['op_campaign_slogan_do_differently'] = df['q14_5'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_campaign_slogan_do_differently'] = {
        'original_variable': 'q14_5',
        'question_label': "Pouvez-vous associer le slogan de campagne suivant à son parti: Faire autrement maintenant?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q15_1 ---
    # op_campaign_promise_dental_care — Promesse de campagne: Offrir une assurance dentaire pour tous
    # Source: Q15_1
    df_clean['op_campaign_promise_dental_care'] = df['q15_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,  # Je ne sais pas
        99.0: np.nan   # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_campaign_promise_dental_care'] = {
        'original_variable': 'q15_1',
        'question_label': "Pouvez-vous associer la promesse de campagne suivante à son parti: Offrir une assurance dentaire pour tous?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q15_2 ---
    # op_campaign_promise_immigrant_threshold — Promesse de campagne: Abaisser le seuil d’immigrants reçus à 40 000 par année
    # Source: Q15_2
    df_clean['op_campaign_promise_immigrant_threshold'] = df['q15_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,  # Je ne sais pas
        99.0: np.nan   # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_campaign_promise_immigrant_threshold'] = {
        'original_variable': 'q15_2',
        'question_label': "Pouvez-vous associer la promesse de campagne suivante à son parti: Abaisser le seuil d’immigrants reçus à 40 000 par année?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q15_3 ---
    # op_campaign_promise_free_public_transport — Promesse de campagne: Offrir la gratuité du transport collectif pour les étudiants et les aînés
    # Source: Q15_3
    df_clean['op_campaign_promise_free_public_transport'] = df['q15_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,  # Je ne sais pas
        99.0: np.nan   # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_campaign_promise_free_public_transport'] = {
        'original_variable': 'q15_3',
        'question_label': "Pouvez-vous associer la promesse de campagne suivante à son parti: Offrir la gratuité du transport collectif pour les étudiants et les aînés?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q15_4 ---
    # op_campaign_promise_subsidized_lunches — Promesse de campagne: Instaurer un service de lunchs subventionnés pour tous les écoliers
    # Source: Q15_4
    df_clean['op_campaign_promise_subsidized_lunches'] = df['q15_4'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        98.0: np.nan,  # Je ne sais pas
        99.0: np.nan   # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_campaign_promise_subsidized_lunches'] = {
        'original_variable': 'q15_4',
        'question_label': "Pouvez-vous associer la promesse de campagne suivante à son parti: Instaurer un service de lunchs subventionnés pour tous les écoliers?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti"
        }
    }

    # --- Q16_1 ---
    # op_issue_party_interests_quebec — Enjeu: les intérêts du Québec associé à quel parti
    # Source: Q16_1
    df_clean['op_issue_party_interests_quebec'] = df['q16_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_interests_quebec'] = {
        'original_variable': 'q16_1',
        'question_label': "Lorsque vous pensez aux intérêts du Québec, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_2 ---
    # op_issue_party_cultural_identity — Enjeu: l'identité et la culture québécoise associé à quel parti
    # Source: Q16_2
    df_clean['op_issue_party_cultural_identity'] = df['q16_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_cultural_identity'] = {
        'original_variable': 'q16_2',
        'question_label': "Lorsque vous pensez à l'identité et la culture québécoise, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_3 ---
    # op_issue_party_economy — Enjeu: l'économie associé à quel parti
    # Source: Q16_3
    df_clean['op_issue_party_economy'] = df['q16_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_economy'] = {
        'original_variable': 'q16_3',
        'question_label': "Lorsque vous pensez à l'économie, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_4 ---
    # op_issue_party_education — Enjeu: l'éducation associé à quel parti
    # Source: Q16_4
    df_clean['op_issue_party_education'] = df['q16_4'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_education'] = {
        'original_variable': 'q16_4',
        'question_label': "Lorsque vous pensez à l'éducation, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_5 ---
    # op_issue_party_environment — Enjeu: l'environnement associé à quel parti
    # Source: Q16_5
    df_clean['op_issue_party_environment'] = df['q16_5'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_environment'] = {
        'original_variable': 'q16_5',
        'question_label': "Lorsque vous pensez à l'environnement, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_6 ---
    # op_issue_party_health — Enjeu: la santé associé à quel parti
    # Source: Q16_6
    df_clean['op_issue_party_health'] = df['q16_6'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_health'] = {
        'original_variable': 'q16_6',
        'question_label': "Lorsque vous pensez à la santé, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_7 ---
    # op_issue_party_poverty — Enjeu: la pauvreté associé à quel parti
    # Source: Q16_7
    df_clean['op_issue_party_poverty'] = df['q16_7'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_poverty'] = {
        'original_variable': 'q16_7',
        'question_label': "Lorsque vous pensez à la pauvreté, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q16_8 ---
    # op_issue_party_immigration — Enjeu: l'immigration associé à quel parti
    # Source: Q16_8
    df_clean['op_issue_party_immigration'] = df['q16_8'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_issue_party_immigration'] = {
        'original_variable': 'q16_8',
        'question_label': "Lorsque vous pensez à l'immigration, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q17_1 ---
    # op_age_group_party_18_34 — Groupe d'âge 18-34 ans associé à quel parti
    # Source: Q17_1
    df_clean['op_age_group_party_18_34'] = df['q17_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_age_group_party_18_34'] = {
        'original_variable': 'q17_1',
        'question_label': "Lorsque vous pensez aux gens de 18 à 34 ans, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q17_2 ---
    # op_age_group_party_35_54 — Groupe d'âge 35-54 ans associé à quel parti
    # Source: Q17_2
    df_clean['op_age_group_party_35_54'] = df['q17_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_age_group_party_35_54'] = {
        'original_variable': 'q17_2',
        'question_label': "Lorsque vous pensez aux gens de 35 à 54 ans, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q17_3 ---
    # op_age_group_party_55_plus — Groupe d'âge 55+ ans associé à quel parti
    # Source: Q17_3
    df_clean['op_age_group_party_55_plus'] = df['q17_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_age_group_party_55_plus'] = {
        'original_variable': 'q17_3',
        'question_label': "Lorsque vous pensez aux gens de 55 ans et plus, à quel parti politique pensez-vous spontanément?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q18 ---
    # op_attachment_quebec — Degré d'attachement au Québec
    # Source: Q18
    df_clean['op_attachment_quebec'] = df['q18'].map({
        1.0: 'very_attached',
        2.0: 'somewhat_attached',
        3.0: 'not_very_attached',
        4.0: 'not_at_all_attached',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_attachment_quebec'] = {
        'original_variable': 'q18',
        'question_label': "Quel est votre degré d'attachement au Québec?",
        'type': 'ordinal',
        'value_labels': {
            'very_attached': "Très attaché(e)",
            'somewhat_attached': "Assez attaché(e)",
            'not_very_attached': "Peu attaché(e)",
            'not_at_all_attached': "Pas du tout attaché(e)"
        }
    }

    # --- Q19 ---
    # op_attachment_canada — Degré d'attachement au Canada
    # Source: Q19
    df_clean['op_attachment_canada'] = df['q19'].map({
        1.0: 'very_attached',
        2.0: 'somewhat_attached',
        3.0: 'not_very_attached',
        4.0: 'not_at_all_attached',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_attachment_canada'] = {
        'original_variable': 'q19',
        'question_label': "Et quel est votre degré d'attachement au Canada?",
        'type': 'ordinal',
        'value_labels': {
            'very_attached': "Très attaché(e)",
            'somewhat_attached': "Assez attaché(e)",
            'not_very_attached': "Peu attaché(e)",
            'not_at_all_attached': "Pas du tout attaché(e)"
        }
    }

    # --- Q20A ---
    # op_identity_quebec_canada_a — Identité québécoise/canadienne (version A)
    # Source: Q20A (split sample 50%-50%)
    df_clean['op_identity_quebec_canada_a'] = df['q20a'].map({
        1.0: 'only_quebec_not_canada',
        2.0: 'first_quebec_then_canada',
        3.0: 'equally_quebec_and_canada',
        4.0: 'first_canada_then_quebec',
        5.0: 'only_canada_not_quebec',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_identity_quebec_canada_a'] = {
        'original_variable': 'q20a',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...? (version A)",
        'type': 'ordinal',
        'value_labels': {
            'only_quebec_not_canada': "Uniquement comme Québécois(e), pas du tout comme Canadien(ne)",
            'first_quebec_then_canada': "D'abord comme Québécois(e), puis comme Canadien(ne)",
            'equally_quebec_and_canada': "Également comme Québécois(e) et comme Canadien(ne)",
            'first_canada_then_quebec': "D'abord comme Canadien(ne), puis comme Québécois(e)",
            'only_canada_not_quebec': "Uniquement comme Canadien(ne), pas du tout comme Québécois(e)"
        }
    }

    # --- Q20B ---
    # op_identity_quebec_canada_b — Identité québécoise/canadienne (version B)
    # Source: Q20B (split sample 50%-50%)
    df_clean['op_identity_quebec_canada_b'] = df['q20b'].map({
        1.0: 'only_canada_not_quebec',
        2.0: 'first_canada_then_quebec',
        3.0: 'equally_canada_and_quebec',
        4.0: 'first_quebec_then_canada',
        5.0: 'only_quebec_not_canada',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_identity_quebec_canada_b'] = {
        'original_variable': 'q20b',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...? (version B)",
        'type': 'ordinal',
        'value_labels': {
            'only_canada_not_quebec': "Uniquement comme Canadien(ne), pas du tout comme Québécois(e)",
            'first_canada_then_quebec': "D'abord comme Canadien(ne), puis comme Québécois(e)",
            'equally_canada_and_quebec': "É également comme Canadien(ne) et comme Québécois(e)",
            'first_quebec_then_canada': "D'abord comme Québécois(e), puis comme Canadien(ne)",
            'only_quebec_not_canada': "Uniquement comme Québécois(e), pas du tout comme Canadien(ne)"
        }
    }

    # --- Q21_1 ---
    # op_trust_media — Confiance envers les médias
    # Source: Q21_1
    df_clean['op_trust_media'] = df['q21_1'].map({
        1.0: 'a_lot',
        2.0: 'somewhat',
        3.0: 'not_much',
        4.0: 'not_at_all',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_trust_media'] = {
        'original_variable': 'q21_1',
        'question_label': "Dans quelle mesure faites-vous confiance aux médias?",
        'type': 'ordinal',
        'value_labels': {
            'a_lot': "Beaucoup",
            'somewhat': "Assez",
            'not_much': "Pas beaucoup",
            'not_at_all': "Pas du tout"
        }
    }

    # --- Q36_1 ---
    # op_political_ideology — Auto-placement idéologie gauche-droite (0-10)
    # Source: Q36_1
    df_clean['op_political_ideology'] = pd.to_numeric(df['q36_1'], errors='coerce')
    CODEBOOK_VARIABLES['op_political_ideology'] = {
        'original_variable': 'q36_1',
        'question_label': "Sur une échelle de 0 à 10, où 0 signifie « gauche » et 10 signifie « droite », comment vous positionneriez-vous?",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q39A ---
    # op_social_opinion_immigrants — Les immigrants apportent une importante contribution au Québec.
    # Source: Q39A
    df_clean['op_social_opinion_immigrants'] = df['q39a'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_social_opinion_immigrants'] = {
        'original_variable': 'q39a',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: Les immigrants apportent une importante contribution au Québec.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # ========================================================================
    # Q31_1-6: Election-related activities in past weeks
    # ========================================================================

    # --- Q31_1 ---
    # op_election_activity_info_session — Attended information session on elections
    # Source: Q31_1
    df_clean['op_election_activity_info_session'] = df['q31_1'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_info_session'] = {
        'original_variable': 'q31_1',
        'question_label': "Avez-vous assisté à une séance d'information sur les elections au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q31_2 ---
    # op_election_activity_search_info — Searched for election information
    # Source: Q31_2
    df_clean['op_election_activity_search_info'] = df['q31_2'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_search_info'] = {
        'original_variable': 'q31_2',
        'question_label': "Avez-vous recherché de l'information sur les elections au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q31_3 ---
    # op_election_activity_talk_candidate — Talked with a political candidate
    # Source: Q31_3
    df_clean['op_election_activity_talk_candidate'] = df['q31_3'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_talk_candidate'] = {
        'original_variable': 'q31_3',
        'question_label': "Avez-vous parlé avec un candidat politique au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q31_4 ---
    # op_election_activity_watch_debate — Watched leaders debate
    # Source: Q31_4
    df_clean['op_election_activity_watch_debate'] = df['q31_4'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_watch_debate'] = {
        'original_variable': 'q31_4',
        'question_label': "Avez-vous écouté un des débats des chefs au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q31_5 ---
    # op_election_activity_school_debate — Attended political debate at school
    # Source: Q31_5
    df_clean['op_election_activity_school_debate'] = df['q31_5'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_school_debate'] = {
        'original_variable': 'q31_5',
        'question_label': "Avez-vous assisté à un débat politique à votre école, cégep ou université au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # --- Q31_6 ---
    # op_election_activity_school_vote — Participated in voting activity at school
    # Source: Q31_6
    df_clean['op_election_activity_school_vote'] = df['q31_6'].map({
        1.0: 'yes',
        2.0: 'no',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_election_activity_school_vote'] = {
        'original_variable': 'q31_6',
        'question_label': "Avez-vous pris part à une activité de vote à votre école (ex: Électeurs en Herbe) au cours des dernières semaines?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # ========================================================================
    # Q32_1-4: Likelihood of political participation
    # ========================================================================

    # --- Q32_1 ---
    # op_likelihood_vote_elections — Likelihood to vote in elections
    # Source: Q32_1
    df_clean['op_likelihood_vote_elections'] = pd.to_numeric(df['q32_1'], errors='coerce')
    CODEBOOK_VARIABLES['op_likelihood_vote_elections'] = {
        'original_variable': 'q32_1',
        'question_label': "Sur une échelle de 0 à 5 (où 0 = pas du tout probable, 5 = fort probable), seriez-vous prêt à voter à des elections si on vous en donnait la chance?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q32_2 ---
    # op_likelihood_vote_referendum — Likelihood to vote in referendums
    # Source: Q32_2
    df_clean['op_likelihood_vote_referendum'] = pd.to_numeric(df['q32_2'], errors='coerce')
    CODEBOOK_VARIABLES['op_likelihood_vote_referendum'] = {
        'original_variable': 'q32_2',
        'question_label': "Sur une échelle de 0 à 5 (où 0 = pas du tout probable, 5 = fort probable), seriez-vous prêt à voter à des référendums sur des questions de politiques publiques si on vous en donnait la chance?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q32_3 ---
    # op_likelihood_protest — Likelihood to participate in demonstrations
    # Source: Q32_3
    df_clean['op_likelihood_protest'] = pd.to_numeric(df['q32_3'], errors='coerce')
    CODEBOOK_VARIABLES['op_likelihood_protest'] = {
        'original_variable': 'q32_3',
        'question_label': "Sur une échelle de 0 à 5 (où 0 = pas du tout probable, 5 = fort probable), seriez-vous prêt à participer à des manifestations si on vous en donnait la chance?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q32_4 ---
    # op_likelihood_public_consultation — Likelihood to participate in public consultations
    # Source: Q32_4
    df_clean['op_likelihood_public_consultation'] = pd.to_numeric(df['q32_4'], errors='coerce')
    CODEBOOK_VARIABLES['op_likelihood_public_consultation'] = {
        'original_variable': 'q32_4',
        'question_label': "Sur une échelle de 0 à 5 (où 0 = pas du tout probable, 5 = fort probable), seriez-vous prêt à participer à un budget participatif ou d'autres formes de consultations publiques si on vous en donnait la chance?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # ========================================================================
    # Q33: Politician feeling thermometer (0-10)
    # ========================================================================

    # --- Q33_a ---
    # op_politician_thermometer_philippe_couillard — Feeling thermometer: Philippe Couillard
    # Source: Q33_a
    df_clean['op_politician_thermometer_philippe_couillard'] = pd.to_numeric(df['q33a'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_philippe_couillard'] = {
        'original_variable': 'q33a',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Philippe Couillard?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33_b ---
    # op_politician_thermometer_jean_francois_lisee — Feeling thermometer: Jean-François Lisée
    # Source: Q33_b
    df_clean['op_politician_thermometer_jean_francois_lisee'] = pd.to_numeric(df['q33b'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_jean_francois_lisee'] = {
        'original_variable': 'q33b',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Jean-François Lisée?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33_c ---
    # op_politician_thermometer_francois_legault — Feeling thermometer: François Legault
    # Source: Q33_c
    df_clean['op_politician_thermometer_francois_legault'] = pd.to_numeric(df['q33c'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_francois_legault'] = {
        'original_variable': 'q33c',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de François Legault?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33_d ---
    # op_politician_thermometer_manon_masse — Feeling thermometer: Manon Massé
    # Source: Q33_d
    df_clean['op_politician_thermometer_manon_masse'] = pd.to_numeric(df['q33d'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_manon_masse'] = {
        'original_variable': 'q33d',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Manon Massé?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33_e ---
    # op_politician_thermometer_veronique_hivon — Feeling thermometer: Véronique Hivon
    # Source: Q33_e
    df_clean['op_politician_thermometer_veronique_hivon'] = pd.to_numeric(df['q33e'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_veronique_hivon'] = {
        'original_variable': 'q33e',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Véronique Hivon?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33_f ---
    # op_politician_thermometer_gabriel_nadeau_dubois — Feeling thermometer: Gabriel Nadeau-Dubois
    # Source: Q33_f
    df_clean['op_politician_thermometer_gabriel_nadeau_dubois'] = pd.to_numeric(df['q33f'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_thermometer_gabriel_nadeau_dubois'] = {
        'original_variable': 'q33f',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Gabriel Nadeau-Dubois?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # ========================================================================
    # Q34: Group feeling thermometer (0-10)
    # ========================================================================

    # --- Q34_a ---
    # op_group_thermometer_ethnocultural_minorities — Feeling thermometer: ethnocultural minorities
    # Source: Q34_a
    df_clean['op_group_thermometer_ethnocultural_minorities'] = pd.to_numeric(df['q34a'], errors='coerce')
    CODEBOOK_VARIABLES['op_group_thermometer_ethnocultural_minorities'] = {
        'original_variable': 'q34a',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un groupe de gens, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous des minorités ethnoculturelles?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q34_b ---
    # op_group_thermometer_immigrants — Feeling thermometer: immigrants
    # Source: Q34_b
    df_clean['op_group_thermometer_immigrants'] = pd.to_numeric(df['q34b'], errors='coerce')
    CODEBOOK_VARIABLES['op_group_thermometer_immigrants'] = {
        'original_variable': 'q34b',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un groupe de gens, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous des immigrants?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q34_c ---
    # op_group_thermometer_francophones — Feeling thermometer: Francophones
    # Source: Q34_c
    df_clean['op_group_thermometer_francophones'] = pd.to_numeric(df['q34c'], errors='coerce')
    CODEBOOK_VARIABLES['op_group_thermometer_francophones'] = {
        'original_variable': 'q34c',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un groupe de gens, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous des Francophones?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q34_d ---
    # op_group_thermometer_anglophones — Feeling thermometer: Anglophones
    # Source: Q34_d
    df_clean['op_group_thermometer_anglophones'] = pd.to_numeric(df['q34d'], errors='coerce')
    CODEBOOK_VARIABLES['op_group_thermometer_anglophones'] = {
        'original_variable': 'q34d',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un groupe de gens, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous des Anglophones?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q34_e ---
    # op_group_thermometer_muslims — Feeling thermometer: Muslims living in Quebec
    # Source: Q34_e
    df_clean['op_group_thermometer_muslims'] = pd.to_numeric(df['q34e'], errors='coerce')
    CODEBOOK_VARIABLES['op_group_thermometer_muslims'] = {
        'original_variable': 'q34e',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un groupe de gens, et 10 veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous des musulmans qui vivent au Québec?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # ========================================================================
    # Q35: Party placement on left-right scale (0-10)
    # ========================================================================

    # --- Q35_1 ---
    # op_party_placement_plq — Party placement: Parti libéral du Québec
    # Source: Q35_1
    df_clean['op_party_placement_plq'] = pd.to_numeric(df['q35_1'], errors='coerce')
    CODEBOOK_VARIABLES['op_party_placement_plq'] = {
        'original_variable': 'q35_1',
        'question_label': "Sur une échelle allant de 0 à 10, où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous le Parti libéral du Québec?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q35_2 ---
    # op_party_placement_pq — Party placement: Parti québécois
    # Source: Q35_2
    df_clean['op_party_placement_pq'] = pd.to_numeric(df['q35_2'], errors='coerce')
    CODEBOOK_VARIABLES['op_party_placement_pq'] = {
        'original_variable': 'q35_2',
        'question_label': "Sur une échelle allant de 0 à 10, où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous le Parti québécois?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q35_3 ---
    # op_party_placement_caq — Party placement: Coalition avenir Québec
    # Source: Q35_3
    df_clean['op_party_placement_caq'] = pd.to_numeric(df['q35_3'], errors='coerce')
    CODEBOOK_VARIABLES['op_party_placement_caq'] = {
        'original_variable': 'q35_3',
        'question_label': "Sur une échelle allant de 0 à 10, où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous la Coalition avenir Québec?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q35_4 ---
    # op_party_placement_qs — Party placement: Québec solidaire
    # Source: Q35_4
    df_clean['op_party_placement_qs'] = pd.to_numeric(df['q35_4'], errors='coerce')
    CODEBOOK_VARIABLES['op_party_placement_qs'] = {
        'original_variable': 'q35_4',
        'question_label': "Sur une échelle allant de 0 à 10, où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous Québec solidaire?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # ========================================================================
    # CONTINUE WITH ALL REMAINING VARIABLES
    df_clean['op_politician_rating_philippe_couillard'] = pd.to_numeric(df['q33a'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_rating_philippe_couillard'] = {
        'original_variable': 'q33a',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N’AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L’AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Philippe Couillard?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- Q33 ---
    # op_politician_rating_jean_francois_lisee — Évaluation de Jean-François Lisée
    # Source: Q33b
    df_clean['op_politician_rating_jean_francois_lisee'] = pd.to_numeric(df['q33b'], errors='coerce')
    CODEBOOK_VARIABLES['op_politician_rating_jean_francois_lisee'] = {
        'original_variable': 'q33b',
        'question_label': "Sur une échelle de 0 à 10, où 0 veut dire que vous N’AIMEZ VRAIMENT PAS DU TOUT un politicien, et 10 veut dire que vous L’AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de Jean-François Lisée?",
        'type': 'ordinal',
        'value_labels': {}
    }

    # ========================================================================
    # CONTINUE WITH ALL REMAINING VARIABLES
    # op_gov_satisfaction — Satisfaction envers le gouvernement libéral
    # Source: Q10
    df_clean['op_gov_satisfaction'] = df['q10'].map({
        1.0: 'very_satisfied',
        2.0: 'somewhat_satisfied',
        3.0: 'not_very_satisfied',
        4.0: 'not_at_all_satisfied',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_gov_satisfaction'] = {
        'original_variable': 'q10',
        'question_label': "Quel est votre niveau global de satisfaction envers la performance du gouvernement libéral de Philippe Couillard?",
        'type': 'ordinal',
        'value_labels': {
            'very_satisfied': "Très satisfait(e)",
            'somewhat_satisfied': "Assez satisfait(e)",
            'not_very_satisfied': "Pas satisfait(e)",
            'not_at_all_satisfied': "Pas du tout satisfait(e)"
        }
    }

    # --- Q11_1 ---
    # op_responsibility_education — Palier de gouvernement responsable de l'éducation
    # Source: Q11_1
    df_clean['op_responsibility_education'] = df['q11_1'].map({
        1.0: 'federal',
        2.0: 'provincial',
        3.0: 'municipal',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_responsibility_education'] = {
        'original_variable': 'q11_1',
        'question_label': "Quel palier de gouvernement est responsable de l'éducation?",
        'type': 'categorical',
        'value_labels': {
            'federal': "Gouvernement fédéral",
            'provincial': "Gouvernement provincial",
            'municipal': "Gouvernement municipal"
        }
    }

    # --- Q11_2 ---
    # op_responsibility_aqueduc — Palier de gouvernement responsable des services d'aqueduc
    # Source: Q11_2
    df_clean['op_responsibility_aqueduc'] = df['q11_2'].map({
        1.0: 'federal',
        2.0: 'provincial',
        3.0: 'municipal',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_responsibility_aqueduc'] = {
        'original_variable': 'q11_2',
        'question_label': "Quel palier de gouvernement est responsable des services d'aqueduc?",
        'type': 'categorical',
        'value_labels': {
            'federal': "Gouvernement fédéral",
            'provincial': "Gouvernement provincial",
            'municipal': "Gouvernement municipal"
        }
    }

    # --- Q12_1 ---
    # op_knowledge_rachel_notley — Connaissance: Rachel Notley
    # Source: Q12_1
    df_clean['op_knowledge_rachel_notley'] = df['q12_1'].map({
        1.0: 'premier_alberta',
        2.0: 'finance_quebec',
        3.0: 'president_france',
        4.0: 'foreign_affairs_canada',
        5.0: 'pm_uk',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_knowledge_rachel_notley'] = {
        'original_variable': 'q12_1',
        'question_label': "Quel poste occupait Rachel Notley au cours de la dernière année?",
        'type': 'categorical',
        'value_labels': {
            'premier_alberta': "Premier ministre de l'Alberta",
            'finance_quebec': "Ministre des finances du Québec",
            'president_france': "Président de la France",
            'foreign_affairs_canada': "Ministre des affaires étrangères du Canada",
            'pm_uk': "Premier ministre du Royaume-Uni"
        }
    }

    # --- Q12_2 ---
    # op_knowledge_carlos_leitao — Connaissance: Carlos Leitão
    # Source: Q12_2
    df_clean['op_knowledge_carlos_leitao'] = df['q12_2'].map({
        1.0: 'premier_alberta',
        2.0: 'finance_quebec',
        3.0: 'president_france',
        4.0: 'foreign_affairs_canada',
        5.0: 'pm_uk',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_knowledge_carlos_leitao'] = {
        'original_variable': 'q12_2',
        'question_label': "Quel poste occupait Carlos Leitão au cours de la dernière année?",
        'type': 'categorical',
        'value_labels': {
            'premier_alberta': "Premier ministre de l'Alberta",
            'finance_quebec': "Ministre des finances du Québec",
            'president_france': "Président de la France",
            'foreign_affairs_canada': "Ministre des affaires étrangères du Canada",
            'pm_uk': "Premier ministre du Royaume-Uni"
        }
    }

    # --- Q12_3 ---
    # op_knowledge_emmanuel_macron — Connaissance: Emmanuel Macron
    # Source: Q12_3
    df_clean['op_knowledge_emmanuel_macron'] = df['q12_3'].map({
        1.0: 'premier_alberta',
        2.0: 'finance_quebec',
        3.0: 'president_france',
        4.0: 'foreign_affairs_canada',
        5.0: 'pm_uk',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_knowledge_emmanuel_macron'] = {
        'original_variable': 'q12_3',
        'question_label': "Quel poste occupait Emmanuel Macron au cours de la dernière année?",
        'type': 'categorical',
        'value_labels': {
            'premier_alberta': "Premier ministre de l'Alberta",
            'finance_quebec': "Ministre des finances du Québec",
            'president_france': "Président de la France",
            'foreign_affairs_canada': "Ministre des affaires étrangères du Canada",
            'pm_uk': "Premier ministre du Royaume-Uni"
        }
    }

    # --- Q12_4 ---
    # op_knowledge_chrystia_freeland — Connaissance: Chrystia Freeland
    # Source: Q12_4
    df_clean['op_knowledge_chrystia_freeland'] = df['q12_4'].map({
        1.0: 'premier_alberta',
        2.0: 'finance_quebec',
        3.0: 'president_france',
        4.0: 'foreign_affairs_canada',
        5.0: 'pm_uk',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_knowledge_chrystia_freeland'] = {
        'original_variable': 'q12_4',
        'question_label': "Quel poste occupait Chrystia Freeland au cours de la dernière année?",
        'type': 'categorical',
        'value_labels': {
            'premier_alberta': "Premier ministre de l'Alberta",
            'finance_quebec': "Ministre des finances du Québec",
            'president_france': "Président de la France",
            'foreign_affairs_canada': "Ministre des affaires étrangères du Canada",
            'pm_uk': "Premier ministre du Royaume-Uni"
        }
    }

    # --- Q18 ---
    # op_attachment_quebec — Degré d'attachement au Québec
    # Source: Q18
    df_clean['op_attachment_quebec'] = df['q18'].map({
        1.0: 'very_attached',
        2.0: 'somewhat_attached',
        3.0: 'not_very_attached',
        4.0: 'not_at_all_attached',
        98.0: np.nan, # Je ne sais pas
        99.0: np.nan  # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_attachment_quebec'] = {
        'original_variable': 'q18',
        'question_label': "Quel est votre degré d’attachement au Québec?",
        'type': 'ordinal',
        'value_labels': {
            'very_attached': "Très attaché(e)",
            'somewhat_attached': "Assez attaché(e)",
            'not_very_attached': "Peu attaché(e)",
            'not_at_all_attached': "Pas du tout attaché(e)"
        }
    }

    # --- Q19 ---
    # op_attachment_canada — Degré d'attachement au Canada
    # Source: Q19
    df_clean['op_attachment_canada'] = df['q19'].map({
        1.0: 'very_attached',
        2.0: 'somewhat_attached',
        3.0: 'not_very_attached',
        4.0: 'not_at_all_attached',
        98.0: np.nan, # Je ne sais pas
        99.0: np.nan  # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_attachment_canada'] = {
        'original_variable': 'q19',
        'question_label': "Et quel est votre degré d’attachement au Canada?",
        'type': 'ordinal',
        'value_labels': {
            'very_attached': "Très attaché(e)",
            'somewhat_attached': "Assez attaché(e)",
            'not_very_attached': "Peu attaché(e)",
            'not_at_all_attached': "Pas du tout attaché(e)"
        }
    }

    # --- Q37A ---
    # op_decision_maker_citizens_politicians — Qui devrait prendre les décisions importantes : citoyens ou politiciens élus
    # Source: Q37A
    df_clean['op_decision_maker_citizens_politicians'] = pd.to_numeric(df['q37a'], errors='coerce')
    CODEBOOK_VARIABLES['op_decision_maker_citizens_politicians'] = {
        'original_variable': 'q37a',
        'question_label': "Sur une échelle de 0 à 5, selon vous, qui devrait prendre les décisions politiques importantes : les citoyens ou les politiciens élus ?",
        'type': 'ordinal',
        'value_labels': {
            0: 'citizens',
            5: 'politicians_elected',
        }
    }

    # --- Q37B ---
    # op_decision_maker_politicians_experts — Qui devrait prendre les décisions importantes : politiciens élus ou experts politiques
    # Source: Q37B
    df_clean['op_decision_maker_politicians_experts'] = pd.to_numeric(df['q37b'], errors='coerce')
    CODEBOOK_VARIABLES['op_decision_maker_politicians_experts'] = {
        'original_variable': 'q37b',
        'question_label': "Sur une échelle de 0 à 5, selon vous, qui devrait prendre les décisions politiques importantes : les politiciens élus ou les experts politiques indépendants ?",
        'type': 'ordinal',
        'value_labels': {
            0: 'politicians_elected',
            5: 'independent_political_experts',
        }
    }

    # --- Q37C ---
    # op_decision_maker_experts_citizens — Qui devrait prendre les décisions importantes : experts politiques ou citoyens
    # Source: Q37C
    df_clean['op_decision_maker_experts_citizens'] = pd.to_numeric(df['q37c'], errors='coerce')
    CODEBOOK_VARIABLES['op_decision_maker_experts_citizens'] = {
        'original_variable': 'q37c',
        'question_label': "Sur une échelle de 0 à 5, selon vous, qui devrait prendre les décisions politiques importantes : les experts politiques indépendants ou les citoyens ?",
        'type': 'ordinal',
        'value_labels': {
            0: 'independent_political_experts',
            5: 'citizens',
        }
    }

    # --- Q38_1 ---
    # op_gov_responsibility_basic_needs — Le gouvernement devrait garantir les besoins fondamentaux
    # Source: Q38_1
    df_clean['op_gov_responsibility_basic_needs'] = df['q38_1'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_gov_responsibility_basic_needs'] = {
        'original_variable': 'q38_1',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: C’est la responsabilité du gouvernement de garantir que les besoins fondamentaux sont satisfaits pour tous.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # --- Q38_2 ---
    # op_nat_assembly_cares — L'Assemblée nationale se soucie des gens comme moi
    # Source: Q38_2
    df_clean['op_nat_assembly_cares'] = df['q38_2'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_nat_assembly_cares'] = {
        'original_variable': 'q38_2',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: L’Assemblée nationale du Québec ne se soucie pas beaucoup de ce que les gens comme moi pensent.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # --- Q38_3 ---
    # op_no_say_prov_gov — Les gens comme moi n'ont rien à dire sur le gouvernement provincial
    # Source: Q38_3
    df_clean['op_no_say_prov_gov'] = df['q38_3'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_no_say_prov_gov'] = {
        'original_variable': 'q38_3',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: Les gens comme moi n’ont rien à dire sur ce que fait le gouvernement provincial à Québec.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # --- Q38_4 ---
    # op_politics_too_complicated — La politique est trop compliquée
    # Source: Q38_4
    df_clean['op_politics_too_complicated'] = df['q38_4'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_politics_too_complicated'] = {
        'original_variable': 'q38_4',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: Parfois la politique et le gouvernement semblent si compliqués qu’une personne comme moi ne peut pas comprendre ce qui se passe.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # --- Q38_5 ---
    # op_most_people_my_age_know_politics — La plupart des gens de mon âge connaissent la politique
    # Source: Q38_5
    df_clean['op_most_people_my_age_know_politics'] = df['q38_5'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neither_agree_nor_disagree',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_most_people_my_age_know_politics'] = {
        'original_variable': 'q38_5',
        'question_label': "Veuillez indiquer si vous êtes tout à fait d’accord, plutôt d’accord, plutôt en désaccord, ou tout à fait en désaccord avec l’énoncé suivant: La plupart des gens de mon âge connaissent la politique.",
        'type': 'ordinal',
        'value_labels': {
            'strongly_disagree': "Tout à fait en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neither_agree_nor_disagree': "Ni en désaccord ni d’accord",
            'somewhat_agree': "Plutôt d’accord",
            'strongly_agree': "Tout à fait d’accord"
        }
    }

    # ========================================================================
    # Q22_1-8: Importance of characteristics to be a "true" Quebecois
    # ========================================================================

    # --- Q22_1 ---
    # op_true_quebecois_born_in_quebec — Born in Quebec
    # Source: Q22_1
    df_clean['op_true_quebecois_born_in_quebec'] = df['q22_1'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_born_in_quebec'] = {
        'original_variable': 'q22_1',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important d'être né au Québec?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_2 ---
    # op_true_quebecois_lived_most_life_in_quebec — Lived most of life in Quebec
    # Source: Q22_2
    df_clean['op_true_quebecois_lived_most_life_in_quebec'] = df['q22_2'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_lived_most_life_in_quebec'] = {
        'original_variable': 'q22_2',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important d'avoir vécu la plus grande partie de sa vie au Québec?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_3 ---
    # op_true_quebecois_speak_french — Speak French
    # Source: Q22_3
    df_clean['op_true_quebecois_speak_french'] = df['q22_3'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_speak_french'] = {
        'original_variable': 'q22_3',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important d'être capable de parler le français?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_4 ---
    # op_true_quebecois_catholic — Be Catholic
    # Source: Q22_4
    df_clean['op_true_quebecois_catholic'] = df['q22_4'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_catholic'] = {
        'original_variable': 'q22_4',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important d'être catholique?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_5 ---
    # op_true_quebecois_respect_quebec_laws — Respect Quebec institutions and laws
    # Source: Q22_5
    df_clean['op_true_quebecois_respect_quebec_laws'] = df['q22_5'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_respect_quebec_laws'] = {
        'original_variable': 'q22_5',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important de respecter les institutions politiques et les lois québécoises?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_6 ---
    # op_true_quebecois_feel_quebecois — Feel Quebecois
    # Source: Q22_6
    df_clean['op_true_quebecois_feel_quebecois'] = df['q22_6'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_feel_quebecois'] = {
        'original_variable': 'q22_6',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important de se sentir Québécois?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_7 ---
    # op_true_quebecois_french_ancestors — Have French ancestors
    # Source: Q22_7
    df_clean['op_true_quebecois_french_ancestors'] = df['q22_7'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_french_ancestors'] = {
        'original_variable': 'q22_7',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important d'avoir des ancêtres français?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # --- Q22_8 ---
    # op_true_quebecois_share_quebec_values — Share Quebec values
    # Source: Q22_8
    df_clean['op_true_quebecois_share_quebec_values'] = df['q22_8'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_true_quebecois_share_quebec_values'] = {
        'original_variable': 'q22_8',
        'question_label': "Pour être vraiment Québécois, à quel point est-il important de partager les valeurs des Québécois?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # ========================================================================
    # Q23: Voted in 1995 referendum
    # ========================================================================

    # --- Q23 ---
    # op_voted_1995_referendum — Voted in 1995 sovereignty referendum
    # Source: Q23 (asked if age < 1977)
    df_clean['op_voted_1995_referendum'] = df['q23'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_voted_1995_referendum'] = {
        'original_variable': 'q23',
        'question_label': "Avez-vous voté lors du référendum de 1995 sur la souveraineté du Québec?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # ========================================================================
    # Q24: Referendum vote choice
    # ========================================================================

    # --- Q24 ---
    # op_1995_referendum_choice — 1995 referendum vote choice
    # Source: Q24 (asked if Q23 = Yes)
    df_clean['op_1995_referendum_choice'] = df['q24'].map({
        1.0: 'yes_sovereignty',
        2.0: 'no_no_sovereignty',
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_1995_referendum_choice'] = {
        'original_variable': 'q24',
        'question_label': "Pour quelle option aviez-vous vote?",
        'type': 'categorical',
        'value_labels': {
            'yes_sovereignty': "Oui (souveraineté)",
            'no_no_sovereignty': "Non (non-souveraineté)"
        }
    }

    # ========================================================================
    # Q25: Importance of Quebec independence
    # ========================================================================

    # --- Q25 ---
    # op_importance_independence — Importance of Quebec independence
    # Source: Q25
    df_clean['op_importance_independence'] = df['q25'].map({
        1.0: 'very_important',
        2.0: 'somewhat_important',
        3.0: 'not_very_important',
        4.0: 'not_at_all_important',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_importance_independence'] = {
        'original_variable': 'q25',
        'question_label': "À quel point l'enjeu de l'indépendance politique du Québec est-il important pour vous, personnellement?",
        'type': 'ordinal',
        'value_labels': {
            'very_important': "Très important",
            'somewhat_important': "Assez important",
            'not_very_important': "Pas très important",
            'not_at_all_important': "Pas du tout important"
        }
    }

    # ========================================================================
    # Q26: Would vote in independence referendum today
    # ========================================================================

    # --- Q26 ---
    # op_vote_independence_referendum_today — Would vote Yes/No in independence referendum today
    # Source: Q26
    df_clean['op_vote_independence_referendum_today'] = df['q26'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan
    })
    CODEBOOK_VARIABLES['op_vote_independence_referendum_today'] = {
        'original_variable': 'q26',
        'question_label': "Si un référendum sur l'indépendance avait lieu aujourd'hui vous demandant si vous voulez que le Québec devienne un pays indépendant, voteriez-vous OUI ou voteriez-vous NON?",
        'type': 'binary',
        'value_labels': {
            'yes': "Oui",
            'no': "Non"
        }
    }

    # ========================================================================
    # Q27: Interest in politics
    # ========================================================================

    # --- Q27 ---
    # op_interest_politics — Interest in politics and public issues
    # Source: Q27
    df_clean['op_interest_politics'] = df['q27'].map({
        1.0: 'very_interested',
        2.0: 'somewhat_interested',
        3.0: 'not_very_interested',
        4.0: 'not_at_all_interested',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_interest_politics'] = {
        'original_variable': 'q27',
        'question_label': "Quel est votre intérêt pour la politique et les enjeux publics en général?",
        'type': 'ordinal',
        'value_labels': {
            'very_interested': "Très intéressé(e)",
            'somewhat_interested': "Assez intéressé(e)",
            'not_very_interested': "Pas très intéressé(e)",
            'not_at_all_interested': "Pas du tout intéressé(e)"
        }
    }

    # ========================================================================
    # Q28: Frequency of following political news
    # ========================================================================

    # --- Q28 ---
    # op_follow_political_news — Frequency of following political news
    # Source: Q28
    df_clean['op_follow_political_news'] = df['q28'].map({
        1.0: 'several_times_per_day',
        2.0: 'once_per_day',
        3.0: 'several_times_per_week',
        4.0: 'once_or_twice_per_week',
        97.0: 'never',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_follow_political_news'] = {
        'original_variable': 'q28',
        'question_label': "À quelle fréquence suivez-vous l'actualité politique sur internet, à la télévision, à la radio ou dans les journaux?",
        'type': 'ordinal',
        'value_labels': {
            'several_times_per_day': "Plusieurs fois par jour",
            'once_per_day': "Une fois par jour",
            'several_times_per_week': "Plusieurs fois par semaine",
            'once_or_twice_per_week': "Une ou deux fois par semaine",
            'never': "Jamais"
        }
    }

    # ========================================================================
    # Q29_1-7: Frequency of discussing politics with various people
    # ========================================================================

    # --- Q29_1 ---
    # op_discuss_politics_parents — Discuss politics with parents
    # Source: Q29_1
    df_clean['op_discuss_politics_parents'] = df['q29_1'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_parents'] = {
        'original_variable': 'q29_1',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec vos parents?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_2 ---
    # op_discuss_politics_family — Discuss politics with other family members
    # Source: Q29_2
    df_clean['op_discuss_politics_family'] = df['q29_2'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_family'] = {
        'original_variable': 'q29_2',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec d'autres membres de votre famille?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_3 ---
    # op_discuss_politics_friends — Discuss politics with friends
    # Source: Q29_3
    df_clean['op_discuss_politics_friends'] = df['q29_3'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_friends'] = {
        'original_variable': 'q29_3',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec vos amis?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_4 ---
    # op_discuss_politics_partner — Discuss politics with partner
    # Source: Q29_4
    df_clean['op_discuss_politics_partner'] = df['q29_4'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_partner'] = {
        'original_variable': 'q29_4',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec votre partenaire de vie?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_5 ---
    # op_discuss_politics_children — Discuss politics with children
    # Source: Q29_5
    df_clean['op_discuss_politics_children'] = df['q29_5'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_children'] = {
        'original_variable': 'q29_5',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec votre enfant/vos enfants?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_6 ---
    # op_discuss_politics_colleagues — Discuss politics with work colleagues
    # Source: Q29_6
    df_clean['op_discuss_politics_colleagues'] = df['q29_6'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_colleagues'] = {
        'original_variable': 'q29_6',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec vos collègues de travail?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # --- Q29_7 ---
    # op_discuss_politics_teachers — Discuss politics with teachers/professors
    # Source: Q29_7
    df_clean['op_discuss_politics_teachers'] = df['q29_7'].map({
        1.0: 'always',
        2.0: 'often',
        3.0: 'sometimes',
        4.0: 'rarely',
        97.0: 'never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_politics_teachers'] = {
        'original_variable': 'q29_7',
        'question_label': "En général, à quelle fréquence discutez-vous de politique et d'enjeux publics avec votre professeur/vos professeurs?",
        'type': 'ordinal',
        'value_labels': {
            'always': "Toujours",
            'often': "Souvent",
            'sometimes': 'Parfois',
            'rarely': "Rarement",
            'never': "Jamais"
        }
    }

    # ========================================================================
    # Q30_1-7: Discussed election with various people
    # ========================================================================

    # --- Q30_1 ---
    # op_discuss_election_parents — Discussed election with parents
    # Source: Q30_1
    df_clean['op_discuss_election_parents'] = df['q30_1'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_parents'] = {
        'original_variable': 'q30_1',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec vos parents?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_2 ---
    # op_discuss_election_family — Discussed election with other family members
    # Source: Q30_2
    df_clean['op_discuss_election_family'] = df['q30_2'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_family'] = {
        'original_variable': 'q30_2',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec d'autres membres de votre famille?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_3 ---
    # op_discuss_election_friends — Discussed election with friends
    # Source: Q30_3
    df_clean['op_discuss_election_friends'] = df['q30_3'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_friends'] = {
        'original_variable': 'q30_3',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec vos amis?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_4 ---
    # op_discuss_election_partner — Discussed election with partner
    # Source: Q30_4
    df_clean['op_discuss_election_partner'] = df['q30_4'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_partner'] = {
        'original_variable': 'q30_4',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec votre partenaire de vie?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_5 ---
    # op_discuss_election_children — Discussed election with children
    # Source: Q30_5
    df_clean['op_discuss_election_children'] = df['q30_5'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_children'] = {
        'original_variable': 'q30_5',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec votre enfant/vos enfants?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_6 ---
    # op_discuss_election_colleagues — Discussed election with work colleagues
    # Source: Q30_6
    df_clean['op_discuss_election_colleagues'] = df['q30_6'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_colleagues'] = {
        'original_variable': 'q30_6',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec vos collègues de travail?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q30_7 ---
    # op_discuss_election_teachers — Discussed election with teachers/professors
    # Source: Q30_7
    df_clean['op_discuss_election_teachers'] = df['q30_7'].map({
        1.0: 'yes_several_times',
        2.0: 'yes_once',
        3.0: 'no_never',
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_discuss_election_teachers'] = {
        'original_variable': 'q30_7',
        'question_label': "Au cours des dernières semaines, avez-vous discuté de l'élection provinciale avec votre professeur/vos professeurs?",
        'type': 'ordinal',
        'value_labels': {
            'yes_several_times': "Oui, plusieurs fois",
            'yes_once': "Oui, une fois",
            'no_never': "Non, jamais"
        }
    }

    # --- Q68 ---
    # op_religious_attendance — Fréquence de participation aux messes
    # Source: Q68
    df_clean['op_religious_attendance'] = df['q68'].map({
        1.0: 'every_week',
        2.0: 'twice_per_month',
        3.0: 'once_per_month',
        4.0: 'once_or_twice_per_year',
        5.0: 'almost_never',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['op_religious_attendance'] = {
        'original_variable': 'q68',
        'question_label': "Sans compter les mariages et les funérailles, combien de fois assistez-vous aux messes à votre lieu de culte?",
        'type': 'ordinal',
        'value_labels': {
            'every_week': "Chaque semaine",
            'twice_per_month': "Deux fois par mois",
            'once_per_month': "Une fois par mois",
            'once_or_twice_per_year': "Une ou deux fois par année",
            'almost_never': "Presque jamais (ou jamais)"
        }
    }

    # --- Q71_1 ---
    # op_ethnic_origin_canadian — Origine ethnique: Canadienne, Québécoise
    # Source: Q71_1
    df_clean['op_ethnic_origin_canadian'] = df['q71_1'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_canadian'] = {
        'original_variable': 'q71_1',
        'question_label': "De quelle origine ethnique êtes-vous? Canadienne, Québécoise",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_2 ---
    # op_ethnic_origin_aboriginal — Origine ethnique: Autochtone
    # Source: Q71_2
    df_clean['op_ethnic_origin_aboriginal'] = df['q71_2'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_aboriginal'] = {
        'original_variable': 'q71_2',
        'question_label': "De quelle origine ethnique êtes-vous? Autochtone (Amérindienne, Premières nations)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_3 ---
    # op_ethnic_origin_north_africa — Origine ethnique: Afrique du Nord
    # Source: Q71_3
    df_clean['op_ethnic_origin_north_africa'] = df['q71_3'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_north_africa'] = {
        'original_variable': 'q71_3',
        'question_label': "De quelle origine ethnique êtes-vous? Afrique du Nord (Maroc, Algérie, Tunisie, Libye, Égypte)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_4 ---
    # op_ethnic_origin_africa — Origine ethnique: Afrique
    # Source: Q71_4
    df_clean['op_ethnic_origin_africa'] = df['q71_4'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_africa'] = {
        'original_variable': 'q71_4',
        'question_label': "De quelle origine ethnique êtes-vous? Afrique (Gabon, Congo, Côte d'Ivoire, Éthiopie, Kenya, Cameroun, Mauritanie, ...) et Afrique du Sud",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_5 ---
    # op_ethnic_origin_central_south_america — Origine ethnique: Amérique centrale et sud
    # Source: Q71_5
    df_clean['op_ethnic_origin_central_south_america'] = df['q71_5'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_central_south_america'] = {
        'original_variable': 'q71_5',
        'question_label': "De quelle origine ethnique êtes-vous? Amérique centrale et sud (Nicaragua, Pérou, Bolivie, Vénézuela, Argentine, El Salvador, Guatemala, ...)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_6 ---
    # op_ethnic_origin_usa — Origine ethnique: Américaine (États-Unis)
    # Source: Q71_6
    df_clean['op_ethnic_origin_usa'] = df['q71_6'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_usa'] = {
        'original_variable': 'q71_6',
        'question_label': "De quelle origine ethnique êtes-vous? Américaine (États-Unis)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_7 ---
    # op_ethnic_origin_mexican — Origine ethnique: Mexicaine
    # Source: Q71_7
    df_clean['op_ethnic_origin_mexican'] = df['q71_7'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_mexican'] = {
        'original_variable': 'q71_7',
        'question_label': "De quelle origine ethnique êtes-vous? Mexicaine",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_8 ---
    # op_ethnic_origin_caribbean — Origine ethnique: Antillaise
    # Source: Q71_8
    df_clean['op_ethnic_origin_caribbean'] = df['q71_8'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_caribbean'] = {
        'original_variable': 'q71_8',
        'question_label': "De quelle origine ethnique êtes-vous? Antillaise (Haïti, Jamaïque, République Dominicaine, ....)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_9 ---
    # op_ethnic_origin_asian — Origine ethnique: Asiatique
    # Source: Q71_9
    df_clean['op_ethnic_origin_asian'] = df['q71_9'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_asian'] = {
        'original_variable': 'q71_9',
        'question_label': "De quelle origine ethnique êtes-vous? Asiatique (Japon, Chine, Vietnam, Corée, Cambodge, ...)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_10 ---
    # op_ethnic_origin_european — Origine ethnique: Européenne
    # Source: Q71_10
    df_clean['op_ethnic_origin_european'] = df['q71_10'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_european'] = {
        'original_variable': 'q71_10',
        'question_label': "De quelle origine ethnique êtes-vous? Européenne (France, Belgique, Italie, Espagne, Portugal, Allemagne, Autriche, Suède, Norvège, Danemark, Pays-Bas, Grèce, ...)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_11 ---
    # op_ethnic_origin_eastern_europe — Origine ethnique: Europe de l'Est
    # Source: Q71_11
    df_clean['op_ethnic_origin_eastern_europe'] = df['q71_11'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_eastern_europe'] = {
        'original_variable': 'q71_11',
        'question_label': "De quelle origine ethnique êtes-vous? Europe de l'Est (Russie, Ukraine, Pologne, Roumanie, Ex-Yougoslavie, Croatie, République Tchèque, République Slovaque, Hongrie, ...)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_12 ---
    # op_ethnic_origin_middle_east — Origine ethnique: Moyen-Orient
    # Source: Q71_12
    df_clean['op_ethnic_origin_middle_east'] = df['q71_12'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_middle_east'] = {
        'original_variable': 'q71_12',
        'question_label': "De quelle origine ethnique êtes-vous? Moyen-Orient, sauf l'Afrique du Nord (Jordanie, Arabie Saoudite, Irak, Liban,...)",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_13 ---
    # op_ethnic_origin_turkey_armenia_iran — Origine ethnique: Turquie, Arménie, Iran, Kurde
    # Source: Q71_13
    df_clean['op_ethnic_origin_turkey_armenia_iran'] = df['q71_13'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_turkey_armenia_iran'] = {
        'original_variable': 'q71_13',
        'question_label': "De quelle origine ethnique êtes-vous? Turquie, Arménie, Iran, Kurde",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_96 ---
    # op_ethnic_origin_other — Origine ethnique: Autre origine
    # Source: Q71_96
    df_clean['op_ethnic_origin_other'] = df['q71_96'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_other'] = {
        'original_variable': 'q71_96',
        'question_label': "De quelle origine ethnique êtes-vous? Autre origine",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_98 ---
    # op_ethnic_origin_dont_know — Origine ethnique: Je ne sais pas
    # Source: Q71_98
    df_clean['op_ethnic_origin_dont_know'] = df['q71_98'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_dont_know'] = {
        'original_variable': 'q71_98',
        'question_label': "De quelle origine ethnique êtes-vous? Je ne sais pas",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # --- Q71_99 ---
    # op_ethnic_origin_refused — Origine ethnique: Je préfère ne pas répondre
    # Source: Q71_99
    df_clean['op_ethnic_origin_refused'] = df['q71_99'].map({
        1.0: 'selected'
    })
    CODEBOOK_VARIABLES['op_ethnic_origin_refused'] = {
        'original_variable': 'q71_99',
        'question_label': "De quelle origine ethnique êtes-vous? Je préfère ne pas répondre",
        'type': 'binary',
        'value_labels': {
            'selected': "Sélectionné"
        }
    }

    # ========================================================================
    # BEHAVIORAL - Party best for issues/groups (Q51A, Q52)
    # ========================================================================

    # --- Q51_1 ---
    # behav_party_best_quebec_interests — Quel parti est meilleur pour défendre les intérêts du Québec
    # Source: Q51A_1
    df_clean['behav_party_best_quebec_interests'] = df['q51a_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_quebec_interests'] = {
        'original_variable': 'q51a_1',
        'question_label': "Selon vous, quel parti est le meilleur pour défendre les intérêts du Québec?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_2 ---
    # behav_party_best_cultural_identity — Quel parti est meilleur pour défendre l'identité et la culture québécoise
    # Source: Q51A_2
    df_clean['behav_party_best_cultural_identity'] = df['q51a_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_cultural_identity'] = {
        'original_variable': 'q51a_2',
        'question_label': "Selon vous, quel parti est le meilleur pour défendre l'identité et la culture québécoise?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_3 ---
    # behav_party_best_economy — Quel parti est meilleur pour gérer l'économie
    # Source: Q51A_3
    df_clean['behav_party_best_economy'] = df['q51a_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_economy'] = {
        'original_variable': 'q51a_3',
        'question_label': "Selon vous, quel parti est le meilleur pour gérer l'économie?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_4 ---
    # behav_party_best_education — Quel parti est meilleur pour améliorer l'éducation
    # Source: Q51A_4
    df_clean['behav_party_best_education'] = df['q51a_4'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_education'] = {
        'original_variable': 'q51a_4',
        'question_label': "Selon vous, quel parti est le meilleur pour améliorer l'éducation?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_5 ---
    # behav_party_best_environment — Quel parti est meilleur pour protéger l'environnement
    # Source: Q51A_5
    df_clean['behav_party_best_environment'] = df['q51a_5'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_environment'] = {
        'original_variable': 'q51a_5',
        'question_label': "Selon vous, quel parti est le meilleur pour protéger l'environnement?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_6 ---
    # behav_party_best_health — Quel parti est meilleur pour gérer le système de santé
    # Source: Q51A_6
    df_clean['behav_party_best_health'] = df['q51a_6'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_health'] = {
        'original_variable': 'q51a_6',
        'question_label': "Selon vous, quel parti est le meilleur pour gérer le système de santé?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_7 ---
    # behav_party_best_poverty — Quel parti est meilleur pour combattre la pauvreté
    # Source: Q51A_7
    df_clean['behav_party_best_poverty'] = df['q51a_7'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_poverty'] = {
        'original_variable': 'q51a_7',
        'question_label': "Selon vous, quel parti est le meilleur pour combattre la pauvreté?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q51_8 ---
    # behav_party_best_immigration — Quel parti est meilleur pour gérer l'intégration des immigrants
    # Source: Q51A_8
    df_clean['behav_party_best_immigration'] = df['q51a_8'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_immigration'] = {
        'original_variable': 'q51a_8',
        'question_label': "Selon vous, quel parti est le meilleur pour gérer l'intégration des immigrants au Québec?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q52_1 ---
    # behav_party_best_age_group_18_34 — Quel parti est meilleur pour les 18-34 ans
    # Source: Q52_1
    df_clean['behav_party_best_age_group_18_34'] = df['q52_1'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_age_group_18_34'] = {
        'original_variable': 'q52_1',
        'question_label': "Selon vous, quel parti est le meilleur pour défendre les intérêts des gens âgés de 18 à 34 ans?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q52_2 ---
    # behav_party_best_age_group_35_54 — Quel parti est meilleur pour les 35-54 ans
    # Source: Q52_2
    df_clean['behav_party_best_age_group_35_54'] = df['q52_2'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_age_group_35_54'] = {
        'original_variable': 'q52_2',
        'question_label': "Selon vous, quel parti est le meilleur pour défendre les intérêts des gens âgés de 35 à 54 ans?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # --- Q52_3 ---
    # behav_party_best_age_group_55_plus — Quel parti est meilleur pour les 55+ ans
    # Source: Q52_3
    df_clean['behav_party_best_age_group_55_plus'] = df['q52_3'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        96.0: 'other_party',
        97.0: 'no_party',
        98.0: np.nan,
        99.0: np.nan
    })
    CODEBOOK_VARIABLES['behav_party_best_age_group_55_plus'] = {
        'original_variable': 'q52_3',
        'question_label': "Selon vous, quel parti est le meilleur pour défendre les intérêts des gens âgés de 55 ans et plus?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Parti libéral du Québec",
            'pq': "Parti québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec solidaire",
            'other_party': "Un autre parti",
            'no_party': "Aucun de ces partis"
        }
    }

    # ========================================================================
    # CONTINUE WITH ALL REMAINING VARIABLES...
    # This is an incomplete version. Need to add all 200+ variables.
    # ========================================================================

    return df_clean

def get_metadata():
    """Retourne les métadonnées enrichies (survey + variables)"""
    return {
        'survey_metadata': SURVEY_METADATA,
        'variables': CODEBOOK_VARIABLES
    }

def map_strates_canoniques(df):
    """
    Retourne les variables de stratification canoniques.
    Inclut toutes les variables démographiques clés pour l'analyse.
    """
    strata_cols = [
        'ses_age', 'ses_gender', 'ses_language', 'ses_education',
        'ses_occupation', 'ses_region_qc', 'ses_region_rmr',
        'ses_marital_status', 'ses_has_children', 'ses_weight',
        'ses_birth_place', 'ses_religious_affiliation',
        'ses_religious_practice', 'ses_income', 'ses_mother_education',
        'ses_father_education', 'ses_home_language', 'ses_live_with_parents'
    ]
    existing = [c for c in strata_cols if c in df.columns]
    return df[existing]

