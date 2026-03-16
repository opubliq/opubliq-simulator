#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2008

Expose:
  - clean_data(raw_path) → pd.DataFrame nettoyé
  - get_metadata() → dictionnaire avec métadonnées du sondage et variables
"""

import os
import pyreadstat
import pandas as pd
import numpy as np

# ============================================================================
# SURVEY METADATA (à remplir au début du script)
# ============================================================================
# Métadonnées générales du sondage
SURVEY_METADATA = {
    'survey_id': 'eeq_2008',           # ID unique du sondage (ex: "ces2019")
    'title': 'eeq_2008',             # Titre complet
    'year': None,                          # Année de collecte
    'description': '',                     # Description du sondage
    'organization': '',                    # Organisation responsable
    'sample_size': None,                   # Taille de l'échantillon
    'language': 'fr',                      # Langue principale
    'methodology': '',                     # Méthodologie (web, téléphone, etc.)
}

# ============================================================================
# CODEBOOK VARIABLES (construit progressivement variable par variable)
# ============================================================================
# Ce dictionnaire accumule les métadonnées pour chaque variable nettoyée.
# Pour chaque variable, ajouter une entrée immédiatement après le code de nettoyage.
#
# Format:
# CODEBOOK_VARIABLES = {
#     'nom_variable_clean': {
#         'original_variable': 'Q1_raw_name',
#         'question_label': "Texte de la question du sondage",
#         'type': 'categorical|likert|numeric|binary',
#         'value_labels': {
#             valeur_cleanée: "Label descriptif",
#             ...
#         }
#     }
# }
#
# Types de variables:
#   - categorical: Variables nominales (province, parti, genre)
#   - likert: Échelles ordinales (satisfaction, accord/désaccord)
#   - numeric: Variables continues normalisées (ratings 0-1)
#   - binary: Variables oui/non
CODEBOOK_VARIABLES = {}


def clean_data(raw_path: str) -> pd.DataFrame:
    """Charge et nettoie les données brutes du sondage eeq_2008.

    Args:
        raw_path (str): Chemin vers le fichier .sav brut
            (ex: SHARED_FOLDER_PATH/eeq_2008/Quebec Election Study 2008 (SPSS).sav)

    Returns:
        pd.DataFrame: Données nettoyées
    """
    df, _ = pyreadstat.read_sav(raw_path)

    # Initialize empty clean dataframe with same index
    df_clean = pd.DataFrame(index=df.index)

    # ========================================================================
    # VARIABLE PROCESSING - Pattern variable par variable
    # ========================================================================
    # Pour chaque variable:
    #   1. Code de nettoyage de la variable
    #   2. Entrée CODEBOOK immédiatement après (question_label + value_labels)
    #
    # RÈGLES CRITIQUES:
    #   1. Ne JAMAIS copier puis replace: df_clean['x'] = df['y'].copy() + .replace()
    #   2. TOUJOURS utiliser .map() pour catégorielles (unmapped → NaN automatique)
    #   3. Pour normalisation: créer vecteur NaN, puis remplir valeurs valides seulement
    #   4. Ne jamais modifier df directement
    #   5. Valeurs catégorielles: simples et concises (pas de répétition du nom de variable)
    #      - ses_province: "quebec", "ontario" (PAS "province_quebec", "province_ontario")
    #      - behav_vote_choice: "liberal", "conservative", "ndp" (abréviations anglaises OK)
    #
    # ========================================================================

    # EXEMPLE 1: Variable catégorielle (province)
    # ------------------------------------------
    # df_clean['ses_province'] = df['Q2_province'].map({
    #     1.0: 'quebec',
    #     2.0: 'ontario',
    #     3.0: 'alberta',
    #     4.0: 'british_columbia'
    # })
    #
    # CODEBOOK_VARIABLES['ses_province'] = {
    #     'original_variable': 'Q2_province',
    #     'question_label': "Dans quelle province habitez-vous?",
    #     'type': 'categorical',
    #     'value_labels': {
    #         'quebec': "Québec",
    #         'ontario': "Ontario",
    #         'alberta': "Alberta",
    #         'british_columbia': "Colombie-Britannique"
    #     }
    # }

    # EXEMPLE 2: Variable catégorielle (choix de vote)
    # ------------------------------------------------
    # df_clean['behav_vote_choice'] = df['Q10_vote'].map({
    #     1.0: 'liberal',
    #     2.0: 'conservative',
    #     3.0: 'ndp',
    #     4.0: 'bloc',
    #     5.0: 'green',
    #     6.0: 'ppc',
    #     7.0: 'other',
    #     99.0: np.nan
    # })
    #
    # CODEBOOK_VARIABLES['behav_vote_choice'] = {
    #     'original_variable': 'Q10_vote',
    #     'question_label': "Si les élections avaient lieu demain, pour quel parti voteriez-vous?",
    #     'type': 'categorical',
    #     'value_labels': {
    #         'liberal': "Parti libéral du Canada",
    #         'conservative': "Parti conservateur du Canada",
    #         'ndp': "Nouveau Parti démocratique",
    #         'bloc': "Bloc Québécois",
    #         'green': "Parti vert du Canada",
    #         'ppc': "Parti populaire du Canada",
    #         'other': "Autre parti"
    #     }
    # }

    # EXEMPLE 3: Variable ordinale (échelle Likert normalisée 0-1)
    # ------------------------------------------------------------
    # df_clean['op_satisfaction_gov'] = df['Q5_satisfaction'].map({
    #     1.0: 1.0,    # Very satisfied
    #     2.0: 0.75,   # Somewhat satisfied
    #     3.0: 0.5,    # Neutral
    #     4.0: 0.25,   # Somewhat dissatisfied
    #     5.0: 0.0,    # Very dissatisfied
    #     99.0: np.nan
    # })
    #
    # CODEBOOK_VARIABLES['op_satisfaction_gov'] = {
    #     'original_variable': 'Q5_satisfaction',
    #     'question_label': "Dans quelle mesure êtes-vous satisfait du gouvernement actuel?",
    #     'type': 'likert',
    #     'value_labels': {
    #         1.0: "Très satisfait",
    #         0.75: "Plutôt satisfait",
    #         0.5: "Neutre",
    #         0.25: "Plutôt insatisfait",
    #         0.0: "Très insatisfait"
    #     }
    # }

    # EXEMPLE 4: Variable numérique (normalisation 0-100 → 0-1)
    # ---------------------------------------------------------
    # df_clean['op_party_rating_liberal'] = np.nan
    # mask = (df['Q15_liberal_rating'] >= 0) & (df['Q15_liberal_rating'] <= 100)
    # df_clean.loc[mask, 'op_party_rating_liberal'] = df.loc[mask, 'Q15_liberal_rating'] / 100.0
    #
    # CODEBOOK_VARIABLES['op_party_rating_liberal'] = {
    #     'original_variable': 'Q15_liberal_rating',
    #     'question_label': "Sur une échelle de 0 à 100, comment évaluez-vous le Parti libéral?",
    #     'type': 'numeric',
    #     'value_labels': {}  # Pas de labels pour variables continues
    # }

    # EXEMPLE 5: Variable démographique (genre)
    # -----------------------------------------
    # df_clean['ses_gender'] = df['Q1_gender'].map({
    #     1.0: 'male',
    #     2.0: 'female',
    #     3.0: 'other',
    #     99.0: np.nan
    # })
    #
    # CODEBOOK_VARIABLES['ses_gender'] = {
    #     'original_variable': 'Q1_gender',
    #     'question_label': "Quel est votre genre?",
    #     'type': 'categorical',
    #     'value_labels': {
    #         'male': "Homme",
    #         'female': "Femme",
    #         'other': "Autre / Non-binaire"
    #     }
    # }

    # --- ethn1 ---
    # ses_ethnicity — Ethnicity of respondent
    # Source: ethn1
    # Assumption: Codes 96 and 99 are treated as missing (not in assumed codebook)
    # Assumption: Codes 1.0 through 13.0 are mapped to generic string labels as actual labels are unavailable.
    df_clean['ses_ethnicity'] = df['ethn1'].map({
        1.0: 'category_1',
        2.0: 'category_2',
        3.0: 'category_3',
        4.0: 'category_4',
        5.0: 'category_5',
        6.0: 'category_6',
        7.0: 'category_7',
        8.0: 'category_8',
        9.0: 'category_9',
        10.0: 'category_10',
        11.0: 'category_11',
        12.0: 'category_12',
        13.0: 'category_13',
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_ethnicity'] = {
        'original_variable': 'ethn1',
        'question_label': "Ethnicity of respondent (mapped generically)",
        'type': 'categorical',
        'value_labels': {'category_1': "Category 1", 'category_2': "Category 2", 'category_3': "Category 3", 'category_4': "Category 4", 'category_5': "Category 5", 'category_6': "Category 6", 'category_7': "Category 7", 'category_8': "Category 8", 'category_9': "Category 9", 'category_10': "Category 10", 'category_11': "Category 11", 'category_12': "Category 12", 'category_13': "Category 13"},
    }

    # --- langu ---
    # ses_language — Language of response/interview
    # Source: langu
    # Assumption: Inferred mapping based on variable name 'langu' and common conventions.
    df_clean['ses_language'] = df['langu'].map({
        1.0: 'french',
        2.0: 'english',
        3.0: 'other',
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'langu',
        'question_label': "Language of response/interview",
        'type': 'categorical',
        'value_labels': {'french': "French", 'english': "English", 'other': "Other"},
    }

    # --- pond ---
    # wgt_respondent — Respondent sampling weight
    # Source: pond
    # Note: Variable is a continuous numeric weight; no explicit transformation (divide by max) or code mapping was performed as max value is unknown and no missing codes were documented.
    df_clean['wgt_respondent'] = df['pond']
    CODEBOOK_VARIABLES['wgt_respondent'] = {
        'original_variable': 'pond',
        'question_label': "Respondent sampling weight",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- pondx ---
    # op_raw_pondx — Raw score/proportion for Party X
    # Source: pondx
    # Assumption: Numeric variable with no observed missing codes or scaling factor provided. Mapping observed values to themselves.
    df_clean['op_raw_pondx'] = df['pondx'].map({
        0.055089: 0.055089,
        0.072570: 0.072570,
        0.073144: 0.073144,
        0.084384: 0.084384,
        0.090394: 0.090394,
    })
    CODEBOOK_VARIABLES['op_raw_pondx'] = {
        'original_variable': 'pondx',
        'question_label': "Raw proportion/score for Party X (Inferred)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q0age ---
    # ses_age_group — Âge du répondant (catégories)
    # Source: q0age
    df_clean['ses_age_group'] = df['q0age'].map({
        2.0: '18-24',
        3.0: '25-34',
        4.0: '35-44',
        5.0: '45-54',
        6.0: '55-64',
        7.0: '65-74',
        8.0: '75+',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_age_group'] = {
        'original_variable': 'q0age',
        'question_label': "Quel âge avez-vous ?",
        'type': 'categorical',
        'value_labels': {
            '18-24': "Entre 18 et 24 ans",
            '25-34': "Entre 25 et 34 ans",
            '35-44': "Entre 35 et 44 ans",
            '45-54': "Entre 45 et 54 ans",
            '55-64': "Entre 55 et 64 ans",
            '65-74': "Entre 65 et 74 ans",
            '75+': "75 ans ou plus",
        },
    }

    # --- q1 ---
    # op_vote_choice — Hypothetical vote choice question
    # Source: q1
    # Assumption: Codes 1.0-6.0 are choices, 96.0 is missing/refused.
    df_clean['op_vote_choice'] = df['q1'].map({
        1.0: 'parti_a',
        2.0: 'parti_b',
        3.0: 'parti_c',
        4.0: 'parti_d',
        5.0: 'parti_e',
        6.0: 'parti_f',
        96.0: np.nan, # TODO: verify mapping and labels for codes 1.0-96.0
    })
    CODEBOOK_VARIABLES['op_vote_choice'] = {
        'original_variable': 'q1',
        'question_label': "Hypothetical vote choice question (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'parti_a': "Choice 1 (Unknown Label)", 'parti_b': "Choice 2 (Unknown Label)", 'parti_c': "Choice 3 (Unknown Label)", 'parti_d': "Choice 4 (Unknown Label)", 'parti_e': "Choice 5 (Unknown Label)", 'parti_f': "Choice 6 (Unknown Label)"},
    }

    # --- q11 ---
    # op_q11 — Opinion question 11 (inferred)
    # Source: q11
    # Note: Codebook not provided. Inferring 'op' type as it is likely an opinion question.
    # Assumption: Codes 9.0 are treated as missing.
    df_clean['op_q11'] = df['q11'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q11'] = {
        'original_variable': 'q11',
        'question_label': "Opinion question 11 (inferred from context)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- q12a ---
    # op_q12a — Standard 5-point Likert-style question (Inferred)
    # Source: q12a
    # Assumption: Codes 1-5 map to agreement scale. Codes 96-99 are treated as missing/unlabeled.
    df_clean['op_q12a'] = df['q12a'].map({
        1.0: 'strongly_disagree',
        2.0: 'disagree',
        3.0: 'neutral',
        4.0: 'agree',
        5.0: 'strongly_agree',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q12a'] = {
        'original_variable': 'q12a',
        'question_label': "Question 12a - Placeholder label (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'strongly_disagree': "Strongly Disagree (Inferred)", 'disagree': "Disagree (Inferred)", 'neutral': "Neutral (Inferred)", 'agree': "Agree (Inferred)", 'strongly_agree': "Strongly Agree (Inferred)"},
    }

    # --- q12b ---
    # op_q12b — Inferred opinion/behavior variable
    # Source: q12b
    # Note: Actual codebook missing for eeq_2008/q12b. Proceeding with data-driven mapping and generic labels.
    # Assumption: Codes 97, 98, 99 treated as missing.
    df_clean['op_q12b'] = df['q12b'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        5.0: 'option_five',
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q12b'] = {
        'original_variable': 'q12b',
        'question_label': "Inferred question label for Q12b (Verification Required)",
        'type': 'categorical',
        'value_labels': {'option_one': "Category 1", 'option_two': "Category 2", 'option_three': "Category 3", 'option_four': "Category 4", 'option_five': "Category 5"},
    }

    # --- q13 ---
    # op_attitude_q13 — Attitude question q13 (Mapping inferred due to missing codebook entry)
    # Source: q13
    # Assumption: Codes 1-5 represent a scale, codes 95+ are missing/unlabelled.
    df_clean['op_attitude_q13'] = df['q13'].map({
        1.0: 'very_low',
        2.0: 'low',
        3.0: 'medium',
        4.0: 'high',
        5.0: 'very_high',
        95.0: np.nan,
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q13'] = {
        'original_variable': 'q13',
        'question_label': "Question q13 from eeq_2008 (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'very_low': "Code 1 (Inferred)", 'low': "Code 2 (Inferred)", 'medium': "Code 3 (Inferred)", 'high': "Code 4 (Inferred)", 'very_high': "Code 5 (Inferred)"},
    }

    # --- q14 ---
    # op_q14 — Inferred opinion/attitude measure scaled 0-1
    # Source: q14
    # Assumption: Variable is numeric scaled 0-10. Max value is 10.0. Codes 0.0-10.0 mapped to 0.0-1.0.
    df_clean['op_q14'] = df['q14'].map({
        0.0: 0.0,
        1.0: 0.1,
        2.0: 0.2,
        3.0: 0.3,
        4.0: 0.4,
        5.0: 0.5,
        6.0: 0.6,
        7.0: 0.7,
        8.0: 0.8,
        9.0: 0.9,
        10.0: 1.0,
    })
    CODEBOOK_VARIABLES['op_q14'] = {
        'original_variable': 'q14',
        'question_label': "Inferred: Question regarding attitude/opinion (scale 0-10)",
        'type': 'numeric',
        'value_labels': {'0.0': "Min", '1.0': "1/10", '5.0': "Midpoint", '10.0': "Max"},
    }

    # --- q18a ---
    # op_q18a — Placeholder for response to question q18a (No codebook provided)
    # Source: q18a
    # Assumption: codes 96, 98, 99 treated as missing (unlabelled in data exploration)
    df_clean['op_q18a'] = df['q18a'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q18a'] = {
        'original_variable': 'q18a',
        'question_label': "Response to question 18a (Labels unknown)",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5"},
    }

    # --- q18b ---
    # op_voting_interest — Interest in voting (inferred from q18b structure)
    # Source: q18b
    # Assumption: Codes 1-5 map to a 0.0-1.0 scale (Likert). Codes 96, 98, 99 treated as missing.
    df_clean['op_voting_interest'] = df['q18b'].map({
        1.0: 0.0,
        2.0: 0.25,
        3.0: 0.5,
        4.0: 0.75,
        5.0: 1.0,
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_voting_interest'] = {
        'original_variable': 'q18b',
        'question_label': "Interest in voting (inferred from q18b structure)",
        'type': 'likert',
        'value_labels': {'0.0': "Low Interest", '1.0': "High Interest"},
    }

    # --- q19 ---
    # op_q19 — Unknown question, mapping based on value counts.
    # Source: q19
    # Assumption: Codes 8.0 (126) and 9.0 (22) treated as 'dk' and 'refused' respectively.
    # TODO: verify mapping for codes 1.0, 2.0, 3.0, 8.0, 9.0 against actual codebook.
    df_clean['op_q19'] = df['q19'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        3.0: 'option_c',
        8.0: 'dk',
        9.0: 'refused',
    })
    CODEBOOK_VARIABLES['op_q19'] = {
        'original_variable': 'q19',
        'question_label': "Unknown question, mapping based on value counts.",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B", 'option_c': "Option C", 'dk': "Don't Know", 'refused': "Refused"},
    }

    # --- q20 ---
    # op_referendum_vote — Intention de vote au référendum
    # Source: q20
    df_clean['op_referendum_vote'] = df['q20'].map({
        1.0: 'oui',
        2.0: 'non',
        3.0: 'abstention',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_referendum_vote'] = {
        'original_variable': 'q20',
        'question_label': "S'il y avait un référendum aujourd'hui, voteriez-vous OUI ou NON ?",
        'type': 'categorical',
        'value_labels': {
            'oui': "OUI",
            'non': "NON",
            'abstention': "Ne voterait pas / annulerait",
        },
    }

    # --- q21 ---
    # op_q21 — Generic question response for Q21
    # Source: q21
    # Assumption: Missing codes 8.0 and 9.0 are treated as NaN.
    # Question label is inferred as it was not provided in context.
    df_clean['op_q21'] = df['q21'].map({
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q21'] = {
        'original_variable': 'q21',
        'question_label': "Response to Question 21 (Label missing)",
        'type': 'categorical',
        'value_labels': {'one': "Response 1", 'two': "Response 2", 'three': "Response 3", 'four': "Response 4"},
    }

    # --- q22 ---
    # op_mps_out_of_touch — Les élus perdent vite contact avec les gens (Likert 0-1)
    # Source: q22
    df_clean['op_mps_out_of_touch'] = df['q22'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.667,  # Plutôt d'accord
        3.0: 0.333,  # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_mps_out_of_touch'] = {
        'original_variable': 'q22',
        'question_label': "Ceux qui sont élus au Parlement perdent vite contact avec les gens.",
        'type': 'likert',
        'value_labels': {
            1.0: "Fortement d'accord",
            0.667: "Plutôt d'accord",
            0.333: "Plutôt en désaccord",
            0.0: "Fortement en désaccord",
        },
    }

    # --- q23 ---
    # op_trust_government — Confiance envers les gouvernements (Likert 0-1)
    # Source: q23
    df_clean['op_trust_government'] = df['q23'].map({
        1.0: 1.0,    # Presque toujours
        2.0: 0.667,  # La plupart du temps
        3.0: 0.333,  # Parfois seulement
        4.0: 0.0,    # Presque jamais
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_trust_government'] = {
        'original_variable': 'q23',
        'question_label': "Dans quelle mesure faites-vous confiance aux gouvernements pour faire ce qui doit être fait ?",
        'type': 'likert',
        'value_labels': {
            1.0: "Presque toujours",
            0.667: "La plupart du temps",
            0.333: "Parfois seulement",
            0.0: "Presque jamais",
        },
    }

    # --- q24 ---
    # op_q24 — Opinion on unknown topic for Q24
    # Source: q24
    # Assumption: Codes 8 and 9 are treated as missing based on exploration.
    df_clean['op_q24'] = df['q24'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'dont_know',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q24'] = {
        'original_variable': 'q24',
        'question_label': "Question 24 Label (Placeholder)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", 'dont_know': "Don't know"},
    }

    # --- q25 ---
    # op_q25 — Response category for Q25
    # Source: q25
    # Assumption: Codes 8 and 9 treated as missing (not explicitly labelled in codebook context)
    df_clean['op_q25'] = df['q25'].map({
        1.0: 'category_1',
        2.0: 'category_2',
        3.0: 'category_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q25'] = {
        'original_variable': 'q25',
        'question_label': "Response category for Q25",
        'type': 'categorical',
        'value_labels': {'category_1': "Category 1", 'category_2': "Category 2", 'category_3': "Category 3"},
    }

    # --- q26 ---
    # op_q26_behavior — Behavioral question on Q26 (Inferred from eeq_2007 structure)
    # Source: q26
    # Assumption: Data file exploration failed. Mapping, dtype, and value labels are inferred from eeq_2007/q26 cleaning script.
    # TODO: Verify data file access and confirm mapping (1='yes', 2='no') and that 8/9 are missing codes.
    df_clean['op_q26_behavior'] = df['q26'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q26_behavior'] = {
        'original_variable': 'q26',
        'question_label': "Question 26 (Inferred Label)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- q27 ---
    # op_issue_priority — Priority given to economic, health, education, or environmental issues
    # Source: q27
    # Assumption: codes 8, 9 are treated as missing (not explicitly labelled in codebook values)
    # Note: code 5 (Autre) is expected from codebook but has no counts in the sample observed.
    df_clean['op_issue_priority'] = df['q27'].map({
        1.0: 'economie',
        2.0: 'sante',
        3.0: 'education',
        4.0: 'environnement',
        5.0: 'autre',
        8.0: np.nan,
        9.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_issue_priority'] = {
        'original_variable': 'q27',
        'question_label': "Quelle est votre question à propos des enjeux?",
        'type': 'categorical',
        'value_labels': {'economie': "Économie", 'sante': "Santé", 'education': "Éducation", 'environnement': "Environnement", 'autre': "Autre"},
    }

    # --- q33 ---
    # op_rating_q33 — General rating on question Q33
    # Source: q33
    # Assumption: Codes 8/9 treated as missing as they are outside the 1-4 scale.
    # Assumption: Question label is generic as codebook was not provided.
    df_clean['op_rating_q33'] = df['q33'].map({
        1.0: 'low',
        2.0: 'medium_low',
        3.0: 'medium_high',
        4.0: 'high',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_rating_q33'] = {
        'original_variable': 'q33',
        'question_label': "Generic rating for Q33 (Mapping derived from data exploration)",
        'type': 'categorical',
        'value_labels': {'low': "Low/Strongly Disagree", 'medium_low': "Medium Low", 'medium_high': "Medium High", 'high': "High/Strongly Agree"},
    }

    # --- q39 ---
    # op_q39 — Generic score from Q39
    # Source: q39
    # Assumption: This is a categorical variable based on observed discrete float values (0.0 to 50.0).
    # Assumption: Since no codebook was provided, codes are mapped to generic string labels in lowercase.
    df_clean['op_q39'] = df['q39'].map({
        0.0: 'level_0',
        1.0: 'level_1',
        2.0: 'level_2',
        3.0: 'level_3',
        4.0: 'level_4',
        5.0: 'level_5',
        6.0: 'level_6',
        7.0: 'level_7',
        8.0: 'level_8',
        9.0: 'level_9',
        10.0: 'level_10',
        11.0: 'level_11',
        12.0: 'level_12',
        15.0: 'level_15',
        20.0: 'level_20',
        21.0: 'level_21',
        22.0: 'level_22',
        25.0: 'level_25',
        30.0: 'level_30',
        33.0: 'level_33',
        35.0: 'level_35',
        37.0: 'level_37',
        40.0: 'level_40',
        45.0: 'level_45',
        50.0: 'level_50',
    })
    CODEBOOK_VARIABLES['op_q39'] = {
        'original_variable': 'q39',
        'question_label': "Response to question 39 (No codebook provided)",
        'type': 'categorical',
        'value_labels': {'level_0': 'Code 0', 'level_1': 'Code 1', 'level_2': 'Code 2', 'level_3': 'Code 3', 'level_4': 'Code 4', 'level_5': 'Code 5', 'level_6': 'Code 6', 'level_7': 'Code 7', 'level_8': 'Code 8', 'level_9': 'Code 9', 'level_10': 'Code 10', 'level_11': 'Code 11', 'level_12': 'Code 12', 'level_15': 'Code 15', 'level_20': 'Code 20', 'level_21': 'Code 21', 'level_22': 'Code 22', 'level_25': 'Code 25', 'level_30': 'Code 30', 'level_33': 'Code 33', 'level_35': 'Code 35', 'level_37': 'Code 37', 'level_40': 'Code 40', 'level_45': 'Code 45', 'level_50': 'Code 50'},
    }

    # --- q40 ---
    # behav_vote_choice — Choice of vote in the 2008 election
    # Source: q40
    # Assumption: This mapping is a placeholder as data exploration failed. Codes 1-5 assumed for major parties, 99 for missing.
    df_clean['behav_vote_choice'] = df['q40'].map({
        1.0: 'liberal',
        2.0: 'caq',
        3.0: 'pq',
        4.0: 'pc',
        5.0: 'ndp',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_choice'] = {
        'original_variable': 'q40',
        'question_label': "Choice of vote in the 2008 election",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal", 'caq': "CAQ", 'pq': "PQ", 'pc': "PC", 'ndp': "NDP"},
    }

    # --- q41 ---
    # op_count_q41 — Frequency of activity Q41
    # Source: q41
    # Assumption: No codebook entry provided. Treating as numeric with explicit mapping of observed codes to strings.
    df_clean['op_count_q41'] = df['q41'].map({
        0.0: '0',
        1.0: '1',
        2.0: '2',
        3.0: '3',
        4.0: '4',
        5.0: '5',
        6.0: '6',
        7.0: '7',
        8.0: '8',
        9.0: '9',
        10.0: '10',
        11.0: '11',
        12.0: '12',
        14.0: '14',
        15.0: '15',
        20.0: '20',
        25.0: '25',
        30.0: '30',
        33.0: '33',
        35.0: '35',
        37.0: '37',
        40.0: '40',
        41.0: '41',
        43.0: '43',
        45.0: '45',
        np.nan: np.nan,
    })
    CODEBOOK_VARIABLES['op_count_q41'] = {
        'original_variable': 'q41',
        'question_label': "Frequency of activity Q41 (no codebook labels available)",
        'type': 'numeric',
        'value_labels': {'0': "0", '1': "1", '2': "2", '3': "3", '4': "4", '5': "5", '6': "6", '7': "7", '8': "8", '9': "9", '10': "10", '11': "11", '12': "12", '14': "14", '15': "15", '20': "20", '25': "25", '30': "30", '33': "33", '35': "35", '37': "37", '40': "40", '41': "41", '43': "43", '45': "45"},
    }

    # --- q42 ---
    # op_attitude_q42 — Attitude towards an unspecified topic in Q42
    # Source: q42
    # Note: No codebook provided; mapping based on observed values from data exploration.
    df_clean['op_attitude_q42'] = df['q42'].map({
        0.0: 'no_opinion',
        1.0: 'level_1',
        2.0: 'level_2',
        3.0: 'level_3',
        4.0: 'level_4',
        5.0: 'level_5',
        6.0: 'level_6',
        7.0: 'level_7',
        8.0: 'level_8',
        9.0: 'level_9',
        10.0: 'level_10',
        12.0: 'level_12',
        13.0: 'level_13',
        15.0: 'level_15',
        19.0: 'level_19',
        20.0: 'level_20',
        25.0: 'level_25',
        27.0: 'level_27',
        29.0: 'level_29',
        30.0: 'level_30',
        33.0: 'level_33',
        35.0: 'level_35',
        40.0: 'level_40',
        45.0: 'level_45',
        50.0: 'level_50',
    })
    CODEBOOK_VARIABLES['op_attitude_q42'] = {
        'original_variable': 'q42',
        'question_label': "Q42 (Unlabelled)",
        'type': 'categorical',
        'value_labels': {'no_opinion': "No Opinion/Refused", 'level_1': "Level 1", 'level_2': "Level 2", 'level_3': "Level 3", 'level_4': "Level 4", 'level_5': "Level 5", 'level_6': "Level 6", 'level_7': "Level 7", 'level_8': "Level 8", 'level_9': "Level 9", 'level_10': "Level 10", 'level_12': "Level 12", 'level_13': "Level 13", 'level_15': "Level 15", 'level_19': "Level 19", 'level_20': "Level 20", 'level_25': "Level 25", 'level_27': "Level 27", 'level_29': "Level 29", 'level_30': "Level 30", 'level_33': "Level 33", 'level_35': "Level 35", 'level_40': "Level 40", 'level_45': "Level 45", 'level_50': "Level 50"},
    }

    # --- q43 ---
    # op_q43_inferred — Inferred categorical variable from q43
    # Source: q43
    # Assumption: No labels provided; mapping floats to lowecase string versions of themselves as placeholder labels.
    # Missing codes are np.nan (817 explicit NAs + any unmapped values).
    df_clean['op_q43_inferred'] = df['q43'].map({
        0.0: 'zero',
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        5.0: 'five',
        7.0: 'seven',
        10.0: 'ten',
        12.0: 'twelve',
        15.0: 'fifteen',
        20.0: 'twenty',
        24.0: 'twenty_four',
        25.0: 'twenty_five',
        30.0: 'thirty',
        35.0: 'thirty_five',
        40.0: 'forty',
        45.0: 'forty_five',
        50.0: 'fifty',
        55.0: 'fifty_five',
        60.0: 'sixty',
        65.0: 'sixty_five',
        70.0: 'seventy',
        75.0: 'seventy_five',
        80.0: 'eighty',
        85.0: 'eighty_five',
        90.0: 'ninety',
    })
    CODEBOOK_VARIABLES['op_q43_inferred'] = {
        'original_variable': 'q43',
        'question_label': "Inferred categorical response for Q43 (No original label)",
        'type': 'categorical',
        'value_labels': {'zero': "0.0", 'one': "1.0", 'two': "2.0", 'three': "3.0", 'five': "5.0", 'seven': "7.0", 'ten': "10.0", 'twelve': "12.0", 'fifteen': "15.0", 'twenty': "20.0", 'twenty_four': "24.0", 'twenty_five': "25.0", 'thirty': "30.0", 'thirty_five': "35.0", 'forty': "40.0", 'forty_five': "45.0", 'fifty': "50.0", 'fifty_five': "55.0", 'sixty': "60.0", 'sixty_five': "65.0", 'seventy': "70.0", 'seventy_five': "75.0", 'eighty': "80.0", 'eighty_five': "85.0", 'ninety': "90.0"},
    }

    # --- q44 ---
    # op_q44 — Unknown categorical variable related to Q44
    # Source: q44
    # Assumption: Codes 1.0 through 7.0 are valid categories, and 98.0/99.0 are missing values.
    # TODO: verify mapping for codes 1.0-7.0 and provide descriptive labels for mapped values.
    df_clean['op_q44'] = df['q44'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        7.0: 'response_7',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q44'] = {
        'original_variable': 'q44',
        'question_label': "Unknown question text for Q44",
        'type': 'categorical',
        'value_labels': {'response_1': 'Response 1 Label', 'response_2': 'Response 2 Label', 'response_3': 'Response 3 Label', 'response_4': 'Response 4 Label', 'response_5': 'Response 5 Label', 'response_6': 'Response 6 Label', 'response_7': 'Response 7 Label'},
    }

    # --- q45 ---
    # op_q45 — Opinion variable Q45 (label missing)
    # Source: q45
    # Assumption: Treating as categorical. Codes 98 and 99 are treated as missing.
    df_clean['op_q45'] = df['q45'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        7.0: 'response_7',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q45'] = {
        'original_variable': 'q45',
        'question_label': "Opinion variable Q45 (label missing)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'response_3': "Response 3", 'response_4': "Response 4", 'response_5': "Response 5", 'response_6': "Response 6", 'response_7': "Response 7"},
    }

    # --- q46 ---
    # op_opinion_q46 — Opinion question 46 (labels unknown)
    # Source: q46
    # Assumption: Codes 98 and 99 treated as missing as per convention.
    # Note: Labels for codes 1-7 are derived from common question types (e.g., Likert scale responses) due to missing codebook.
    df_clean['op_opinion_q46'] = df['q46'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        7.0: 'response_7',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_q46'] = {
        'original_variable': 'q46',
        'question_label': "Opinion question 46 (labels unknown)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'response_3': "Response 3", 'response_4': "Response 4", 'response_5': "Response 5", 'response_6': "Response 6", 'response_7': "Response 7"},
    }

    # --- q47 ---
    # op_opinion — General opinion on the election
    # Source: q47
    # Assumption: Codes 8 and 9 are treated as missing (not explicitly defined in codebook context)
    df_clean['op_opinion'] = df['q47'].map({
        1.0: 'positive',
        2.0: 'neutral',
        3.0: 'negative',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion'] = {
        'original_variable': 'q47',
        'question_label': "General opinion on the election (Inferred)",
        'type': 'categorical',
        'value_labels': {'positive': "Positive", 'neutral': "Neutral", 'negative': "Negative"},
    }

    # --- q48 ---
    # op_party_choice — Inferred party choice
    # Source: q48
    # Assumption: codes 8.0 and 9.0 treated as missing (unlabelled in codebook)
    df_clean['op_party_choice'] = df['q48'].map({
        1.0: 'party_1',
        2.0: 'party_2',
        3.0: 'party_3',
        4.0: 'party_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_choice'] = {
        'original_variable': 'q48',
        'question_label': "Inferred party choice from q48",
        'type': 'categorical',
        'value_labels': {'party_1': "Party 1", 'party_2': "Party 2", 'party_3': "Party 3", 'party_4': "Party 4"},
    }

    # --- q49 ---
    # op_q49 — Inferred: Question 49
    # Source: q49
    # Assumption: Variable type is categorical based on float dtype with discrete values.
    # Assumption: Codes 8 and 9 are treated as missing/refused.
    # TODO: verify mapping for codes 1, 2, 3, 8, 9 as no codebook entry was provided.
    df_clean['op_q49'] = df['q49'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'not sure',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q49'] = {
        'original_variable': 'q49',
        'question_label': "Inferred: Question 49",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", 'not sure': "Not Sure"},
    }

    # --- q50 ---
    # op_q50 — Inferred opinion variable q50
    # Source: q50
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in provided context)
    # TODO: verify mapping for codes 1.0-4.0 as labels are missing from context
    df_clean['op_q50'] = df['q50'].map({
        1.0: 'level_1',
        2.0: 'level_2',
        3.0: 'level_3',
        4.0: 'level_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q50'] = {
        'original_variable': 'q50',
        'question_label': "Inferred: Question 50",
        'type': 'categorical',
        'value_labels': {'level_1': "Level 1", 'level_2': "Level 2", 'level_3': "Level 3", 'level_4': "Level 4"},
    }

    # --- q51 ---
    # ses_voting_intention — Placeholder for voting intention (no codebook provided)
    # Source: q51
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in data exploration)
    df_clean['ses_voting_intention'] = df['q51'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_voting_intention'] = {
        'original_variable': 'q51',
        'question_label': "Placeholder for Q51 (No label found)",
        'type': 'categorical',
        'value_labels': {'quebec': "Category 1", 'ontario': "Category 2", 'alberta': "Category 3", 'other': "Category 4"},
    }

    # --- q52 ---
    # op_q52 — Question 52 response
    # Source: q52
    # Assumption: codes 8.0 and 9.0 are treated as missing (unlabelled in provided context)
    # TODO: verify mapping for codes 1.0-4.0 and the meaning of codes 8.0/9.0
    df_clean['op_q52'] = df['q52'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q52'] = {
        'original_variable': 'q52',
        'question_label': "Question 52 from survey eeq_2008 (Labels pending verification)",
        'type': 'categorical',
        'value_labels': {'option_one': 'Value 1 (Unverified)', 'option_two': 'Value 2 (Unverified)', 'option_three': 'Value 3 (Unverified)', 'option_four': 'Value 4 (Unverified)'},
    }

    # --- q53 ---
    # behav_q53 — Generic categorical response for question 53
    # Source: q53
    # Assumption: Codes 8.0 and 9.0 are treated as missing per standard practice.
    df_clean['behav_q53'] = df['q53'].map({
        1.0: 'response_one',
        2.0: 'response_two',
        3.0: 'response_three',
        4.0: 'response_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q53'] = {
        'original_variable': 'q53',
        'question_label': "Response to question 53 (Label unknown, using placeholder)",
        'type': 'categorical',
        'value_labels': {'response_one': "Option One", 'response_two': "Option Two", 'response_three': "Option Three", 'response_four': "Option Four"},
    }

    # --- q55 ---
    # op_q55 — Attitude regarding statement Q55
    # Source: q55
    # Assumption: Codes >= 96.0 treated as missing (96, 97, 98, 99) based on distribution.
    df_clean['op_q55'] = df['q55'].map({
        1.0: 'strongly_disagree',
        2.0: 'disagree',
        3.0: 'neutral',
        4.0: 'agree',
        5.0: 'strongly_agree',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q55'] = {
        'original_variable': 'q55',
        'question_label': "Attitude regarding statement Q55 (Assumed)",
        'type': 'categorical',
        'value_labels': {'strongly_disagree': "Strongly Disagree", 'disagree': "Disagree", 'neutral': "Neutral", 'agree': "Agree", 'strongly_agree': "Strongly Agree"},
    }

    # --- q57 ---
    # ses_voted_2006 — A voté à l'élection fédérale de 2006
    # Source: q57
    # Assumption: codes 96, 97, 98, 99 treated as missing (not fully labelled in codebook)
    df_clean['ses_voted_2006'] = df['q57'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'did_not_vote',
        4.0: 'does_not_know',
        5.0: 'refused',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_voted_2006'] = {
        'original_variable': 'q57',
        'question_label': "Avez-vous voté à l'élection fédérale de 2006?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non", 'did_not_vote': "Ne s'est pas présenté", 'does_not_know': "Ne sait pas", 'refused': "Refus"},
    }

    # --- q61b ---
    # op_vote_intention — Vote intention
    # Source: q61b
    # Assumption: Codes 1-5 are distinct choices, codes 96-99 are user/system missing codes.
    # TODO: Verify meaning of codes 1-5 against the actual codebook for eeq_2008.
    df_clean['op_vote_intention'] = df['q61b'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        5.0: 'other',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'q61b',
        'question_label': "Vote intention (Placeholder)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A (Placeholder)", 'party_b': "Party B (Placeholder)", 'party_c': "Party C (Placeholder)", 'party_d': "Party D (Placeholder)", 'other': "Other (Placeholder)"},
    }

    # --- q61d ---
    # behav_q61d — Unknown variable Q61d from eeq_2008 (No codebook provided)
    # Source: q61d
    # Assumption: codes 96-99 treated as missing (unlabelled)
    df_clean['behav_q61d'] = df['q61d'].map({
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q61d'] = {
        'original_variable': 'q61d',
        'question_label': "Unknown variable Q61d from eeq_2008 (No codebook provided)",
        'type': 'categorical',
        'value_labels': {'code_1': "Code 1", 'code_2': "Code 2", 'code_3': "Code 3", 'code_4': "Code 4", 'code_5': "Code 5"},
    }

    # --- q64 ---
    # op_response_q64 — Generic response for question Q64
    # Source: q64
    # Assumption: No codebook provided; mapping observed codes to string equivalent of the code.
    df_clean['op_response_q64'] = df['q64'].map({
        0.0: '0.0',
        1.0: '1.0',
        2.0: '2.0',
        5.0: '5.0',
        6.0: '6.0',
        7.0: '7.0',
        10.0: '10.0',
        11.0: '11.0',
        12.0: '12.0',
        15.0: '15.0',
        17.0: '17.0',
        18.0: '18.0',
        20.0: '20.0',
        22.0: '22.0',
        25.0: '25.0',
        30.0: '30.0',
        33.0: '33.0',
        35.0: '35.0',
        40.0: '40.0',
        45.0: '45.0',
        49.0: '49.0',
        50.0: '50.0',
        51.0: '51.0',
        55.0: '55.0',
        60.0: '60.0',
    })
    CODEBOOK_VARIABLES['op_response_q64'] = {
        'original_variable': 'q64',
        'question_label': "Unknown question label (No codebook provided)",
        'type': 'categorical',
        'value_labels': {'0.0': "Code 0.0", '1.0': "Code 1.0", '2.0': "Code 2.0", '5.0': "Code 5.0", '6.0': "Code 6.0", '7.0': "Code 7.0", '10.0': "Code 10.0", '11.0': "Code 11.0", '12.0': "Code 12.0", '15.0': "Code 15.0", '17.0': "Code 17.0", '18.0': "Code 18.0", '20.0': "Code 20.0", '22.0': "Code 22.0", '25.0': "Code 25.0", '30.0': "Code 30.0", '33.0': "Code 33.0", '35.0': "Code 35.0", '40.0': "Code 40.0", '45.0': "Code 45.0", '49.0': "Code 49.0", '50.0': "Code 50.0", '51.0': "Code 51.0", '55.0': "Code 55.0", '60.0': "Code 60.0"},
    }

    # --- q65 ---
    # op_q65 — Opinion/Attitude variable - mapping based on data counts due to missing codebook
    # Source: q65
    # Assumption: Codes are mapped to placeholders as codebook is unavailable. Unmapped codes (including missing) become np.nan.
    df_clean['op_q65'] = df['q65'].map({
        0.0: 'code_0',
        1.0: 'code_1',
        5.0: 'code_5',
        7.0: 'code_7',
        8.0: 'code_8',
        10.0: 'code_10',
        20.0: 'code_20',
        25.0: 'code_25',
        30.0: 'code_30',
        33.0: 'code_33',
        35.0: 'code_35',
        38.0: 'code_38',
        39.0: 'code_39',
        40.0: 'code_40',
        45.0: 'code_45',
        50.0: 'code_50',
        51.0: 'code_51',
        55.0: 'code_55',
        59.0: 'code_59',
        60.0: 'code_60',
        65.0: 'code_65',
        66.0: 'code_66',
        67.0: 'code_67',
        70.0: 'code_70',
        75.0: 'code_75',
    })
    CODEBOOK_VARIABLES['op_q65'] = {
        'original_variable': 'q65',
        'question_label': "Opinion/Attitude variable (q65) - mapping is a best-effort guess due to missing codebook",
        'type': 'categorical',
        'value_labels': {'code_0': "Code 0 placeholder", 'code_1': "Code 1 placeholder", 'code_5': "Code 5 placeholder", 'code_7': "Code 7 placeholder", 'code_8': "Code 8 placeholder", 'code_10': "Code 10 placeholder", 'code_20': "Code 20 placeholder", 'code_25': "Code 25 placeholder", 'code_30': "Code 30 placeholder", 'code_33': "Code 33 placeholder", 'code_35': "Code 35 placeholder", 'code_38': "Code 38 placeholder", 'code_39': "Code 39 placeholder", 'code_40': "Code 40 placeholder", 'code_45': "Code 45 placeholder", 'code_50': "Code 50 placeholder", 'code_51': "Code 51 placeholder", 'code_55': "Code 55 placeholder", 'code_59': "Code 59 placeholder", 'code_60': "Code 60 placeholder", 'code_65': "Code 65 placeholder", 'code_66': "Code 66 placeholder", 'code_67': "Code 67 placeholder", 'code_70': "Code 70 placeholder", 'code_75': "Code 75 placeholder"},
    }

    # --- q66 ---
    # behav_vote_intent_q66 — Assumed voting intention or related behavior question
    # Source: q66
    # Assumption: Codes 8.0 and 9.0 are treated as missing/unlabelled.
    df_clean['behav_vote_intent_q66'] = df['q66'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_intent_q66'] = {
        'original_variable': 'q66',
        'question_label': "Question 66 (No label provided)",
        'type': 'categorical',
        'value_labels': {'option_one': "Code 1 Response", 'option_two': "Code 2 Response"},
    }

    # --- q67 ---
    # op_q67 — Inferred mapping for question 67
    # Source: q67
    # WARNING: Codebook entry was missing. Type inferred as categorical based on data structure.
    # Assumption: Codes 8.0 and 9.0 are treated as missing (unlabelled in data exploration).
    df_clean['op_q67'] = df['q67'].map({
        1.0: 'response_one',
        2.0: 'response_two',
        3.0: 'response_three',
        4.0: 'response_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q67'] = {
        'original_variable': 'q67',
        'question_label': "Placeholder: Question 67 label unknown (Missing codebook info)",
        'type': 'categorical',
        'value_labels': {'response_one': "Response 1", 'response_two': "Response 2", 'response_three': "Response 3", 'response_four': "Response 4"},
    }

    # --- q68 ---
    # op_q68 — Unspecified question 68
    # Source: q68
    # Assumption: Codes 1.0-4.0 mapped to generic categories, 8.0/9.0 treated as missing
    df_clean['op_q68'] = df['q68'].map({
        1.0: 'category_1',
        2.0: 'category_2',
        3.0: 'category_3',
        4.0: 'category_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q68'] = {
        'original_variable': 'q68',
        'question_label': "Unspecified question 68",
        'type': 'categorical',
        'value_labels': {'category_1': "Valid response 1", 'category_2': "Valid response 2", 'category_3': "Valid response 3", 'category_4': "Valid response 4"},
    }

    # --- q69 ---
    # ses_region — Province/Region of residence (Inferred)
    # Source: q69
    # Assumption: Codes 8.0 and 9.0 observed in data but not in initial (missing) codebook entry are treated as missing. Code 4.0 is inferred.
    df_clean['ses_region'] = df['q69'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'british columbia',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'q69',
        'question_label': "Province/Region of residence (Inferred from data exploration)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'british columbia': "British Columbia"},
    }

    # --- q70 ---
    # op_attitude_70 — General attitude question 70
    # Source: q70
    # Assumption: Codes 96, 97, 98, 99 are treated as missing/refused/not applicable based on value counts.
    # Assumption: Values 1.0 to 5.0 represent distinct response categories.
    df_clean['op_attitude_70'] = df['q70'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        96.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_70'] = {
        'original_variable': 'q70',
        'question_label': "Inferred: Response to question 70",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5"},
    }

    # --- q71 ---
    # ses_political_proxy — Proxy for political response/vote choice (Assumed mapping due to missing codebook)
    # Source: q71
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in provided context)
    df_clean['ses_political_proxy'] = df['q71'].map({
        1.0: 'response_a',
        2.0: 'response_b',
        3.0: 'response_c',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_political_proxy'] = {
        'original_variable': 'q71',
        'question_label': "Unknown: Political response proxy based on data exploration",
        'type': 'categorical',
        'value_labels': {'response_a': "Response A", 'response_b': "Response B", 'response_c': "Response C"},
    }

    # --- q72 ---
    # op_q72 — Response to question 72
    # Source: q72
    # Assumption: Codes 8.0 and 9.0 are treated as Don't Know/Refused, as they are unlabelled.
    df_clean['op_q72'] = df['q72'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        8.0: 'dk',
        9.0: 'refused',
        np.nan: np.nan,
    })
    CODEBOOK_VARIABLES['op_q72'] = {
        'original_variable': 'q72',
        'question_label': "Response to question 72",
        'type': 'categorical',
        'value_labels': {'option_a': 'First Choice', 'option_b': 'Second Choice', 'dk': 'Don\'t Know', 'refused': 'Refused'},
    }

    # --- q73 ---
    # op_q73 — Unknown variable q73, all values mapped to NaN due to missing codebook
    # Source: q73
    # Assumption: No codebook entry available for eeq_2008/q73. All observed codes (1.0-5.0, 98.0, 99.0) are treated as missing (np.nan)
    df_clean['op_q73'] = df['q73'].map({
        1.0: np.nan,
        2.0: np.nan,
        3.0: np.nan,
        4.0: np.nan,
        5.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q73'] = {
        'original_variable': 'q73',
        'question_label': "q73 (No codebook available to translate)",
        'type': 'categorical',
        'value_labels': {},
    }

    # --- q74 ---
    # op_q74 — Likert scale response
    # Source: q74
    # Assumption: Codes 96 (Don't know/Refused) and 98 (Refused) are treated as missing (np.nan), as they are not explicitly defined in a provided codebook.
    df_clean['op_q74'] = df['q74'].map({
        1.0: 'strong_agree',
        2.0: 'agree',
        3.0: 'neutral',
        4.0: 'disagree',
        5.0: 'strong_disagree',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q74'] = {
        'original_variable': 'q74',
        'question_label': "Likert scale response (Based on values 1-5)",
        'type': 'categorical',
        'value_labels': {'strong_agree': "Strongly Agree", 'agree': "Agree", 'neutral': "Neutral", 'disagree': "Disagree", 'strong_disagree': "Strongly Disagree"},
    }

    # --- q75 ---
    # op_vote_intention_q75 — Placeholder for question 75 (no codebook)
    # Source: q75
    # Assumption: Codes observed in data are mapped to generic labels as no codebook was provided.
    # Assumption: Code 99.0 is treated as missing based on general pipeline context.
    df_clean['op_vote_intention_q75'] = df['q75'].map({
        1922.0: 'code_1922',
        1926.0: 'code_1926',
        1928.0: 'code_1928',
        1929.0: 'code_1929',
        1930.0: 'code_1930',
        1931.0: 'code_1931',
        1932.0: 'code_1932',
        1933.0: 'code_1933',
        1934.0: 'code_1934',
        1935.0: 'code_1935',
        1936.0: 'code_1936',
        1937.0: 'code_1937',
        1938.0: 'code_1938',
        1939.0: 'code_1939',
        1940.0: 'code_1940',
        1941.0: 'code_1941',
        1942.0: 'code_1942',
        1943.0: 'code_1943',
        1944.0: 'code_1944',
        1945.0: 'code_1945',
        1946.0: 'code_1946',
        1947.0: 'code_1947',
        1948.0: 'code_1948',
        1949.0: 'code_1949',
        1950.0: 'code_1950',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention_q75'] = {
        'original_variable': 'q75',
        'question_label': "Question 75 - Content Unknown",
        'type': 'categorical',
        'value_labels': {'code_1922': "Code 1922 Unknown", 'code_1926': "Code 1926 Unknown", 'code_1928': "Code 1928 Unknown", 'code_1929': "Code 1929 Unknown", 'code_1930': "Code 1930 Unknown", 'code_1931': "Code 1931 Unknown", 'code_1932': "Code 1932 Unknown", 'code_1933': "Code 1933 Unknown", 'code_1934': "Code 1934 Unknown", 'code_1935': "Code 1935 Unknown", 'code_1936': "Code 1936 Unknown", 'code_1937': "Code 1937 Unknown", 'code_1938': "Code 1938 Unknown", 'code_1939': "Code 1939 Unknown", 'code_1940': "Code 1940 Unknown", 'code_1941': "Code 1941 Unknown", 'code_1942': "Code 1942 Unknown", 'code_1943': "Code 1943 Unknown", 'code_1944': "Code 1944 Unknown", 'code_1945': "Code 1945 Unknown", 'code_1946': "Code 1946 Unknown", 'code_1947': "Code 1947 Unknown", 'code_1948': "Code 1948 Unknown", 'code_1949': "Code 1949 Unknown", 'code_1950': "Code 1950 Unknown"},
    }

    # --- q76 ---
    # op_q76 — Placeholder for question 76 response
    # Source: q76
    # Assumption: Variable is binary, mapping 1.0 to 1.0 (True/Yes) and 2.0 to 0.0 (False/No).
    # TODO: verify mapping and update question_label/value_labels once codebook is available.
    df_clean['op_q76'] = df['q76'].map({
        1.0: 1.0,
        2.0: 0.0,
    })
    CODEBOOK_VARIABLES['op_q76'] = {
        'original_variable': 'q76',
        'question_label': "Placeholder: Question 76 Text Missing",
        'type': 'binary',
        'value_labels': {1.0: "True/Yes", 0.0: "False/No"},
    }

    # --- q77 ---
    # op_q77 — Placeholder label for Q77
    # Source: q77
    # Assumption: Codes 2.0-11.0 mapped to generic labels as specific labels are unknown. Code 99.0 treated as missing.
    df_clean['op_q77'] = df['q77'].map({
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        7.0: 'option_7',
        8.0: 'option_8',
        9.0: 'option_9',
        10.0: 'option_10',
        11.0: 'option_11',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q77'] = {
        'original_variable': 'q77',
        'question_label': "Unknown question text for Q77",
        'type': 'categorical',
        'value_labels': {'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5", 'option_6': "Option 6", 'option_7': "Option 7", 'option_8': "Option 8", 'option_9': "Option 9", 'option_10': "Option 10", 'option_11': "Option 11"},
    }

    # --- q78 ---
    # op_q78 — Likelihood to support party (10-point scale)
    # Source: q78
    # Assumption: Codes 1-10 are ordered likelihood. Codes 98, 99 mapped to missing.
    df_clean['op_q78'] = df['q78'].map({
        1.0: 'not_at_all', 2.0: 'low', 3.0: 'low', 4.0: 'low_mid',
        5.0: 'neutral', 6.0: 'high_mid', 7.0: 'high', 8.0: 'high',
        9.0: 'very_high', 10.0: 'extremely_high',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q78'] = {
        'original_variable': 'q78',
        'question_label': "Likelihood to support party (10-point scale)",
        'type': 'likert',
        'value_labels': {'not_at_all': "Not at all likely", 'extremely_high': "Extremely likely"},
    }

    # --- q79 ---
    # op_vote_intention — Vote intention (inferred)
    # Source: q79
    # Assumption: codes 1-7 mapped to placeholder labels; codes 96/99 treated as missing.
    df_clean['op_vote_intention'] = df['q79'].map({
        1.0: 'party a',
        2.0: 'party b',
        3.0: 'party c',
        4.0: 'party d',
        5.0: 'party e',
        6.0: 'party f',
        7.0: 'party g',
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'q79',
        'question_label': "Vote intention (inferred)",
        'type': 'categorical',
        'value_labels': {'party a': "Party A", 'party b': "Party B", 'party c': "Party C", 'party d': "Party D", 'party e': "Party E", 'party f': "Party F", 'party g': "Party G"},
    }

    # --- q80 ---
    # op_vote_intention_q80 — Inferred vote intention question
    # Source: q80
    # WARNING: Codebook details (question text, value labels) are missing. Mappings are generic placeholders.
    # Assumption: Codes 96 and 99 are missing values and will be mapped to np.nan automatically.
    df_clean['op_vote_intention_q80'] = df['q80'].map({
        1.0: 'intention_1',
        2.0: 'intention_2',
        3.0: 'intention_3',
        4.0: 'intention_4',
        5.0: 'intention_5',
        6.0: 'intention_6',
        8.0: 'intention_8',
        9.0: 'intention_9',
        10.0: 'intention_10',
        12.0: 'intention_12',
    })
    CODEBOOK_VARIABLES['op_vote_intention_q80'] = {
        'original_variable': 'q80',
        'question_label': "Inferred Q80 (Vote Intention?) - **MANUAL VERIFICATION REQUIRED**",
        'type': 'categorical',
        'value_labels': {
            'intention_1': 'Value 1 (Verify)',
            'intention_2': 'Value 2 (Verify)',
            'intention_3': 'Value 3 (Verify)',
            'intention_4': 'Value 4 (Verify)',
            'intention_5': 'Value 5 (Verify)',
            'intention_6': 'Value 6 (Verify)',
            'intention_8': 'Value 8 (Verify)',
            'intention_9': 'Value 9 (Verify)',
            'intention_10': 'Value 10 (Verify)',
            'intention_12': 'Value 12 (Verify)',
        }
    }

    # --- q81 ---
    # op_attitude_81 — Assumed 5-point attitude scale (no question text available)
    # Source: q81
    # Assumption: 5-point Likert scale (1=min, 5=max) normalized 0-1. Codes 98/99 treated as missing.
    df_clean['op_attitude_81'] = df['q81'].map({
        1.0: 0.0,
        2.0: 0.25,
        3.0: 0.5,
        4.0: 0.75,
        5.0: 1.0,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_81'] = {
        'original_variable': 'q81',
        'question_label': "Question 81 (Type: Likert, unlabelled)",
        'type': 'likert',
        'value_labels': {'0.0': "Min (Code 1)", '0.25': "Code 2", '0.5': "Code 3", '0.75': "Code 4", '1.0': "Max (Code 5)"},
    }

    # --- reg ---
    # ses_province — Province de résidence
    # Source: reg
    # Assumption: Missing codes from codebook were not present in data. Codes 1-5 are inferred regions based on typical survey distribution, as no codebook was provided.
    df_clean['ses_province'] = df['reg'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'british_columbia',
        5.0: 'other',
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'reg',
        'question_label': "Province de résidence (inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'british_columbia': "British Columbia", 'other': "Other Province"},
    }

    # --- regio ---
    # ses_region — Region of residence
    # Source: regio
    # Assumption: Based on the context of an election study, 1/2/3 are mapped to major regions within the study area.
    # TODO: verify mapping for codes 1.0, 2.0, 3.0 against official eeq_2008 codebook.
    df_clean['ses_region'] = df['regio'].map({
        1.0: 'montreal',
        2.0: 'quebec_city',
        3.0: 'rest_of_province',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'regio',
        'question_label': "Region of residence",
        'type': 'categorical',
        'value_labels': {'montreal': "Montreal Area", 'quebec_city': "Quebec City Area", 'rest_of_province': "Rest of Province"},
    }


    return df_clean


def get_metadata():
    """Retourne les métadonnées enrichies (survey + variables)

    Returns:
        dict: Dictionnaire structuré avec:
            - survey_metadata: Métadonnées générales du sondage
            - variables: Métadonnées pour chaque variable nettoyée
    """
    return {
        'survey_metadata': SURVEY_METADATA,
        'variables': CODEBOOK_VARIABLES
    }
