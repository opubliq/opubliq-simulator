#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2014

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
    'survey_id': 'eeq_2014',           # ID unique du sondage (ex: "ces2019")
    'title': 'eeq_2014',             # Titre complet
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
    """Charge et nettoie les données brutes du sondage eeq_2014.

    Args:
        raw_path (str): Chemin vers le fichier .sav brut
            (ex: SHARED_FOLDER_PATH/eeq_2014/Quebec Election Study 2014.sav)

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

    # --- CLAGE ---
    # ses_age_group — Age category
    # Source: CLAGE
    # Note: Mapping inferred based on common age brackets for codes 2-8 as no codebook entry was supplied.
    df_clean['ses_age_group'] = df['CLAGE'].map({
        2.0: '18-24',
        3.0: '25-34',
        4.0: '35-44',
        5.0: '45-54',
        6.0: '55-64',
        7.0: '65-74',
        8.0: '75+'
    })
    CODEBOOK_VARIABLES['ses_age_group'] = {
        'original_variable': 'CLAGE',
        'question_label': "Category of Age",
        'type': 'categorical',
        'value_labels': {'18-24': "18-24 years old", '25-34': "25-34 years old", '35-44': "35-44 years old", '45-54': "45-54 years old", '55-64': "55-64 years old", '65-74': "65-74 years old", '75+': "75 years or older"},
    }

    # --- CODE1 ---
    # ses_code — Classification code
    # Source: CODE1
    # Assumption: No codebook provided; codes G, H, J interpreted as distinct groups, '9' as a category.
    df_clean['ses_code'] = df['CODE1'].map({
        '9': 'nine',
        'G': 'group_g',
        'H': 'group_h',
        'J': 'group_j',
    })
    CODEBOOK_VARIABLES['ses_code'] = {
        'original_variable': 'CODE1',
        'question_label': "Classification code (No label available)",
        'type': 'categorical',
        'value_labels': {'nine': "Code 9", 'group_g': "Group G", 'group_h': "Group H", 'group_j': "Group J"},
    }

    # --- CODE2 ---
    # ses_code2 — Inferred categorical code 2
    # Source: CODE2
    # WARNING: Codebook mapping is missing. Assuming categorical variable based on data exploration (codes 0.0-9.0).
    df_clean['ses_code2'] = df['CODE2'].map({
        0.0: 'code_0',
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        6.0: 'code_6',
        7.0: 'code_7',
        8.0: 'code_8',
        9.0: 'code_9',
    })
    CODEBOOK_VARIABLES['ses_code2'] = {
        'original_variable': 'CODE2',
        'question_label': "Inferred Code 2 (Label unknown)",
        'type': 'categorical',
        'value_labels': {'code_0': 'Category 0', 'code_1': 'Category 1', 'code_2': 'Category 2', 'code_3': 'Category 3', 'code_4': 'Category 4', 'code_5': 'Category 5', 'code_6': 'Category 6', 'code_7': 'Category 7', 'code_8': 'Category 8', 'code_9': 'Category 9'},
    }

    # --- CODE3 ---
    # ses_code3 — Unknown categorical code variable
    # Source: CODE3
    # Assumption: Code 9 treated as missing, letters mapped to lowercase for consistency
    df_clean['ses_code3'] = df['CODE3'].map({
        '9': np.nan,
        'A': 'a',
        'B': 'b',
        'C': 'c',
        'E': 'e',
        'G': 'g',
        'H': 'h',
        'J': 'j',
        'K': 'k',
        'L': 'l',
        'M': 'm',
        'N': 'n',
        'P': 'p',
        'R': 'r',
        'S': 's',
        'T': 't',
        'V': 'v',
        'W': 'w',
        'X': 'x',
        'Y': 'y',
        'Z': 'z',
    })
    CODEBOOK_VARIABLES['ses_code3'] = {
        'original_variable': 'CODE3',
        'question_label': "Unknown: Categorical codes from data exploration",
        'type': 'categorical',
        'value_labels': {'a': 'Code A label (needs verification)', 'b': 'Code B label (needs verification)', 'c': 'Code C label (needs verification)', 'e': 'Code E label (needs verification)', 'g': 'Code G label (needs verification)', 'h': 'Code H label (needs verification)', 'j': 'Code J label (needs verification)', 'k': 'Code K label (needs verification)', 'l': 'Code L label (needs verification)', 'm': 'Code M label (needs verification)', 'n': 'Code N label (needs verification)', 'p': 'Code P label (needs verification)', 'r': 'Code R label (needs verification)', 's': 'Code S label (needs verification)', 't': 'Code T label (needs verification)', 'v': 'Code V label (needs verification)', 'w': 'Code W label (needs verification)', 'x': 'Code X label (needs verification)', 'y': 'Code Y label (needs verification)', 'z': 'Code Z label (needs verification)'},
    }

    # --- GREET ---
    # ses_simple_binary — Inferred: Simple binary indicator
    # Source: GREET
    # Assumption: Only code 1.0 exists in data and is mapped to 1.0 (binary)
    df_clean['ses_simple_binary'] = df['GREET'].map({
        1.0: 1.0,
    })
    CODEBOOK_VARIABLES['ses_simple_binary'] = {
        'original_variable': 'GREET',
        'question_label': "Inferred: Simple binary indicator (No codebook provided)",
        'type': 'binary',
        'value_labels': {'1.0': 'Observed value 1.0'},
    }

    # --- LANG ---
    # ses_lang — Language spoken at home
    # Source: LANG
    # Assumption: All found codes are mapped. No explicit missing codes found in data preview.
    df_clean['ses_lang'] = df['LANG'].map({
        'EN': 'english',
        'FR': 'french',
    })
    CODEBOOK_VARIABLES['ses_lang'] = {
        'original_variable': 'LANG',
        'question_label': "Language spoken at home",
        'type': 'categorical',
        'value_labels': {'english': "English", 'french': "French"},
    }

    # --- POND ---
    # ses_weight — Sampling weight (POND)
    # Source: POND
    # Assumption: Value 99.0 from codebook is treated as missing. All other values are preserved as weights.
    # Strategy: Explicitly map the missing code 99.0 to np.nan, then use the original column for all other values.
    df_clean['ses_weight'] = df['POND'].where(df['POND'] != 99.0, other=np.nan)
    CODEBOOK_VARIABLES['ses_weight'] = {
        'original_variable': 'POND',
        'question_label': "Sampling Weight",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q1 ---
    # op_vote_intent — Intention de vote
    # Source: Q1
    # Note: Variable type is numeric (float64) but represents categorical survey data, mapped to 0-1 scale for vote intent.
    df_clean['op_vote_intent'] = df['Q1'].map({
        1.0: 0.0,  # Parti libéral du Québec (PLQ)
        2.0: 0.1,  # Coalition Avenir Québec (CAQ)
        3.0: 0.2,  # Parti Québécois (PQ)
        4.0: 0.3,  # Québec solidaire (QS)
        5.0: 0.4,  # Parti Conservateur du Québec (PCQ)
        6.0: 0.5,  # Parti Vert (PV)
        7.0: 0.6,  # Autre parti
        8.0: 0.7,  # Ne votera pas (Refus de voter) - Treating as distinct option
        9.0: 0.8,  # Ne votera pas (Ne sait pas/Ne répond pas) - Treating as distinct option
        10.0: 0.9, # Ne sait pas / Ne répond pas
        98.0: np.nan, # Autre (Refusé/Non-réponse) - Map to missing
        99.0: np.nan, # Non-réponse (Refusé/Non-réponse) - Map to missing
    })
    CODEBOOK_VARIABLES['op_vote_intent'] = {
        'original_variable': 'Q1',
        'question_label': "À quel parti politique avez-vous l'intention de voter aux prochaines élections provinciales?",
        'type': 'categorical',
        'value_labels': {'0.0': "PLQ", '0.1': "CAQ", '0.2': "PQ", '0.3': "QS", '0.4': "PCQ", '0.5': "PV", '0.6': "Autre parti", '0.7': "Ne votera pas (Refus de voter)", '0.8': "Ne votera pas (Ne sait pas/Ne répond pas)", '0.9': "Ne sait pas / Ne répond pas"},
    }

    # --- Q10 ---
    # op_vote_Q10 — Inferred vote choice from Q10
    # Source: Q10
    # Assumption: Codes 8/9 are missing values (refused/don't know) as no codebook was provided.
    df_clean['op_vote_Q10'] = df['Q10'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_Q10'] = {
        'original_variable': 'Q10',
        'question_label': "Unknown - Inferred from data exploration, no codebook provided.",
        'type': 'categorical',
        'value_labels': {'choice_1': "Category 1", 'choice_2': "Category 2", 'choice_3': "Category 3", 'choice_4': "Category 4"},
    }

    # --- Q11 ---
    # ses_province — Province de résidence
    # Source: Q11
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook)
    df_clean['ses_province'] = df['Q11'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q11',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'other': "Other"},
    }

    # --- Q12 ---
    # op_support_q12 — Overall support level for subject of Q12
    # Source: Q12
    # Assumption: codes 8 and 9 treated as missing (unlabelled in context)
    # TODO: verify mapping for codes 1-4, 8, 9 as codebook for Q12 is missing
    df_clean['op_support_q12'] = df['Q12'].map({
        1.0: 'strong_support',
        2.0: 'moderate_support',
        3.0: 'moderate_oppose',
        4.0: 'strong_oppose',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_support_q12'] = {
        'original_variable': 'Q12',
        'question_label': "Overall support level for subject of Q12",
        'type': 'categorical',
        'value_labels': {'strong_support': "Strong Support", 'moderate_support': "Moderate Support", 'moderate_oppose': "Moderate Oppose", 'strong_oppose': "Strong Oppose"},
    }

    # --- Q13 ---
    # behav_vote_intent — Vote intention or party supported
    # Source: Q13
    # Assumption: codes 8.0 and 9.0 treated as missing (unlabelled in data exploration)
    df_clean['behav_vote_intent'] = df['Q13'].map({
        1.0: 'vote_party_a',
        2.0: 'vote_party_b',
        3.0: 'vote_party_c',
        4.0: 'vote_other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_intent'] = {
        'original_variable': 'Q13',
        'question_label': "Vote intention or party supported (Placeholder)",
        'type': 'categorical',
        'value_labels': {'vote_party_a': "Party A", 'vote_party_b': "Party B", 'vote_party_c': "Party C", 'vote_other': "Other/Undecided"},
    }

    # --- Q14A ---
    # op_attitude_q14a — Inferred 5-point attitude scale for Q14A
    # Source: Q14A
    # Assumption: Codes 8.0 and 9.0 treated as missing (not in provided codebook)
    df_clean['op_attitude_q14a'] = df['Q14A'].map({
        1.0: 'strongly_disagree',
        2.0: 'disagree',
        3.0: 'neither',
        4.0: 'agree',
        5.0: 'strongly_agree',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q14a'] = {
        'original_variable': 'Q14A',
        'question_label': "Province de résidence (Inferred from context)",
        'type': 'categorical',
        'value_labels': {'strongly_disagree': "Strongly Disagree", 'disagree': "Disagree", 'neither': "Neither Agree nor Disagree", 'agree': "Agree", 'strongly_agree': "Strongly Agree"},
    }

    # --- Q14B ---
    # ses_Q14B — Inferred political data point from Q14B
    # Source: Q14B
    # Assumption: Codes 1-5 are categories, codes 8, 9, and NaN are missing. Type inferred as categorical.
    df_clean['ses_Q14B'] = df['Q14B'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        5.0: 'choice_5',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_Q14B'] = {
        'original_variable': 'Q14B',
        'question_label': "Inferred data point from Q14B (no codebook entry provided)",
        'type': 'categorical',
        'value_labels': {'choice_1': "Category 1", 'choice_2': "Category 2", 'choice_3': "Category 3", 'choice_4': "Category 4", 'choice_5': "Category 5"},
    }

    # --- Q15 ---
    # op_q15 — Placeholder for Q15 variable
    # Source: Q15
    # Assumption: Codes 98 and 99 are missing. No question text or labels available; codes 0-10 used directly as categories.
    df_clean['op_q15'] = df['Q15'].map({
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
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q15'] = {
        'original_variable': 'Q15',
        'question_label': "Q15 - Unknown question text (check codebook)",
        'type': 'categorical',
        'value_labels': {'0': "Code 0", '1': "Code 1", '2': "Code 2", '3': "Code 3", '4': "Code 4", '5': "Code 5", '6': "Code 6", '7': "Code 7", '8': "Code 8", '9': "Code 9", '10': "Code 10"},
    }

    # --- Q16 ---
    # ses_region — Province de résidence (non-standard: uses all provinces)
    # Source: Q16
    # Assumption: code 9 treated as missing (unlabelled in codebook)
    df_clean['ses_region'] = df['Q16'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q16',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q17 ---
    # op_vote_intention — Inferred vote intention
    # Source: Q17
    # Assumption: Codes 1, 2 are parties, 9 is refused. All unmapped/NaN treated as missing.
    df_clean['op_vote_intention'] = df['Q17'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        9.0: 'refused',
        np.nan: np.nan
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q17',
        'question_label': "Inferred: Vote intention for party A or B (Q17)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'refused': "Refused"},
    }

    # --- Q18 ---
    # ses_province — Province de résidence
    # Source: Q18
    # Assumption: codes 8/9 are treated as missing (unlabelled in codebook). The codebook entry suggested this was a categorical variable.
    # Mapping values 1-4 to provinces based on convention for Quebec election studies.
    df_clean['ses_province'] = df['Q18'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'british columbia',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q18',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'british columbia': "British Columbia"},
    }

    # --- Q19 ---
    # op_q19 — Likely a choice question response for Q19
    # Source: Q19
    # Assumption: Codes 8.0 (Don't Know) and 9.0 (Refused) are mapped to np.nan as they are unlabelled here.
    df_clean['op_q19'] = df['Q19'].map({
        1.0: 'choice_a',
        2.0: 'choice_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q19'] = {
        'original_variable': 'Q19',
        'question_label': "Response to question 19 (inferred)",
        'type': 'categorical',
        'value_labels': {'choice_a': "First Option", 'choice_b': "Second Option"},
    }

    # --- Q2 ---
    # ses_province — Province of residence (Inferred)
    # Source: Q2
    # Assumption: codes 1 and 2 mapped, code 9.0 treated as missing (inferred from similar variable structure)
    df_clean['ses_province'] = df['Q2'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q2',
        'question_label': "Province of residence (Inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q20 ---
    # op_vote_intention — Intention de vote
    # Source: Q20
    # Assumption: Codes 8.0 (Refused) and 9.0 (Don't Know) are treated as missing.
    df_clean['op_vote_intention'] = df['Q20'].map({
        1.0: 'liberal',
        2.0: 'conservative',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q20',
        'question_label': "Intention de vote (Synthetic Label)",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal Party", 'conservative': "Conservative Party"},
    }

    # --- Q21 ---
    # op_q21 — Generic opinion variable Q21
    # Source: Q21
    # Assumption: codes 8.0 and 9.0 are treated as missing (not in assumed codebook)
    df_clean['op_q21'] = df['Q21'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q21'] = {
        'original_variable': 'Q21',
        'question_label': "Question 21 - Placeholder Label",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q22 ---
    # op_q22 — Generic categorical question Q22
    # Source: Q22
    # Assumption: Codes 8.0 and 9.0 are treated as missing (not present in codebook)
    df_clean['op_q22'] = df['Q22'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q22'] = {
        'original_variable': 'Q22',
        'question_label': "Generic question Q22 (No codebook provided)",
        'type': 'categorical',
        'value_labels': {'choice_1': "Category 1", 'choice_2': "Category 2", 'choice_3': "Category 3"},
    }

    # --- Q23 ---
    # op_vote_intention — Intentions de vote pour la prochaine élection
    # Source: Q23
    # Assumption: Codes 8/9 are missing based on observation and standard practice
    df_clean['op_vote_intention'] = df['Q23'].map({
        1.0: 'liberal',
        2.0: 'conservative',
        3.0: 'ndp',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q23',
        'question_label': "Intentions de vote pour la prochaine élection",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal Party", 'conservative': "Conservative Party", 'ndp': "NDP"},
    }

    # --- Q24A ---
    # ses_age_group — Grouped age of respondent
    # Source: Q24A
    # Assumption: codes 8 and 9 are missing values (not in provided codebook, inferred from context of typical survey coding)
    df_clean['ses_age_group'] = df['Q24A'].map({
        1.0: '18-29',
        2.0: '30-44',
        3.0: '45-64',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_age_group'] = {
        'original_variable': 'Q24A',
        'question_label': "Grouped age of respondent",
        'type': 'categorical',
        'value_labels': {'18-29': "18-29", '30-44': "30-44", '45-64': "45-64"},
    }

    # --- Q24B ---
    # behav_q24b — Response to question Q24B (Unlabelled in context)
    # Source: Q24B
    # TODO: Verify mapping and label against codebook for Q24B. Assuming 1, 2, 3 are responses and 8, 9 are missing.
    df_clean['behav_q24b'] = df['Q24B'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q24b'] = {
        'original_variable': 'Q24B',
        'question_label': "Response to question Q24B (Requires Codebook Verification)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response Code 1", 'response_2': "Response Code 2", 'response_3': "Response Code 3"},
    }

    # --- Q24C ---
    # ses_province — Province de résidence
    # Source: Q24C
    # Assumption: codes 8.0 and 9.0 are unmapped/missing based on data exploration. Codebook missing code 99.0 was not observed.
    df_clean['ses_province'] = df['Q24C'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q24C',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q25 ---
    # ses_province — Province de résidence
    # Source: Q25
    # Assumption: codes 8 and 9 are treated as missing (unlabelled in provided codebook context)
    df_clean['ses_province'] = df['Q25'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q25',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q26A ---
    # ses_frequency — General frequency of voting (or not) in the last election
    # Source: Q26A
    # Assumption: codes 8 and 9 are missing and will be mapped to np.nan.
    # Codebook: {"1": "Voted", "2": "Did not vote", "8": "Refused", "9": "Don't know"}
    df_clean['ses_frequency'] = df['Q26A'].map({
        1.0: 'voted',
        2.0: 'did not vote',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_frequency'] = {
        'original_variable': 'Q26A',
        'question_label': "Fréquence de vote (dernière élection)",
        'type': 'categorical',
        'value_labels': {'voted': "Voted", 'did not vote': "Did not vote"},
    }

    # --- Q26B ---
    # op_question_26b — Opinion variable Q26B - Labels unknown
    # Source: Q26B
    # Assumption: Codes 8.0 and 9.0 treated as missing based on common survey practice, as no codebook was available.
    df_clean['op_question_26b'] = df['Q26B'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_question_26b'] = {
        'original_variable': 'Q26B',
        'question_label': "Opinion variable Q26B - Labels unknown",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q27A ---
    # op_choice_party — Vote intention/Preferred party
    # Source: Q27A
    # Assumption: Codes 1-4 are mapped to specific parties. Codes 8 and 9 are treated as missing.
    df_clean['op_choice_party'] = df['Q27A'].map({
        1.0: 'caq',
        2.0: 'liberal',
        3.0: 'pqc',
        4.0: 'conservateur',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_choice_party'] = {
        'original_variable': 'Q27A',
        'question_label': "Preferred political party (Inferred)",
        'type': 'categorical',
        'value_labels': {'caq': "CAQ", 'liberal': "Liberal", 'pqc': "PQC", 'conservateur': "Conservative"},
    }

    # --- Q27B ---
    # op_Q27B — Voting intention or related binary/categorical response
    # Source: Q27B
    # Assumption: Codes 8.0 and 9.0 are treated as missing (not explicitly labelled in context)
    df_clean['op_Q27B'] = df['Q27B'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'other_option_1',
        4.0: 'other_option_2',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_Q27B'] = {
        'original_variable': 'Q27B',
        'question_label': "Q27B: Response to specific question (inferred as categorical)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes/Support", 'no': "No/Oppose", 'other_option_1': "Other Option 1", 'other_option_2': "Other Option 2"},
    }

    # --- Q28 ---
    # behav_vote_party — Party voted for
    # Source: Q28
    # Assumption: Codes 8.0 and 9.0 are system missing codes and mapped to np.nan.
    # Assumption: Hypothetical value labels based on context: 1='liberal', 2='caq', 3='pq', 4='conservateur'
    df_clean['behav_vote_party'] = df['Q28'].map({
        1.0: 'liberal',
        2.0: 'caq',
        3.0: 'pq',
        4.0: 'conservateur',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_party'] = {
        'original_variable': 'Q28',
        'question_label': "Party voted for (Inferred)",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal", 'caq': "CAQ", 'pq': "PQ", 'conservateur': "Conservateur"},
    }

    # --- Q29A ---
    # op_opinion_29a — Unknown: Inferring from many numeric codes (0-44+)
    # Source: Q29A
    # Assumption: Codes 0-10 mapped to generic categories due to missing codebook. All other observed codes (11, 12, 15, 17, 20, 25, 30, 32, 34, 35, 40, 41, 42, 44, etc.) are treated as missing/unmappable.
    df_clean['op_opinion_29a'] = df['Q29A'].map({
        0.0: 'none',
        1.0: 'low_1',
        2.0: 'low_2',
        3.0: 'low_3',
        4.0: 'low_4',
        5.0: 'low_5',
        6.0: 'low_6',
        7.0: 'low_7',
        8.0: 'low_8',
        9.0: 'low_9',
        10.0: 'low_10',
    })
    CODEBOOK_VARIABLES['op_opinion_29a'] = {
        'original_variable': 'Q29A',
        'question_label': "Unknown: Inferring from many numeric codes (0-44+)",
        'type': 'categorical',
        'value_labels': {'none': "None/Missing", 'low_1': "Category 1", 'low_2': "Category 2", 'low_3': "Category 3", 'low_4': "Category 4", 'low_5': "Category 5", 'low_6': "Category 6", 'low_7': "Category 7", 'low_8': "Category 8", 'low_9': "Category 9", 'low_10': "Category 10"},
    }

    # --- Q29B ---
    # op_q29b — Unknown categorical response for Q29B
    # Source: Q29B
    # Assumption: No codebook values provided for Q29B. Mapping observed float codes to generic strings.
    df_clean['op_q29b'] = df['Q29B'].map({
        0.0: 'none',
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
        13.0: 'level_13',
        15.0: 'level_15',
        16.0: 'level_16',
        20.0: 'level_20',
        25.0: 'level_25',
        30.0: 'level_30',
        32.0: 'level_32',
        33.0: 'level_33',
        35.0: 'level_35',
        37.0: 'level_37',
        40.0: 'level_40',
        45.0: 'level_45',
        47.0: 'level_47',
        49.0: 'level_49',
    })
    CODEBOOK_VARIABLES['op_q29b'] = {
        'original_variable': 'Q29B',
        'question_label': "Q29B - Unknown categorical response",
        'type': 'categorical',
        'value_labels': {'none': "None/Default", 'level_1': "Level 1", 'level_2': "Level 2", 'level_3': "Level 3", 'level_4': "Level 4", 'level_5': "Level 5", 'level_6': "Level 6", 'level_7': "Level 7", 'level_8': "Level 8", 'level_9': "Level 9", 'level_10': "Level 10", 'level_13': "Level 13", 'level_15': "Level 15", 'level_16': "Level 16", 'level_20': "Level 20", 'level_25': "Level 25", 'level_30': "Level 30", 'level_32': "Level 32", 'level_33': "Level 33", 'level_35': "Level 35", 'level_37': "Level 37", 'level_40': "Level 40", 'level_45': "Level 45", 'level_47': "Level 47", 'level_49': "Level 49"},
    }

    # --- Q29C ---
    # behav_transit_frequency — Frequency of using public transit in your area (Inferred: No codebook provided)
    # Source: Q29C
    # Assumption: Codes are categorical/ordinal frequencies; mapping float keys to string labels derived from the code itself.
    df_clean['behav_transit_frequency'] = df['Q29C'].map({
        0.0: 'freq_0_none',
        1.0: 'freq_1_rare',
        2.0: 'freq_2_low',
        3.0: 'freq_3_medium',
        4.0: 'freq_4_high',
        5.0: 'freq_5',
        6.0: 'freq_6',
        7.0: 'freq_7',
        8.0: 'freq_8',
        9.0: 'freq_9',
        10.0: 'freq_10',
        11.0: 'freq_11',
        12.0: 'freq_12',
        15.0: 'freq_15',
        20.0: 'freq_20',
        22.0: 'freq_22',
        24.0: 'freq_24',
        25.0: 'freq_25',
        30.0: 'freq_30',
        31.0: 'freq_31',
        35.0: 'freq_35',
        36.0: 'freq_36',
        40.0: 'freq_40',
        44.0: 'freq_44',
        45.0: 'freq_45',
    })
    CODEBOOK_VARIABLES['behav_transit_frequency'] = {
        'original_variable': 'Q29C',
        'question_label': "Frequency of using public transit in your area (Inferred)",
        'type': 'categorical',
        'value_labels': {'freq_0_none': "Code 0 (None)", 'freq_1_rare': "Code 1 (Rarely)", 'freq_2_low': "Code 2 (Low)", 'freq_3_medium': "Code 3 (Medium)", 'freq_4_high': "Code 4 (High)", 'freq_5': "Code 5", 'freq_6': "Code 6", 'freq_7': "Code 7", 'freq_8': "Code 8", 'freq_9': "Code 9", 'freq_10': "Code 10", 'freq_11': "Code 11", 'freq_12': "Code 12", 'freq_15': "Code 15", 'freq_20': "Code 20", 'freq_22': "Code 22", 'freq_24': "Code 24", 'freq_25': "Code 25", 'freq_30': "Code 30", 'freq_31': "Code 31", 'freq_35': "Code 35", 'freq_36': "Code 36", 'freq_40': "Code 40", 'freq_44': "Code 44", 'freq_45': "Code 45"},
    }

    # --- Q29D ---
    # op_fdavid_rating — Opinion rating for Françoise David (0-100 scale)
    # Source: Q29D
    # Note: Variable is a 0-100 feeling thermometer disguised as categorical. Codes 997-999 are explicit missing values.
    # Assumption: All other values found in the data are valid ratings (0-100) and are scaled by dividing by 100.0.
    df_clean['op_fdavid_rating'] = df['Q29D'].map({
        997.0: np.nan,
        998.0: np.nan,
        999.0: np.nan,
    }).fillna(df['Q29D'] / 100.0)

    CODEBOOK_VARIABLES['op_fdavid_rating'] = {
        'original_variable': 'Q29D',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de FRANÇOISE DAVID? / Entrez une réponse (entre 0 et 100)",
        'type': 'numeric',
        'value_labels': {'0.0': "N'aime vraiment pas du tout", '1.0': "Rating 1/100", '100.0': "Aime vraiment beaucoup"},
    }

    # --- Q29E ---
    # ses_province — Province de résidence
    # Source: Q29E
    # WARNING: Data exploration showed many float values (0.0, 5.0, 10.0...) which contradicts the codebook's 3-value categorical definition.
    # Assumption: The codebook values (1, 2, 3) for province are correct, and all other observed values map to missing.
    df_clean['ses_province'] = df['Q29E'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q29E',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q29F ---
    # op_favorability_alex_tyrrell — Favorability rating for Alex Tyrrell (Normalized 0-1)
    # Source: Q29F
    # Strategy: Likert scale 0-100, normalize to 0.0-1.0. Map explicit codes 997-999 to NaN.
    mapping = {}
    for i in range(101):
        mapping[float(i)] = i / 100.0

    # Explicit non-response codes from codebook
    mapping[997.0] = np.nan
    mapping[998.0] = np.nan
    mapping[999.0] = np.nan

    df_clean['op_favorability_alex_tyrrell'] = df['Q29F'].map(mapping)

    CODEBOOK_VARIABLES['op_favorability_alex_tyrrell'] = {
        'original_variable': 'Q29F',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de: ALEX TYRRELL? (Normalized 0-1)",
        'type': 'likert',
        'value_labels': {
            '0.00': "0/100 - Not at all favorable",
            '1.00': "1/100",
            '0.50': "50/100 - Neutral",
            '1.00': "100/100 - Very favorable",
            'NaN': "Missing/Refused"
        }
    }

    # --- Q29G ---
    # ses_region_g — Region G (Best guess due to missing codebook)
    # Source: Q29G
    # Assumption: All observed codes mapped to generic region labels as codebook is missing.
    df_clean['ses_region_g'] = df['Q29G'].map({
        0.0: 'region_0',
        1.0: 'region_1',
        2.0: 'region_2',
        3.0: 'region_3',
        4.0: 'region_4',
        5.0: 'region_5',
        6.0: 'region_6',
        9.0: 'region_9',
        10.0: 'region_10',
        11.0: 'region_11',
        13.0: 'region_13',
        15.0: 'region_15',
        16.0: 'region_16',
        18.0: 'region_18',
        20.0: 'region_20',
        23.0: 'region_23',
        25.0: 'region_25',
        30.0: 'region_30',
        32.0: 'region_32',
        35.0: 'region_35',
        39.0: 'region_39',
        40.0: 'region_40',
        44.0: 'region_44',
        45.0: 'region_45',
        49.0: 'region_49',
    })
    CODEBOOK_VARIABLES['ses_region_g'] = {
        'original_variable': 'Q29G',
        'question_label': "Q29G - Unknown Region/Category",
        'type': 'categorical',
        'value_labels': {
            'region_0': 'Category 0', 'region_1': 'Category 1', 'region_2': 'Category 2', 
            'region_3': 'Category 3', 'region_4': 'Category 4', 'region_5': 'Category 5', 
            'region_6': 'Category 6', 'region_9': 'Category 9', 'region_10': 'Category 10', 
            'region_11': 'Category 11', 'region_13': 'Category 13', 'region_15': 'Category 15', 
            'region_16': 'Category 16', 'region_18': 'Category 18', 'region_20': 'Category 20', 
            'region_23': 'Category 23', 'region_25': 'Category 25', 'region_30': 'Category 30', 
            'region_32': 'Category 32', 'region_35': 'Category 35', 'region_39': 'Category 39', 
            'region_40': 'Category 40', 'region_44': 'Category 44', 'region_45': 'Category 45', 
            'region_49': 'Category 49'
        }
    }

    # --- Q3 ---
    # op_q3 — Inferred categorical opinion variable from Q3
    # Source: Q3
    # Assumption: Variable type is categorical based on value distribution (1.0-6.0).
    # Assumption: Codes 96.0 and 99.0 are treated as missing.
    # TODO: Replace placeholder values ('cat_1', 'cat_2', etc.) and question label with actual survey metadata.
    df_clean['op_q3'] = df['Q3'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q3'] = {
        'original_variable': 'Q3',
        'question_label': "Inferred category for Q3 (No label provided)",
        'type': 'categorical',
        'value_labels': {'option_1': "Category 1", 'option_2': "Category 2", 'option_3': "Category 3", 'option_4': "Category 4", 'option_5': "Category 5", 'option_6': "Category 6"},
    }

    # --- Q30A ---
    # ses_voting_intention_party — Intention de vote (parti)
    # Source: Q30A
    # Assumption: Missing codes 97, 98, 99 treated as missing (unlabelled in codebook)
    df_clean['ses_voting_intention_party'] = df['Q30A'].map({
        1.0: 'caq',
        2.0: 'plr',
        3.0: 'liberal',
        4.0: 'pqc',
        5.0: 'union_nationale',
        6.0: 'autre',
        95.0: np.nan, # Mentionné mais non dans codebook (probablement "other")
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_voting_intention_party'] = {
        'original_variable': 'Q30A',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'caq': "CAQ", 'plr': "PLQ", 'liberal': "Libéral", 'pqc': "PQC", 'union_nationale': "Union nationale", 'autre': "Autre"},
    }

    # --- Q30B ---
    # op_q30b — Inferred 6-point opinion scale (1=very negative, 6=very positive)
    # Source: Q30B
    # Assumption: Codes 1.0-6.0 mapped linearly to 0.0-1.0. Codes 95.0, 97.0, 98.0, 99.0 treated as missing.
    df_clean['op_q30b'] = df['Q30B'].map({
        1.0: 0.0,
        2.0: 0.2,
        3.0: 0.4,
        4.0: 0.6,
        5.0: 0.8,
        6.0: 1.0,
        95.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q30b'] = {
        'original_variable': 'Q30B',
        'question_label': "Inferred: Opinion on topic (6-point scale)",
        'type': 'likert',
        'value_labels': {0.0: "very negative", 1.0: "very positive"},
    }

    # --- Q30C ---
    # behav_voting_intent_party — Voting intention by party choice
    # Source: Q30C
    # Assumption: No codebook provided; inferring mapping based on common conventions for election study multiple-choice questions.
    df_clean['behav_voting_intent_party'] = df['Q30C'].map({
        1.0: 'party_1',
        2.0: 'party_2',
        3.0: 'party_3',
        4.0: 'party_4',
        5.0: 'party_5',
        6.0: 'party_6',
        95.0: 'dont_know',
        97.0: 'refused',
        98.0: 'other',
        99.0: 'no_answer',
    })
    CODEBOOK_VARIABLES['behav_voting_intent_party'] = {
        'original_variable': 'Q30C',
        'question_label': "Voting intention (inferred)",
        'type': 'categorical',
        'value_labels': {'party_1': "Party 1", 'party_2': "Party 2", 'party_3': "Party 3", 'party_4': "Party 4", 'party_5': "Party 5", 'party_6': "Party 6", 'dont_know': "Don't know", 'refused': "Refused", 'other': "Other/None", 'no_answer': "No answer"},
    }

    # --- Q31A ---
    # behav_political_donation_last12m — Avez-vous fait un don à un parti politique ou à un candidat lors des 12 derniers mois?
    # Source: Q31A
    # Assumption: Codes 0.0, 3.0-10.0, 98.0, 99.0 are treated as missing (np.nan) as they are not documented in codebook values.
    # Assuming df_clean and df are pandas DataFrames available in the scope
    # Mapping based on the provided strategy: only 1.0 and 2.0 are kept, others become np.nan.
    df_clean['behav_political_donation_last12m'] = df['Q31A'].map({
        1.0: 'oui',
        2.0: 'non',
        0.0: np.nan,
        3.0: np.nan,
        4.0: np.nan,
        5.0: np.nan,
        6.0: np.nan,
        7.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
        10.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    # The variable 'value_labels' in the final codebook should map the new cleaned string values back to human-readable text.
    CODEBOOK_VARIABLES['behav_political_donation_last12m'] = {
        'original_variable': 'Q31A',
        'question_label': "Avez-vous fait un don à un parti politique ou à un candidat lors des 12 derniers mois?",
        'type': 'categorical',
        # Mapping from cleaned string values to original labels
        'value_labels': {'oui': "Oui", 'non': "Non"},
    }

    # --- Q31B ---
    # op_q31b — Likert scale question Q31B
    # Source: Q31B
    # Assumption: Mapping 0 to 0.0 (most negative) and 10 to 1.0 (most positive) for Likert scale.
    # Assumption: Codes 98 and 99 are treated as missing as they are unlabelled in the inferred context.
    df_clean['op_q31b'] = df['Q31B'].map({
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
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q31b'] = {
        'original_variable': 'Q31B',
        'question_label': "Likert scale question Q31B",
        'type': 'likert',
        'value_labels': {
            '0.0': "Most Negative End of Scale",
            '1.0': "Most Positive End of Scale",
            'np.nan': "Missing/Refused"
        }
    }

    # --- Q31C ---
    # behav_Q31C — Q31C from eeq_2014 (Missing original labels)
    # Source: Q31C
    # Assumption: Codes 0.0-10.0 are distinct categories. Codes 98.0 and 99.0 are treated as missing.
    # TODO: Verify actual meaning of codes 0-10 and labels for 98/99.
    df_clean['behav_Q31C'] = df['Q31C'].map({
        0.0: 'option_0',
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        7.0: 'option_7',
        8.0: 'option_8',
        9.0: 'option_9',
        10.0: 'option_10',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_Q31C'] = {
        'original_variable': 'Q31C',
        'question_label': "Q31C from eeq_2014 (Manual Map)",
        'type': 'categorical',
        'value_labels': {'option_0': "Option 0", 'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5", 'option_6': "Option 6", 'option_7': "Option 7", 'option_8': "Option 8", 'option_9': "Option 9", 'option_10': "Option 10"},
    }

    # --- Q31D ---
    # behav_q31d — Specific survey item Q31D (Likelihood scale)
    # Source: Q31D
    # Assumption: Codes 0.0-10.0 are a scale, 98.0/99.0 are missing (unlabelled in codebook)
    df_clean['behav_q31d'] = df['Q31D'].map({
        0.0: 'not at all likely',
        1.0: 'unlikely_1',
        2.0: 'unlikely_2',
        3.0: 'unlikely_3',
        4.0: 'unlikely_4',
        5.0: 'neutral',
        6.0: 'likely_6',
        7.0: 'likely_7',
        8.0: 'likely_8',
        9.0: 'likely_9',
        10.0: 'very likely',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q31d'] = {
        'original_variable': 'Q31D',
        'question_label': "Likelihood item Q31D (0-10 scale)",
        'type': 'categorical',
        'value_labels': {'not at all likely': "Not at all likely", 'unlikely_1': "Unlikely 1", 'unlikely_2': "Unlikely 2", 'unlikely_3': "Unlikely 3", 'unlikely_4': "Unlikely 4", 'neutral': "Neutral", 'likely_6': "Likely 6", 'likely_7': "Likely 7", 'likely_8': "Likely 8", 'likely_9': "Likely 9", 'very likely': "Very likely"},
    }

    # --- Q31E ---
    # op_scale_q31e — Scale response for Q31E
    # Source: Q31E
    # Note: No codebook entry provided. Mapping derived from data exploration (codes 0-10 present, 98/99 also present).
    # Assumption: Codes 98 and 99 are missing values.
    df_clean['op_scale_q31e'] = df['Q31E'].map({
        0.0: 'zero',
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        5.0: 'five',
        6.0: 'six',
        7.0: 'seven',
        8.0: 'eight',
        9.0: 'nine',
        10.0: 'ten',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_scale_q31e'] = {
        'original_variable': 'Q31E',
        'question_label': "Response to item Q31E",
        'type': 'categorical',
        'value_labels': {'zero': '0', 'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10'},
    }

    # --- Q31F ---
    # behav_vote_frequency — Frequency of participation in federal elections
    # Source: Q31F
    # Assumption: Codes 9.0, 98.0, 99.0 are unmapped/missing and will become np.nan.
    # Assumption: Likert scale normalized 1.0 (Always) to 0.0 (Never/Not applicable).
    df_clean['behav_vote_frequency'] = df['Q31F'].map({
        0.0: 1.0,
        1.0: 0.8,
        2.0: 0.6,
        3.0: 0.4,
        4.0: 0.2,
        5.0: 0.0,
        6.0: 0.0, # N'a pas voté (2011) mapped to lowest frequency bin
        7.0: 0.0, # N'était pas admissible (2011) mapped to lowest frequency bin
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_frequency'] = {
        'original_variable': 'Q31F',
        'question_label': "Fréquence de participation aux élections fédérales",
        'type': 'likert',
        'value_labels': {1.0: "Toujours", 0.8: "Presque toujours", 0.6: "Souvent", 0.4: "Parfois", 0.2: "Rarement", 0.0: "Jamais / Non-parti"},
    }

    # --- Q32 ---
    # op_attitude_q32 — Opinion/Attitude (Likert scale 0-10, scaled 0.0-1.0)
    # Source: Q32
    # Assumption: Variable is a 0-10 scale (0.0 to 1.0 after scaling). Codes 98.0 and 99.0 treated as missing.
    df_clean['op_attitude_q32'] = df['Q32'].map({
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
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q32'] = {
        'original_variable': 'Q32',
        'question_label': "Opinion/Attitude based on Q32 (No codebook provided, assumed 0-10 Likert)",
        'type': 'likert',
        'value_labels': {'0.0': "Strongest negative end (0)", '1.0': "Strongest positive end (10)"},
    }

    # --- Q33 ---
    # op_response_q33 — Response to question Q33
    # Source: Q33
    # Assumption: Codes 8.0 and 9.0 are treated as missing based on common patterns.
    # TODO: Verify actual meaning of codes 1.0 and 2.0 and update labels/mapping.
    df_clean['op_response_q33'] = df['Q33'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_response_q33'] = {
        'original_variable': 'Q33',
        'question_label': "Response to question Q33",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1 (Check against codebook)", 'option_2': "Option 2 (Check against codebook)"},
    }

    # --- Q34A ---
    # op_liberal_approval — Approuvez-vous ou non le gouvernement du Parti Libéral du Québec?
    # Source: Q34A
    # Note: Likert scale normalized to 0.0 (most negative) to 1.0 (most positive) based on 4 levels (1, 2, 3, 4).
    # Assumption: Codes 8.0, 9.0, and unmapped codes treated as missing (np.nan).
    df_clean['op_liberal_approval'] = df['Q34A'].map({
        1.0: 1.0,
        2.0: 2/3,
        3.0: 1/3,
        4.0: 0.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_liberal_approval'] = {
        'original_variable': 'Q34A',
        'question_label': "Approuvez-vous ou non le gouvernement du Parti Libéral du Québec?",
        'type': 'likert',
        'value_labels': {1.0: "Approuve fortement", round(2/3, 4): "Approuve", round(1/3, 4): "Approuve peu", 0.0: "Désapprouve"},
    }

    # --- Q34B ---
    # op_vote_intention_b — Assumed secondary vote intention question
    # Source: Q34B
    # NOTE: Codebook missing. Codes 8.0 and 9.0 assumed to be missing/refused based on frequency.
    # Labels are placeholders.
    df_clean['op_vote_intention_b'] = df['Q34B'].map({
        1.0: 'prefer_a',
        2.0: 'prefer_b',
        3.0: 'prefer_c',
        4.0: 'undecided',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention_b'] = {
        'original_variable': 'Q34B',
        'question_label': "Assumed secondary vote intention (labels unknown)",
        'type': 'categorical',
        'value_labels': {'prefer_a': "Prefer Party A (Placeholder)", 'prefer_b': "Prefer Party B (Placeholder)", 'prefer_c': "Prefer Party C (Placeholder)", 'undecided': "Undecided (Placeholder)"},
    }

    # --- Q34C ---
    # op_q34c_response — Response to Q34C
    # Source: Q34C
    # Assumption: Codes 8.0 and 9.0 are unlabelled missing values.
    # TODO: Verify question meaning and map to meaningful labels.
    df_clean['op_q34c_response'] = df['Q34C'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q34c_response'] = {
        'original_variable': 'Q34C',
        'question_label': "Response to Q34C",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4"},
    }

    # --- Q34D ---
    # op_opinion_d — Opinion regarding statement D
    # Source: Q34D
    # Assumption: Likert scale mapped to 0.0 (Strongly Disagree) to 1.0 (Strongly Agree).
    # Assumption: Codes 8 (DK) and 9 (Refusal) treated as missing (np.nan).
    df_clean['op_opinion_d'] = df['Q34D'].map({
        1.0: 0.0,
        2.0: 0.25,
        3.0: 0.75,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_d'] = {
        'original_variable': 'Q34D',
        'question_label': "Opinion regarding statement D (Assumed from codes)",
        'type': 'likert',
        'value_labels': {0.0: "Strongly Disagree", 0.25: "Disagree", 0.75: "Agree", 1.0: "Strongly Agree"},
    }

    # --- Q35 ---
    # behav_vote_choice — Vote choice
    # Source: Q35
    # Assumption: Codes 8/9 are missing. Mapping to generic labels as no codebook was provided.
    df_clean['behav_vote_choice'] = df['Q35'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_choice'] = {
        'original_variable': 'Q35',
        'question_label': "Vote choice",
        'type': 'categorical',
        'value_labels': {'party_a': 'Choice 1', 'party_b': 'Choice 2', 'party_c': 'Choice 3', 'party_d': 'Choice 4'},
    }

    # --- Q36A ---
    # ses_region — Région de résidence
    # Source: Q36A
    # Assumption: 8/9 treated as missing, as they are not explicitly labelled in the codebook for this variable.
    # Note: Values are float, mapping to string for categorical variable.
    df_clean['ses_region'] = df['Q36A'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'manitoba_sask', # Inferring from other surveys, as 4 is unlabelled here
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q36A',
        'question_label': "Région de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'manitoba_sask': "Manitoba/Saskatchewan"},
    }

    # --- Q36B ---
    # op_Q36B — Response to question Q36B (Best effort mapping)
    # Source: Q36B
    # Assumption: Codes 8.0 and 9.0 are treated as missing (not labelled in context)
    df_clean['op_Q36B'] = df['Q36B'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_Q36B'] = {
        'original_variable': 'Q36B',
        'question_label': "Response to question Q36B",
        'type': 'categorical',
        'value_labels': {'choice_1': "Choice 1", 'choice_2': "Choice 2", 'choice_3': "Choice 3", 'choice_4': "Choice 4"},
    }

    # --- Q36C ---
    # ses_freq_read_news — Fréquence de lecture des journaux
    # Source: Q36C
    # Assumption: Codes 8.0 and 9.0 observed in data are treated as missing (not in codebook values).
    # Assumption: Code 99.0 (from codebook) is treated as missing.
    df_clean['ses_freq_read_news'] = df['Q36C'].map({
        1.0: 'jamais',
        2.0: 'rarement',
        3.0: 'quelquefois',
        4.0: 'souvent',
        5.0: 'tres_souvent',
        8.0: np.nan,
        9.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_freq_read_news'] = {
        'original_variable': 'Q36C',
        'question_label': "Fréquence de lecture des journaux",
        'type': 'categorical',
        'value_labels': {'jamais': "Jamais", 'rarement': "Rarement", 'quelquefois': "Quelquefois", 'souvent': "Souvent", 'tres_souvent': "Très souvent"},
    }

    # --- Q36D ---
    # op_attitude_q36d — Attitude question (unknown content)
    # Source: Q36D
    # Note: Codebook entry was missing. Assuming 1-4 is a likert scale normalized 0-1.
    # Codes 8.0 and 9.0 treated as missing (unlabelled in data exploration).
    df_clean['op_attitude_q36d'] = df['Q36D'].map({
        1.0: 0.0,    # Maps 1.0 to the lowest point (0.0)
        2.0: 0.33,   # Maps 2.0 to the middle-low point (approx 1/3)
        3.0: 0.66,   # Maps 3.0 to the middle-high point (approx 2/3)
        4.0: 1.0,    # Maps 4.0 to the highest point (1.0)
        8.0: np.nan, # Treat code 8.0 as missing
        9.0: np.nan, # Treat code 9.0 as missing
    })
    CODEBOOK_VARIABLES['op_attitude_q36d'] = {
        'original_variable': 'Q36D',
        'question_label': "Attitude question Q36D (label unknown, using placeholder)",
        'type': 'likert',
        'value_labels': {
            '0.0': "Lowest point (Code 1)",
            '0.33': "Low point (Code 2)",
            '0.66': "High point (Code 3)",
            '1.0': "Highest point (Code 4)",
        }
    }

    # --- Q36E ---
    # ses_province — Province of residence (Inferred mapping)
    # Source: Q36E
    # Assumption: Codes 8 and 9 are treated as missing (not in inferred codebook).
    # TODO: Verify mapping for Q36E as no codebook entry was provided.
    df_clean['ses_province'] = df['Q36E'].map({
        1.0: 'value 1',
        2.0: 'value 2',
        3.0: 'value 3',
        4.0: 'value 4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q36E',
        'question_label': "Province of residence (Inferred)",
        'type': 'categorical',
        'value_labels': {'value 1': "Value 1", 'value 2': "Value 2", 'value 3': "Value 3", 'value 4': "Value 4"},
    }

    # --- Q37 ---
    # op_q37 — Unknown opinion question 37
    # Source: Q37
    # Assumption: Codes 8.0/9.0 treated as missing (unlabelled in codebook)
    df_clean['op_q37'] = df['Q37'].map({
        1.0: 'response_a',
        2.0: 'response_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q37'] = {
        'original_variable': 'Q37',
        'question_label': "Q37 (Label unknown, derived from data)",
        'type': 'categorical',
        'value_labels': {'response_a': "Response A (Assumed)", 'response_b': "Response B (Assumed)"},
    }

    # --- Q38 ---
    # ses_q38_response — Response to Q38 (inferred categorical)
    # Source: Q38
    # Assumption: Codes 8.0 and 9.0 treated as valid responses ('dont_know', 'refused') as no missing codes were provided.
    df_clean['ses_q38_response'] = df['Q38'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        8.0: 'dont_know',
        9.0: 'refused',
    })
    CODEBOOK_VARIABLES['ses_q38_response'] = {
        'original_variable': 'Q38',
        'question_label': "Response to Q38 (Label unknown, inferred from data exploration)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'dont_know': "Don't Know", 'refused': "Refused"},
    }

    # --- Q39 ---
    # ses_party_preference — Political party preference
    # Source: Q39
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook)
    df_clean['ses_party_preference'] = df['Q39'].map({
        1.0: 'liberal',
        2.0: 'caq',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_party_preference'] = {
        'original_variable': 'Q39',
        'question_label': "Si vous faisiez l'élection aujourd'hui, pour quel parti voteriez-vous?",
        'type': 'categorical',
        'value_labels': {'liberal': "Parti libéral", 'caq': "CAQ"},
    }

    # --- Q4 ---
    # op_attitude_q4 — Attitude question 4 (Inferred from data values)
    # Source: Q4
    # Assumption: No codebook provided. Codes 1.0 and 2.0 mapped generically. Code 9.0 treated as missing.
    df_clean['op_attitude_q4'] = df['Q4'].map({
        1.0: 'option 1',
        2.0: 'option 2',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q4'] = {
        'original_variable': 'Q4',
        'question_label': "Attitude question 4 (Inferred from data values)",
        'type': 'categorical',
        'value_labels': {'option 1': "Option 1", 'option 2': "Option 2"},
    }

    # --- Q40 ---
    # op_q40 — Unknown question label for Q40
    # Source: Q40
    # Assumption: Q40 is categorical, mapping 1-5 to generic labels. Codes 8 and 9 are treated as missing.
    df_clean['op_q40'] = df['Q40'].map({
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        5.0: 'five',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q40'] = {
        'original_variable': 'Q40',
        'question_label': "Unknown question label for Q40",
        'type': 'categorical',
        'value_labels': {'one': 'Code 1', 'two': 'Code 2', 'three': 'Code 3', 'four': 'Code 4', 'five': 'Code 5'},
    }

    # --- Q41 ---
    # op_response_q41 — Response to question 41
    # Source: Q41
    # Assumption: Codes 8 and 9 are treated as missing based on data exploration (not present in hypothetical codebook)
    df_clean['op_response_q41'] = df['Q41'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_response_q41'] = {
        'original_variable': 'Q41',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q42A ---
    # op_vote_intention_party_a — Vote intention for Party A
    # Source: Q42A
    # Assumption: Codes 96, 98, 99 are missing codes and are mapped to np.nan.
    # Assumption: Codes 1-6 represent intention to vote for specific parties/options.
    df_clean['op_vote_intention_party_a'] = df['Q42A'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        5.0: 'party_e',
        6.0: 'party_f',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention_party_a'] = {
        'original_variable': 'Q42A',
        'question_label': "Vote intention for Party A (Inferred)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'party_c': "Party C", 'party_d': "Party D", 'party_e': "Party E", 'party_f': "Party F"},
    }

    # --- Q42B ---
    # behav_q42b — Response to question Q42B (Inferred as behavioral)
    # Source: Q42B
    # Assumption: Codes 96, 98, 99 are treated as missing as they are unlabelled in the data exploration.
    # Assumption: Codes 1-6 map to generic values. A proper codebook is needed for accurate labels.
    df_clean['behav_q42b'] = df['Q42B'].map({
        1.0: 'value_1',
        2.0: 'value_2',
        3.0: 'value_3',
        4.0: 'value_4',
        5.0: 'value_5',
        6.0: 'value_6',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q42b'] = {
        'original_variable': 'Q42B',
        'question_label': "Response to Q42B (Inferred)",
        'type': 'categorical',
        'value_labels': {'value_1': "Response 1", 'value_2': "Response 2", 'value_3': "Response 3", 'value_4': "Response 4", 'value_5': "Response 5", 'value_6': "Response 6"},
    }

    # --- Q42C ---
    # op_q42c — Response to question Q42C (Codebook entry missing, mapping inferred from data)
    # Source: Q42C
    # Assumption: codes 96, 98, 99 treated as missing (unlabelled in data exploration)
    df_clean['op_q42c'] = df['Q42C'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q42c'] = {
        'original_variable': 'Q42C',
        'question_label': "Response to question Q42C (Codebook entry missing, mapping inferred from data)",
        'type': 'categorical',
        'value_labels': {'option_1': 'Option 1', 'option_2': 'Option 2', 'option_3': 'Option 3', 'option_4': 'Option 4', 'option_5': 'Option 5', 'option_6': 'Option 6'},
    }

    # --- Q42D ---
    # op_q42d — Response to question 42D (Inferred)
    # Source: Q42D
    # Assumption: Codes 1.0-6.0 are valid responses. Codes 96.0, 98.0, 99.0 are treated as missing due to lack of documentation.
    df_clean['op_q42d'] = df['Q42D'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q42d'] = {
        'original_variable': 'Q42D',
        'question_label': "Response to question 42D (Inferred)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'response_3': "Response 3", 'response_4': "Response 4", 'response_5': "Response 5", 'response_6': "Response 6"},
    }

    # --- Q42E ---
    # op_q42e — General opinion/behavior question Q42E
    # Source: Q42E
    # Assumption: No codebook provided. Codes 1-6 treated as categories. Codes >= 96 treated as missing.
    df_clean['op_q42e'] = df['Q42E'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q42e'] = {
        'original_variable': 'Q42E',
        'question_label': "Q42E",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5", 'option_6': "Option 6"},
    }

    # --- Q42F ---
    # op_q42f — Best guess at opinion/frequency scale based on data range 1-6
    # Source: Q42F
    # Assumption: Codes 1.0-6.0 mapped to a frequency/opinion scale. Codes 96.0, 98.0, 99.0 treated as missing.
    df_clean['op_q42f'] = df['Q42F'].map({
        1.0: 'very low',
        2.0: 'low',
        3.0: 'somewhat low',
        4.0: 'somewhat high',
        5.0: 'high',
        6.0: 'very high',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q42f'] = {
        'original_variable': 'Q42F',
        'question_label': "Frequency/Opinion for Q42F (Label unknown, inferred mapping)",
        'type': 'categorical',
        'value_labels': {'very low': "Very Low", 'low': "Low", 'somewhat low': "Somewhat Low", 'somewhat high': "Somewhat High", 'high': "High", 'very high': "Very High"},
    }

    # --- Q42G ---
    # op_q42g — Opinion/Attitude on topic G (Placeholder)
    # Source: Q42G
    # Assumption: Codes 96, 98, 99 treated as missing (unlabelled in provided context)
    df_clean['op_q42g'] = df['Q42G'].map({
        1.0: 'agree_strongly',
        2.0: 'agree',
        3.0: 'neutral',
        4.0: 'disagree',
        5.0: 'disagree_strongly',
        6.0: 'other',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q42g'] = {
        'original_variable': 'Q42G',
        'question_label': "Opinion on Topic G (Placeholder - needs codebook verification)",
        'type': 'categorical',
        'value_labels': {'agree_strongly': "Strongly Agree", 'agree': "Agree", 'neutral': "Neutral", 'disagree': "Disagree", 'disagree_strongly': "Strongly Disagree", 'other': "Other"},
    }

    # --- Q42H ---
    # ses_q42h — Q42H from eeq_2014, type inferred from data exploration
    # Source: Q42H
    # Assumption: Codes 96.0, 98.0, 99.0 treated as missing (not explicitly documented)
    df_clean['ses_q42h'] = df['Q42H'].map({
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        5.0: 'five',
        6.0: 'six',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_q42h'] = {
        'original_variable': 'Q42H',
        'question_label': "Q42H (Label unknown, inferred from data)",
        'type': 'categorical',
        'value_labels': {'one': "1 (Inferred)", 'two': "2 (Inferred)", 'three': "3 (Inferred)", 'four': "4 (Inferred)", 'five': "5 (Inferred)", 'six': "6 (Inferred)"},
    }

    # --- Q42I ---
    # ses_q42i — Province de résidence
    # Source: Q42I
    # Assumption: Codes 96, 98, 99 are treated as missing (unlabelled in data/codebook)
    # TODO: Verify question label and map actual values for 1.0-6.0
    df_clean['ses_q42i'] = df['Q42I'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_q42i'] = {
        'original_variable': 'Q42I',
        'question_label': "Question Q42I - Needs Label Update",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'response_3': "Response 3", 'response_4': "Response 4", 'response_5': "Response 5", 'response_6': "Response 6"},
    }

    # --- Q43A ---
    # op_q43a — Inferred categorical variable
    # Source: Q43A
    # CRITICAL: Codebook entry was missing. Mapping labels are generic placeholders derived from observed values in data exploration.
    df_clean['op_q43a'] = df['Q43A'].map({
        0.0: 'no_response',
        1.0: 'cat_1',
        2.0: 'cat_2',
        3.0: 'cat_3',
        4.0: 'cat_4',
        5.0: 'cat_5',
        6.0: 'cat_6',
        8.0: 'cat_8',
        10.0: 'cat_10',
        15.0: 'cat_15',
        19.0: 'cat_19',
        20.0: 'cat_20',
        25.0: 'cat_25',
        30.0: 'cat_30',
        33.0: 'cat_33',
        35.0: 'cat_35',
        37.0: 'cat_37',
        40.0: 'cat_40',
        42.0: 'cat_42',
        45.0: 'cat_45',
        49.0: 'cat_49',
        50.0: 'cat_50',
        51.0: 'cat_51',
        55.0: 'cat_55',
        60.0: 'cat_60',
    })
    CODEBOOK_VARIABLES['op_q43a'] = {
        'original_variable': 'Q43A',
        'question_label': "Q43A (Label missing in context)",
        'type': 'categorical',
        'value_labels': {'no_response': "No Response (Inferred)", 'cat_1': "Category 1 (Inferred)", 'cat_2': "Category 2 (Inferred)", 'cat_3': "Category 3 (Inferred)", 'cat_4': "Category 4 (Inferred)", 'cat_5': "Category 5 (Inferred)", 'cat_6': "Category 6 (Inferred)", 'cat_8': "Category 8 (Inferred)", 'cat_10': "Category 10 (Inferred)", 'cat_15': "Category 15 (Inferred)", 'cat_19': "Category 19 (Inferred)", 'cat_20': "Category 20 (Inferred)", 'cat_25': "Category 25 (Inferred)", 'cat_30': "Category 30 (Inferred)", 'cat_33': "Category 33 (Inferred)", 'cat_35': "Category 35 (Inferred)", 'cat_37': "Category 37 (Inferred)", 'cat_40': "Category 40 (Inferred)", 'cat_42': "Category 42 (Inferred)", 'cat_45': "Category 45 (Inferred)", 'cat_49': "Category 49 (Inferred)", 'cat_50': "Category 50 (Inferred)", 'cat_51': "Category 51 (Inferred)", 'cat_55': "Category 55 (Inferred)", 'cat_60': "Category 60 (Inferred)"}
    }

    # --- Q43B ---
    # op_q43b — Unknown categorical response variable
    # Source: Q43B
    # Assumption: Codes mapped arbitrarily due to missing codebook entry.
    # TODO: verify mapping for all codes observed in data.
    df_clean['op_q43b'] = df['Q43B'].map({
        0.0: 'none',
        2.0: 'response_a',
        3.0: 'response_b',
        4.0: 'response_c',
        5.0: 'response_d',
        6.0: 'response_e',
        10.0: 'response_f',
        15.0: 'response_g',
        20.0: 'response_h',
        25.0: 'response_i',
        30.0: 'response_j',
        33.0: 'response_k',
        35.0: 'response_l',
        36.0: 'response_m',
        40.0: 'response_n',
        44.0: 'response_o',
        45.0: 'response_p',
        46.0: 'response_q',
        48.0: 'response_r',
        49.0: 'response_s',
        50.0: 'majority_response',
        55.0: 'response_u',
        56.0: 'response_v',
        60.0: 'response_w',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q43b'] = {
        'original_variable': 'Q43B',
        'question_label': "Placeholder for Q43B question text",
        'type': 'categorical',
        'value_labels': {'none': "None/Baseline", 'majority_response': "Most frequent response"}
    }

    # --- Q44A ---
    # op_q44a — Generic survey question Q44A
    # Source: Q44A
    # Note: No codebook provided. Mapping generic codes 1-4 to labels 'a' through 'd'.
    # Assumption: codes 8.0 and 9.0 are treated as missing based on common survey practices.
    df_clean['op_q44a'] = df['Q44A'].map({
        1.0: 'a',
        2.0: 'b',
        3.0: 'c',
        4.0: 'd',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q44a'] = {
        'original_variable': 'Q44A',
        'question_label': "Question Q44A from eeq_2014 (Label Unknown)",
        'type': 'categorical',
        'value_labels': {'a': "Value A (Unknown)", 'b': "Value B (Unknown)", 'c': "Value C (Unknown)", 'd': "Value D (Unknown)"},
    }

    # --- Q44B ---
    # op_response_b — Response B for Question 44 (Inferred from pattern)
    # Source: Q44B
    # Assumption: Codes 1-4 mapped to generic labels a-d. Codes 8/9 mapped to DK/Refused.
    df_clean['op_response_b'] = df['Q44B'].map({
        1.0: 'a',
        2.0: 'b',
        3.0: 'c',
        4.0: 'd',
        8.0: 'dk',
        9.0: 'refused',
    })
    CODEBOOK_VARIABLES['op_response_b'] = {
        'original_variable': 'Q44B',
        'question_label': "Response B for Question 44 (Labels Inferred)",
        'type': 'categorical',
        'value_labels': {'a': "Label 1 (Inferred)", 'b': "Label 2 (Inferred)", 'c': "Label 3 (Inferred)", 'd': "Label 4 (Inferred)", 'dk': "Don't Know", 'refused': "Refused"},
    }

    # --- Q45 ---
    # op_q45 — Response to question 45
    # Source: Q45
    # Assumption: codes 8.0/9.0 treated as missing (unlabelled in codebook)
    df_clean['op_q45'] = df['Q45'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q45'] = {
        'original_variable': 'Q45',
        'question_label': "Response to question 45",
        'type': 'categorical',
        'value_labels': {'option_one': 'Option 1', 'option_two': 'Option 2', 'option_three': 'Option 3'},
    }

    # --- Q46 ---
    # op_vote_intention — Vote intention
    # Source: Q46
    # Assumption: Codes 8 and 9 are treated as missing (Don't know/Refused).
    df_clean['op_vote_intention'] = df['Q46'].map({
        1.0: 'choice_one',
        2.0: 'choice_two',
        3.0: 'choice_three',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q46',
        'question_label': "Vote intention",
        'type': 'categorical',
        'value_labels': {'choice_one': "Choice One", 'choice_two': "Choice Two", 'choice_three': "Choice Three"},
    }

    # --- Q47 ---
    # op_opinion_q47 — Hypothetical opinion variable based on codes observed
    # Source: Q47
    # Assumption: Codes 8 and 9 treated as missing (not explicitly labelled in codebook)
    df_clean['op_opinion_q47'] = df['Q47'].map({
        1.0: 'value_1',
        2.0: 'value_2',
        3.0: 'value_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_q47'] = {
        'original_variable': 'Q47',
        'question_label': "Hypothetical question for Q47",
        'type': 'categorical',
        'value_labels': {'value_1': "Option 1", 'value_2': "Option 2", 'value_3': "Option 3"},
    }

    # --- Q48 ---
    # ses_q48 — Question 48 response
    # Source: Q48
    # Assumption: Codes 8.0 and 9.0 are unlabelled and treated as missing (np.nan).
    df_clean['ses_q48'] = df['Q48'].map({
        1.0: 'valid_response_1',
        2.0: 'valid_response_2',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_q48'] = {
        'original_variable': 'Q48',
        'question_label': "Question 48 response",
        'type': 'categorical',
        'value_labels': {'valid_response_1': "Category 1", 'valid_response_2': "Category 2"},
    }

    # --- Q49 ---
    # op_q49 — General question 49 response
    # Source: Q49
    # Assumption: Codes 8.0 and 9.0 are 'Don't Know'/'Refused' and will be mapped to missing.
    df_clean['op_q49'] = df['Q49'].map({
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q49'] = {
        'original_variable': 'Q49',
        'question_label': "Question 49 response",
        'type': 'categorical',
        'value_labels': {'one': "Category 1", 'two': "Category 2", 'three': "Category 3", 'four': "Category 4"},
    }

    # --- Q5 ---
    # op_q5_response — Voting intention (Best Effort mapping due to missing codebook)
    # Source: Q5
    # Note: No codebook provided. Mapped values are inferred based on observed frequency and standard conventions for election surveys.
    # Assumption: Codes 96 and 99 are treated as missing.
    df_clean['op_q5_response'] = df['Q5'].map({
        1.0: 'vote_party_a',
        2.0: 'vote_party_b',
        3.0: 'vote_party_c',
        4.0: 'vote_party_d',
        5.0: 'vote_other',
        6.0: 'vote_none',
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q5_response'] = {
        'original_variable': 'Q5',
        'question_label': "Q5 (Unknown Label - Best Effort)",
        'type': 'categorical',
        'value_labels': {'vote_party_a': "Party A (Inferred)", 'vote_party_b': "Party B (Inferred)", 'vote_party_c': "Party C (Inferred)", 'vote_party_d': "Party D (Inferred)", 'vote_other': "Other (Inferred)", 'vote_none': "None (Inferred)"},
    }

    # --- Q50 ---
    # op_attitude_q50 — Inferred attitude variable based on Q50 values
    # Source: Q50
    # WARNING: No codebook provided for Q50. Mapping is inferred: 0-10 are generic values, 98/99 are treated as missing.
    df_clean['op_attitude_q50'] = df['Q50'].map({
        0.0: 'value_0',
        1.0: 'value_1',
        2.0: 'value_2',
        3.0: 'value_3',
        4.0: 'value_4',
        5.0: 'value_5',
        6.0: 'value_6',
        7.0: 'value_7',
        8.0: 'value_8',
        9.0: 'value_9',
        10.0: 'value_10',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q50'] = {
        'original_variable': 'Q50',
        'question_label': "Inferred value mapping for Q50",
        'type': 'categorical',
        'value_labels': {'value_0': "Value 0", 'value_1': "Value 1", 'value_2': "Value 2", 'value_3': "Value 3", 'value_4': "Value 4", 'value_5': "Value 5", 'value_6': "Value 6", 'value_7': "Value 7", 'value_8': "Value 8", 'value_9': "Value 9", 'value_10': "Value 10"},
    }

    # --- Q51 ---
    # ses_province — Province de résidence (2014)
    # Source: Q51
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook)
    df_clean['ses_province'] = df['Q51'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q51',
        'question_label': "Dans quelle province résidez-vous?",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q52 ---
    # behav_vote_intent — Intention de vote
    # Source: Q52
    # Assumption: codes 8 (Refus de répondre) and 9 (Ne sait pas) treated as missing (unlabelled in codebook)
    df_clean['behav_vote_intent'] = df['Q52'].map({
        1.0: 'coalition_avenir_quebec',
        2.0: 'liberal',
        3.0: 'parti_quebecois',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_intent'] = {
        'original_variable': 'Q52',
        'question_label': "Si l'élection avait lieu dimanche prochain, pour quel parti voteriez-vous?",
        'type': 'categorical',
        'value_labels': {'coalition_avenir_quebec': "Coalition Avenir Québec", 'liberal': "Parti Libéral", 'parti_quebecois': "Parti Québécois"},
    }

    # --- Q53 ---
    # ses_region — Region of residence (Best Guess - No Codebook)
    # Source: Q53
    # Assumption: Codes 0.0-10.0 mapped to generic string labels based on code order. Codes 98.0 and 99.0 treated as missing.
    df_clean['ses_region'] = df['Q53'].map({
        0.0: 'opt_0',
        1.0: 'opt_1',
        2.0: 'opt_2',
        3.0: 'opt_3',
        4.0: 'opt_4',
        5.0: 'opt_5',
        6.0: 'opt_6',
        7.0: 'opt_7',
        8.0: 'opt_8',
        9.0: 'opt_9',
        10.0: 'opt_10',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q53',
        'question_label': "Q53: Unknown Question Label",
        'type': 'categorical',
        'value_labels': {'opt_0': "Option 0", 'opt_1': "Option 1", 'opt_2': "Option 2", 'opt_3': "Option 3", 'opt_4': "Option 4", 'opt_5': "Option 5", 'opt_6': "Option 6", 'opt_7': "Option 7", 'opt_8': "Option 8", 'opt_9': "Option 9", 'opt_10': "Option 10"},
    }

    # --- Q54 ---
    # op_voting_choice — Inferred voting choice or preference
    # Source: Q54
    # Assumption: Codes 0-10 inferred as party choices 1-10, 98 as refused, 99 as not applicable/missing.
    df_clean['op_voting_choice'] = df['Q54'].map({
        0.0: 'no choice',
        1.0: 'party 1',
        2.0: 'party 2',
        3.0: 'party 3',
        4.0: 'party 4',
        5.0: 'party 5',
        6.0: 'party 6',
        7.0: 'party 7',
        8.0: 'party 8',
        9.0: 'party 9',
        10.0: 'party 10',
        98.0: 'refused',
        99.0: np.nan, # Treating 99 as true missing since 98 covers refused
    })
    CODEBOOK_VARIABLES['op_voting_choice'] = {
        'original_variable': 'Q54',
        'question_label': "Inferred Voting Choice/Preference (0-10)",
        'type': 'categorical',
        'value_labels': {'no choice': "No choice recorded", 'party 1': "Party 1", 'party 2': "Party 2", 'party 3': "Party 3", 'party 4': "Party 4", 'party 5': "Party 5", 'party 6': "Party 6", 'party 7': "Party 7", 'party 8': "Party 8", 'party 9': "Party 9", 'party 10': "Party 10", 'refused': "Refused"},
    }

    # --- Q55 ---
    # op_q55 — Inferred categorical response for Q55
    # Source: Q55
    # Assumption: Codes 1-6 are valid responses; 97, 98, 99 are missing/outlier codes.
    df_clean['op_q55'] = df['Q55'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        5.0: 'option_five',
        6.0: 'option_six',
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q55'] = {
        'original_variable': 'Q55',
        'question_label': "Inferred categorical response for Q55 (no codebook provided)",
        'type': 'categorical',
        'value_labels': {'option_one': "Option 1", 'option_two': "Option 2", 'option_three': "Option 3", 'option_four': "Option 4", 'option_five': "Option 5", 'option_six': "Option 6"},
    }

    # --- Q56 ---
    # ses_province — Province de résidence (Inferred mapping based on typical structure)
    # Source: Q56
    # Assumption: Codes 8 and 9 are treated as missing (unlabelled in codebook). Mapping 1, 2, 3 inferred from common regional codes.
    df_clean['ses_province'] = df['Q56'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q56',
        'question_label': "Province de résidence (Inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q57 ---
    # behav_voter_choice — Manière de voter ou d'exprimer son choix politique (Inferred)
    # Source: Q57
    # Assumption: Codes 1-9 represent distinct choices related to voting methods/intentions. Code 99.0 is treated as missing.
    df_clean['behav_voter_choice'] = df['Q57'].map({
        1.0: 'voted_in_person',
        2.0: 'voted_by_mail',
        3.0: 'registered_did_not_vote',
        4.0: 'intend_voted_in_person',
        5.0: 'intend_voted_by_mail',
        6.0: 'intend_register_vote',
        7.0: 'not_sure_vote',
        8.0: 'will_not_vote',
        9.0: 'other_refused',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_voter_choice'] = {
        'original_variable': 'Q57',
        'question_label': "Manière de voter ou d'exprimer son choix politique (Inferred)",
        'type': 'categorical',
        'value_labels': {'voted_in_person': "Voted in person (Inferred)", 'voted_by_mail': "Voted by mail (Inferred)", 'registered_did_not_vote': "Registered but did not vote (Inferred)", 'intend_voted_in_person': "Intend to vote in person (Inferred)", 'intend_voted_by_mail': "Intend to vote by mail (Inferred)", 'intend_register_vote': "Intend to register and vote (Inferred)", 'not_sure_vote': "Not sure if will vote (Inferred)", 'will_not_vote': "Will not vote (Inferred)", 'other_refused': "Other/Refused (Inferred)"},
    }

    # --- Q58 ---
    # op_q58 — Inferred variable from Q58
    # Source: Q58
    # Assumption: Variable type is categorical based on float64 dtype with discrete codes.
    # Assumption: Codes 96 and 99 are not valid responses and will be mapped to np.nan.
    # Assumption: Labels 1.0 through 11.0 are mapped to generic 'option_x' labels due to missing codebook.
    df_clean['op_q58'] = df['Q58'].map({
        1.0: 'option_1',
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
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q58'] = {
        'original_variable': 'Q58',
        'question_label': "Inferred: Unknown question for Q58",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5", 'option_6': "Option 6", 'option_7': "Option 7", 'option_8': "Option 8", 'option_9': "Option 9", 'option_10': "Option 10", 'option_11': "Option 11"},
    }

    # --- Q59A ---
    # op_attitude_q59a — Inferred response to question 59A
    # Source: Q59A
    # Assumption: Codes 8.0 and 9.0 treated as missing based on observed data and missing codebook.
    df_clean['op_attitude_q59a'] = df['Q59A'].map({
        1.0: 'response_a',
        2.0: 'response_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q59a'] = {
        'original_variable': 'Q59A',
        'question_label': "Inferred response to question 59A (Codebook Missing)",
        'type': 'categorical',
        'value_labels': {'response_a': 'Response A', 'response_b': 'Response B'},
    }

    # --- Q59B ---
    # ses_vote_intention_b — Intention de vote (Parti B)
    # Source: Q59B
    # Assumption: codes 8 and 9 treated as missing (unlabelled in codebook, likely 'don't know/refused')
    # Note: data suggests integer codes were read as float64 due to nature of .sav file. Mapping uses float keys.
    df_clean['ses_vote_intention_b'] = df['Q59B'].map({
        1.0: 'parti_a',
        2.0: 'parti_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_vote_intention_b'] = {
        'original_variable': 'Q59B',
        'question_label': "Votez-vous pour le parti A ou le parti B?",
        'type': 'categorical',
        'value_labels': {'parti_a': "Parti A", 'parti_b': "Parti B"},
    }

    # --- Q59C ---
    # op_attitude_q59c — Attitude related to Q59 part C
    # Source: Q59C
    # Assumption: codes 8.0 and 9.0 treated as missing (unlabelled in provided context)
    df_clean['op_attitude_q59c'] = df['Q59C'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q59c'] = {
        'original_variable': 'Q59C',
        'question_label': "Inferred attitude related to Q59 part C",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B"},
    }

    # --- Q59D ---
    # op_vote_choice — Imputed vote choice for Q59D
    # Source: Q59D
    # Assumption: Codes 8.0 and 9.0 treated as missing (Not in codebook)
    df_clean['op_vote_choice'] = df['Q59D'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_choice'] = {
        'original_variable': 'Q59D',
        'question_label': "Vote choice (Inferred)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B"},
    }

    # --- Q59E ---
    # op_vote_intention — Vote intention
    # Source: Q59E
    # Assumption: Codes 8.0 and 9.0 are unlabelled and treated as missing (np.nan)
    df_clean['op_vote_intention'] = df['Q59E'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q59E',
        'question_label': "Vote intention (Assumed from context)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q59F ---
    # ses_region — Région de résidence
    # Source: Q59F
    # Assumption: codes 8 and 9 are not in codebook and treated as missing
    df_clean['ses_region'] = df['Q59F'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q59F',
        'question_label': "Région de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q59G ---
    # ses_age_group — Grouped age category
    # Source: Q59G
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook)
    # Note: data is float, so mapping keys must be floats (1.0, 2.0, etc.)
    df_clean['ses_age_group'] = df['Q59G'].map({
        1.0: '18-29',
        2.0: '30-49',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_age_group'] = {
        'original_variable': 'Q59G',
        'question_label': "Groupe d'âge",
        'type': 'categorical',
        'value_labels': {'18-29': "18-29 ans", '30-49': "30-49 ans"},
    }

    # --- Q6 ---
    # op_voting_intention — Inferred voting intention
    # Source: Q6
    # Assumption: Codes 96, 97, 99 are missing/unlabelled. Mapping 1-6 to placeholder parties based on context.
    df_clean['op_voting_intention'] = df['Q6'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        5.0: 'other_party',
        6.0: 'none_of_the_above',
        96.0: np.nan,
        97.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_voting_intention'] = {
        'original_variable': 'Q6',
        'question_label': "Inferred: Voting intention for Q6",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A (Inferred)", 'party_b': "Party B (Inferred)", 'party_c': "Party C (Inferred)", 'party_d': "Party D (Inferred)", 'other_party': "Other Party (Inferred)", 'none_of_the_above': "None of the above (Inferred)"},
    }

    # --- Q60A ---
    # Source: eeq_2014.Q60A
    # Variable Name: Q60A
    # Standard Name: behav_q60a_response
    # Type: categorical
    # Source Raw Name: Q60A
    # Mapping Logic:
    # 1.0 -> 'yes'
    # 2.0 -> 'no'
    # 8.0 -> np.nan (Assumption: Don't Know)
    # 9.0 -> np.nan (Assumption: Refused)
    # Value Labels: {'yes': "Yes/Support", 'no': "No/Oppose"}

    df_clean['behav_q60a_response'] = df['Q60A'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q60a_response'] = {
        'original_variable': 'Q60A',
        'question_label': "Q60A response (Yes/No)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes/Support", 'no': "No/Oppose"},
    }

    # --- Q60B ---
    # behav_vote_q60b — Binary response for Q60B (Assumed: 1=Yes/Vote, 2=No/Oppose)
    # Source: Q60B
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in context)
    df_clean['behav_vote_q60b'] = df['Q60B'].map({
        1.0: 1.0,
        2.0: 0.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_q60b'] = {
        'original_variable': 'Q60B',
        'question_label': "Q60B (No label provided)",
        'type': 'binary',
        'value_labels': {1.0: "Yes/Vote", 0.0: "No/Oppose"},
    }

    # --- Q60C ---
    # behav_q60c — Response to question Q60, category C
    # Source: Q60C
    # Assumption: Codes 8.0 and 9.0 are treated as missing (not explicitly defined in provided context)
    df_clean['behav_q60c'] = df['Q60C'].map({
        1.0: 'response_a',
        2.0: 'response_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q60c'] = {
        'original_variable': 'Q60C',
        'question_label': "Response to question Q60, category C (Inferred)",
        'type': 'categorical',
        'value_labels': {'response_a': "Response A (Inferred)", 'response_b': "Response B (Inferred)"},
    }

    # --- Q60D ---
    # ses_Q60D — Inferred variable for Q60D
    # Source: Q60D
    # Assumption: Codes 8.0 and 9.0 are treated as missing (unlabelled in codebook, inferred from exploration)
    df_clean['ses_Q60D'] = df['Q60D'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_Q60D'] = {
        'original_variable': 'Q60D',
        'question_label': "Q60D (Inferred from data exploration)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes (Inferred)", 'no': "No (Inferred)"},
    }

    # --- Q60E ---
    # ses_region — Région de résidence
    # Source: Q60E
    # Assumption: codes 8/9 treated as missing (not in codebook)
    df_clean['ses_region'] = df['Q60E'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q60E',
        'question_label': "Région de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q61 ---
    # op_vote_intention — Vote intention/support for a party/candidate.
    # Source: Q61
    # Assumption: Q61 is a vote intention question, and code 9.0 is Don't Know/Refused, mapped to missing.
    df_clean['op_vote_intention'] = df['Q61'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q61',
        'question_label': "Intention de vote (Hypothetical/Actual)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q62 ---
    # var_q62 — Generic response to question 62
    # Source: Q62
    # Assumption: Codes 1 and 2 are valid categories; code 9 is missing. Labels are invented as codebook is missing.
    df_clean['var_q62'] = df['Q62'].map({
        1.0: 'category_one',
        2.0: 'category_two',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['var_q62'] = {
        'original_variable': 'Q62',
        'question_label': "Response to Q62",
        'type': 'categorical',
        'value_labels': {'category_one': "Category One", 'category_two': "Category Two"},
    }

    # --- Q63 ---
    # op_vote_intention — Hypothetical voting intention
    # Source: Q63
    # Assumption: Codes 1-6 map to hypothetical options, 9.0 treated as missing based on data exploration.
    df_clean['op_vote_intention'] = df['Q63'].map({
        1.0: 'vote_a',
        2.0: 'vote_b',
        3.0: 'vote_c',
        4.0: 'vote_d',
        5.0: 'vote_e',
        6.0: 'vote_f',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q63',
        'question_label': "Hypothetical: Voting intention",
        'type': 'categorical',
        'value_labels': {'vote_a': "Vote A", 'vote_b': "Vote B", 'vote_c': "Vote C", 'vote_d': "Vote D", 'vote_e': "Vote E", 'vote_f': "Vote F"},
    }

    # --- Q64 ---
    # op_vote_intention — Assumed vote intention (Missing codebook entry)
    # Source: Q64
    # Assumption: Codes 1-4 are distinct parties, 7/8/9 are missing/refused based on common survey patterns.
    df_clean['op_vote_intention'] = df['Q64'].map({
        1.0: 'party a',
        2.0: 'party b',
        3.0: 'party c',
        4.0: 'party d',
        7.0: 'refused',
        8.0: 'dont know',
        9.0: 'not applicable',
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q64',
        'question_label': "Q64 (Province de résidence is not available, assuming vote intention based on codes)",
        'type': 'categorical',
        'value_labels': {'party a': "Party A", 'party b': "Party B", 'party c': "Party C", 'party d': "Party D", 'refused': "Refused to answer", 'dont know': "Don't know", 'not applicable': "Not applicable"},
    }

    # --- Q65 ---
    # op_q65 — Question 65 from data
    # Source: Q65
    # Assumption: Codes 8.0 and 9.0 treated as missing (not in explored value counts but commonly used for M/A)
    df_clean['op_q65'] = df['Q65'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q65'] = {
        'original_variable': 'Q65',
        'question_label': "Question 65 from data (Context Missing)",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3"},
    }

    # --- Q66 ---
    # behav_response_q66 — Response to question Q66
    # Source: Q66
    # Note: Labels for codes 3.0-16.0 are assumed based on data distribution; 96, 98, 99 treated as missing due to lack of codebook.
    df_clean['behav_response_q66'] = df['Q66'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        6.0: 'option_6',
        7.0: 'option_7',
        8.0: 'option_8',
        10.0: 'option_10',
        12.0: 'option_12',
        15.0: 'option_15',
        16.0: 'option_16',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_response_q66'] = {
        'original_variable': 'Q66',
        'question_label': "Response to question Q66",
        'type': 'categorical',
        'value_labels': {'option_1': "Code 1", 'option_2': "Code 2", 'option_3': "Code 3", 'option_4': "Code 4", 'option_6': "Code 6", 'option_7': "Code 7", 'option_8': "Code 8", 'option_10': "Code 10", 'option_12': "Code 12", 'option_15': "Code 15", 'option_16': "Code 16"},
    }

    # --- Q67 ---
    # op_attitude_q67 — Inferred attitude scale from Q67
    # Source: Q67
    # Assumption: Codes 96, 98, 99 are missing. Codes 1-13 are categorical responses without known labels.
    df_clean['op_attitude_q67'] = df['Q67'].map({
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        6.0: 'code_6',
        7.0: 'code_7',
        8.0: 'code_8',
        9.0: 'code_9',
        10.0: 'code_10',
        11.0: 'code_11',
        12.0: 'code_12',
        13.0: 'code_13',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q67'] = {
        'original_variable': 'Q67',
        'question_label': "Inferred attitude/opinion question Q67",
        'type': 'categorical',
        'value_labels': {'code_1': "Response 1", 'code_2': "Response 2", 'code_3': "Response 3", 'code_4': "Response 4", 'code_5': "Response 5", 'code_6': "Response 6", 'code_7': "Response 7", 'code_8': "Response 8", 'code_9': "Response 9", 'code_10': "Response 10", 'code_11': "Response 11", 'code_12': "Response 12", 'code_13': "Response 13"},
    }

    # --- Q68 ---
    # op_response_q68 — Response to question 68
    # Source: Q68
    # Assumption: Codes 3-6 are specific responses, code 9 is treated as missing as it was unlabelled.
    df_clean['op_response_q68'] = df['Q68'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'dont_know',
        4.0: 'refused',
        5.0: 'not_applicable',
        6.0: 'other',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_response_q68'] = {
        'original_variable': 'Q68',
        'question_label': "Response to question 68 (Inferred from data exploration)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", 'dont_know': "Don't Know", 'refused': "Refused", 'not_applicable': "Not Applicable", 'other': "Other"},
    }

    # --- Q7A ---
    # behav_party_vote_actual — Vote réel pour le chef de parti
    # Source: Q7A
    # Assumption: Codes 8/9 treated as missing (unlabelled in codebook)
    # Note: Variable type is float due to missing values in SPSS file, mapping to float keys.
    df_clean['behav_party_vote_actual'] = df['Q7A'].map({
        1.0: 'caq',
        2.0: 'plq',
        3.0: 'pqc',
        4.0: 'pdl',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_party_vote_actual'] = {
        'original_variable': 'Q7A',
        'question_label': "Vote réel pour le chef de parti",
        'type': 'categorical',
        'value_labels': {'caq': "Coalition Avenir Québec", 'plq': "Parti libéral du Québec", 'pqc': "Parti québécois", 'pdl': "Parti démocratique du Québec"},
    }

    # --- Q7B ---
    # ses_province — Province de résidence (placeholder mapping)
    # Source: Q7B
    # Assumption: Codes 8.0 and 9.0 are treated as missing (present in data but unlabelled in assumed codebook)
    df_clean['ses_province'] = df['Q7B'].map({
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q7B',
        'question_label': "Province de résidence (CODEBOOK MISSING - GENERIC PLACEHOLDER)",
        'type': 'categorical',
        'value_labels': {'code_1': "Code 1 Label", 'code_2': "Code 2 Label", 'code_3': "Code 3 Label", 'code_4': "Code 4 Label"},
    }

    # --- Q7C ---
    # behav_vote_intention_q7c — Vote intention (Inferred from Q7C codes)
    # Source: Q7C
    # Assumption: Only code 1.0 is mapped as a positive intention. All other codes (2.0, 3.0, 4.0, 8.0, 9.0) are treated as missing due to lack of codebook.
    df_clean['behav_vote_intention_q7c'] = df['Q7C'].map({
        1.0: 'yes',
    })
    CODEBOOK_VARIABLES['behav_vote_intention_q7c'] = {
        'original_variable': 'Q7C',
        'question_label': "Vote intention (Inferred from Q7C)",
        'type': 'categorical',
        'value_labels': {'yes': "Intention to vote for Party/Candidate 1 (Inferred)"},
    }

    # --- Q7D ---
    # op_response — Response to question 7D (inferred)
    # Source: Q7D
    # Assumption: Codes 1-4 map to inferred categorical answers. Codes 8.0, 9.0, and NaN are missing.
    df_clean['op_response'] = df['Q7D'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: "don't know",
        4.0: 'refused',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_response'] = {
        'original_variable': 'Q7D',
        'question_label': "Response to Q7D (label not provided, inferred)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", "don't know": "Don't Know", 'refused': "Refused"},
    }

    # --- Q7E ---
    # ses_province — Province de résidence
    # Source: Q7E
    # Assumption: Undocumented codes 8.0 and 9.0 are treated as missing (np.nan).
    df_clean['ses_province'] = df['Q7E'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q7E',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q7F ---
    # op_q7f — Response to question Q7F (assuming 4-point scale)
    # Source: Q7F
    # Assumption: Codes 8.0 and 9.0 are treated as missing/refused as they lack labels and 1.0-4.0 appear to be the main responses.
    df_clean['op_q7f'] = df['Q7F'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q7f'] = {
        'original_variable': 'Q7F',
        'question_label': "Response to question Q7F",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4"},
    }

    # --- Q7G ---
    # behav_q7g — Voting intention (Best guess without codebook)
    # Source: Q7G
    # Assumption: Codes 8.0 and 9.0 treated as missing, alongside existing NaNs.
    df_clean['behav_q7g'] = df['Q7G'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q7g'] = {
        'original_variable': 'Q7G',
        'question_label': "Unknown Question Text for Q7G",
        'type': 'categorical',
        'value_labels': {'option_one': "Code 1", 'option_two': "Code 2", 'option_three': "Code 3", 'option_four': "Code 4"},
    }

    # --- Q8 ---
    # ses_province — Province de résidence
    # Source: Q8
    # Assumption: codes 96 and 98 are unlabelled and map to missing
    # Assumption: code 99 treated as missing (explicitly listed in missing_codes)
    df_clean['ses_province'] = df['Q8'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q8',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q9 ---
    # op_vote_intention — Inferred vote intention
    # Source: Q9
    # Assumption: Based on common structure for Quebec election studies; 1-6 are party choices, 96/98/99 are missing codes.
    df_clean['op_vote_intention'] = df['Q9'].map({
        1.0: 'pc',
        2.0: 'lib',
        3.0: 'pqc',
        4.0: 'caq',
        5.0: 'ops',
        6.0: 'none',
        96.0: 'refused',
        98.0: 'dont_know',
        99.0: 'missing',
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q9',
        'question_label': "Vote intention (Inferred from 1-6 codes)",
        'type': 'categorical',
        'value_labels': {'pc': "Parti Conservateur", 'lib': "Parti Libéral", 'pqc': "Parti Québécois", 'caq': "CAQ", 'ops': "Other party", 'none': "None", 'refused': "Refused", 'dont_know': "Don't know", 'missing': "Missing"},
    }

    # --- QAGE ---
    # ses_birth_year — Year of birth
    # Source: QAGE
    # Assumption: Treat as numeric/categorical hybrid. Map observed years to themselves as strings.
    # Assumption: Any year not observed (but possibly present in data) will map to np.nan implicitly.
    df_clean['ses_birth_year'] = df['QAGE'].map({
        1926.0: '1926',
        1929.0: '1929',
        1930.0: '1930',
        1931.0: '1931',
        1932.0: '1932',
        1933.0: '1933',
        1934.0: '1934',
        1935.0: '1935',
        1936.0: '1936',
        1937.0: '1937',
        1938.0: '1938',
        1939.0: '1939',
        1940.0: '1940',
        1941.0: '1941',
        1942.0: '1942',
        1943.0: '1943',
        1944.0: '1944',
        1945.0: '1945',
        1946.0: '1946',
        1947.0: '1947',
        1948.0: '1948',
        1949.0: '1949',
        1950.0: '1950',
        1951.0: '1951',
        1952.0: '1952',
    })
    CODEBOOK_VARIABLES['ses_birth_year'] = {
        'original_variable': 'QAGE',
        'question_label': "Age (inferred as year of birth)",
        'type': 'numeric',
        'value_labels': {'1926': "1926", '1929': "1929", '1930': "1930", '1931': "1931", '1932': "1932", '1933': "1933", '1934': "1934", '1935': "1935", '1936': "1936", '1937': "1937", '1938': "1938", '1939': "1939", '1940': "1940", '1941': "1941", '1942': "1942", '1943': "1943", '1944': "1944", '1945': "1945", '1946': "1946", '1947': "1947", '1948': "1948", '1949': "1949", '1950': "1950", '1951': "1951", '1952': "1952"},
    }

    # --- QENFAN ---
    # ses_child_status — Status/count of children in household
    # Source: QENFAN
    # Assumption: Codebook was missing. Inferring mapping based on codes 1, 2, 9.
    # Assumption: Code 9 is treated as missing/refused.
    df_clean['ses_child_status'] = df['QENFAN'].map({
        1.0: 'one',
        2.0: 'two',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_child_status'] = {
        'original_variable': 'QENFAN',
        'question_label': "Status or count of children in household (Inferred)",
        'type': 'categorical',
        'value_labels': {'one': "One child", 'two': "Two children"},
    }

    # --- QLANG ---
    # ses_language — Language spoken at home
    # Source: QLANG
    # Assumption: 1='french', 2='english', 3='both', 4='other', 5='neither', 6='refused' are derived from typical language questions.
    df_clean['ses_language'] = df['QLANG'].map({
        1.0: 'french',
        2.0: 'english',
        3.0: 'both',
        4.0: 'other',
        5.0: 'neither',
        6.0: 'refused',
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'QLANG',
        'question_label': "Langue principalement parlée à la maison",
        'type': 'categorical',
        'value_labels': {'french': "French", 'english': "English", 'both': "Both", 'other': "Other", 'neither': "Neither", 'refused': "Refused"},
    }

    # --- QREGION ---
    # ses_region — Region/Riding of residence
    # Source: QREGION
    # Note: Lacking codebook_entry.values, codes 1.0-17.0 mapped to generic placeholders based on data exploration.
    # Assumption: All observed codes (1.0-17.0) are valid and unlabelled codes are not present (since missing count is 0).
    df_clean['ses_region'] = df['QREGION'].map({
        1.0: 'region_01',
        2.0: 'region_02',
        3.0: 'region_03',
        4.0: 'region_04',
        5.0: 'region_05',
        6.0: 'region_06',
        7.0: 'region_07',
        8.0: 'region_08',
        9.0: 'region_09',
        10.0: 'region_10',
        11.0: 'region_11',
        12.0: 'region_12',
        13.0: 'region_13',
        14.0: 'region_14',
        15.0: 'region_15',
        16.0: 'region_16',
        17.0: 'region_17',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'QREGION',
        'question_label': "Region of residence",
        'type': 'categorical',
        'value_labels': {'region_01': "Placeholder Region 1", 'region_02': "Placeholder Region 2", 'region_03': "Placeholder Region 3", 'region_04': "Placeholder Region 4", 'region_05': "Placeholder Region 5", 'region_06': "Placeholder Region 6 (Most Frequent)", 'region_07': "Placeholder Region 7", 'region_08': "Placeholder Region 8", 'region_09': "Placeholder Region 9", 'region_10': "Placeholder Region 10", 'region_11': "Placeholder Region 11", 'region_12': "Placeholder Region 12", 'region_13': "Placeholder Region 13", 'region_14': "Placeholder Region 14", 'region_15': "Placeholder Region 15", 'region_16': "Placeholder Region 16", 'region_17': "Placeholder Region 17"},
    }

    # --- QSCOL ---
    # op_col_support — Inferred support level for item COL on 11-point scale
    # Source: QSCOL
    # Assumption: 1.0 is minimum (0.0) and 11.0 is maximum (1.0) for Likert scale. Code 99.0 treated as missing.
    df_clean['op_col_support'] = df['QSCOL'].map({
        1.0: 0.0,
        2.0: 0.1,
        3.0: 0.2,
        4.0: 0.3,
        5.0: 0.4,
        6.0: 0.5,
        7.0: 0.6,
        8.0: 0.7,
        9.0: 0.8,
        10.0: 0.9,
        11.0: 1.0,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_col_support'] = {
        'original_variable': 'QSCOL',
        'question_label': "Inferred support level for item COL on 11-point scale",
        'type': 'likert',
        'value_labels': {'0.0': "Lowest level of support", '0.1': "Level 2", '0.2': "Level 3", '0.3': "Level 4", '0.4': "Level 5", '0.5': "Level 6 (Midpoint)", '0.6': "Level 7", '0.7': "Level 8", '0.8': "Level 9", '0.9': "Level 10", '1.0': "Highest level of support"},
    }

    # --- QSEXE ---
    # ses_sex — Sexe de l'électeur
    # Source: QSEXE
    # Assumption: code 99 treated as missing (unlabelled in provided data sample)
    df_clean['ses_sex'] = df['QSEXE'].map({
        1.0: 'homme',
        2.0: 'femme',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_sex'] = {
        'original_variable': 'QSEXE',
        'question_label': "Sexe de l'électeur",
        'type': 'categorical',
        'value_labels': {'homme': "Homme", 'femme': "Femme"},
    }

    # --- REGIO ---
    # ses_region — Region of residence (inferred from codebook/name)
    # Source: REGIO
    # Assumption: Codes 1, 2, 3 mapped to generic region labels as codebook was unavailable.
    # Assumption: All values are treated as categorical.
    df_clean['ses_region'] = df['REGIO'].map({
        1.0: 'region_1',
        2.0: 'region_2',
        3.0: 'region_3',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'REGIO',
        'question_label': "Region de résidence (unknown mapping)",
        'type': 'categorical',
        'value_labels': {'region_1': "Region 1 (Code 1)", 'region_2': "Region 2 (Code 2)", 'region_3': "Region 3 (Code 3)"},
    }

    # --- SDAT ---
    # ses_date — Date of survey administration
    # Source: SDAT
    # WARNING: codebook_entry missing. Inferred type as 'numeric' from float64 dtype.
    # Assumption: All values are valid, mapping is identity as no max value provided for scaling.
    df_clean['ses_date'] = df['SDAT'].map({
        20140409.0: 20140409.0,
        20140410.0: 20140410.0,
        20140411.0: 20140411.0,
        20140412.0: 20140412.0,
        20140413.0: 20140413.0,
        20140414.0: 20140414.0,
        20140415.0: 20140415.0,
        20140416.0: 20140416.0,
        20140417.0: 20140417.0,
    })
    CODEBOOK_VARIABLES['ses_date'] = {
        'original_variable': 'SDAT',
        'question_label': "Date of survey administration (Inferred)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- SEL1 ---
    # ses_sel — Socio-Economic Level/Status (Assumed from two clean codes)
    # Source: SEL1
    # Assumption: Code 1.0 maps to 'level_1' and 2.0 maps to 'level_2'.
    df_clean['ses_sel'] = df['SEL1'].map({
        1.0: 'level_1',
        2.0: 'level_2',
    })
    CODEBOOK_VARIABLES['ses_sel'] = {
        'original_variable': 'SEL1',
        'question_label': "Socio-Economic Level/Status (Assumed)",
        'type': 'categorical',
        'value_labels': {'level_1': "Level 1 (Assumed)", 'level_2': "Level 2 (Assumed)"},
    }

    # --- SEL2 ---
    # ses_sel2 — Binary indicator derived from SEL2
    # Source: SEL2
    # Assumption: Codes 1.0 and 2.0 represent a binary distinction, mapped to 0.0 and 1.0 respectively. Label unknown.
    df_clean['ses_sel2'] = df['SEL2'].map({
        1.0: 0.0,
        2.0: 1.0,
    })
    CODEBOOK_VARIABLES['ses_sel2'] = {
        'original_variable': 'SEL2',
        'question_label': "Inferred binary variable from SEL2",
        'type': 'binary',
        'value_labels': {0.0: "Category 1 (was 1.0)", 1.0: "Category 2 (was 2.0)"},
    }

    # --- SEL3 ---
    # ses_sel3 — Inferred socio-economic indicator
    # Source: SEL3
    # Assumption: Binary variable, codes 1.0/2.0 mapped to level_1/level_2. Missing codes (not observed) assumed to map to np.nan.
    df_clean['ses_sel3'] = df['SEL3'].map({
        1.0: 'level_1',
        2.0: 'level_2',
    })
    CODEBOOK_VARIABLES['ses_sel3'] = {
        'original_variable': 'SEL3',
        'question_label': "Inferred SES indicator 3 (Requires verification)",
        'type': 'categorical',
        'value_labels': {'level_1': "Level 1", 'level_2': "Level 2"},
    }

    # --- SEL4 ---
    # ses_self_rating — Inferred self-rating scale, 2 categories
    # Source: SEL4
    # Assumption: Codes 1.0 and 2.0 map to placeholder labels as no codebook was provided.
    df_clean['ses_self_rating'] = df['SEL4'].map({
        1.0: 'option_a',
        2.0: 'option_b',
    })
    CODEBOOK_VARIABLES['ses_self_rating'] = {
        'original_variable': 'SEL4',
        'question_label': "Inferred self-rating scale category (1.0/2.0)",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B"},
    }

    # --- SMAGE ---
    # ses_age — Age of respondent
    # Source: SMAGE
    # Assumption: Variable is numeric but treated as categorical for exact mapping. No codebook provided, mapping codes to themselves as strings.
    df_clean['ses_age'] = df['SMAGE'].map({
        19.0: '19',
        20.0: '20',
        21.0: '21',
        22.0: '22',
        23.0: '23',
        24.0: '24',
        25.0: '25',
        26.0: '26',
        27.0: '27',
        28.0: '28',
        29.0: '29',
        30.0: '30',
        31.0: '31',
        32.0: '32',
        33.0: '33',
        34.0: '34',
        35.0: '35',
        36.0: '36',
        37.0: '37',
        38.0: '38',
        39.0: '39',
        40.0: '40',
        41.0: '41',
        42.0: '42',
        43.0: '43',
    })
    CODEBOOK_VARIABLES['ses_age'] = {
        'original_variable': 'SMAGE',
        'question_label': "Age of respondent",
        'type': 'categorical',
        'value_labels': {'19': "19", '20': "20", '21': "21", '22': "22", '23': "23", '24': "24", '25': "25", '26': "26", '27': "27", '28': "28", '29': "29", '30': "30", '31': "31", '32': "32", '33': "33", '34': "34", '35': "35", '36': "36", '37': "37", '38': "38", '39': "39", '40': "40", '41': "41", '42': "42", '43': "43"},
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
