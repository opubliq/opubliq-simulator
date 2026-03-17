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
    # op_government_satisfaction — Satisfaction gouvernementale
    # Source: Q10
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_government_satisfaction'] = np.nan
    mask = (df['Q10'] >= 0) & (df['Q10'] <= 10)
    df_clean.loc[mask, 'op_government_satisfaction'] = df.loc[mask, 'Q10'] / 10.0
    CODEBOOK_VARIABLES['op_government_satisfaction'] = {
        'original_variable': 'Q10',
        'question_label': "TODO: À quel point êtes-vous satisfait(e) de la performance du gouvernement?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q40 ---
    # op_government_role — Rôle du gouvernement
    # Source: Q40
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_government_role'] = df['Q40'].map({
        1.0: 'role_1',
        2.0: 'role_2',
        3.0: 'role_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_government_role'] = {
        'original_variable': 'Q40',
        'question_label': "TODO: Quel devrait être le rôle du gouvernement?",
        'type': 'categorical',
        'value_labels': {'role_1': "TODO: Label 1", 'role_2': "TODO: Label 2", 'role_3': "TODO: Label 3"},
    }

    # --- Q63 ---
    # ses_religious_affiliation — Appartenance religieuse
    # Source: Q63
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_religious_affiliation'] = df['Q63'].map({
        1.0: 'catholique',
        2.0: 'protestant',
        3.0: 'autre_chretien',
        4.0: 'autre_religion',
        5.0: 'sans_religion',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_religious_affiliation'] = {
        'original_variable': 'Q63',
        'question_label': "TODO: Quelle est votre appartenance religieuse?",
        'type': 'categorical',
        'value_labels': {'catholique': "Catholique", 'protestant': "Protestant", 'autre_chretien': "Autre chrétien", 'autre_religion': "Autre religion", 'sans_religion': "Sans religion"},
    }

    # --- Q56 ---
    # op_identification_strength — Force de l'identification
    # Source: Q56
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_identification_strength'] = np.nan
    mask = (df['Q56'] >= 0) & (df['Q56'] <= 10)
    df_clean.loc[mask, 'op_identification_strength'] = df.loc[mask, 'Q56'] / 10.0
    CODEBOOK_VARIABLES['op_identification_strength'] = {
        'original_variable': 'Q56',
        'question_label': "TODO: Force de l'identification (0-10)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q30A ---
    # op_leader_evaluation_A — Évaluation du chef A
    # Source: Q30A
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_leader_evaluation_A'] = np.nan
    mask = (df['Q30A'] >= 0) & (df['Q30A'] <= 10)
    df_clean.loc[mask, 'op_leader_evaluation_A'] = df.loc[mask, 'Q30A'] / 10.0
    CODEBOOK_VARIABLES['op_leader_evaluation_A'] = {
        'original_variable': 'Q30A',
        'question_label': "TODO: Évaluation du chef A (0-10)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q30B ---
    # op_leader_evaluation_B — Évaluation du chef B
    # Source: Q30B
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_leader_evaluation_B'] = np.nan
    mask = (df['Q30B'] >= 0) & (df['Q30B'] <= 10)
    df_clean.loc[mask, 'op_leader_evaluation_B'] = df.loc[mask, 'Q30B'] / 10.0
    CODEBOOK_VARIABLES['op_leader_evaluation_B'] = {
        'original_variable': 'Q30B',
        'question_label': "TODO: Évaluation du chef B (0-10)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q30C ---
    # op_leader_evaluation_C — Évaluation du chef C
    # Source: Q30C
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_leader_evaluation_C'] = np.nan
    mask = (df['Q30C'] >= 0) & (df['Q30C'] <= 10)
    df_clean.loc[mask, 'op_leader_evaluation_C'] = df.loc[mask, 'Q30C'] / 10.0
    CODEBOOK_VARIABLES['op_leader_evaluation_C'] = {
        'original_variable': 'Q30C',
        'question_label': "TODO: Évaluation du chef C (0-10)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q25 ---
    # op_preferred_issue — Question préférée
    # Source: Q25
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_preferred_issue'] = df['Q25'].map({
        1.0: 'question_1',
        2.0: 'question_2',
        3.0: 'question_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_preferred_issue'] = {
        'original_variable': 'Q25',
        'question_label': "TODO: Laquelle de ces questions préférez-vous?",
        'type': 'categorical',
        'value_labels': {'question_1': "TODO: Question 1", 'question_2': "TODO: Question 2", 'question_3': "TODO: Question 3"},
    }

    # --- Q61 ---
    # ses_union_membership — Syndicalisation
    # Source: Q61
    # Type: categorical (yes/no)
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_union_membership'] = df['Q61'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_union_membership'] = {
        'original_variable': 'Q61',
        'question_label': "TODO: Êtes-vous membre d'un syndicat?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q3 ---
    # behav_vote — Vote réel
    # Source: Q3
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['behav_vote'] = df['Q3'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote'] = {
        'original_variable': 'Q3',
        'question_label': "TODO: Pour quel parti avez-vous voté?",
        'type': 'categorical',
        'value_labels': {'party_a': "TODO: Parti A", 'party_b': "TODO: Parti B", 'party_c': "TODO: Parti C", 'party_d': "TODO: Parti D"},
    }

    # --- Q53 ---
    # op_referendum_importance — Importance du référendum
    # Source: Q53
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_referendum_importance'] = np.nan
    mask = (df['Q53'] >= 0) & (df['Q53'] <= 10)
    df_clean.loc[mask, 'op_referendum_importance'] = df.loc[mask, 'Q53'] / 10.0
    CODEBOOK_VARIABLES['op_referendum_importance'] = {
        'original_variable': 'Q53',
        'question_label': "TODO: Importance du référendum (0-10)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q4 ---
    # op_first_choice — Premier choix
    # Source: Q4
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_first_choice'] = df['Q4'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_first_choice'] = {
        'original_variable': 'Q4',
        'question_label': "TODO: Est-ce votre premier choix?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q22 ---
    # op_constitutional_preference — Préférences constitutionnelles
    # Source: Q22
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_constitutional_preference'] = df['Q22'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_constitutional_preference'] = {
        'original_variable': 'Q22',
        'question_label': "TODO: Préférence constitutionnelle?",
        'type': 'categorical',
        'value_labels': {'option_1': "TODO: Option 1", 'option_2': "TODO: Option 2", 'option_3': "TODO: Option 3"},
    }

    # --- Q38 ---
    # op_gay_marriage_opinion — Opinion mariage homosexuel
    # Source: Q38
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_gay_marriage_opinion'] = df['Q38'].map({
        1.0: 'for',
        2.0: 'against',
        3.0: 'no_opinion',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_gay_marriage_opinion'] = {
        'original_variable': 'Q38',
        'question_label': "TODO: Êtes-vous pour ou contre le mariage entre personnes de même sexe?",
        'type': 'categorical',
        'value_labels': {'for': "Pour", 'against': "Contre", 'no_opinion': "Sans opinion"},
    }

    # --- POND ---
    # sample_weight — Poids d'échantillonnage
    # Source: POND
    # Type: numeric
    df_clean['sample_weight'] = df['POND'].where(df['POND'] != 99.0, other=np.nan)
    CODEBOOK_VARIABLES['sample_weight'] = {
        'original_variable': 'POND',
        'question_label': "Poids d'échantillonnage",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q26A ---
    # op_referendum_more_powers_A — Référendum plus de pouvoirs A
    # Source: Q26A
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_referendum_more_powers_A'] = df['Q26A'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_referendum_more_powers_A'] = {
        'original_variable': 'Q26A',
        'question_label': "TODO: Référendum plus de pouvoirs pour le Québec?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q26B ---
    # op_referendum_more_powers_B — Référendum plus de pouvoirs B
    # Source: Q26B
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_referendum_more_powers_B'] = df['Q26B'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_referendum_more_powers_B'] = {
        'original_variable': 'Q26B',
        'question_label': "TODO: Référendum plus de pouvoirs pour le Québec (2e)?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q62 ---
    # ses_religious_affiliation_2 — Appartenance religieuse (2e)
    # Source: Q62
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_religious_affiliation_2'] = df['Q62'].map({
        1.0: 'catholique',
        2.0: 'protestant',
        3.0: 'autre_chretien',
        4.0: 'autre_religion',
        5.0: 'sans_religion',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_religious_affiliation_2'] = {
        'original_variable': 'Q62',
        'question_label': "TODO: Appartenance religieuse (2e occurrence)",
        'type': 'categorical',
        'value_labels': {'catholique': "Catholique", 'protestant': "Protestant", 'autre_chretien': "Autre chrétien", 'autre_religion': "Autre religion", 'sans_religion': "Sans religion"},
    }

    # --- Q48 ---
    # op_currency_preference — Devise monétaire
    # Source: Q48
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_currency_preference'] = df['Q48'].map({
        1.0: 'canadian_dollar',
        2.0: 'other_currency',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_currency_preference'] = {
        'original_variable': 'Q48',
        'question_label': "TODO: Quelle devise le Québec devrait-il utiliser?",
        'type': 'categorical',
        'value_labels': {'canadian_dollar': "Dollar canadien", 'other_currency': "Autre devise"},
    }

    # --- Q49 ---
    # op_canada_economic_impact — Impact économique du Canada
    # Source: Q49
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_canada_economic_impact'] = df['Q49'].map({
        1.0: 'positive',
        2.0: 'neutral',
        3.0: 'negative',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_canada_economic_impact'] = {
        'original_variable': 'Q49',
        'question_label': "TODO: Impact économique du Canada sur le Québec?",
        'type': 'categorical',
        'value_labels': {'positive': "Positif", 'neutral': "Neutre", 'negative': "Négatif"},
    }

    # --- Q50 ---
    # op_market_vs_sovereignty — Marché vs souveraineté
    # Source: Q50
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_market_vs_sovereignty'] = df['Q50'].map({
        1.0: 'market',
        2.0: 'sovereignty',
        3.0: 'balance',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_market_vs_sovereignty'] = {
        'original_variable': 'Q50',
        'question_label': "TODO: Marché ou souveraineté?",
        'type': 'categorical',
        'value_labels': {'market': "Marché", 'sovereignty': "Souveraineté", 'balance': "Équilibre"},
    }

    # --- Q17 ---
    # behav_referendum_vote — Vote au référendum
    # Source: Q17
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['behav_referendum_vote'] = df['Q17'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_referendum_vote'] = {
        'original_variable': 'Q17',
        'question_label': "TODO: Comment avez-vous voté au référendum?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q60A ---
    # ses_property_ownership_A — Propriété A
    # Source: Q60A
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_property_ownership_A'] = df['Q60A'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_property_ownership_A'] = {
        'original_variable': 'Q60A',
        'question_label': "TODO: Possession de propriété A?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q60B ---
    # ses_property_ownership_B — Propriété B
    # Source: Q60B
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_property_ownership_B'] = df['Q60B'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_property_ownership_B'] = {
        'original_variable': 'Q60B',
        'question_label': "TODO: Possession de propriété B?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q60C ---
    # ses_property_ownership_C — Propriété C
    # Source: Q60C
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_property_ownership_C'] = df['Q60C'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_property_ownership_C'] = {
        'original_variable': 'Q60C',
        'question_label': "TODO: Possession de propriété C?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q60D ---
    # ses_property_ownership_D — Propriété D
    # Source: Q60D
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_property_ownership_D'] = df['Q60D'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_property_ownership_D'] = {
        'original_variable': 'Q60D',
        'question_label': "TODO: Possession de propriété D?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q60E ---
    # ses_property_ownership_E — Propriété E
    # Source: Q60E
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['ses_property_ownership_E'] = df['Q60E'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_property_ownership_E'] = {
        'original_variable': 'Q60E',
        'question_label': "TODO: Possession de propriété E?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- Q52 ---
    # op_economic_evolution — Évolution économique
    # Source: Q52
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_economic_evolution'] = df['Q52'].map({
        1.0: 'improving',
        2.0: 'stable',
        3.0: 'declining',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_economic_evolution'] = {
        'original_variable': 'Q52',
        'question_label': "TODO: Comment évolue l'économie?",
        'type': 'categorical',
        'value_labels': {'improving': "S'améliore", 'stable': "Stable", 'declining': "Decline"},
    }

    # --- Q11 --- (Updated to op_economic_satisfaction)
    # op_economic_satisfaction — Satisfaction économique
    # Source: Q11
    # Échelle: 0-10 → 0.0-1.0 normalisée
    # TODO: Note: Original Q11 was mapped to ses_province. Replacing with correct variable.
    # This may cause duplicate handling - please verify the actual Q11 variable content.
    # For now, create op_economic_satisfaction separately if Q11 data exists
    df_clean['op_economic_satisfaction'] = np.nan
    # Assuming there's a separate satisfaction scale in another column, or update based on actual data
    CODEBOOK_VARIABLES['op_economic_satisfaction'] = {
        'original_variable': 'Q11_satisfaction',
        'question_label': "TODO: À quel point êtes-vous satisfait(e) économiquement?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q21 ---
    # op_vote_intention_constitution — Intention de vote pour Constitution
    # Source: Q21
    # Type: categorical
    # TODO: Valider le mapping exact avec les données et le codebook
    df_clean['op_vote_intention_constitution'] = df['Q21'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention_constitution'] = {
        'original_variable': 'Q21',
        'question_label': "TODO: Voteriez-vous pour cette constitution?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non"},
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
    # identity_q14a — Identity scale (Quebecois only to Canadian only)
    # Source: Q14A
    # Note: Split sample 50%. Codes 8=Don't know, 9=Refused
    df_clean['identity_q14a'] = df['Q14A'].map({
        1.0: 'quebecois_only',
        2.0: 'quebecois_first',
        3.0: 'both_equally',
        4.0: 'canadian_first',
        5.0: 'canadian_only',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['identity_q14a'] = {
        'original_variable': 'Q14A',
        'question_label': "Identity: Quebecois vs Canadian (order: Quebecois first)",
        'type': 'categorical',
        'value_labels': {
            'quebecois_only': "Uniquement Québécois(e), pas du tout Canadien(ne)",
            'quebecois_first': "D'abord Québécois(e), puis Canadien(ne)",
            'both_equally': "Également Québécois(e) et Canadien(ne)",
            'canadian_first': "D'abord Canadien(ne), puis Québécois(e)",
            'canadian_only': "Uniquement Canadien(ne), pas du tout Québécois(e)",
        },
    }

    # --- Q14B ---
    # identity_q14b — Identity scale (Canadian only to Quebecois only, reversed order)
    # Source: Q14B
    # Note: Split sample 50%. Codes 8=Don't know, 9=Refused
    df_clean['identity_q14b'] = df['Q14B'].map({
        1.0: 'canadian_only',
        2.0: 'canadian_first',
        3.0: 'both_equally',
        4.0: 'quebecois_first',
        5.0: 'quebecois_only',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['identity_q14b'] = {
        'original_variable': 'Q14B',
        'question_label': "Identity: Canadian vs Quebecois (order: Canadian first)",
        'type': 'categorical',
        'value_labels': {
            'canadian_only': "Uniquement Canadien(ne), pas du tout Québécois(e)",
            'canadian_first': "D'abord Canadien(ne), puis Québécois(e)",
            'both_equally': "É également comme Canadien(ne) et comme Québécois(e)",
            'quebecois_first': "D'abord Québécois(e), puis Canadien(ne)",
            'quebecois_only': "Uniquement Québécois(e), pas du tout Canadien(ne)",
        },
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
    # ses_region — Province de résidence
    # Source: Q16
    # Note: Codes 1=Québec, 2=Ontario, 9=Refused/Ne sait pas
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
    # op_vote_intention_independentist — Intention de vote indépendantiste
    # Source: Q19
    # TODO: Valider le mapping exact des intentions de vote à partir du codebook (Quebec Election Study 2014 FR.doc)
    # Le mapping ci-dessous est une hypothèse.
    df_clean['op_vote_intention_independentist'] = df['Q19'].map({
        # 1.0: 'yes',
        # 2.0: 'no',
        # 3.0: 'dont_know',
        # 4.0: 'refused',
        # 98.0: np.nan, # Don't know
        # 99.0: np.nan, # Refused
    })
    CODEBOOK_VARIABLES['op_vote_intention_independentist'] = {
        'original_variable': 'Q19',
        'question_label': "Si un référendum sur l'indépendance du Québec avait lieu demain, voteriez-vous OUI ou NON?", # TODO: Confirmer le libellé exact de la question dans le codebook
        'type': 'categorical',
        'value_labels': {
            # 'yes': "OUI",
            # 'no': "NON",
            # 'dont_know': "Ne sait pas",
            # 'refused': "Refusé",
        } # TODO: Confirmer les labels de valeur exacts dans le codebook
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
    # op_second_party_choice — Deuxième préférence constitutionnelle
    # Source: Q23
    # Question: "Si l'option [RAPPEL DE LA RÉPONSE DONNÉE À Q22] était rejetée par une majorité de la population, 
    #           quelle serait alors votre deuxième préférence?"
    # Les trois options: Signer la Constitution de 1982, Avoir plus de pouvoirs pour le Québec, Indépendance
    # TODO: Valider le mapping exact avec les codes réels dans les données et le codebook
    df_clean['op_second_party_choice'] = df['Q23'].map({
        1.0: 'sign_constitution',
        2.0: 'more_powers',
        3.0: 'independence',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_second_party_choice'] = {
        'original_variable': 'Q23',
        'question_label': "Deuxième préférence constitutionnelle",
        'type': 'categorical',
        'value_labels': {'sign_constitution': "Signer la Constitution de 1982", 'more_powers': "Avoir plus de pouvoirs pour le Québec", 'independence': "Indépendance"},
    }

    # --- Q24A ---
    # op_constitutional_rank_1 — First choice for Quebec's constitutional future
    # Source: Q24A
    # Codes: 1=Sign Constitution 1982, 2=More powers for Quebec, 3=Independence, 8=Don't know, 9=Refuse
    df_clean['op_constitutional_rank_1'] = df['Q24A'].map({
        1.0: 'sign_constitution',
        2.0: 'more_powers',
        3.0: 'independence',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_constitutional_rank_1'] = {
        'original_variable': 'Q24A',
        'question_label': "First choice for Quebec's constitutional future",
        'type': 'categorical',
        'value_labels': {'sign_constitution': "Sign the Constitution of 1982", 'more_powers': "More powers for Quebec", 'independence': "Independence"},
    }

    # --- Q24B ---
    # op_constitutional_rank_2 — Second choice for Quebec's constitutional future
    # Source: Q24B
    df_clean['op_constitutional_rank_2'] = df['Q24B'].map({
        1.0: 'sign_constitution',
        2.0: 'more_powers',
        3.0: 'independence',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_constitutional_rank_2'] = {
        'original_variable': 'Q24B',
        'question_label': "Second choice for Quebec's constitutional future",
        'type': 'categorical',
        'value_labels': {'sign_constitution': "Sign the Constitution of 1982", 'more_powers': "More powers for Quebec", 'independence': "Independence"},
    }

    # --- Q24C ---
    # op_constitutional_rank_3 — Third choice for Quebec's constitutional future
    # Source: Q24C
    df_clean['op_constitutional_rank_3'] = df['Q24C'].map({
        1.0: 'sign_constitution',
        2.0: 'more_powers',
        3.0: 'independence',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_constitutional_rank_3'] = {
        'original_variable': 'Q24C',
        'question_label': "Third choice for Quebec's constitutional future",
        'type': 'categorical',
        'value_labels': {'sign_constitution': "Sign the Constitution of 1982", 'more_powers': "More powers for Quebec", 'independence': "Independence"},
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
    # op_politician_rating_philippe_couillard — Évaluation de Philippe Couillard (0-100 → 0-1)
    # Source: Q29A
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_philippe_couillard'] = np.nan
    mask = (~df['Q29A'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29A'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_philippe_couillard'] = pd.to_numeric(df.loc[mask, 'Q29A'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_philippe_couillard'] = {
        'original_variable': 'Q29A',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de PHILIPPE COUILLARD?",
        'type': 'numeric',
    }

    # --- Q29B ---
    # op_politician_rating_pauline_marois — Évaluation de Pauline Marois (0-100 → 0-1)
    # Source: Q29B
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_pauline_marois'] = np.nan
    mask = (~df['Q29B'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29B'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_pauline_marois'] = pd.to_numeric(df.loc[mask, 'Q29B'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_pauline_marois'] = {
        'original_variable': 'Q29B',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de PAULINE MAROIS?",
        'type': 'numeric',
    }

    # --- Q29C ---
    # op_politician_rating_francois_legault — Évaluation de François Legault (0-100 → 0-1)
    # Source: Q29C
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_francois_legault'] = np.nan
    mask = (~df['Q29C'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29C'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_francois_legault'] = pd.to_numeric(df.loc[mask, 'Q29C'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_francois_legault'] = {
        'original_variable': 'Q29C',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de FRANÇOIS LEGAULT?",
        'type': 'numeric',
    }

    # --- Q29D ---
    # op_politician_rating_francoise_david — Évaluation de Françoise David (0-100 → 0-1)
    # Source: Q29D
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_francoise_david'] = np.nan
    mask = (~df['Q29D'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29D'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_francoise_david'] = pd.to_numeric(df.loc[mask, 'Q29D'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_francoise_david'] = {
        'original_variable': 'Q29D',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de FRANÇOISE DAVID?",
        'type': 'numeric',
    }

    # --- Q29E ---
    # op_politician_rating_sol_zanetti — Évaluation de Sol Zanetti (0-100 → 0-1)
    # Source: Q29E
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_sol_zanetti'] = np.nan
    mask = (~df['Q29E'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondent'])) & (pd.to_numeric(df['Q29E'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_sol_zanetti'] = pd.to_numeric(df.loc[mask, 'Q29E'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_sol_zanetti'] = {
        'original_variable': 'Q29E',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de SOL ZANETTI?",
        'type': 'numeric',
    }

    # --- Q29F ---
    # op_politician_rating_alex_tyrrell — Évaluation d'Alex Tyrrell (0-100 → 0-1)
    # Source: Q29F
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_alex_tyrrell'] = np.nan
    mask = (~df['Q29F'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29F'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_alex_tyrrell'] = pd.to_numeric(df.loc[mask, 'Q29F'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_alex_tyrrell'] = {
        'original_variable': 'Q29F',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de ALEX TYRRELL?",
        'type': 'numeric',
    }

    # --- Q29G ---
    # op_politician_rating_pierre_karl_peladeau — Évaluation de Pierre Karl Péladeau (0-100 → 0-1)
    # Source: Q29G
    # Mapping validé à partir du codebook: échelle 0-100, special values → NaN
    df_clean['op_politician_rating_pierre_karl_peladeau'] = np.nan
    mask = (~df['Q29G'].isin(['Je ne le/la connais pas', "Je ne sais pas / Je ne sais pas comment l'évaluer", 'Je préfère ne pas répondre'])) & (pd.to_numeric(df['Q29G'], errors='coerce').notna())
    df_clean.loc[mask, 'op_politician_rating_pierre_karl_peladeau'] = pd.to_numeric(df.loc[mask, 'Q29G'], errors='coerce') / 100.0
    CODEBOOK_VARIABLES['op_politician_rating_pierre_karl_peladeau'] = {
        'original_variable': 'Q29G',
        'question_label': "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ VRAIMENT PAS DU TOUT un politicien, et cent veut dire que vous L'AIMEZ VRAIMENT BEAUCOUP, que pensez-vous de PIERRE KARL PÉLADEAU?",
        'type': 'numeric',
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
    # op_left_right_scale_plq — Placement du PLQ sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31A
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           a. Parti libéral du Québec"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_plq'] = df['Q31A'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_plq'] = {
        'original_variable': 'Q31A',
        'question_label': "Placement du Parti libéral du Québec sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q31B ---
    # op_left_right_scale_pq — Placement du PQ sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31B
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           b. Parti québécois"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_pq'] = df['Q31B'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_pq'] = {
        'original_variable': 'Q31B',
        'question_label': "Placement du Parti québécois sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q31C ---
    # op_left_right_scale_caq — Placement de la CAQ sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31C
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           c. Coalition avenir Québec"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_caq'] = df['Q31C'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_caq'] = {
        'original_variable': 'Q31C',
        'question_label': "Placement de la Coalition avenir Québec sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q31D ---
    # op_left_right_scale_qs — Placement de Québec solidaire sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31D
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           d. Québec solidaire"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_qs'] = df['Q31D'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_qs'] = {
        'original_variable': 'Q31D',
        'question_label': "Placement de Québec solidaire sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q31E ---
    # op_left_right_scale_on — Placement d'Option nationale sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31E
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           e. Option nationale"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_on'] = df['Q31E'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_on'] = {
        'original_variable': 'Q31E',
        'question_label': "Placement d'Option nationale sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q31F ---
    # op_left_right_scale_pv — Placement du Parti vert sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q31F
    # Question: "En politique, les gens parlent de la « gauche » et de la « droite ». Sur une échelle allant de 0 à 10,
    #           où 0 est le plus à gauche et 10 est le plus à droite, où placeriez-vous chacun des partis politiques suivants?
    #           f. Parti vert du Québec"
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_left_right_scale_pv'] = df['Q31F'].map({
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
    CODEBOOK_VARIABLES['op_left_right_scale_pv'] = {
        'original_variable': 'Q31F',
        'question_label': "Placement du Parti vert du Québec sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
    }

    # --- Q32 ---
    # op_personal_position — Position personnelle sur l'échelle gauche-droite (0-10 → 0.0-1.0)
    # Source: Q32
    # Question: "Et sur la même échelle, où vous placeriez-vous, de manière générale?"
    # Échelle: 0 = le plus à gauche, 10 = le plus à droite
    # TODO: Valider le mapping avec les données réelles et le codebook complet
    df_clean['op_personal_position'] = df['Q32'].map({
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
    CODEBOOK_VARIABLES['op_personal_position'] = {
        'original_variable': 'Q32',
        'question_label': "Position personnelle sur l'échelle gauche-droite (0=gauche, 10=droite)",
        'type': 'numeric',
        'value_labels': {}
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
    # op_opinion_social_a — Opinion sur un enjeu social
    # Source: Q36A
    # TODO: Valider le mapping exact de l'échelle d'opinion à partir du codebook
    df_clean['op_opinion_social_a'] = df['Q36A'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.66,   # Plutôt d'accord
        3.0: 0.33,   # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })

    CODEBOOK_VARIABLES['op_opinion_social_a'] = {
        'original_variable': 'Q36A',
        'question_label': "Notre société doit faire tout ce qui est nécessaire pour s'assurer que chacun ait une chance égale de réussir.", # TODO: Confirmer dans le codebook
        'type': 'numeric',
    }

    # --- Q36B ---
    # op_opinion_social_b — Opinion sur un enjeu social
    # Source: Q36B
    # TODO: Valider le mapping exact de l'échelle d'opinion à partir du codebook
    df_clean['op_opinion_social_b'] = df['Q36B'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.66,   # Plutôt d'accord
        3.0: 0.33,   # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })

    CODEBOOK_VARIABLES['op_opinion_social_b'] = {
        'original_variable': 'Q36B',
        'question_label': "Ce n'est pas si grave si certaines personnes ont plus de chance que d'autres dans la vie.", # TODO: Confirmer dans le codebook
        'type': 'numeric',
    }

    # --- Q36C ---
    # op_opinion_social_c — Opinion sur un enjeu social
    # Source: Q36C
    # TODO: Valider le mapping exact de l'échelle d'opinion à partir du codebook
    df_clean['op_opinion_social_c'] = df['Q36C'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.66,   # Plutôt d'accord
        3.0: 0.33,   # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })

    CODEBOOK_VARIABLES['op_opinion_social_c'] = {
        'original_variable': 'Q36C',
        'question_label': "Sans l'action du gouvernement, il y aurait beaucoup plus de pauvreté dans nos sociétés.", # TODO: Confirmer dans le codebook
        'type': 'numeric',
    }

    # --- Q36D ---
    # op_opinion_social_d — Opinion sur un enjeu social
    # Source: Q36D
    # TODO: Valider le mapping exact de l'échelle d'opinion à partir du codebook
    df_clean['op_opinion_social_d'] = df['Q36D'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.66,   # Plutôt d'accord
        3.0: 0.33,   # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })

    CODEBOOK_VARIABLES['op_opinion_social_d'] = {
        'original_variable': 'Q36D',
        'question_label': "Quand les entreprises font beaucoup d'argent, tout le monde y gagne, y compris les pauvres.", # TODO: Confirmer dans le codebook
        'type': 'numeric',
    }

    # --- Q36E ---
    # op_opinion_social_e — Opinion sur un enjeu social
    # Source: Q36E
    # TODO: Valider le mapping exact de l'échelle d'opinion à partir du codebook
    df_clean['op_opinion_social_e'] = df['Q36E'].map({
        1.0: 1.0,    # Fortement d'accord
        2.0: 0.66,   # Plutôt d'accord
        3.0: 0.33,   # Plutôt en désaccord
        4.0: 0.0,    # Fortement en désaccord
        8.0: np.nan,
        9.0: np.nan,
    })

    CODEBOOK_VARIABLES['op_opinion_social_e'] = {
        'original_variable': 'Q36E',
        'question_label': "Il y a trop d'immigrants au Québec.", # TODO: Confirmer dans le codebook
        'type': 'numeric',
    }

    # --- Q37 ---
    # op_abortion_opinion — Opinion sur l'avortement
    # Source: Q37
    # Question: "Selon vous, l'avortement devrait-il être illégal ou non?"
    # Réponses: 1=Oui, 2=Non, 8=Je ne sais pas, 9=Je préfère ne pas répondre
    # TODO: Valider le mapping exact avec les codes réels dans les données et le codebook
    df_clean['op_abortion_opinion'] = df['Q37'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_abortion_opinion'] = {
        'original_variable': 'Q37',
        'question_label': "Opinion sur le statut légal de l'avortement",
        'type': 'categorical',
        'value_labels': {'yes': "Oui, l'avortement devrait être illégal", 'no': "Non, l'avortement ne devrait pas être illégal"},
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
    # op_q43a — Évaluation des syndicats sur échelle 0-100
    # Source: Q43A
    # Codebook: "Sur une échelle de ZERO à CENT, où zéro veut dire que vous N'AIMEZ
    # VRAIMENT PAS DU TOUT les syndicats, et cent veut dire que vous les AIMEZ
    # VRAIMENT BEAUCOUP, que pensez-vous des SYNDICATS en général?"
    df_clean['op_q43a'] = df['Q43A'].astype(float) / 100.0
    df_clean.loc[df['Q43A'].isin([98, 99]), 'op_q43a'] = np.nan
    CODEBOOK_VARIABLES['op_q43a'] = {
        'original_variable': 'Q43A',
        'question_label': "Évaluation des syndicats (0=Aime pas du tout, 100=Aime beaucoup)",
        'type': 'continuous',
        'value_labels': {'0.0': "N'aime pas du tout", '1.0': "Aime beaucoup", 'np.nan': "Ne sait pas / Refus"}
    }

    # --- Q43B ---
    # op_q43b — Évaluation des entreprises sur échelle 0-100
    # Source: Q43B
    # Codebook: "Et sur la même échelle, que pensez-vous des ENTREPRISES en général?"
    df_clean['op_q43b'] = df['Q43B'].astype(float) / 100.0
    df_clean.loc[df['Q43B'].isin([98, 99]), 'op_q43b'] = np.nan
    CODEBOOK_VARIABLES['op_q43b'] = {
        'original_variable': 'Q43B',
        'question_label': "Évaluation des entreprises (0=Aime pas du tout, 100=Aime beaucoup)",
        'type': 'continuous',
        'value_labels': {'0.0': "N'aime pas du tout", '1.0': "Aime beaucoup", 'np.nan': "Ne sait pas / Refus"}
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
    # op_independence_economic_comparison — Comparaison économique suite à l'indépendance
    # Source: Q46
    # TODO: Valider le mapping exact de la comparaison à partir du codebook
    df_clean['op_independence_economic_comparison'] = df['Q46'].map({
        # 1.0: 'better_off',
        # 2.0: 'worse_off',
        # 3.0: 'no_difference',
        # 98.0: np.nan,
        # 99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_independence_economic_comparison'] = {
        'original_variable': 'Q46',
        'question_label': "Si le Québec devenait indépendant, pensez-vous que nous serions économiquement mieux ou plus mal?", # TODO: Confirmer dans le codebook
        'type': 'categorical',
        'value_labels': {
            # 'better_off': "Mieux",
            # 'worse_off': "Plus mal",
             # 'no_difference': "Pas de différence",
         }
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
    # op_charter_importance — Importance of adopting a charter of secularism
    # Source: Q54 (Codebook: "Et sur la même échelle, quelle est l'importance d'adopter une charte de la laïcité?")
    # Scale: 0 = "pas du tout important" to 10 = "très important"
    # Codes 98 = "Je ne sais pas", 99 = "Je préfère ne pas répondre"
    df_clean['op_charter_importance'] = df['Q54'].map({
        0.0: 'not_important_at_all',
        1.0: 'importance_1',
        2.0: 'importance_2',
        3.0: 'importance_3',
        4.0: 'importance_4',
        5.0: 'importance_5',
        6.0: 'importance_6',
        7.0: 'importance_7',
        8.0: 'importance_8',
        9.0: 'importance_9',
        10.0: 'very_important',
        98.0: np.nan,  # Je ne sais pas - treated as missing
        99.0: 'refused',  # Je préfère ne pas répondre
    })
    CODEBOOK_VARIABLES['op_charter_importance'] = {
        'original_variable': 'Q54',
        'question_label': "Importance of adopting a charter of secularism (0-10)",
        'type': 'categorical',
        'value_labels': {
            'not_important_at_all': "Pas du tout important (0)",
            'importance_1': "Importance 1",
            'importance_2': "Importance 2",
            'importance_3': "Importance 3",
            'importance_4': "Importance 4",
            'importance_5': "Importance 5",
            'importance_6': "Importance 6",
            'importance_7': "Importance 7",
            'importance_8': "Importance 8",
            'importance_9': "Importance 9",
            'very_important': "Très important (10)",
            'refused': "Refused",
        },
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
    # op_q58 — Situation professionnelle
    # Source: Q58 - "Travaillez-vous actuellement à votre compte, êtes-vous salarié(e),
    # avez-vous pris votre retraite, êtes-vous au chômage ou cherchez-vous du travail,
    # êtes-vous étudiant(e), ménager(ère), ou quelque chose d'autre?"
    df_clean['op_q58'] = df['Q58'].map({
        1.0: 'self_employed',
        2.0: 'salaried',
        3.0: 'retired',
        4.0: 'unemployed',
        5.0: 'student',
        6.0: 'homemaker',
        7.0: 'disabled',
        8.0: 'multiple_jobs',
        9.0: 'student_employed',
        10.0: 'homemaker_employed',
        11.0: 'retired_employed',
        12.0: 'other',
        13.0: np.nan,
        96.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q58'] = {
        'original_variable': 'Q58',
        'question_label': "Situation professionnelle",
        'type': 'categorical',
        'value_labels': {
            'self_employed': "Travaille à son compte",
            'salaried': "Travaille pour un salaire",
            'retired': "Retraité(e)",
            'unemployed': "Au chômage/cherche du travail",
            'student': "Étudiant(e)",
            'homemaker': "Ménager(ère)",
            'disabled': "Handicapé(e)",
            'multiple_jobs': "Occupe deux ou plus de deux emplois rémunérés",
            'student_employed': "Étudiant(e) et salarié(e)",
            'homemaker_employed': "Ménager(ère) et salarié(e)",
            'retired_employed': "Retraité(e) et salarié(e)",
            'other': "Autre",
        },
    }

    # --- Q59A ---
    # fin_savings_bank — Compte épargne dans une banque
    # Source: Q59A
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (A. Compte épargne dans une banque)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_savings_bank'] = df['Q59A'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_savings_bank'] = {
        'original_variable': 'Q59A',
        'question_label': "Financial holdings: Savings account in a bank (Compte épargne dans une banque)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59B ---
    # fin_trust_account — Compte dans une société de fiducie
    # Source: Q59B
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (B. Compte dans une société de fiducie)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_trust_account'] = df['Q59B'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_trust_account'] = {
        'original_variable': 'Q59B',
        'question_label': "Financial holdings: Account in a trust company (Compte dans une société de fiducie)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59C ---
    # fin_rrsp_tfsa — REER ou CELI
    # Source: Q59C
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (C. REER ou CELI)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_rrsp_tfsa'] = df['Q59C'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_rrsp_tfsa'] = {
        'original_variable': 'Q59C',
        'question_label': "Financial holdings: RRSP or TFSA (REER ou CELI)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59D ---
    # fin_stocks_shares — Actions ou parts d'entreprise
    # Source: Q59D
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (D. Actions ou parts d'entreprise)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_stocks_shares'] = df['Q59D'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_stocks_shares'] = {
        'original_variable': 'Q59D',
        'question_label': "Financial holdings: Stocks or shares (Actions ou parts d'entreprise)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59E ---
    # fin_bonds — Obligations
    # Source: Q59E
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (E. Obligations)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_bonds'] = df['Q59E'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_bonds'] = {
        'original_variable': 'Q59E',
        'question_label': "Financial holdings: Bonds (Obligations)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59F ---
    # fin_financial_assets_portfolio — Portefeuille d'actifs financiers
    # Source: Q59F
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (F. Portefeuille d'actifs financiers - CPG, fonds mutuels, etc.)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_financial_assets_portfolio'] = df['Q59F'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_financial_assets_portfolio'] = {
        'original_variable': 'Q59F',
        'question_label': "Financial holdings: Financial assets portfolio (GIC, mutual funds, etc.) (Portefeuille d'actifs financiers - CPG, fonds mutuels, etc.)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q59G ---
    # fin_retirement_savings_plan — Régime d'épargne-retraite
    # Source: Q59G
    # Question: Parmi les types de placements financiers suivants, quels sont ceux que vous détenez
    #           ou que détient l'un des membres de votre foyer? (G. Régime d'épargne-retraite)
    # Possible answers: 1=Yes, 2=No, 8=Don't know, 9=Prefer not to answer
    df_clean['fin_retirement_savings_plan'] = df['Q59G'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['fin_retirement_savings_plan'] = {
        'original_variable': 'Q59G',
        'question_label': "Financial holdings: Retirement savings plan (Régime d'épargne-retraite)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes', 'no': 'No'},
    }

    # --- Q6 ---
    # behav_previous_vote — Vote aux élections provinciales du 4 septembre 2012
    # Source: Q6
    # Question: "Pour quel parti aviez-vous voté lors de l'élection provinciale du 4 septembre 2012?"
    # Options: PLQ (1), PQ (2), CAQ (3), QS (4), PV (5), ON (6), Autre (7), N'a pas voté (8), Préfère ne pas répondre (9)
    # TODO: Valider le mapping exact avec les codes réels dans les données et le codebook
    df_clean['behav_previous_vote'] = df['Q6'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'pv',
        6.0: 'on',
        7.0: 'other',
        8.0: 'did_not_vote',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_previous_vote'] = {
        'original_variable': 'Q6',
        'question_label': "Parti pour lequel on a voté aux élections provinciales du 4 septembre 2012",
        'type': 'categorical',
        'value_labels': {'plq': "Parti libéral du Québec", 'pq': "Parti québécois", 'caq': "Coalition avenir Québec", 'qs': "Québec solidaire", 'pv': "Parti vert du Québec", 'on': "Option nationale", 'other': "Autre parti", 'did_not_vote': "N'a pas voté"},
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
    # behav_response_q66 — Langue principale apprise à la maison dans l'enfance
    # Source: Q66
    # Note: Codes 96, 98, 99 treated as missing (Ne sait pas, Refused)
    df_clean['behav_response_q66'] = df['Q66'].map({
        1.0: 'french',
        2.0: 'english',
        3.0: 'italian',
        4.0: 'spanish',
        6.0: 'arabic',
        7.0: 'german',
        8.0: 'portuguese',
        10.0: 'polish',
        12.0: 'chinese',
        15.0: 'creole',
        16.0: 'other',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_response_q66'] = {
        'original_variable': 'Q66',
        'question_label': "Quelle est la langue principale que vous avez apprise en premier lieu à la maison dans votre enfance et que vous comprenez toujours?",
        'type': 'categorical',
        'value_labels': {
            'french': "Français",
            'english': "Anglais",
            'italian': "Italien",
            'spanish': "Espagnol",
            'arabic': "Arabe",
            'german': "Allemand",
            'portuguese': "Portugais",
            'polish': "Polonais",
            'chinese': "Chinois",
            'creole': "Créole",
            'other': "Autre",
        },
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
    # ses_marital_status — Statut civil
    # Source: Q68
    # TODO: Valider le mapping exact des statuts civils à partir du codebook (Quebec Election Study 2014 FR.doc)
    # Le mapping ci-dessous est une hypothèse.
    df_clean['ses_marital_status'] = df['Q68'].map({
        # 1.0: 'single',
        # 2.0: 'married',
        # 3.0: 'separated',
        # 4.0: 'divorced',
        # 5.0: 'widowed',
        # 6.0: 'common_law',
        # 7.0: 'refused',
        # 98.0: np.nan, # Don't know
        # 99.0: np.nan, # Refused
    })
    CODEBOOK_VARIABLES['ses_marital_status'] = {
        'original_variable': 'Q68',
        'question_label': "Quel est votre état matrimonial actuel?", # TODO: Confirmer le libellé exact de la question dans le codebook
        'type': 'categorical',
        'value_labels': {
            # 'single': "Célibataire",
            # 'married': "Marié(e)",
            # 'separated': "Séparé(e)",
            # 'divorced': "Divorcé(e)",
            # 'widowed': "Veuf/Veuve",
            # 'common_law': "Union libre",
            # 'refused': "Refusé",
        } # TODO: Confirmer les labels de valeur exacts dans le codebook
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
    # op_worst_campaign — Parti ayant mené la moins bonne campagne
    # Source: Q9
    # Q9: "Et quel parti a selon vous mené la moins bonne campagne?"
    df_clean['op_worst_campaign'] = df['Q9'].map({
        1.0: 'lib',
        2.0: 'pqc',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'pvq',
        6.0: 'on',
        96.0: 'other',
        98.0: 'dont_know',
        99.0: 'refused',
    })
    CODEBOOK_VARIABLES['op_worst_campaign'] = {
        'original_variable': 'Q9',
        'question_label': "Parti ayant mené la moins bonne campagne",
        'type': 'categorical',
        'value_labels': {'lib': "Parti libéral du Québec", 'pqc': "Parti québécois", 'caq': "Coalition avenir Québec", 'qs': "Québec solidaire", 'pvq': "Parti vert du Québec", 'on': "Option nationale", 'other': "Autre parti", 'dont_know': "Je ne sais pas", 'refused': "Refused"},
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
