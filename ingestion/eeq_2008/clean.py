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
    df_clean['ses_language'] = df['langu'].map({
        1.0: 'french',
        2.0: 'english',
        3.0: 'other',
        9.0: np.nan
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

    # --- q76 ---
    # ses_gender — Sexe du répondant
    # Source: q76
    df_clean['ses_gender'] = df['q76'].map({
        1.0: 'homme',
        2.0: 'femme',
    })
    CODEBOOK_VARIABLES['ses_gender'] = {
        'original_variable': 'q76',
        'question_label': "Sexe du répondant",
        'type': 'categorical',
        'value_labels': {
            'homme': "Homme",
            'femme': "Femme",
        },
    }

    # --- reg ---
    # ses_region — Province/Region of residence
    # Source: reg
    df_clean['ses_region'] = df['reg'].map({
        1.0: 'MTL',
        2.0: 'QC',
        3.0: 'Régions',
        4.0: 'Régions',
        5.0: 'Régions',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'reg',
        'question_label': "Region of residence",
        'type': 'categorical',
        'value_labels': {
            'MTL': "Montréal RMR",
            'QC': "Québec RMR",
            'Régions': "Autres régions"
        },
    }
    
    return df_clean


def get_metadata():
    """Retourne les métadonnées du sondage et des variables.

    Returns:
        dict: Dictionnaire contenant SURVEY_METADATA et CODEBOOK_VARIABLES
    """
    return {
        'survey_metadata': SURVEY_METADATA,
        'codebook_variables': CODEBOOK_VARIABLES
    }

def map_strates_canoniques(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise les colonnes SES selon les strates canoniques.
    
    Args:
        df (pd.DataFrame): DataFrame nettoyé avec les colonnes `ses_age_group`,
                           `ses_language`, `ses_region`, `ses_gender`.
    
    Returns:
        pd.DataFrame: DataFrame avec les colonnes standardisées.
    """
    df_strata = pd.DataFrame(index=df.index)
    
    # age_group ∈ {18-34, 35-54, 55+}
    age_map = {
        '18-24': '18-34',
        '25-34': '18-34',
        '35-44': '35-54',
        '45-54': '35-54',
        '55-64': '55+',
        '65-74': '55+',
        '75+': '55+'
    }
    df_strata['age_group'] = df['ses_age_group'].map(age_map)
    
    # langue ∈ {francophone, anglo-autre}
    langue_map = {
        'french': 'francophone',
        'english': 'anglo-autre',
        'other': 'anglo-autre'
    }
    df_strata['langue'] = df['ses_language'].map(langue_map)

    # region ∈ {MTL, QC, Couronne, Régions}
    # Note: "Couronne" n'est pas disponible dans les données.
    region_map = {
        'MTL': 'MTL',
        'QC': 'QC',
        'Régions': 'Régions'
    }
    df_strata['region'] = df['ses_region'].map(region_map)
    
    # genre ∈ {homme, femme}
    df_strata['genre'] = df['ses_gender']
    
    return df_strata
