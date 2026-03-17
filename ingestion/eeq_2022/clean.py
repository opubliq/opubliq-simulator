#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2022

Ce script est exécuté par lambda_raffineur_nettoyage dans pipeline_sondages.
La lambda appelle:
  - clean_data(df) → retourne DataFrame nettoyé
  - get_metadata() → retourne dictionnaire avec métadonnées du sondage et variables

Les métadonnées enrichies sont sauvegardées comme codebook.json dans S3 et utilisées
par les marts downstream pour l'interprétation sémantique des données.
"""

import pandas as pd

# ============================================================================
# SURVEY METADATA (à remplir au début du script)
# ============================================================================
SURVEY_METADATA = {
    'survey_id': 'eeq_2022',
    'title': 'Quebec Election Study 2022',
    'year': 2022,
    'description': 'Quebec Election Study 2022',
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

    # --- cps_impissue_matrix ---
    # cps_impissue_matrix — Quel est l'enjeu le plus important, pour vous personnellement
    # Source: cps_impissue_matrix
    df_clean['cps_impissue_matrix'] = df['cps_impissue_matrix'].map({
        1.0: 'economy',
        2.0: 'health',
        3.0: 'environment_climate',
        4.0: 'education',
        5.0: 'poverty',
        6.0: 'housing_crisis',
        7.0: 'politician_integrity_corruption',
        8.0: 'taxes_public_finances',
        9.0: 'quebec_sovereignty',
        10.0: 'cost_of_living',
        11.0: 'immigration',
        12.0: 'third_quebec_city_link',
        13.0: 'french_language',
        14.0: 'gun_violence'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix'] = {
        'original_variable': 'cps_impissue_matrix',
        'question_label': "Quel est l'enjeu le plus important, pour vous personnellement, dans cette élection?",
        'type': 'ordinal',
        'value_labels': {
            'economy': "L'économie",
            'health': "La santé",
            'environment_climate': "L'environnement et la crise climatique",
            'education': "L'éducation",
            'poverty': "La pauvreté",
            'housing_crisis': "La crise du logement",
            'politician_integrity_corruption': "L'intégrité des politiciens et la corruption",
            'taxes_public_finances': "Les taxes et les finances publiques",
            'quebec_sovereignty': "La souveraineté du Québec",
            'cost_of_living': "Le coût de la vie",
            'immigration': "L'immigration",
            'third_quebec_city_link': "Le troisième lien routier à Québec",
            'french_language': "La langue française",
            'gun_violence': "La violence par arme à feu"
        }
    }

    # --- cps_spendcrime ---
    # cps_spendcrime — Opinion sur les dépenses pour lutter contre le crime
    # Source: cps_spendcrime
    df_clean['cps_spendcrime'] = df['cps_spendcrime'].map({
        1.0: 'spend_less',
        2.0: 'spend_about_same',
        3.0: 'spend_more'
    })
    CODEBOOK_VARIABLES['cps_spendcrime'] = {
        'original_variable': 'cps_spendcrime',
        'question_label': "Combien le gouvernement provincial devrait-il dépenser pour lutter contre le crime?",
        'type': 'categorical',
        'value_labels': {
            'spend_less': "Dépenser moins",
            'spend_about_same': "Dépenser à peu près autant",
            'spend_more': "Dépenser plus"
        }
    }

    # --- cps_immig ---
    # cps_immig — Opinion sur le niveau d'immigration au Canada
    # Source: cps_immig
    df_clean['cps_immig'] = df['cps_immig'].map({
        1.0: 'more_immigrants',
        2.0: 'fewer_immigrants',
        3.0: 'same_immigrants'
    })
    CODEBOOK_VARIABLES['cps_immig'] = {
        'original_variable': 'cps_immig',
        'question_label': "Pensez-vous que le Canada devrait admettre...",
        'type': 'categorical',
        'value_labels': {
            'more_immigrants': "Plus d'immigrants",
            'fewer_immigrants': "Moins d'immigrants",
            'same_immigrants': "À peu près autant d'immigrants que maintenant"
        }
    }

    # --- cps_ideoparty_DO_1 ---
    # cps_ideoparty_DO_1 — Display order for Parti libéral du Québec in cps_ideoparty
    # Source: cps_ideoparty_DO_1
    df_clean['cps_ideoparty_DO_1'] = df['cps_ideoparty_DO_1']
    CODEBOOK_VARIABLES['cps_ideoparty_DO_1'] = {
        'original_variable': 'cps_ideoparty_DO_1',
        'question_label': "Display order for Parti libéral du Québec in cps_ideoparty",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_1 ---
    # cps_impissue_matrix_DO_1 — Display order for first issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_1
    df_clean['cps_impissue_matrix_DO_1'] = df['cps_impissue_matrix_DO_1']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_1'] = {
        'original_variable': 'cps_impissue_matrix_DO_1',
        'question_label': "Display order for first issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_2 ---
    # cps_impissue_matrix_DO_2 — Display order for second issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_2
    df_clean['cps_impissue_matrix_DO_2'] = df['cps_impissue_matrix_DO_2']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_2'] = {
        'original_variable': 'cps_impissue_matrix_DO_2',
        'question_label': "Display order for second issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_3 ---
    # cps_impissue_matrix_DO_3 — Display order for third issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_3
    df_clean['cps_impissue_matrix_DO_3'] = df['cps_impissue_matrix_DO_3']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_3'] = {
        'original_variable': 'cps_impissue_matrix_DO_3',
        'question_label': "Display order for third issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_4 ---
    # cps_impissue_matrix_DO_4 — Display order for fourth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_4
    df_clean['cps_impissue_matrix_DO_4'] = df['cps_impissue_matrix_DO_4']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_4'] = {
        'original_variable': 'cps_impissue_matrix_DO_4',
        'question_label': "Display order for fourth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_5 ---
    # cps_impissue_matrix_DO_5 — Display order for fifth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_5
    df_clean['cps_impissue_matrix_DO_5'] = df['cps_impissue_matrix_DO_5']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_5'] = {
        'original_variable': 'cps_impissue_matrix_DO_5',
        'question_label': "Display order for fifth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_6 ---
    # cps_impissue_matrix_DO_6 — Display order for sixth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_6
    df_clean['cps_impissue_matrix_DO_6'] = df['cps_impissue_matrix_DO_6']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_6'] = {
        'original_variable': 'cps_impissue_matrix_DO_6',
        'question_label': "Display order for sixth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_7 ---
    # cps_impissue_matrix_DO_7 — Display order for seventh issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_7
    df_clean['cps_impissue_matrix_DO_7'] = df['cps_impissue_matrix_DO_7']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_7'] = {
        'original_variable': 'cps_impissue_matrix_DO_7',
        'question_label': "Display order for seventh issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_8 ---
    # cps_impissue_matrix_DO_8 — Display order for eighth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_8
    df_clean['cps_impissue_matrix_DO_8'] = df['cps_impissue_matrix_DO_8']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_8'] = {
        'original_variable': 'cps_impissue_matrix_DO_8',
        'question_label': "Display order for eighth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_9 ---
    # cps_impissue_matrix_DO_9 — Display order for ninth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_9
    df_clean['cps_impissue_matrix_DO_9'] = df['cps_impissue_matrix_DO_9']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_9'] = {
        'original_variable': 'cps_impissue_matrix_DO_9',
        'question_label': "Display order for ninth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_10 ---
    # cps_impissue_matrix_DO_10 — Display order for tenth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_10
    df_clean['cps_impissue_matrix_DO_10'] = df['cps_impissue_matrix_DO_10']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_10'] = {
        'original_variable': 'cps_impissue_matrix_DO_10',
        'question_label': "Display order for tenth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_11 ---
    # cps_impissue_matrix_DO_11 — Display order for immigration option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_11
    df_clean['cps_impissue_matrix_DO_11'] = df['cps_impissue_matrix_DO_11']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_11'] = {
        'original_variable': 'cps_impissue_matrix_DO_11',
        'question_label': "Display order for immigration option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_12 ---
    # cps_impissue_matrix_DO_12 — Display order for twelfth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_12
    df_clean['cps_impissue_matrix_DO_12'] = df['cps_impissue_matrix_DO_12']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_12'] = {
        'original_variable': 'cps_impissue_matrix_DO_12',
        'question_label': "Display order for twelfth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_13 ---
    # cps_impissue_matrix_DO_13 — Display order for thirteenth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_13
    df_clean['cps_impissue_matrix_DO_13'] = df['cps_impissue_matrix_DO_13']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_13'] = {
        'original_variable': 'cps_impissue_matrix_DO_13',
        'question_label': "Display order for thirteenth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_14 ---
    # cps_impissue_matrix_DO_14 — Display order for fourteenth issue option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_14
    df_clean['cps_impissue_matrix_DO_14'] = df['cps_impissue_matrix_DO_14']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_14'] = {
        'original_variable': 'cps_impissue_matrix_DO_14',
        'question_label': "Display order for fourteenth issue option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    return df_clean

def get_metadata():
    """Retourne les métadonnées du sondage

    Returns:
        dict: Métadonnées et codebook
    """
    return {
        'survey_metadata': SURVEY_METADATA,
        'codebook': CODEBOOK_VARIABLES
    }
