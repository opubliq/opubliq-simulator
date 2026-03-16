#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2007

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
    'survey_id': 'eeq_2007',           # ID unique du sondage (ex: "ces2019")
    'title': 'eeq_2007',             # Titre complet
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
    """Charge et nettoie les données brutes du sondage eeq_2007.

    Args:
        raw_path (str): Chemin vers le fichier .sav brut
            (ex: SHARED_FOLDER_PATH/eeq_2007/Quebec Election Study 2007 (SPSS).sav)

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

    # --- All respondents from Quebec ---
    # ses_province — Province de résidence (tous au Québec dans cet échantillon)
    # Source: (tous les répondants eeq_2007 sont QC)
    df_clean['ses_province'] = 'quebec'
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'N/A',
        'question_label': "Province de résidence (tous au Québec)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec"},
    }

    # --- codep ---
    # geo_postal_code — CODE POSTAL (Postal code)
    # Source: codep
    # Note: Variable is text/string containing Canadian postal codes (FSA format: 1 letter, 1 digit, 1 letter)
    # Assumption: code '999' treated as missing (standard missing indicator)
    # 39 records have code 999, treated as missing values
    df_clean['geo_postal_code'] = df['codep'].map(
        lambda x: x.lower() if x != '999' else None
    )
    # Convert None to np.nan for consistency
    df_clean['geo_postal_code'] = df_clean['geo_postal_code'].where(df_clean['geo_postal_code'].notna(), np.nan)

    CODEBOOK_VARIABLES['geo_postal_code'] = {
        'original_variable': 'codep',
        'question_label': 'CODEP. CODE POSTAL',
        'type': 'categorical',
        'value_labels': {},  # Postal codes are individual identifiers, no standardized labels
    }

    # --- ethn1 ---
    # ses_ethnicity — Origine ethnique
    # Source: ethn1
    df_clean['ses_ethnicity'] = df['ethn1'].map({
        '01': 'canadienne_quebecoise',
        '02': 'afrique_nord',
        '03': 'afrique_subsaharienne',
        '04': 'amerindienne',
        '05': 'americaine_usa',
        '06': 'amerique_centrale_sud',
        '07': 'mexique',
        '08': 'antillaise',
        '09': 'asiatique',
        '10': 'europe_ouest',
        '11': 'europe_est',
        '12': 'moyen_orient',
        '13': 'turquie_armenie_iran',
        '96': 'autre',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_ethnicity'] = {
        'original_variable': 'ethn1',
        'question_label': "De quelle origine ethnique êtes-vous?",
        'type': 'categorical',
        'value_labels': {
            'canadienne_quebecoise': "Canadienne-québécoise",
            'afrique_nord': "Afrique du Nord",
            'afrique_subsaharienne': "Afrique subsaharienne",
            'amerindienne': "Amérindienne",
            'americaine_usa': "Américaine (USA)",
            'amerique_centrale_sud': "Amérique Centrale et du Sud",
            'mexique': "Mexique",
            'antillaise': "Antillaise / Haïti / Jamaïque",
            'asiatique': "Asiatique",
            'europe_ouest': "Europe de l'Ouest",
            'europe_est': "Europe de l'Est",
            'moyen_orient': "Moyen-Orient",
            'turquie_armenie_iran': "Turquie / Arménie / Iran / Kurde",
            'autre': "Autre",
        },
    }

    # --- langu ---
    # ses_language — Langue apprise en premier à la maison
    # Source: langu
    df_clean['ses_language'] = df['langu'].map({
        '1': 'french',
        '2': 'english',
        '9': np.nan,  # Nsp/Refus
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'langu',
        'question_label': "Quelle est la langue que vous avez apprise en premier lieu à la maison dans votre enfance et que vous comprenez toujours?",
        'type': 'categorical',
        'value_labels': {'french': "Français", 'english': "Anglais"},
    }

    # --- nomx ---
    # ses_region — Région strate canonique (MTL/QC/Couronne/Régions)
    # Source: nomx (21 régions administratives QC)
    df_clean['ses_region'] = df['nomx'].map({
        '06': 'montreal',           # MONTREAL
        '13': 'montreal',           # LAVAL (couronne MTL)
        '16': 'couronne',           # MONTEREGIE - RMR
        '26': 'couronne',           # MONTEREGIE - AUTRES
        '14': 'couronne',           # LANAUDIERE - RMR
        '24': 'couronne',           # LANAUDIERE - AUTRES
        '15': 'couronne',           # LAURENTIDES - RMR
        '25': 'couronne',           # LAURENTIDES - AUTRES
        '03': 'quebec',             # QUEBEC - RMR
        '33': 'quebec',             # QUEBEC - AUTRES
        '17': 'regions',            # CENTRE-DU-QUEBEC
        '12': 'regions',            # CHAUDIERE-APPALACHES - RMR
        '32': 'regions',            # CHAUDIERE-APPALACHES - AUTRES
        '05': 'regions',            # ESTRIE
        '01': 'regions',            # BAS-SAINT-LAURENT
        '11': 'regions',            # GASPESIE
        '02': 'regions',            # SAGUENAY/LAC-SAINT-JEAN
        '04': 'regions',            # MAURICIE
        '07': 'regions',            # OUTAOUAIS
        '08': 'regions',            # ABITIBI/TEMISCAMINGUE
        '09': 'regions',            # COTE-NORD
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'nomx',
        'question_label': "Région strate canonique (4 catégories)",
        'type': 'categorical',
        'value_labels': {
            'montreal': "Montréal",
            'quebec': "Québec",
            'couronne': "Couronne MTL",
            'regions': "Autres régions",
        },
    }

    # --- pond ---
    # ses_weight — Survey weight variable
    # Source: pond
    # Assumption: Treating as numeric weight variable, normalized by maximum observed value (5.990714).
    df_clean['ses_weight'] = df['pond'] / 5.990714
    CODEBOOK_VARIABLES['ses_weight'] = {
        'original_variable': 'pond',
        'question_label': "Survey weight (inferred)",
        'type': 'numeric',
        'value_labels': {'normalized_range': 'Normalized to range [0, 1]'},
    }

    # --- q1 ---
    # op_top_issue — Most important issue in the provincial election
    # Source: q1
    # Assumption: code 96 ("write-in response") treated as valid category 'other'
    df_clean['op_top_issue'] = df['q1'].map({
        '01': 'health',
        '02': 'change_government',
        '03': 'change',
        '04': 'economy',
        '05': 'finances',
        '06': 'education',
        '07': 'family',
        '08': 'environment',
        '09': 'remove_charest',
        '10': 'sovereignty',
        '11': 'question_model',
        '12': 'diminish_pq',
        '13': 'tax_cuts',
        '14': 'minority_govt',
        '15': 'lesson_to_parties',
        '16': 'integrity',
        '17': 'continuity',
        '18': 'future_quebec',
        '19': 'accommodation_immigration',
        '96': 'other',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_top_issue'] = {
        'original_variable': 'q1',
        'question_label': "Pour vous personnellement, quel était l'enjeu le plus important de cette élection provinciale?",
        'type': 'categorical',
        'value_labels': {
            'health': "La santé / l'état du système de santé",
            'change_government': "Changer de gouvernement",
            'change': "Le changement",
            'economy': "L'économie / le développement économique",
            'finances': "Les finances publiques / la dette / la gestion des finances",
            'education': "L'éducation",
            'family': "La famille",
            'environment': "L'environnement",
            'remove_charest': "Se débarrasser de Jean Charest",
            'sovereignty': "La souveraineté / l'indépendance du Québec",
            'question_model': "Renverser / remmettre en question le modèle québécois",
            'diminish_pq': "Reléguer le PQ au rang de tiers parti",
            'tax_cuts': "Les baisses d'impôts / de taxes",
            'minority_govt': "Élire un gouvernement minoritaire",
            'lesson_to_parties': "Servir une leçon au vieux partis",
            'integrity': "La franchise / l'intégrité des partis politiques",
            'continuity': "La continuité / un gouvernement libéral majoritaire",
            'future_quebec': "L'avenir du Québec",
            'accommodation_immigration': "Accomodements raisonnables / immigration / intégration",
            'other': "Réponse écrite par le répondant",
        },
    }

    # --- q10 ---
    # op_aid_families_importance — Importance of family aid as election issue
    # Source: q10
    df_clean['op_aid_families_importance'] = df['q10'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_aid_families_importance'] = {
        'original_variable': 'q10',
        'question_label': "Est-ce que l'aide aux familles ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'1.0': 'très important', '0.667': 'assez important', '0.333': 'peu important', '0.0': 'pas du tout important'},
    }

    # --- q10b ---
    # op_accommodations_importance — Importance of reasonable accommodations as political issue
    # Source: q10b
    df_clean['op_accommodations_importance'] = df['q10b'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_accommodations_importance'] = {
        'original_variable': 'q10b',
        'question_label': "Est-ce que les accomodements raisonnables ÉTAIENT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {1.0: "très important", 0.667: "assez important", 0.333: "peu important", 0.0: "pas du tout important"},
    }

    # --- q11 ---
    # q11 variable definition as per context provided for validation
    CODEBOOK_VARIABLES['q11'] = {
        "question": "Rôle de l'économie comme enjeu électoral",
        "type": "likert",
        "values": {
            "1": "très important",
            "2": "assez important",
            "3": "peu important",
            "4": "pas du tout important"
        },
        "missing_codes": [8, 9]
    }

    # --- q12 ---
    # behav_party_vote — Parti pour lequel le répondant a voté
    # Source: q12
    # Note: variable contains 185 empty strings treated as missing
    df_clean['behav_party_vote'] = df['q12'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'qs',
        '05': 'green',
        '95': 'no_vote',
        '96': 'other_party',
        '97': 'none',
        '98': 'dont_know',
        '99': 'refusal',
        '': np.nan,
    })
    CODEBOOK_VARIABLES['behav_party_vote'] = {
        'original_variable': 'q12',
        'question_label': "Pour quel parti avez-vous voté ? Le Parti libéral, le Parti québécois, l'ADQ, Québec solidaire, le Parti vert ou un autre parti ?",
        'type': 'categorical',
        'value_labels': {
            'liberal': "Parti libéral",
            'pq': "Parti québécois",
            'adq': "ADQ (Action démocratique du Québec)",
            'qs': "Québec Solidaire",
            'green': "Parti vert",
            'no_vote': "n'a pas voté",
            'other_party': "autre parti (spécifiez)",
            'none': "aucun",
            'dont_know': "ne sais pas",
            'refusal': "Refus",
        },
    }

    # --- q13 ---
    # behav_second_choice — Deuxième choix de parti
    # Source: q13
    # Note: blank string (433 cases) represents skip logic / non-response
    df_clean['behav_second_choice'] = df['q13'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'qs',
        '05': 'vert',
        '95': 'not_voted',
        '96': 'other',
        '97': 'none',
        '98': np.nan,
        '99': np.nan,
        '': np.nan,
    })
    CODEBOOK_VARIABLES['behav_second_choice'] = {
        'original_variable': 'q13',
        'question_label': "Quel parti était votre deuxième choix ?",
        'type': 'categorical',
        'value_labels': {
            'liberal': "Parti libéral",
            'pq': "Parti québécois",
            'adq': "ADQ",
            'qs': "Québec Solidaire",
            'vert': "Parti vert",
            'not_voted': "N'a pas voté",
            'other': "Autre parti",
            'none': "Aucun",
        },
    }

    # --- q14 ---
    # know_election_interest — Interest in provincial election on 0-10 scale
    # Source: q14
    # Assumption: codes 0-10 are valid scale values; 98/99 treated as missing (not interest codes)
    df_clean['know_election_interest'] = df['q14'].map({
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
    CODEBOOK_VARIABLES['know_election_interest'] = {
        'original_variable': 'q14',
        'question_label': "Sur une échelle de 0 à 10 où 0 veut dire aucun intérêt et 10 veut dire beaucoup d'intérêt, quel a été votre intérêt pour l'élection PROVINCIALE qui vient d'avoir lieu ?",
        'type': 'likert',
        'value_labels': {'0.0': 'aucun intérêt', '0.5': 'intérêt moyen', '1.0': 'beaucoup d\'intérêt'},
    }

    # --- q15 ---
    # op_interest_politics — Interest in politics in general (0-10 scale normalized to 0-1)
    # Source: q15
    # Assumption: codes 98.0 and 99.0 treated as missing (documented as don't know / refusal)
    df_clean['op_interest_politics'] = df['q15'].map({
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
    CODEBOOK_VARIABLES['op_interest_politics'] = {
        'original_variable': 'q15',
        'question_label': "Et toujours avec la même échelle, quel est votre intérêt pour la politique en général? (Sur une échelle de 0 à 10 où 0 veut dire aucun intérêt et 10 veut dire beaucoup d'intérêt)",
        'type': 'likert',
        'value_labels': {
            0.0: 'no interest',
            0.1: 'minimal interest',
            0.2: 'very low interest',
            0.3: 'low interest',
            0.4: 'below average interest',
            0.5: 'moderate interest',
            0.6: 'above average interest',
            0.7: 'high interest',
            0.8: 'very high interest',
            0.9: 'very strong interest',
            1.0: 'maximum interest',
        },
    }

    # --- q16 ---
    # behav_main_info_source — Main source of information on politics
    # Source: q16
    df_clean['behav_main_info_source'] = df['q16'].map({
        '01': 'television',
        '02': 'radio',
        '03': 'newspapers',
        '04': 'websites',
        '05': 'blogs',
        '06': 'family',
        '07': 'friends',
        '08': 'coworkers',
        '09': 'party_organizations',
        '96': 'other',
        '97': 'no_sources',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['behav_main_info_source'] = {
        'original_variable': 'q16',
        'question_label': "Quelle est votre PRINCIPALE source d'information sur la politique ? / NE PAS LIRE",
        'type': 'categorical',
        'value_labels': {
            'television': 'Télévision',
            'radio': 'Radio',
            'newspapers': 'Journaux',
            'websites': 'Sites web',
            'blogs': 'Blogs',
            'family': 'Famille',
            'friends': 'Amis',
            'coworkers': 'Collègues de travail',
            'party_organizations': 'Associations, organisations de partis politiques',
            'other': 'Autres',
            'no_sources': 'Pas de sources d\'information',
        },
    }

    # --- q17 ---
    # behav_info_source_2 — Deuxième source d'information sur la politique
    # Source: q17
    # Note: codes '', 42 found in data but not documented in codebook (treated as missing)
    # Assumption: codes 97, 98, 99 treated as missing/refusal
    df_clean['behav_info_source_2'] = df['q17'].astype(str).replace('', np.nan).map({
        '01': 'television',
        '02': 'radio',
        '03': 'newspapers',
        '04': 'websites',
        '05': 'blogs',
        '06': 'family',
        '07': 'friends',
        '08': 'colleagues',
        '09': 'organizations',
        '96': 'other',
        '97': np.nan,
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['behav_info_source_2'] = {
        'original_variable': 'q17',
        'question_label': "Quelle est votre deuxième source d'information sur la politique?",
        'type': 'categorical',
        'value_labels': {
            'television': 'Télévision',
            'radio': 'Radio',
            'newspapers': 'Journaux',
            'websites': 'Sites web',
            'blogs': 'Blogs',
            'family': 'Famille',
            'friends': 'Amis',
            'colleagues': 'Collègues de travail',
            'organizations': 'Associations, organisations de partis politiques',
            'other': 'Autres',
        },
        'missing_codes': ['8', '9', '97', '98', '99', ''],
    }

    # --- q18a ---
    # op_canadian_identity — How respondent identifies (Quebec vs Canada)
    # Source: q18a
    # Note: 1093 blank values (empty strings) treated as missing
    df_clean['op_canadian_identity'] = df['q18a'].map({
        '01': 'quebec_only',
        '02': 'quebec_first',
        '03': 'equal',
        '04': 'canada_first',
        '05': 'canada_only',
        '96': 'other',
        '98': np.nan,
        '99': np.nan,
        '': np.nan,
    })
    CODEBOOK_VARIABLES['op_canadian_identity'] = {
        'original_variable': 'q18a',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...?",
        'type': 'categorical',
        'value_labels': {
            'quebec_only': "...uniquement comme Québécois(e)",
            'quebec_first': "...d'abord comme Québécois(e), puis comme Canadien(ne)",
            'equal': "...également comme Canadien(ne) et comme Québécois(e)",
            'canada_first': "...d'abord comme Canadien(ne), puis comme Québécois(e)",
            'canada_only': "...ou uniquement comme Canadien(ne)?",
            'other': "autres (précisez)",
        },
    }

    # --- q18b ---
    # op_identity_qc_can — National identity orientation (Quebec-Canada)
    # Source: q18b
    # Note: value 1082 appears to be SPSS system missing (not in codebook), treated as np.nan
    df_clean['op_identity_qc_can'] = df['q18b'].map({
        '01': 'canadian',
        '02': 'canadian_first',
        '03': 'both_equally',
        '04': 'quebec_first',
        '05': 'quebec',
        '96': 'other',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_identity_qc_can'] = {
        'original_variable': 'q18b',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...? / LIRE LES 5 CHOIX",
        'type': 'categorical',
        'value_labels': {
            'canadian': "uniquement comme Canadien(ne)",
            'canadian_first': "d'abord comme Canadien(ne), puis comme Québécois(e)",
            'both_equally': "également comme Canadien(ne) et comme Québécois(e)",
            'quebec_first': "d'abord comme Québécois(e), puis comme Canadien(ne)",
            'quebec': "uniquement comme Québécois(e)",
            'other': "autre (précisez)",
        },
    }

    # --- q19 ---
    # behav_referendum_vote_1995 — Vote intention on 1995-style sovereignty referendum
    # Source: q19
    # Assumption: codes 8 (don't know) and 9 (refusal) treated as missing
    df_clean['behav_referendum_vote_1995'] = df['q19'].map({
        '1': 'oui',
        '2': 'non',
        '3': 'abstain',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['behav_referendum_vote_1995'] = {
        'original_variable': 'q19',
        'question_label': "Si un référendum avait lieu aujourd'hui sur la même question que celle qui a été posée lors du dernier référendum de 1995, c'est-à-dire sur la souveraineté assortie d'une offre de partenariat au reste du Canada, voteriez-vous OUI ou voteriez-vous NON ?",
        'type': 'categorical',
        'value_labels': {'oui': "Oui", 'non': "Non", 'abstain': "Ne voterait pas/annulerait"},
    }

    # --- q2 ---
    # op_health_importance — Importance of health as an election issue (Likert: 4-point scale)
    # Source: q2
    df_clean['op_health_importance'] = df['q2'].map({
        '1': 1.0,
        '2': 2.0 / 3,
        '3': 1.0 / 3,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_health_importance'] = {
        'original_variable': 'q2',
        'question_label': "Est-ce que la santé était un enjeu important pour vous dans cette élection ? Diriez-vous que c'est un enjeu...?",
        'type': 'likert',
        'value_labels': {1.0: "très important", 2.0 / 3: "assez important", 1.0 / 3: "peu important", 0.0: "pas du tout important"},
    }

    # --- q20 ---
    # behav_referendum_intent — Hypothetical referendum voting intent
    # Source: q20
    df_clean['behav_referendum_intent'] = df['q20'].map({
        '1': 'yes',
        '2': 'no',
        '3': 'would_not_vote',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['behav_referendum_intent'] = {
        'original_variable': 'q20',
        'question_label': "Même si vous n'avez peut-être pas encore fait votre choix, s'il y avait un référendum aujourd'hui sur cette question, seriez-vous tenté(e) de voter pour le OUI ou pour le NON ?",
        'type': 'categorical',
        'value_labels': {'yes': 'Oui', 'no': 'Non', 'would_not_vote': 'Ne voterait pas/annulerait'},
    }

    # --- q21 ---
    # op_political_efficacy — Political efficacy belief (government cares about people)
    # Source: q21
    # Note: Statement is negative ("I don't believe governments care..."), so scale is reversed
    # 1 = strongly agree with negative statement → 0.0 (low efficacy)
    # 4 = strongly disagree with negative statement → 1.0 (high efficacy)
    df_clean['op_political_efficacy'] = df['q21'].map({
        '1': 0.0,
        '2': 0.33,
        '3': 0.67,
        '4': 1.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_political_efficacy'] = {
        'original_variable': 'q21',
        'question_label': "Je ne crois pas que les gouvernements se soucient beaucoup de ce que les gens comme moi pensent.",
        'type': 'likert',
        'value_labels': {0.0: 'strongly disagree (government cares)', 0.33: 'somewhat disagree', 0.67: 'somewhat agree', 1.0: 'strongly agree (government does not care)'},
    }

    # --- q22 ---
    # q22 — Opinion sur le gouvernement libéral de M. Charest (1-5)
    # Source: q22
    df_clean['q22'] = df['q22'].map({
        '1': 'opt_1',
        '2': 'opt_2',
        '3': 'opt_3',
        '4': 'opt_4',
        '5': 'opt_5',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['q22'] = {
        'original_variable': 'q22',
        'question_label': "Opinion sur le gouvernement libéral de M. Charest (1-5)",
        'type': 'likert',
        'value_labels': {'opt_1': "Très bonne performance", 'opt_2': "Bonne performance", 'opt_3': "Performance moyenne", 'opt_4': "Mauvaise performance", 'opt_5': "Très mauvaise performance"},
        'missing_codes': [8, 9]
    }

    # --- q23 ---
    # op_trust_government — Trust in governments to do what is right
    # Source: q23
    # Assumption: codes 8/9 treated as missing (Don't know, Refused)
    df_clean['op_trust_government'] = df['q23'].map({
        '1': 'almost_always',
        '2': 'most_of_time',
        '3': 'sometimes',
        '4': 'almost_never',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_trust_government'] = {
        'original_variable': 'q23',
        'question_label': "Dans quelle mesure faites-vous confiance aux gouvernements pour faire ce qui doit être fait ?",
        'type': 'likert',
        'value_labels': {
            'almost_always': "presque toujours",
            'most_of_time': "la plupart du temps",
            'sometimes': "parfois seulement",
            'almost_never': "presque jamais"
        },
        'likert_direction': 'higher_is_more_trust'
    }

    # --- q24 ---
    # op_gov_waste — Perception of government waste of tax money
    # Source: q24
    # No codebook_entry provided in task; values derived from codebook.json
    # Question: "Pensez-vous que les gens au gouvernement gaspillent BEAUCOUP, QUELQUE PEU ou PAS BEAUCOUP nos taxes ?"
    df_clean['op_gov_waste'] = df['q24'].map({
        '1': 'waste_much',
        '2': 'waste_some',
        '3': 'waste_little',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_gov_waste'] = {
        'original_variable': 'q24',
        'question_label': "Pensez-vous que les gens au gouvernement gaspillent BEAUCOUP, QUELQUE PEU ou PAS BEAUCOUP nos taxes ?",
        'type': 'categorical',
        'value_labels': {'waste_much': "Gaspillent beaucoup de nos taxes", 'waste_some': "En gaspillent quelque peu", 'waste_little': "N'en gaspillent pas beaucoup"},
    }

    # --- q25 ---
    # op_government_honesty — Perception of government leaders' honesty
    # Source: q25
    # Question: "Pensen-vous que...? / LIRE LES 3 CHOIX"
    # 1 = many are dishonest (low trust), 2 = some are dishonest, 3 = almost none are dishonest (high trust)
    # 8/9 treated as missing (don't know / refused)
    df_clean['op_government_honesty'] = df['q25'].map({
        '1': 'many_dishonest',
        '2': 'some_dishonest',
        '3': 'almost_none_dishonest',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_government_honesty'] = {
        'original_variable': 'q25',
        'question_label': "Pensen-vous que...? / LIRE LES 3 CHOIX",
        'type': 'categorical',
        'value_labels': {'many_dishonest': "Plusieurs dirigeants gouvernementaux sont malhonnêtes", 'some_dishonest': "Certains d'entre eux sont malhonnêtes", 'almost_none_dishonest': "Presqu'aucun d'entre eux n'est malhonnête"},
    }

    # --- q26 ---
    # op_government_directed — Gouvernements dirigés pour intérêts quelques personnes vs tous
    # Source: q26
    df_clean['op_government_directed'] = df['q26'].map({
        '1': 0.0,    # Dirigés pour intérêts de quelques personnes importantes
        '2': 1.0,    # Dirigés pour bénéfice de tous
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_government_directed'] = {
        'original_variable': 'q26',
        'question_label': "En général, diriez-vous que les gouvernements sont dirigés pour le bénéfice des intérêts de quelques personnes ou pour le bénéfice de tous?",
        'type': 'likert',
        'value_labels': {0.0: "Intérêts de quelques personnes", 1.0: "Bénéfice de tous"},
    }

    # --- q27 ---
    # op_democracy_satisfaction — Satisfaction démocratie québécoise (Likert 0-1)
    # Source: q27
    df_clean['op_democracy_satisfaction'] = df['q27'].map({
        '1': 1.0,    # Très satisfait
        '2': 0.667,  # Assez satisfait
        '3': 0.333,  # Pas très satisfait
        '4': 0.0,    # Pas du tout satisfait
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_democracy_satisfaction'] = {
        'original_variable': 'q27',
        'question_label': "Dans l'ensemble, êtes-vous satisfait de la façon dont la démocratie fonctionne au Québec?",
        'type': 'likert',
        'value_labels': {1.0: "Très satisfait", 0.667: "Assez satisfait", 0.333: "Pas très satisfait", 0.0: "Pas du tout satisfait"},
    }

    # --- q28 ---
    # op_q28 — Inferred Opinion/Attitude Scale (0-46)
    # Source: q28
    # Assumption: Variable is numeric, scaled by max value 46.0.
    # NOTE: Using .map() structure as per template, mapping observed values to their normalized equivalent.
    df_clean['op_q28'] = df['q28'].map({
        0.0: 0.0, # 0.0 / 46.0
        1.0: 1.0 / 46.0, # 1.0 / 46.0
        2.0: 2.0 / 46.0, # 2.0 / 46.0
        3.0: 3.0 / 46.0, # 3.0 / 46.0
        4.0: 4.0 / 46.0, # 4.0 / 46.0
        5.0: 5.0 / 46.0, # 5.0 / 46.0
        6.0: 6.0 / 46.0, # 6.0 / 46.0
        7.0: 7.0 / 46.0, # 7.0 / 46.0
        8.0: 8.0 / 46.0, # 8.0 / 46.0
        9.0: 9.0 / 46.0, # 9.0 / 46.0
        10.0: 10.0 / 46.0, # 10.0 / 46.0
        11.0: 11.0 / 46.0, # 11.0 / 46.0
        12.0: 12.0 / 46.0, # 12.0 / 46.0
        13.0: 13.0 / 46.0, # 13.0 / 46.0
        15.0: 15.0 / 46.0, # 15.0 / 46.0
        20.0: 20.0 / 46.0, # 20.0 / 46.0
        25.0: 25.0 / 46.0, # 25.0 / 46.0
        30.0: 30.0 / 46.0, # 30.0 / 46.0
        35.0: 35.0 / 46.0, # 35.0 / 46.0
        36.0: 36.0 / 46.0, # 36.0 / 46.0
        40.0: 40.0 / 46.0, # 40.0 / 46.0
        42.0: 42.0 / 46.0, # 42.0 / 46.0
        43.0: 43.0 / 46.0, # 43.0 / 46.0
        45.0: 45.0 / 46.0, # 45.0 / 46.0
        46.0: 1.0, # 46.0 / 46.0
    })
    CODEBOOK_VARIABLES['op_q28'] = {
        'original_variable': 'q28',
        'question_label': "Inferred Opinion/Attitude Scale (0-46)",
        'type': 'numeric',
        'value_labels': {'scaling_factor': 46.0},
    }

    # --- q29 ---
    # op_q29 — Unknown question from Q29 (no codebook provided)
    # Source: q29
    # Assumption: Codes mapped to generic labels since codebook was not provided for this variable.
    df_clean['op_q29'] = df['q29'].map({
        0.0: 'response_0',
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        7.0: 'response_7',
        8.0: 'response_8',
        9.0: 'response_9',
        10.0: 'response_10',
        15.0: 'response_15',
        18.0: 'response_18',
        20.0: 'response_20',
        21.0: 'response_21',
        22.0: 'response_22',
        25.0: 'response_25',
        28.0: 'response_28',
        30.0: 'response_30',
        33.0: 'response_33',
        34.0: 'response_34',
        35.0: 'response_35',
        39.0: 'response_39',
        40.0: 'response_40',
        43.0: 'response_43',
    })
    CODEBOOK_VARIABLES['op_q29'] = {
        'original_variable': 'q29',
        'question_label': "Unknown question from Q29 (no codebook provided)",
        'type': 'categorical',
        'value_labels': {'response_0': "0.0", 'response_1': "1.0", 'response_2': "2.0", 'response_3': "3.0", 'response_4': "4.0", 'response_5': "5.0", 'response_6': "6.0", 'response_7': "7.0", 'response_8': "8.0", 'response_9': "9.0", 'response_10': "10.0", 'response_15': "15.0", 'response_18': "18.0", 'response_20': "20.0", 'response_21': "21.0", 'response_22': "22.0", 'response_25': "25.0", 'response_28': "28.0", 'response_30': "30.0", 'response_33': "33.0", 'response_34': "34.0", 'response_35': "35.0", 'response_39': "39.0", 'response_40': "40.0", 'response_43': "43.0"},
    }

    # --- q3 ---
    # op_education_importance — Importance of education as an election issue
    # Source: q3
    # Type: Likert scale (4-point) normalized to 0-1
    # Assumption: codes 8 and 9 (missing/refusal) treated as missing
    df_clean['op_education_importance'] = df['q3'].astype(float).map({
        1.0: 1.0,
        2.0: 0.67,
        3.0: 0.33,
        4.0: 0.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_education_importance'] = {
        'original_variable': 'q3',
        'question_label': "Est-ce que l'éducation ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'0.0': 'not at all important', '0.33': 'little important', '0.67': 'fairly important', '1.0': 'very important'},
    }

    # --- q30 ---
    # op_q30 — Attitude/Opinion question 30 (Mapping placeholders due to missing codebook)
    # Source: q30
    # Assumption: All observed codes (0.0 to 39.0) are treated as distinct categories. No explicit missing codes observed.
    df_clean['op_q30'] = df['q30'].map({
        0.0: 'none',
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
        15.0: 'code_15',
        16.0: 'code_16',
        18.0: 'code_18',
        20.0: 'code_20',
        21.0: 'code_21',
        22.0: 'code_22',
        25.0: 'code_25',
        27.0: 'code_27',
        30.0: 'code_30',
        31.0: 'code_31',
        33.0: 'code_33',
        35.0: 'code_35',
        37.0: 'code_37',
        39.0: 'code_39',
    })
    CODEBOOK_VARIABLES['op_q30'] = {
        'original_variable': 'q30',
        'question_label': "Question 30 label missing, requires review",
        'type': 'categorical',
        'value_labels': {'none': "None", 'code_1': "Code 1 Label", 'code_2': "Code 2 Label", 'code_3': "Code 3 Label", 'code_4': "Code 4 Label", 'code_5': "Code 5 Label", 'code_6': "Code 6 Label", 'code_7': "Code 7 Label", 'code_8': "Code 8 Label", 'code_9': "Code 9 Label", 'code_10': "Code 10 Label", 'code_15': "Code 15 Label", 'code_16': "Code 16 Label", 'code_18': "Code 18 Label", 'code_20': "Code 20 Label", 'code_21': "Code 21 Label", 'code_22': "Code 22 Label", 'code_25': "Code 25 Label", 'code_27': "Code 27 Label", 'code_30': "Code 30 Label", 'code_31': "Code 31 Label", 'code_33': "Code 33 Label", 'code_35': "Code 35 Label", 'code_37': "Code 37 Label", 'code_39': "Code 39 Label"},
    }
    # TODO: verify mapping for q30 — codebook entry missing, labels are placeholders.

    # --- q31 ---
    # op_q31 — Unknown variable q31
    # Source: q31
    # Assumption: Codes observed in data are mapped to generic strings. Unobserved codes and 99.0 are treated as missing.
    df_clean['op_q31'] = df['q31'].map({
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
        10.0: 'code_10',
        12.0: 'code_12',
        15.0: 'code_15',
        19.0: 'code_19',
        20.0: 'code_20',
        24.0: 'code_24',
        25.0: 'code_25',
        29.0: 'code_29',
        30.0: 'code_30',
        33.0: 'code_33',
        35.0: 'code_35',
        40.0: 'code_40',
        41.0: 'code_41',
        45.0: 'code_45',
        50.0: 'code_50',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q31'] = {
        'original_variable': 'q31',
        'question_label': "Unknown variable q31",
        'type': 'categorical',
        'value_labels': {'code_0': "0", 'code_1': "1", 'code_2': "2", 'code_3': "3", 'code_4': "4", 'code_5': "5", 'code_6': "6", 'code_7': "7", 'code_8': "8", 'code_9': "9", 'code_10': "10", 'code_12': "12", 'code_15': "15", 'code_19': "19", 'code_20': "20", 'code_24': "24", 'code_25': "25", 'code_29': "29", 'code_30': "30", 'code_33': "33", 'code_35': "35", 'code_40': "40", 'code_41': "41", 'code_45': "45", 'code_50': "50"},
    }

    # --- q32 ---
    # behav_q32 — Unknown category, mapped from numeric codes
    # Source: q32
    # Assumption: Variable is categorical based on float type and discrete values. Mapping float codes directly to strings due to missing codebook entry.
    df_clean['behav_q32'] = df['q32'].map({
        0.0: '0.0',
        1.0: '1.0',
        2.0: '2.0',
        3.0: '3.0',
        4.0: '4.0',
        5.0: '5.0',
        6.0: '6.0',
        7.0: '7.0',
        8.0: '8.0',
        9.0: '9.0',
        10.0: '10.0',
        12.0: '12.0',
        15.0: '15.0',
        20.0: '20.0',
        22.0: '22.0',
        25.0: '25.0',
        30.0: '30.0',
        35.0: '35.0',
        40.0: '40.0',
        45.0: '45.0',
        50.0: '50.0',
        52.0: '52.0',
        55.0: '55.0',
        60.0: '60.0',
        65.0: '65.0',
    })
    CODEBOOK_VARIABLES['behav_q32'] = {
        'original_variable': 'q32',
        'question_label': "Unknown question text for q32",
        'type': 'categorical',
        'value_labels': {'0.0': "Code 0.0", '1.0': "Code 1.0", '2.0': "Code 2.0", '3.0': "Code 3.0", '4.0': "Code 4.0", '5.0': "Code 5.0", '6.0': "Code 6.0", '7.0': "Code 7.0", '8.0': "Code 8.0", '9.0': "Code 9.0", '10.0': "Code 10.0", '12.0': "Code 12.0", '15.0': "Code 15.0", '20.0': "Code 20.0", '22.0': "Code 22.0", '25.0': "Code 25.0", '30.0': "Code 30.0", '35.0': "Code 35.0", '40.0': "Code 40.0", '45.0': "Code 45.0", '50.0': "Code 50.0", '52.0': "Code 52.0", '55.0': "Code 55.0", '60.0': "Code 60.0", '65.0': "Code 65.0"},
    }

    # --- q33 ---
    # op_fixed_election_dates — Dates fixes pour élections provinciales
    # Source: q33
    df_clean['op_fixed_election_dates'] = df['q33'].map({
        '1': 'yes',
        '2': 'no',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_fixed_election_dates'] = {
        'original_variable': 'q33',
        'question_label': "Pensez-vous que les élections provinciales au Québec devraient avoir lieu à des dates fixes?",
        'type': 'binary',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- q34 ---
    # op_voting_system_acceptable — Système majoritaire sans majorité votes acceptable?
    # Source: q34
    df_clean['op_voting_system_acceptable'] = df['q34'].map({
        1.0: 'acceptable',
        2.0: 'inacceptable',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_voting_system_acceptable'] = {
        'original_variable': 'q34',
        'question_label': "Dans notre système actuel, un parti peut gagner une majorité de sièges sans obtenir une majorité de votes. Est-ce acceptable?",
        'type': 'binary',
        'value_labels': {'acceptable': "Acceptable", 'inacceptable': "Inacceptable"},
    }

    # --- q35 ---
    # op_electoral_reform_favorable — Modification mode scrutin favorable?
    # Source: q35
    df_clean['op_electoral_reform_favorable'] = df['q35'].map({
        '1': 1.0,   # Favorable
        '2': 0.0,   # Défavorable
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_electoral_reform_favorable'] = {
        'original_variable': 'q35',
        'question_label': "Récemment, le gouvernement du Québec a commencé à envisager la possibilité de modifier le mode de scrutin. Êtes-vous favorable ou défavorable?",
        'type': 'likert',
        'value_labels': {1.0: "Favorable", 0.0: "Défavorable"},
    }

    # --- q35a ---
    # op_pq_referendum_favorable — Référendum PQ favorable (4-point Likert)
    # Source: q35a
    df_clean['op_pq_referendum_favorable'] = df['q35a'].map({
        '1': 1.0,    # Très favorable
        '2': 0.667,  # Assez favorable
        '3': 0.333,  # Assez défavorable
        '4': 0.0,    # Très défavorable
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_pq_referendum_favorable'] = {
        'original_variable': 'q35a',
        'question_label': "Le Parti québécois s'est engagé durant la campagne à tenir un référendum le plus tôt possible après son élection. Êtes-vous très favorable, assez favorable, assez défavorable, ou très défavorable?",
        'type': 'likert',
        'value_labels': {1.0: "Très favorable", 0.667: "Assez favorable", 0.333: "Assez défavorable", 0.0: "Très défavorable"},
    }

    # --- q36 ---
    # op_effective_change — Moyen efficace de changer les choses
    # Source: q36
    df_clean['op_effective_change'] = df['q36'].map({
        '1': 'parti_politique',
        '2': 'groupe_interets',
        '3': 'les_deux',
        '4': 'ni_lun_ni_lautre',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_effective_change'] = {
        'original_variable': 'q36',
        'question_label': "À votre avis, quel est le moyen le plus efficace de changer les choses: être membre d'un parti politique ou un groupe d'intérêts?",
        'type': 'categorical',
        'value_labels': {'parti_politique': "Parti politique", 'groupe_interets': "Groupe d'intérêts", 'les_deux': "Les deux", 'ni_lun_ni_lautre': "Ni l'un ni l'autre"},
    }

    # --- q37 ---
    # op_statement_q37 — Accord/désaccord énoncé (4-point Likert)
    # Source: q37
    df_clean['op_statement_q37'] = df['q37'].map({
        '1': 1.0,    # Fortement d'accord
        '2': 0.667,  # Plutôt d'accord
        '3': 0.333,  # Plutôt en désaccord
        '4': 0.0,    # Fortement en désaccord
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q37'] = {
        'original_variable': 'q37',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord, ou fortement en désaccord avec les énoncés suivants.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q38 ---
    # op_mps_out_of_touch — Partis QC tous pareils (accord/désaccord 4-point)
    # Source: q38
    df_clean['op_mps_out_of_touch'] = df['q38'].map({
        '1': 1.0,    # Fortement d'accord
        '2': 0.667,  # Plutôt d'accord
        '3': 0.333,  # Plutôt en désaccord
        '4': 0.0,    # Fortement en désaccord
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_mps_out_of_touch'] = {
        'original_variable': 'q38',
        'question_label': "Tous les partis politiques provinciaux sont essentiellement pareils; on n'a pas vraiment de choix.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q39 ---
    # op_party_rating_liberal — Évaluation Parti libéral (0-100 → 0-1)
    # Source: q39
    df_clean['op_party_rating_liberal'] = np.nan
    mask = (df['q39'] >= 0) & (df['q39'] <= 100)
    df_clean.loc[mask, 'op_party_rating_liberal'] = df.loc[mask, 'q39'] / 100.0
    CODEBOOK_VARIABLES['op_party_rating_liberal'] = {
        'original_variable': 'q39',
        'question_label': "Sur une échelle de zéro à cent, zéro veut dire que vous n'aimez vraiment pas du tout un parti, et cent veut dire que vous l'aimez vraiment beaucoup. Parti libéral?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q4 ---
    # op_unemployment_importance — Unemployment as election issue importance
    # Source: q4
    # Assumption: codes 8/9 treated as missing (don't know/refusal)
    df_clean['op_unemployment_importance'] = df['q4'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_unemployment_importance'] = {
        'original_variable': 'q4',
        'question_label': "Est-ce que le chômage ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'0.0': 'pas du tout important', '0.333': 'peu important', '0.667': 'assez important', '1.0': 'très important'},
    }

    # --- q40 ---
    # op_party_rating_pq — Évaluation Parti québécois (0-100 → 0-1)
    # Source: q40
    df_clean['op_party_rating_pq'] = np.nan
    mask = (df['q40'] >= 0) & (df['q40'] <= 100)
    df_clean.loc[mask, 'op_party_rating_pq'] = df.loc[mask, 'q40'] / 100.0
    CODEBOOK_VARIABLES['op_party_rating_pq'] = {
        'original_variable': 'q40',
        'question_label': "Sur une échelle de zéro à cent, Parti québécois?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q41 ---
    # op_party_rating_adq — Évaluation ADQ (0-100 → 0-1)
    # Source: q41
    df_clean['op_party_rating_adq'] = np.nan
    mask = (df['q41'] >= 0) & (df['q41'] <= 100)
    df_clean.loc[mask, 'op_party_rating_adq'] = df.loc[mask, 'q41'] / 100.0
    CODEBOOK_VARIABLES['op_party_rating_adq'] = {
        'original_variable': 'q41',
        'question_label': "Sur une échelle de zéro à cent, ADQ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q42 ---
    # op_party_rating_qs — Évaluation Québec Solidaire (0-100 → 0-1)
    # Source: q42
    df_clean['op_party_rating_qs'] = np.nan
    mask = (df['q42'] >= 0) & (df['q42'] <= 100)
    df_clean.loc[mask, 'op_party_rating_qs'] = df.loc[mask, 'q42'] / 100.0
    CODEBOOK_VARIABLES['op_party_rating_qs'] = {
        'original_variable': 'q42',
        'question_label': "Sur une échelle de zéro à cent, Québec Solidaire?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q43 ---
    # op_party_rating_green — Évaluation Parti vert (0-100 → 0-1)
    # Source: q43
    df_clean['op_party_rating_green'] = np.nan
    mask = (df['q43'] >= 0) & (df['q43'] <= 100)
    df_clean.loc[mask, 'op_party_rating_green'] = df.loc[mask, 'q43'] / 100.0
    CODEBOOK_VARIABLES['op_party_rating_green'] = {
        'original_variable': 'q43',
        'question_label': "Sur une échelle de zéro à cent, Parti vert?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q44 ---
    # op_best_leader_competent — Quel chef est le plus compétent?
    # Source: q44
    df_clean['op_best_leader_competent'] = df['q44'].map({
        '01': 'charest',
        '02': 'boisclair',
        '03': 'dumont',
        '04': 'david',
        '05': 'mckay',
        '06': 'aucun',
        '07': 'tous',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_leader_competent'] = {
        'original_variable': 'q44',
        'question_label': "Selon vous, lequel des chefs de parti est le plus compétent?",
        'type': 'categorical',
        'value_labels': {'charest': "Jean Charest", 'boisclair': "André Boisclair", 'dumont': "Mario Dumont", 'david': "Françoise David", 'mckay': "Scott McKay", 'aucun': "Aucun", 'tous': "Tous"},
    }

    # --- q45 ---
    # op_best_leader_honest — Quel chef est le plus honnête?
    # Source: q45
    df_clean['op_best_leader_honest'] = df['q45'].map({
        '01': 'charest',
        '02': 'boisclair',
        '03': 'dumont',
        '04': 'david',
        '05': 'mckay',
        '06': 'aucun',
        '07': 'tous',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_leader_honest'] = {
        'original_variable': 'q45',
        'question_label': "Selon vous, lequel des chefs de parti est le plus honnête?",
        'type': 'categorical',
        'value_labels': {'charest': "Jean Charest", 'boisclair': "André Boisclair", 'dumont': "Mario Dumont", 'david': "Françoise David", 'mckay': "Scott McKay", 'aucun': "Aucun", 'tous': "Tous"},
    }

    # --- q46 ---
    # op_best_leader_close — Quel chef est le plus proche des gens?
    # Source: q46
    df_clean['op_best_leader_close'] = df['q46'].map({
        '01': 'charest',
        '02': 'boisclair',
        '03': 'dumont',
        '04': 'david',
        '05': 'mckay',
        '06': 'aucun',
        '07': 'tous',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_leader_close'] = {
        'original_variable': 'q46',
        'question_label': "Selon vous, lequel des chefs de parti est le plus proche des gens?",
        'type': 'categorical',
        'value_labels': {'charest': "Jean Charest", 'boisclair': "André Boisclair", 'dumont': "Mario Dumont", 'david': "Françoise David", 'mckay': "Scott McKay", 'aucun': "Aucun", 'tous': "Tous"},
    }

    # --- q47 ---
    # op_economy_assessment — Évaluation économie québécoise
    # Source: q47
    df_clean['op_economy_assessment'] = df['q47'].map({
        '1': 'amelioree',
        '2': 'deterioree',
        '3': 'meme',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_economy_assessment'] = {
        'original_variable': 'q47',
        'question_label': "Selon vous, l'économie québécoise s'est-elle améliorée, détériorée, ou est-elle restée à peu près la même?",
        'type': 'categorical',
        'value_labels': {'amelioree': "Améliorée", 'deterioree': "Détériorée", 'meme': "À peu près la même"},
    }

    # --- q48 ---
    # --- q48 ---
    # op_political_identity — Identité politique (fédéraliste/souverainiste)
    # Source: q48
    df_clean['op_political_identity'] = df['q48'].map({
        '1': 'federaliste',
        '2': 'souverainiste',
        '3': 'entre_les_deux',
        '4': 'ni_lun_ni_lautre',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_political_identity'] = {
        'original_variable': 'q48',
        'question_label': "Vous considérez-vous surtout comme un(e) fédéraliste, surtout comme un(e) souverainiste, comme quelqu'un qui est entre les deux, ou ni l'un ni l'autre?",
        'type': 'categorical',
        'value_labels': {'federaliste': "Fédéraliste", 'souverainiste': "Souverainiste", 'entre_les_deux': "Entre les deux", 'ni_lun_ni_lautre': "Ni l'un ni l'autre"},
    }

    # --- q49 ---
    # --- q49 ---
    # op_power_distribution — Distribution des pouvoirs fédéral/provincial
    # Source: q49
    df_clean['op_power_distribution'] = df['q49'].map({
        '1': 'plus_provincial',
        '2': 'plus_federal',
        '3': 'statu_quo',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_power_distribution'] = {
        'original_variable': 'q49',
        'question_label': "D'après vous, à l'avenir les gouvernements provinciaux devraient avoir plus de pouvoirs, le gouvernement fédéral devrait avoir plus de pouvoirs, ou les choses devraient rester comme elles sont?",
        'type': 'categorical',
        'value_labels': {'plus_provincial': "Plus de pouvoirs aux provinces", 'plus_federal': "Plus de pouvoirs au fédéral", 'statu_quo': "Statu quo"},
    }

    # --- q5 ---
    # op_env_importance — Environmental importance in election
    # Source: q5
    df_clean['op_env_importance'] = df['q5'].astype(float).map({
        1.0: 1.0,
        2.0: 0.667,
        3.0: 0.333,
        4.0: 0.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_env_importance'] = {
        'original_variable': 'q5',
        'question_label': "Est-ce que l'environnement ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'very_important': 'très important', 'somewhat_important': 'assez important', 'little_important': 'peu important', 'not_important': 'pas du tout important'},
    }

    # --- q50 ---
    # op_statement_q50 — Accord/désaccord énoncé q50 (4-point Likert)
    # Source: q50
    df_clean['op_statement_q50'] = df['q50'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q50'] = {
        'original_variable': 'q50',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q51 ---
    # op_statement_q51 — Accord/désaccord énoncé q51 (4-point Likert)
    # Source: q51
    df_clean['op_statement_q51'] = df['q51'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q51'] = {
        'original_variable': 'q51',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q52 ---
    # op_statement_q52 — Accord/désaccord énoncé q52 (4-point Likert)
    # Source: q52
    df_clean['op_statement_q52'] = df['q52'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q52'] = {
        'original_variable': 'q52',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q53 ---
    # op_statement_q53 — Accord/désaccord énoncé q53 (4-point Likert)
    # Source: q53
    df_clean['op_statement_q53'] = df['q53'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q53'] = {
        'original_variable': 'q53',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q54 ---
    # op_statement_q54 — Accord/désaccord énoncé q54 (4-point Likert)
    # Source: q54
    df_clean['op_statement_q54'] = df['q54'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q54'] = {
        'original_variable': 'q54',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q55 ---
    # op_best_party_sante — Meilleur parti pour améliorer les soins de santé
    # Source: q55
    df_clean['op_best_party_sante'] = df['q55'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_sante'] = {
        'original_variable': 'q55',
        'question_label': "Selon vous, quel parti politique est le meilleur pour améliorer les soins de santé?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q56 ---
    # op_best_party_education — Meilleur parti pour améliorer l'éducation
    # Source: q56
    df_clean['op_best_party_education'] = df['q56'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_education'] = {
        'original_variable': 'q56',
        'question_label': "Quel parti est le meilleur pour améliorer l'éducation?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q57 ---
    # op_best_party_chomage — Meilleur parti pour faire baisser le chômage
    # Source: q57
    df_clean['op_best_party_chomage'] = df['q57'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_chomage'] = {
        'original_variable': 'q57',
        'question_label': "Quel parti est le meilleur pour faire baisser le taux de chômage?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q58 ---
    # op_best_party_pauvrete — Meilleur parti pour lutter contre la pauvreté
    # Source: q58
    df_clean['op_best_party_pauvrete'] = df['q58'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_pauvrete'] = {
        'original_variable': 'q58',
        'question_label': "Quel parti est le meilleur pour lutter contre la pauvreté?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q59 ---
    # op_best_party_environnement — Meilleur parti pour protéger l'environnement
    # Source: q59
    df_clean['op_best_party_environnement'] = df['q59'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_environnement'] = {
        'original_variable': 'q59',
        'question_label': "Quel parti est le meilleur pour protéger l'environnement?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q6 ---
    # op_fiscal_imbalance_importance — Déséquilibre fiscal — importance de l'enjeu
    # Source: q6
    df_clean['op_fiscal_imbalance_importance'] = df['q6'].map({
        '1': 1.0,
        '2': 0.67,
        '3': 0.33,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_fiscal_imbalance_importance'] = {
        'original_variable': 'q6',
        'question_label': "Est-ce que le déséquilibre fiscal ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'1.0': "très important", '0.67': "assez important", '0.33': "peu important", '0.0': "pas du tout important"},
    }

    # --- q60 ---
    # op_best_party_familles — Meilleur parti pour aider les familles
    # Source: q60
    df_clean['op_best_party_familles'] = df['q60'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_familles'] = {
        'original_variable': 'q60',
        'question_label': "Quel parti est le meilleur pour aider les familles?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q61a ---
    # op_best_party_interets_qc — Meilleur parti pour défendre les intérêts du Québec
    # Source: q61a
    df_clean['op_best_party_interets_qc'] = df['q61a'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_interets_qc'] = {
        'original_variable': 'q61a',
        'question_label': "Quel parti est le meilleur pour défendre les intérêts du Québec?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q61b ---
    # op_best_party_identite_qc — Meilleur parti pour défendre l'identité et la culture québécoise
    # Source: q61b
    df_clean['op_best_party_identite_qc'] = df['q61b'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_identite_qc'] = {
        'original_variable': 'q61b',
        'question_label': "Quel parti est le meilleur pour défendre l'identité et la culture québécoise?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q61c ---
    # op_best_party_interets_region — Meilleur parti pour défendre les intérêts de votre région
    # Source: q61c
    df_clean['op_best_party_interets_region'] = df['q61c'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_interets_region'] = {
        'original_variable': 'q61c',
        'question_label': "Quel parti est le meilleur pour défendre les intérêts de votre région?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q61d ---
    # op_best_party_dette — Meilleur parti pour réduire la dette du Québec
    # Source: q61d
    df_clean['op_best_party_dette'] = df['q61d'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_dette'] = {
        'original_variable': 'q61d',
        'question_label': "Quel parti est le meilleur pour réduire la dette du Québec?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q62 ---
    # op_best_party_citoyens — Meilleur parti pour défendre les intérêts des citoyens ordinaires
    # Source: q62
    df_clean['op_best_party_citoyens'] = df['q62'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_best_party_citoyens'] = {
        'original_variable': 'q62',
        'question_label': "Quel parti est le meilleur pour défendre les intérêts des citoyens ordinaires?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q64 ---
    # --- q64 ---
    # op_rating_syndicats — Évaluation des syndicats (0-100 → 0-1)
    # Source: q64
    df_clean['op_rating_syndicats'] = np.nan
    mask = (df['q64'] >= 0) & (df['q64'] <= 100)
    df_clean.loc[mask, 'op_rating_syndicats'] = df.loc[mask, 'q64'] / 100.0
    CODEBOOK_VARIABLES['op_rating_syndicats'] = {
        'original_variable': 'q64',
        'question_label': "Sur une échelle qui va de 0 à 100, où 0 veut dire que vous n'aimez vraiment pas du tout les syndicats, et 100 veut dire que vous les aimez vraiment beaucoup. En général, aimez-vous les syndicats?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q65 ---
    # op_rating_entreprises — Évaluation des entreprises (0-100 → 0-1)
    # Source: q65
    df_clean['op_rating_entreprises'] = np.nan
    mask = (df['q65'] >= 0) & (df['q65'] <= 100)
    df_clean.loc[mask, 'op_rating_entreprises'] = df.loc[mask, 'q65'] / 100.0
    CODEBOOK_VARIABLES['op_rating_entreprises'] = {
        'original_variable': 'q65',
        'question_label': "Sur une échelle qui va de 0 à 100, où 0 veut dire que vous n'aimez vraiment pas du tout les entreprises, et 100 veut dire que vous les aimez vraiment beaucoup. En général, aimez-vous les entreprises?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q66 ---
    # op_same_sex_marriage — Pour ou contre le mariage entre personnes de même sexe
    # Source: q66
    df_clean['op_same_sex_marriage'] = df['q66'].map({
        '1': 1.0,   # Pour
        '2': 0.0,   # Contre
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_same_sex_marriage'] = {
        'original_variable': 'q66',
        'question_label': "Êtes-vous pour ou contre le mariage entre personnes de même sexe?",
        'type': 'likert',
        'value_labels': {'intention_a': "Intention Parti A", 'intention_b': "Intention Parti B"},
    }

    # --- q67 ---
    # op_statement_q67 — Accord/désaccord énoncé q67 (4-point Likert)
    # Source: q67
    df_clean['op_statement_q67'] = df['q67'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q67'] = {
        'original_variable': 'q67',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q68 ---
    # op_statement_q68 — Accord/désaccord énoncé q68 (4-point Likert)
    # Source: q68
    df_clean['op_statement_q68'] = df['q68'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q68'] = {
        'original_variable': 'q68',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q69 ---
    # op_statement_q69 — Accord/désaccord énoncé q69 (4-point Likert)
    # Source: q69
    df_clean['op_statement_q69'] = df['q69'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_statement_q69'] = {
        'original_variable': 'q69',
        'question_label': "Veuillez indiquer si vous êtes fortement d'accord, plutôt d'accord, plutôt en désaccord ou fortement en désaccord.",
        'type': 'likert',
        'value_labels': {1.0: "Fortement d'accord", 0.667: "Plutôt d'accord", 0.333: "Plutôt en désaccord", 0.0: "Fortement en désaccord"},
    }

    # --- q7 ---
    # op_tax_cuts_importance — Importance of tax cuts in election
    # Source: q7
    # Assumption: codes 8/9 treated as missing (no opinion/refusal, not in importance scale)
    df_clean['op_tax_cuts_importance'] = df['q7'].map({
        '1': 1.0,
        '2': 0.667,
        '3': 0.333,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_tax_cuts_importance'] = {
        'original_variable': 'q7',
        'question_label': "Est-ce que les baisses d'impôts ÉTAIENT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'1.0': 'très important', '0.667': 'assez important', '0.333': 'peu important', '0.0': 'pas du tout important'},
    }

    # --- q70 ---
    # op_party_identification — Identification partisane (parti QC)
    # Source: q70
    df_clean['op_party_identification'] = df['q70'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_party_identification'] = {
        'original_variable': 'q70',
        'question_label': "En politique provinciale, habituellement est-ce que vous vous identifiez à un parti?",
        'type': 'categorical',
        'value_labels': {'liberal': "Parti Libéral", 'pq': "Parti Québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre parti", 'aucun': "À aucun parti"},
    }

    # --- q71 ---
    # op_party_closeness — Proximité au parti identifié (très/assez/pas très proche)
    # Source: q71
    df_clean['op_party_closeness'] = df['q71'].map({
        '1': 1.0,    # Très proche
        '2': 0.5,    # Assez proche
        '3': 0.0,    # Pas très proche
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_party_closeness'] = {
        'original_variable': 'q71',
        'question_label': "Vous sentez-vous très proche du parti, assez proche, ou pas très proche?",
        'type': 'likert',
        'value_labels': {1.0: "Très proche", 0.5: "Assez proche", 0.0: "Pas très proche"},
    }

    # --- q72 ---
    # op_q72 — Question 72 response
    # Source: q72
    # Assumption: Codes '8' and '9' are treated as missing as no codebook was provided.
    df_clean['op_q72'] = df['q72'].map({
        '1': 'yes',
        '2': 'no',
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_q72'] = {
        'original_variable': 'q72',
        'question_label': "Vous sentez-vous UN PEU plus proche de l'un ou l'autre des partis provinciaux?",
        'type': 'binary',
        'value_labels': {'yes': "Oui", 'no': "Non"},
    }

    # --- q73 ---
    # op_party_closest — Parti provincial le plus proche de vous
    # Source: q73
    df_clean['op_party_closest'] = df['q73'].map({
        '01': 'liberal',
        '02': 'pq',
        '03': 'adq',
        '04': 'quebec_solidaire',
        '05': 'parti_vert',
        '96': 'autre',
        '97': 'aucun',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['op_party_closest'] = {
        'original_variable': 'q73',
        'question_label': "De quel parti vous sentez-vous le plus proche?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'pq': "Parti québécois", 'adq': "ADQ", 'quebec_solidaire': "Québec Solidaire", 'parti_vert': "Parti Vert", 'autre': "Autre", 'aucun': "Aucun"},
    }

    # --- q74 ---
    # behav_vote_federal_2006 — Vote fédéral 2006 (élection de janvier 2006)
    # Source: q74
    df_clean['behav_vote_federal_2006'] = df['q74'].map({
        '01': 'liberal',
        '02': 'conservateur',
        '03': 'npd',
        '04': 'bloc_quebecois',
        '05': 'parti_vert',
        '06': 'na_pas_vote',
        '07': 'annule',
        '96': 'autre',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_federal_2006'] = {
        'original_variable': 'q74',
        'question_label': "Lors de la dernière élection fédérale en janvier 2006, pour quel parti avez-vous voté?",
        'type': 'categorical',
        'value_labels': {'liberal': "Libéral", 'conservateur': "Conservateur", 'npd': "NPD", 'bloc_quebecois': "Bloc Québécois", 'parti_vert': "Parti Vert", 'na_pas_vote': "N'a pas voté", 'annule': "A annulé son vote", 'autre': "Autre"},
    }

    # --- q75 ---
    # ses_age_year — Année de naissance
    # Source: q75 (Notez l'année de naissance)
    df_clean['ses_age_year'] = df['q75'].copy()
    df_clean.loc[df_clean['ses_age_year'] == 9999.0, 'ses_age_year'] = np.nan
    CODEBOOK_VARIABLES['ses_age_year'] = {
        'original_variable': 'q75',
        'question_label': "Pour terminer l'entrevue, nous aimerions avoir quelques informations qui nous aideront à vérifier si notre échantillon représente bien l'ensemble de la population québécoise. D'abord, en quelle année êtes-vous né(e)?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q76 ---
    # ses_gender — Genre du répondant
    # Source: q76 (NE PAS LIRE - Indiquez le sexe du répondant)
    df_clean['ses_gender'] = df['q76'].map({
        '1': 'homme',
        '2': 'femme',
    })
    CODEBOOK_VARIABLES['ses_gender'] = {
        'original_variable': 'q76',
        'question_label': "(NE PAS LIRE) Indiquez le sexe du répondant",
        'type': 'categorical',
        'value_labels': {'homme': "Masculin", 'femme': "Féminin"},
    }

    # --- q77 ---
    # ses_education — Niveau d'éducation
    # Source: q77
    df_clean['ses_education'] = df['q77'].map({
        '01': 'aucune_scolarite',
        '02': 'primaire_sans_diplome',
        '03': 'primaire_avec_diplome',
        '04': 'secondaire_sans_diplome',
        '05': 'secondaire_avec_diplome',
        '06': 'technique_cegep_sans_diplome',
        '07': 'technique_cegep_avec_diplome',
        '08': 'universite_non_complete',
        '09': 'baccalaureat',
        '10': 'maitrise',
        '11': 'doctorat_professionnel',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_education'] = {
        'original_variable': 'q77',
        'question_label': "Quel est votre niveau d'éducation?",
        'type': 'categorical',
        'value_labels': {
            'aucune_scolarite': "Aucune scolarité",
            'primaire_sans_diplome': "Cours primaire (sans diplôme)",
            'primaire_avec_diplome': "Cours primaire (avec diplôme)",
            'secondaire_sans_diplome': "Cours secondaire (sans diplôme)",
            'secondaire_avec_diplome': "Cours secondaire (avec diplôme)",
            'technique_cegep_sans_diplome': "Technique/cégep (sans diplôme)",
            'technique_cegep_avec_diplome': "Technique/cégep (avec diplôme)",
            'universite_non_complete': "Université non complétée",
            'baccalaureat': "Baccalauréat",
            'maitrise': "Maîtrise",
            'doctorat_professionnel': "Diplôme professionnel ou doctorat",
        },
    }

    # --- q78 ---
    # ses_income — Revenu total du ménage avant impôts
    # Source: q78
    df_clean['ses_income'] = df['q78'].map({
        '01': 'moins_20k',
        '02': '20k_30k',
        '03': '30k_40k',
        '04': '40k_50k',
        '05': '50k_60k',
        '06': '60k_70k',
        '07': '70k_80k',
        '08': '80k_90k',
        '09': '90k_100k',
        '10': 'plus_100k',
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_income'] = {
        'original_variable': 'q78',
        'question_label': "Le revenu total de votre ménage avant impôts en 2006?",
        'type': 'categorical',
        'value_labels': {
            'moins_20k': "Moins de $20,000",
            '20k_30k': "Entre $20,000 et $29,999",
            '30k_40k': "Entre $30,000 et $39,999",
            '40k_50k': "Entre $40,000 et $49,999",
            '50k_60k': "Entre $50,000 et $59,999",
            '60k_70k': "Entre $60,000 et $69,999",
            '70k_80k': "Entre $70,000 et $79,999",
            '80k_90k': "Entre $80,000 et $89,999",
            '90k_100k': "Entre $90,000 et $99,999",
            'plus_100k': "Plus de $100,000",
        },
    }

    # --- q79 ---
    # ses_employment — Statut d'emploi
    # Source: q79
    df_clean['ses_employment'] = df['q79'].map({
        '01': 'travailleur_autonome',
        '02': 'salarie',
        '03': 'retraite',
        '04': 'chomage',
        '05': 'etudiant',
        '06': 'menagere',
        '07': 'handicape',
        '08': 'deux_emplois_plus',
        '09': 'etudiant_et_salarie',
        '10': 'famille_et_salarie',
        '11': 'retraite_et_salarie',
        '96': 'autre',
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_employment'] = {
        'original_variable': 'q79',
        'question_label': "Travaillez-vous actuellement à votre compte, êtes-vous salarié, avez-vous pris votre retraite, êtes-vous au chômage, étudiant(e), ménagère?",
        'type': 'categorical',
        'value_labels': {
            'travailleur_autonome': "Travaille à son compte",
            'salarie': "Salarié(e)",
            'retraite': "Retraité(e)",
            'chomage': "Au chômage / cherche du travail",
            'etudiant': "Étudiant(e)",
            'menagere': "Ménagère",
            'handicape': "Handicapé(e)",
            'deux_emplois_plus': "Deux emplois ou plus",
            'etudiant_et_salarie': "Étudiant(e) et salarié(e)",
            'famille_et_salarie': "En charge d'une famille et salarié(e)",
            'retraite_et_salarie': "Retraité(e) et salarié(e)",
            'autre': "Autre",
        },
    }

    # --- q8 ---
    # op_issue_quebec_status — Importance of Quebec political status as election issue
    # Source: q8
    # Likert scale (1–4) normalized to 0–1; codes 8–9 treated as missing
    df_clean['op_issue_quebec_status'] = df['q8'].map({
        '1': 1.0,
        '2': 0.67,
        '3': 0.33,
        '4': 0.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_issue_quebec_status'] = {
        'original_variable': 'q8',
        'question_label': "Est-ce que le statut politique du Québec ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'1.0': 'très important', '0.67': 'assez important', '0.33': 'peu important', '0.0': 'pas du tout important'},
    }

    # --- q80 ---
    # ses_language_home — Langue parlée le plus souvent à la maison
    # Source: q80
    df_clean['ses_language_home'] = df['q80'].map({
        '01': 'anglais',
        '02': 'francais',
        '03': 'chinois',
        '04': 'italien',
        '05': 'portugais',
        '06': 'espagnol',
        '07': 'allemand',
        '08': 'polonais',
        '09': 'punjabi',
        '10': 'grec',
        '11': 'vietnamien',
        '12': 'arabe',
        '13': 'cri',
        '14': 'tagal',
        '15': 'ukrainien_russe',
        '96': 'autre',
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_language_home'] = {
        'original_variable': 'q80',
        'question_label': "Quelle langue parlez-vous le plus souvent à la maison?",
        'type': 'categorical',
        'value_labels': {
            'anglais': "Anglais", 'francais': "Français", 'chinois': "Chinois",
            'italien': "Italien", 'portugais': "Portugais", 'espagnol': "Espagnol",
            'allemand': "Allemand", 'polonais': "Polonais", 'punjabi': "Punjabi",
            'grec': "Grec", 'vietnamien': "Vietnamien", 'arabe': "Arabe",
            'cri': "Cri", 'tagal': "Tagal (philippin)", 'ukrainien_russe': "Ukrainien / Russe",
            'autre': "Autre",
        },
    }

    # --- q81 ---
    # ses_religiosity — Fréquence de pratique religieuse (messe)
    # Source: q81
    df_clean['ses_religiosity'] = df['q81'].map({
        '01': 1.0,    # Chaque semaine ou plus souvent
        '02': 0.75,   # Deux fois par mois
        '03': 0.5,    # Une fois par mois
        '04': 0.25,   # Une ou deux fois par an
        '05': 0.0,    # Presque jamais ou jamais
        '98': np.nan,
        '99': np.nan,
    })
    CODEBOOK_VARIABLES['ses_religiosity'] = {
        'original_variable': 'q81',
        'question_label': "Sans compter les mariages et les funérailles, combien de fois assistez-vous aux messes à votre lieu de culte?",
        'type': 'likert',
        'value_labels': {1.0: "Chaque semaine (ou plus souvent)", 0.75: "Deux fois par mois", 0.5: "Une fois par mois", 0.25: "Une ou deux fois par an", 0.0: "Presque jamais (ou jamais)"},
    }

    # --- q9 ---
    # op_poverty_importance — Importance of poverty as election issue
    # Source: q9
    df_clean['op_poverty_importance'] = df['q9'].map({
        '1': 0.0,
        '2': 0.33,
        '3': 0.67,
        '4': 1.0,
        '8': np.nan,
        '9': np.nan,
    })
    CODEBOOK_VARIABLES['op_poverty_importance'] = {
        'original_variable': 'q9',
        'question_label': "Est-ce que la pauvreté ÉTAIT un enjeu important pour vous dans cette élection ?",
        'type': 'likert',
        'value_labels': {'0.0': 'pas du tout important', '0.33': 'peu important', '0.67': 'assez important', '1.0': 'très important'},
    }

    # --- quest ---
    # id_respondent — Questionnaire identifier / Numéro de questionnaire
    # Source: quest
    # Note: Sequential numeric ID (1-2175), stored as string in SPSS file
    df_clean['id_respondent'] = pd.to_numeric(df['quest'], errors='coerce')
    CODEBOOK_VARIABLES['id_respondent'] = {
        'original_variable': 'quest',
        'question_label': "Questionnaire identifier",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- type ---
    # type_inferred — Inferred variable type (1 or 2)
    # Source: type
    # Note: No codebook entry provided. Codes 1.0 and 2.0 mapped to generic 'one'/'two'.
    df_clean['type_inferred'] = df['type'].map({
        1.0: 'one',
        2.0: 'two',
    })
    CODEBOOK_VARIABLES['type_inferred'] = {
        'original_variable': 'type',
        'question_label': "Inferred type variable",
        'type': 'categorical',
        'value_labels': {'one': "Type One", 'two': "Type Two"},
    }


    # ========================================================================
    # STRATES CANONIQUES
    # ========================================================================
    df_clean = map_strates_canoniques(df_clean)

    return df_clean


def map_strates_canoniques(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les 4 colonnes de strates canoniques standardisées.

    Sources:
        - age_group: ses_age_year (q75) → année naissance → groupe d'âge (2007 comme référence)
        - langue: ses_language (langu) → francophone / anglo_autre
        - region: ses_region (nomx) → déjà en format canonique (montreal/quebec/couronne/regions)
        - genre: ses_gender (q76) → homme / femme

    Returns:
        pd.DataFrame: df_clean avec colonnes strate_age_group, strate_langue, strate_region, strate_genre ajoutées
    """
    # --- strate_age_group (depuis année de naissance, enquête 2007) ---
    ANNEE_ENQUETE = 2007
    age = ANNEE_ENQUETE - df_clean['ses_age_year']
    df_clean['strate_age_group'] = pd.Series(dtype='object', index=df_clean.index)
    df_clean.loc[(age >= 18) & (age <= 34), 'strate_age_group'] = '18-34'
    df_clean.loc[(age >= 35) & (age <= 54), 'strate_age_group'] = '35-54'
    df_clean.loc[age >= 55, 'strate_age_group'] = '55+'

    # --- strate_langue (depuis langue maternelle) ---
    df_clean['strate_langue'] = df_clean['ses_language'].map({
        'french': 'francophone',
        'english': 'anglo_autre',
    })

    # --- strate_region (déjà canonique depuis nomx) ---
    df_clean['strate_region'] = df_clean['ses_region']

    # --- strate_genre ---
    df_clean['strate_genre'] = df_clean['ses_gender']

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
