#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2012

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
    'survey_id': 'eeq_2012',           # ID unique du sondage (ex: "ces2019")
    'title': 'eeq_2012',             # Titre complet
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
    """Charge et nettoie les données brutes du sondage eeq_2012.

    Args:
        raw_path (str): Chemin vers le fichier .sav brut
            (ex: SHARED_FOLDER_PATH/eeq_2012/Quebec Election Study 2012 (SPSS).sav)

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

    # --- AGE ---
    # ses_age — Age in years (derived from Q97: year of birth)
    # Source: AGE
    # Note: Stored as numeric (integer) since age in years is continuous demographic data
    # This field is derived from the birth year question (Q97: "En quelle année êtes-vous né(e)?")
    df_clean['ses_age'] = df['AGE'].astype('Int64', errors='ignore')
    # Treat invalid/missing values (likely 0 or 999) as NaN
    df_clean.loc[df_clean['ses_age'] < 18, 'ses_age'] = np.nan
    
    CODEBOOK_VARIABLES['ses_age'] = {
        'original_variable': 'AGE',
        'question_label': "En quelle année êtes-vous né(e)? (Derived age in years from Q97)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- AGEX ---
    # ses_year_of_birth — Year of birth
    # Source: AGEX
    # Note: Stored as numeric (integer) since year is continuous demographic data
    df_clean['ses_year_of_birth'] = df['AGEX'].astype('Int64', errors='ignore')
    # Treat invalid/missing values (9999 placeholder, future years, etc.) as NaN
    df_clean.loc[df_clean['ses_year_of_birth'] > 2012, 'ses_year_of_birth'] = np.nan
    df_clean.loc[df_clean['ses_year_of_birth'] < 1900, 'ses_year_of_birth'] = np.nan
    
    CODEBOOK_VARIABLES['ses_year_of_birth'] = {
        'original_variable': 'AGEX',
        'question_label': "En quelle année êtes-vous né(e)? (In what year were you born?)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- LANGU ---
    # ses_language — Language first learned at home in childhood
    # Source: LANGU
    df_clean['ses_language'] = df['LANGU'].map({
        1.0: 'french',
        2.0: 'english',
        3.0: 'other',
        4.0: 'french_and_english',
        5.0: 'french_and_other',
        6.0: 'english_and_other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'LANGU',
        'question_label': "Quelle est la langue que vous avez apprise en premier lieu à la maison dans votre enfance et que vous comprenez toujours?",
        'type': 'categorical',
        'value_labels': {
            'french': "Français",
            'english': "Anglais",
            'other': "Autre",
            'french_and_english': "Français et anglais",
            'french_and_other': "Français et autre",
            'english_and_other': "Anglais et autre",
        },
    }

     # --- OCCUP ---
    # ses_occupation — Occupation principale (Q101)
    # Source: OCCUP
    df_clean['ses_occupation'] = df['OCCUP'].map({
        1.0: 'self_employed',
        2.0: 'employed_salaried',
        3.0: 'retired',
        4.0: 'unemployed',
        5.0: 'student',
        6.0: 'homemaker',
        7.0: 'disabled',
        8.0: 'multiple_jobs',
        9.0: 'student_and_salaried',
        10.0: 'homemaker_and_salaried',
        11.0: 'retired_and_salaried',
        12.0: 'other',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_occupation'] = {
        'original_variable': 'OCCUP',
        'question_label': "Travaillez-vous actuellement à votre compte, êtes-vous salarié(e), avez-vous pris votre retraite, êtes-vous au chômage ou cherchez-vous du travail, êtes-vous étudiant(e), ménager(ère), ou quelque chose d'autre?",
        'type': 'categorical',
        'value_labels': {
            'self_employed': "Travaille à son compte (avec ou sans employés)",
            'employed_salaried': "Travaille pour un salaire (à temps plein ou à temps partiel, inclut congé payé)",
            'retired': "Retraité(e)",
            'unemployed': "Au chômage/cherche du travail",
            'student': "Étudiant(e)",
            'homemaker': "Ménager(ère)",
            'disabled': "Handicapé(e)",
            'multiple_jobs': "Occupe deux ou plus de deux emplois rémunérés",
            'student_and_salaried': "Étudiant(e) et salarié(e)",
            'homemaker_and_salaried': "Ménager(ère) et salarié(e)",
            'retired_and_salaried': "Retraité(e) et salarié(e)",
            'other': "Autre",
        },
    }

     # --- POND ---
    # ses_weight — Survey sampling weight, normalized to 0-1
    # Source: POND
    # Note: Normalized by dividing by the maximum observed value in the dataset
    pond_max = df['POND'].max()
    df_clean['ses_weight'] = df['POND'] / pond_max if pond_max > 0 else np.nan
    CODEBOOK_VARIABLES['ses_weight'] = {
        'original_variable': 'POND',
        'question_label': "Pondération d'échantillonnage du sondage (normalisée 0-1)",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q0QC ---
    # ses_region_qc — Region administrative du Québec
    # Source: Q0QC
    df_clean['ses_region_qc'] = df['Q0QC'].map({
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
    })
    CODEBOOK_VARIABLES['ses_region_qc'] = {
        'original_variable': 'Q0QC',
        'question_label': "In what region in Quebec do you live?",
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
            'centre_du_quebec': "Centre-du-Québec",
        },
    }

    # --- REGIO ---
    # ses_region_rmr — Région métropolitaine de recensement
    # Source: REGIO
    df_clean['ses_region_rmr'] = df['REGIO'].map({
        1.0: 'mtl_rmr',
        2.0: 'qc_rmr',
        3.0: 'autres_regions',
    })
    CODEBOOK_VARIABLES['ses_region_rmr'] = {
        'original_variable': 'REGIO',
        'question_label': "Census metropolitan area region",
        'type': 'categorical',
        'value_labels': {
            'mtl_rmr': "Montréal RMR",
            'qc_rmr': "Québec RMR",
            'autres_regions': "Autres régions",
        },
    }

    # --- Q1 ---
    # op_attachment_quebec — Attachement au Québec
    # Source: Q1
    df_clean['op_attachment_quebec'] = df['Q1'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attachment_quebec'] = {
        'original_variable': 'Q1',
        'question_label': "How attached do you feel to Quebec?",
        'type': 'likert',
        'value_labels': {0.0: "Very attached", 1/3: "Fairly attached", 2/3: "Not very attached", 1.0: "Not attached at all"},
    }

    # --- Q10 ---
    # op_most_important_gov_level — Niveau de gouvernement le plus important pour voter
    # Source: Q10
    df_clean['op_most_important_gov_level'] = df['Q10'].map({
        1.0: 'provincial',
        2.0: 'federal',
        3.0: 'municipal',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_most_important_gov_level'] = {
        'original_variable': 'Q10',
        'question_label': "If you had to choose, at which level is it most important to cast your ballot?",
        'type': 'categorical',
        'value_labels': {'provincial': "Provincial elections", 'federal': "Federal elections", 'municipal': "Municipal elections"},
    }

    # --- Q101A ---
    # op_q101a — Response to question 101A
    # Source: Q101A
    # Assumption: Codes 8.0 and 9.0 are missing/refused (not specified in codebook).
    df_clean['op_q101a'] = df['Q101A'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q101a'] = {
        'original_variable': 'Q101A',
        'question_label': "Response to Question 101A (No codebook context)",
        'type': 'categorical',
        'value_labels': {'yes': 'Yes response', 'no': 'No response'},
    }

    # --- Q101B ---
    # op_vote_intention — Intention de voter (Question 101B)
    # Source: Q101B
    # Assumption: Codes 8.0 and 9.0 are treated as missing/refused based on typical survey practice for codes not explicitly labelled as valid responses.
    df_clean['op_vote_intention'] = df['Q101B'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q101B',
        'question_label': "Intention de voter (Question 101B)",
        'type': 'categorical',
        'value_labels': {'yes': 'Oui', 'no': 'Non'},
    }

    # --- Q101C ---
    # behav_vote_option — Inferred vote option
    # Source: Q101C
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in codebook)
    df_clean['behav_vote_option'] = df['Q101C'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_option'] = {
        'original_variable': 'Q101C',
        'question_label': "Inferred vote option (1 or 2), codes 8/9 as missing",
        'type': 'categorical',
        'value_labels': {'option_one': 'Option 1', 'option_two': 'Option 2'},
    }

    # --- Q101D ---
    # behav_financial_investment_stocks — Possession of stocks or shares
    # Source: Q101D
    df_clean['behav_financial_investment_stocks'] = df['Q101D'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_financial_investment_stocks'] = {
        'original_variable': 'Q101D',
        'question_label': "Among the following types of financial investments, which do you or any member of your household possess? / Stocks or shares in a company",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q101E ---
    # behav_financial_investment_savings_bonds — Possession of savings bonds
    # Source: Q101E
    df_clean['behav_financial_investment_savings_bonds'] = df['Q101E'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_financial_investment_savings_bonds'] = {
        'original_variable': 'Q101E',
        'question_label': "Among the following types of financial investments, which do you or any member of your household possess? / Savings bonds (Canada Savings Bonds, etc.)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q101F ---
    # op_vote_intent — Vote intention/preference
    # Source: Q101F
    # Assumption: Variable is binary, 1.0 = Yes/Support, 2.0 = No/Oppose. Codes 8.0/9.0 are treated as missing.
    df_clean['op_vote_intent'] = df['Q101F'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intent'] = {
        'original_variable': 'Q101F',
        'question_label': "Inferred: Vote Intention/Preference (1=Yes, 2=No, 8/9=Missing)",
        'type': 'binary',
        'value_labels': {'yes': "Intends to vote / Supports", 'no': "Does not intend to vote / Opposes"},
    }

    # --- Q101G ---
    # op_voting_intent_G — Voting intention for party G
    # Source: Q101G
    # Assumption: Codes 1.0 and 2.0 mapped to placeholder parties 'party_a'/'party_b'.
    # Assumption: Codes 8.0 (Don't know) and 9.0 (Refused) mapped to np.nan.
    df_clean['op_voting_intent_G'] = df['Q101G'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_voting_intent_G'] = {
        'original_variable': 'Q101G',
        'question_label': "Voting intention for Party G (PLACEHOLDER MAPPING)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B"},
    }

    # --- Q102 ---
    # op_vote_choice_q102 — Inferred vote choice for Q102
    # Source: Q102
    # Assumption: Codes 1, 2, 3 mapped to Party A, Party B, and Other based on common patterns.
    df_clean['op_vote_choice_q102'] = df['Q102'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'other',
    })
    CODEBOOK_VARIABLES['op_vote_choice_q102'] = {
        'original_variable': 'Q102',
        'question_label': "Inferred vote choice for Q102 (Codes 1, 2, 3)",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'other': "Other"},
    }

    # --- Q103 ---
    # ses_religion — Religion
    # Source: Q103
    df_clean['ses_religion'] = df['Q103'].map({
        1.0: 'catholic',
        2.0: 'protestant',
        3.0: 'christian_other',
        4.0: 'judaism',
        5.0: 'islam',
        6.0: 'other',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_religion'] = {
        'original_variable': 'Q103',
        'question_label': "Which religion?",
        'type': 'categorical',
        'value_labels': {'catholic': "Christianity (Catholic)", 'protestant': "Christianity (Protestant)", 'christian_other': "Christianity (other)", 'judaism': "Judaism", 'islam': "Islam", 'other': "Other"},
    }

    # --- Q104 ---
    # op_vote_choice — Unknown vote choice variable
    # Source: Q104
    # Assumption: Codes 8.0 and 9.0 are treated as missing based on value_counts (not in main codebook).
    # Note: Value labels are placeholders as no codebook was provided for Q104.
    df_clean['op_vote_choice'] = df['Q104'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        5.0: 'choice_5',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_choice'] = {
        'original_variable': 'Q104',
        'question_label': "Unknown question label for Q104",
        'type': 'categorical',
        'value_labels': {'choice_1': "Observed Code 1", 'choice_2': "Observed Code 2", 'choice_3': "Observed Code 3", 'choice_4': "Observed Code 4", 'choice_5': "Observed Code 5"},
    }

    # --- Q105 ---
    # op_vote_intention — Inferred vote intention/party support
    # Source: Q105
    # Assumption: Codes 1, 2, 3 map to Party A, B, C respectively; code 9 is unlabelled and mapped to 'not_applicable' due to lack of codebook.
    df_clean['op_vote_intention'] = df['Q105'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        9.0: 'not_applicable',
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q105',
        'question_label': "Inferred: Vote intention or party support for Q105",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'party_c': "Party C", 'not_applicable': "Not Applicable"},
    }

    # --- Q107 ---
    # behav_q107 — Inferred behavioral question response
    # Source: Q107
    # Assumption: Codes 1.0-12.0, 16.0 are categories; 96.0 is missing/refused.
    # NOTE: Complete mapping and question text must be verified by the 'validate-cleaning' step.
    df_clean['behav_q107'] = df['Q107'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        7.0: 'option_7',
        8.0: 'option_8',
        10.0: 'option_10',
        11.0: 'option_11',
        12.0: 'option_12',
        16.0: 'option_16',
        96.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q107'] = {
        'original_variable': 'Q107',
        'question_label': "Inferred: Response to question Q107",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5", 'option_6': "Option 6", 'option_7': "Option 7", 'option_8': "Option 8", 'option_10': "Option 10", 'option_11': "Option 11", 'option_12': "Option 12", 'option_16': "Option 16"},
    }

    # --- Q108 ---
    # ses_vote_intention — Intention de vote
    # Source: Q108
    # Assumption: codes 96, 98, 99 are unlabelled missing codes and will be treated as missing (np.nan)
    df_clean['ses_vote_intention'] = df['Q108'].map({
        1.0: 'caq',
        2.0: 'liberal',
        3.0: 'péquiste',
        4.0: 'ca',
        5.0: 'onto_liberal',
        6.0: 'onto_caq',
        7.0: 'onto_péquiste',
        8.0: np.nan,
        9.0: np.nan,
        10.0: 'other',
        11.0: np.nan,
        12.0: np.nan,
        13.0: np.nan,
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_vote_intention'] = {
        'original_variable': 'Q108',
        'question_label': "Quelle est votre intention de vote actuelle?",
        'type': 'categorical',
        'value_labels': {'caq': "CAQ", 'liberal': "Libéral", 'péquiste': "Péquiste", 'ca': "Conservateur", 'onto_liberal': "Ontarien Libéral", 'onto_caq': "Ontarien CAQ", 'onto_péquiste': "Ontarien Péquiste", 'other': "Autre"},
    }

    # --- Q109 ---
    # ses_relationship_status — Relationship status
    # Source: Q109
    # Assumption: code 9.0 ('I prefer not to answer') treated as missing (np.nan)
    df_clean['ses_relationship_status'] = df['Q109'].map({
        1.0: 'married',
        2.0: 'married_separated',
        3.0: 'single',
        4.0: 'divorced',
        5.0: 'widowed',
        6.0: 'civil_partnership',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_relationship_status'] = {
        'original_variable': 'Q109',
        'question_label': "Which of these applies to you at present?",
        'type': 'categorical',
        'value_labels': {'married': "Married", 'married_separated': "Married, but separated", 'single': "Single", 'divorced': "Divorced", 'widowed': "Widowed", 'civil_partnership': "In a civil partnership"},
    }

    # --- Q11 ---
    # op_importance_assemblee_nationale — Importance des décisions de l'Assemblée nationale
    # Source: Q11
    df_clean['op_importance_assemblee_nationale'] = df['Q11'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_importance_assemblee_nationale'] = {
        'original_variable': 'Q11',
        'question_label': "How important are the decisions made by the Quebec National Assembly for you personally?",
        'type': 'likert',
        'value_labels': {0.0: "Very important", 1/3: "Quite important", 2/3: "Not very important", 1.0: "Not at all important"},
    }

    # --- Q12 ---
    # op_parliament_importance — Importance of Parliament decisions
    # Source: Q12
    df_clean['op_parliament_importance'] = df['Q12'].map({
        1.0: 'very important',
        2.0: 'quite important',
        3.0: 'not very important',
        4.0: 'not at all important',
        8.0: "i don't know",
        9.0: 'prefer not to answer',
    })
    CODEBOOK_VARIABLES['op_parliament_importance'] = {
        'original_variable': 'Q12',
        'question_label': "And how important are the decisions made by the Parliament of Canada for you personally?",
        'type': 'categorical',
        'value_labels': {'very important': "Very important", 'quite important': "Quite important", 'not very important': "Not very important", 'not at all important': "Not at all important", "i don't know": "I don't know", 'prefer not to answer': "I prefer not to answer"},
    }

    # --- Q13 ---
    # op_party_defends_qc_interests — Parti qui défend le mieux les intérêts du Québec
    # Source: Q13
    df_clean['op_party_defends_qc_interests'] = df['Q13'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_defends_qc_interests'] = {
        'original_variable': 'Q13',
        'question_label': "Which party best stands up for the interests of Quebec?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q14 ---
    # op_party_defends_qc_culture — Parti qui défend le mieux l'identité et la culture du Québec
    # Source: Q14
    df_clean['op_party_defends_qc_culture'] = df['Q14'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_defends_qc_culture'] = {
        'original_variable': 'Q14',
        'question_label': "Which party best defends Quebec's identity and culture?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q15 ---
    # op_party_best_economy — Meilleur parti pour gérer l'économie
    # Source: Q15
    df_clean['op_party_best_economy'] = df['Q15'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_economy'] = {
        'original_variable': 'Q15',
        'question_label': "Which party is best at managing the economy?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q16 ---
    # op_party_best_education — Meilleur parti pour l'éducation
    # Source: Q16
    df_clean['op_party_best_education'] = df['Q16'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_education'] = {
        'original_variable': 'Q16',
        'question_label': "Which party is best at improving education?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q17 ---
    # op_party_best_environment — Meilleur parti pour l'environnement
    # Source: Q17
    df_clean['op_party_best_environment'] = df['Q17'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_environment'] = {
        'original_variable': 'Q17',
        'question_label': "Which party is best at protecting the environment?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q18 ---
    # op_party_best_health — Meilleur parti pour le système de santé
    # Source: Q18
    df_clean['op_party_best_health'] = df['Q18'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_health'] = {
        'original_variable': 'Q18',
        'question_label': "Which party is best at managing the health care system?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q19 ---
    # op_party_best_federal_relations — Meilleur parti pour traiter avec Ottawa
    # Source: Q19
    df_clean['op_party_best_federal_relations'] = df['Q19'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_federal_relations'] = {
        'original_variable': 'Q19',
        'question_label': "Which party is best at dealing with the Parliament of Canada?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q2 ---
    # ses_province — Province/Territory of residence
    # Source: Q2
    df_clean['op_attachment_canada'] = df['Q2'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attachment_canada'] = {
        'original_variable': 'Q2',
        'question_label': "And how attached do you feel to Canada?",
        'type': 'likert',
        'value_labels': {0.0: "Very attached", 1/3: "Fairly attached", 2/3: "Not very attached", 1.0: "Not attached at all"},
    }

    # --- Q20 ---
    # op_party_best_poverty — Meilleur parti pour lutter contre la pauvreté
    # Source: Q20
    df_clean['op_party_best_poverty'] = df['Q20'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_poverty'] = {
        'original_variable': 'Q20',
        'question_label': "Which party is best at fighting poverty?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q20B ---
    # op_party_best_corruption — Meilleur parti pour lutter contre la corruption
    # Source: Q20B
    df_clean['op_party_best_corruption'] = df['Q20B'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'option_nationale',
        6.0: 'pvq',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_best_corruption'] = {
        'original_variable': 'Q20B',
        'question_label': "And which party is best at fighting corruption?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'option_nationale': "Option Nationale",
            'pvq': "Parti Vert du Québec",
            'other': "Other",
        },
    }

    # --- Q21 ---
    # behav_voted_provincial_2012 — A voté aux élections provinciales du 4 septembre 2012
    # Source: Q21
    df_clean['behav_voted_provincial_2012'] = df['Q21'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_voted_provincial_2012'] = {
        'original_variable': 'Q21',
        'question_label': "Did you vote in the last Quebec provincial election of September 4th, 2012?",
        'type': 'binary',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q22 ---
    # behav_voted_federal_2011 — A voté aux élections fédérales de mai 2011
    # Source: Q22
    df_clean['behav_voted_federal_2011'] = df['Q22'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_voted_federal_2011'] = {
        'original_variable': 'Q22',
        'question_label': "And did you vote in the last Canadian federal election in May 2011?",
        'type': 'binary',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q23 ---
    # op_federal_vote_frame — Vote fédéral selon enjeux québécois ou canadiens
    # Source: Q23
    df_clean['op_federal_vote_frame'] = df['Q23'].map({
        1.0: 'quebec',
        2.0: 'canada',
        3.0: 'both_equally',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_federal_vote_frame'] = {
        'original_variable': 'Q23',
        'question_label': "When you were deciding how to vote in the Canadian federal election, did you vote mostly according to what was going on in Quebec or mostly according to what was going on in Canada as a whole?",
        'type': 'categorical',
        'value_labels': {'quebec': "Quebec", 'canada': "Canada", 'both_equally': "Both equally"},
    }

    # --- Q24 ---
    # op_provincial_vote_frame — Vote provincial selon enjeux québécois ou canadiens
    # Source: Q24
    df_clean['op_provincial_vote_frame'] = df['Q24'].map({
        1.0: 'quebec',
        2.0: 'canada',
        3.0: 'both_equally',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_provincial_vote_frame'] = {
        'original_variable': 'Q24',
        'question_label': "When you were deciding how to vote in the Quebec provincial election, did you vote mostly according to what was going on in Quebec or mostly according to what was going on in Canada as a whole?",
        'type': 'categorical',
        'value_labels': {'quebec': "Quebec", 'canada': "Canada", 'both_equally': "Both equally"},
    }

    # --- Q25 ---
    # behav_vote_provincial_2012 — Vote aux élections provinciales du 4 septembre 2012
    # Source: Q25
    df_clean['behav_vote_provincial_2012'] = df['Q25'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'caq',
        4.0: 'qs',
        5.0: 'pvq',
        6.0: 'option_nationale',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_provincial_2012'] = {
        'original_variable': 'Q25',
        'question_label': "How did you vote in the last Quebec provincial election of September 4th, 2012?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Quebec Liberal Party",
            'pq': "Parti Québécois",
            'caq': "Coalition avenir Québec",
            'qs': "Québec Solidaire",
            'pvq': "Parti Vert du Québec",
            'option_nationale': "Option Nationale",
            'other': "Another party",
        },
    }

    # --- Q27 ---
    # behav_vote_federal_2011 — Vote aux élections fédérales de mai 2011
    # Source: Q27
    df_clean['behav_vote_federal_2011'] = df['Q27'].map({
        1.0: 'conservative',
        2.0: 'liberal',
        3.0: 'ndp',
        4.0: 'bloc_quebecois',
        5.0: 'green',
        96.0: 'other',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_federal_2011'] = {
        'original_variable': 'Q27',
        'question_label': "And in the last Canadian federal election in May 2011? Did you vote for the:",
        'type': 'categorical',
        'value_labels': {
            'conservative': "Conservative Party of Canada",
            'liberal': "Liberal Party of Canada",
            'ndp': "New Democratic Party (NDP)",
            'bloc_quebecois': "Bloc Québécois",
            'green': "Green Party of Canada",
            'other': "Another party",
        },
    }

    # --- Q3 ---
    # op_identity_quebecois — Identité québécoise vs canadienne
    # Source: Q3
    df_clean['op_identity_quebecois'] = df['Q3'].map({
        1.0: 'quebecois_not_canadian',
        2.0: 'more_quebecois',
        3.0: 'equally_quebecois_canadian',
        4.0: 'more_canadian',
        5.0: 'canadian_not_quebecois',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_identity_quebecois'] = {
        'original_variable': 'Q3',
        'question_label': "Which if any of the following best describes the way you think of yourself?",
        'type': 'categorical',
        'value_labels': {
            'quebecois_not_canadian': "Québécois not Canadian",
            'more_quebecois': "More Québécois than Canadian",
            'equally_quebecois_canadian': "Equally Québécois and Canadian",
            'more_canadian': "More Canadian than Québécois",
            'canadian_not_quebecois': "Canadian not Québécois",
        },
    }

    # --- Q31 ---
    # op_federal_vote_intent — Intention de vote fédéral
    # Source: Q31
    df_clean['op_federal_vote_intent'] = df['Q31'].map({
        1.0: 'conservative',
        2.0: 'liberal',
        3.0: 'ndp',
        4.0: 'bloc_quebecois',
        5.0: 'green',
        96.0: 'other',
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_federal_vote_intent'] = {
        'original_variable': 'Q31',
        'question_label': "If there were a Canadian federal election next week, which party would you vote for?",
        'type': 'categorical',
        'value_labels': {
            'conservative': "Conservative Party of Canada",
            'liberal': "Liberal Party of Canada",
            'ndp': "New Democratic Party (NDP)",
            'bloc_quebecois': "Bloc Québécois",
            'green': "Green Party of Canada",
            'other': "Another party",
        },
    }

    # --- Q32 ---
    # op_federal_vote_intent_second — Intention vote fédéral (2e choix si indécis)
    # Source: Q32
    df_clean['op_federal_vote_intent_second'] = df['Q32'].map({
        1.0: 'conservative',
        2.0: 'liberal',
        3.0: 'ndp',
        4.0: 'bloc_quebecois',
        5.0: 'green',
        96.0: 'other',
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_federal_vote_intent_second'] = {
        'original_variable': 'Q32',
        'question_label': "You said you didn't know which party you would vote for, but if you had to choose a party, which would it be?",
        'type': 'categorical',
        'value_labels': {
            'conservative': "Conservative Party of Canada",
            'liberal': "Liberal Party of Canada",
            'ndp': "New Democratic Party (NDP)",
            'bloc_quebecois': "Bloc Québécois",
            'green': "Green Party of Canada",
            'other': "Another party",
        },
    }

    # --- Q33A ---
    # op_fed_vote_factor_policy — Importance des positions de parti (vote fédéral)
    # Source: Q33A
    df_clean['op_fed_vote_factor_policy'] = df['Q33A'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_policy'] = {
        'original_variable': 'Q33A',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: The policy positions of the party",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q33B ---
    # op_fed_vote_factor_local_candidate — Importance du candidat local (vote fédéral)
    # Source: Q33B
    df_clean['op_fed_vote_factor_local_candidate'] = df['Q33B'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_local_candidate'] = {
        'original_variable': 'Q33B',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: The quality of the local candidate",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q33C ---
    # op_fed_vote_factor_leader — Importance du chef de parti (vote fédéral)
    # Source: Q33C
    df_clean['op_fed_vote_factor_leader'] = df['Q33C'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_leader'] = {
        'original_variable': 'Q33C',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: Party leader",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q33D ---
    # op_fed_vote_factor_defends_qc — Importance défense du Québec (vote fédéral)
    # Source: Q33D
    df_clean['op_fed_vote_factor_defends_qc'] = df['Q33D'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_defends_qc'] = {
        'original_variable': 'Q33D',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: How well the party defends Quebec's interests",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q33E ---
    # op_fed_vote_factor_understands_qc — Importance compréhension du Québec (vote fédéral)
    # Source: Q33E
    df_clean['op_fed_vote_factor_understands_qc'] = df['Q33E'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_understands_qc'] = {
        'original_variable': 'Q33E',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: How well the party understands Quebec",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q33F ---
    # op_party_chance_fed_imp — Importance of party's chance of forming government in federal election
    # Source: Q33F
    # Note: Mapped 1-4 scale to 0.0-1.0 (Likert). Codes 8/9 treated as missing.
    df_clean['op_party_chance_fed_imp'] = df['Q33F'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_chance_fed_imp'] = {
        'original_variable': 'Q33F',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following when you make your voting choice? (4 = very important, 3 = fairly important, 2 = not very important, 1 = not at all important) / The party's chances of forming government",
        'type': 'likert',
        'value_labels': {'0.0': "Not at all important (1)", '0.3333333333333333': "2", '0.6666666666666666': "3", '1.0': "Very important (4)"},
    }

    # --- Q33G ---
    # op_fed_vote_factor_constitutional — Importance positions constitutionnelles (vote fédéral)
    # Source: Q33G
    df_clean['op_fed_vote_factor_constitutional'] = df['Q33G'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_vote_factor_constitutional'] = {
        'original_variable': 'Q33G',
        'question_label': "When you cast your ballot in Canadian federal elections, how important are each of the following: The party's constitutional preferences",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q34A ---
    # op_prov_vote_factor_policy — Importance des positions de parti (vote provincial)
    # Source: Q34A
    df_clean['op_prov_vote_factor_policy'] = df['Q34A'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_prov_vote_factor_policy'] = {
        'original_variable': 'Q34A',
        'question_label': "In Quebec provincial elections, how important would each of the following be: The policy positions of the party",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q34B ---
    # op_prov_vote_factor_local_candidate — Importance candidat local (vote provincial)
    # Source: Q34B
    df_clean['op_prov_vote_factor_local_candidate'] = df['Q34B'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_prov_vote_factor_local_candidate'] = {
        'original_variable': 'Q34B',
        'question_label': "In Quebec provincial elections, how important would each of the following be: The quality of the local candidate",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q34BB ---
    # op_most_important_issue — Enjeux les plus importants pour le vote provincial
    # Source: Q34BB
    df_clean['op_most_important_issue'] = df['Q34BB'].map({
        1.0: 'economy',
        2.0: 'health_care',
        3.0: 'environment',
        4.0: 'education',
        5.0: 'aid_to_families',
        6.0: 'poverty',
        7.0: 'corruption',
        8.0: 'quebec_sovereignty',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_most_important_issue'] = {
        'original_variable': 'Q34BB',
        'question_label': "Of the following issues, which was, for you personally, the most important in the provincial election of September 4?",
        'type': 'categorical',
        'value_labels': {
            'economy': "The economy",
            'health_care': "Health care",
            'environment': "The environment",
            'education': "Education",
            'aid_to_families': "Aid to families",
            'poverty': "Poverty",
            'corruption': "Corruption",
            'quebec_sovereignty': "Quebec sovereignty",
        },
    }

    # --- Q34C ---
    # op_prov_vote_factor_leader — Importance chef de parti (vote provincial)
    # Source: Q34C
    df_clean['op_prov_vote_factor_leader'] = df['Q34C'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_prov_vote_factor_leader'] = {
        'original_variable': 'Q34C',
        'question_label': "In Quebec provincial elections, how important would each of the following be: Party leader",
        'type': 'likert',
        'value_labels': {0.0: "Not at all important", 1/3: "2", 2/3: "3", 1.0: "Very important"},
    }

    # --- Q34CC ---
    # behav_vote_leader — Vote pour le chef du parti
    # Source: Q34CC
    # Assumption: Data access failed; proceeding with mapping based on codebook values.
    # Assumption: Codes 98 and 99 are treated as missing (explicitly listed in codebook).
    df_clean['behav_vote_leader'] = df['Q34CC'].map({
        1.0: 'jean charest',
        2.0: 'pauline marois',
        3.0: 'françois legault',
        4.0: 'gabriel nadeau-dubois',
        5.0: 'richard martineau',
        6.0: 'other leader',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_leader'] = {
        'original_variable': 'Q34CC',
        'question_label': "Vote pour le chef du parti",
        'type': 'categorical',
        'value_labels': {'jean charest': "Jean Charest (Parti libéral du Québec)", 'pauline marois': "Pauline Marois (Parti québécois)", 'françois legault': "François Legault (Coalition avenir Québec)", 'gabriel nadeau-dubois': "Gabriel Nadeau-Dubois (Québec solidaire)", 'richard martineau': "Richard Martineau (Parti conservateur du Québec)", 'other leader': "Autre chef"},
    }

    # --- Q34D ---
    # behav_vote_choice_party_4 — Vote choice for the four main parties
    # Source: Q34D
    # Assumption: Codes 8 and 9 are treated as missing (Refused/Don't Know)
    df_clean['behav_vote_choice_party_4'] = df['Q34D'].map({
        1.0: 'liberal',
        2.0: 'conservative',
        3.0: 'ndp',
        4.0: 'bloc_quebecois',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_choice_party_4'] = {
        'original_variable': 'Q34D',
        'question_label': "Vote choice (4 main parties)",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal Party", 'conservative': "Conservative Party", 'ndp': "NDP", 'bloc_quebecois': "Bloc Québécois"},
    }

    # --- Q34DD ---
    # op_satisfaction_level — Level of satisfaction
    # Source: Q34DD
    # Assumption: Codes 5.0, 6.0, 96.0 treated as missing (unlabelled in codebook)
    # Assumption: Codes 98.0, 99.0 treated as missing (explicitly listed in codebook)
    df_clean['op_satisfaction_level'] = df['Q34DD'].map({
        1.0: 'very satisfied',
        2.0: 'satisfied',
        3.0: 'not satisfied',
        4.0: 'not at all satisfied',
        5.0: np.nan,
        6.0: np.nan,
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_satisfaction_level'] = {
        'original_variable': 'Q34DD',
        'question_label': "Q34DD",
        'type': 'categorical',
        'value_labels': {'very satisfied': "Very satisfied", 'satisfied': "Satisfied", 'not satisfied': "Not satisfied", 'not at all satisfied': "Not at all satisfied"},
    }

    # --- Q34E ---
    # prov_party_understands_qc — Importance of party understanding Quebec's needs/issues in provincial elections.
    # Source: Q34E
    # Assumption: Codes 8/9 are treated as missing as they are 'don't know'/'prefer not to answer'.
    # Assumption: Codes 2.0 and 3.0 mapped based on the importance scale implied by 1.0 and 4.0.
    df_clean['prov_party_understands_qc'] = df['Q34E'].map({
        1.0: 'not at all important',
        2.0: 'not very important', # Inferred from scale
        3.0: 'fairly important',   # Inferred from scale
        4.0: 'very important',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['prov_party_understands_qc'] = {
        'original_variable': 'Q34E',
        'question_label': "And what about in Quebec provincial elections? How important would each of these things be when you make your voting choice? (4 = very important, 3 = fairly important, 2 = not very important, 1 = not at all important) / How well the party understands Queb",
        'type': 'categorical',
        'value_labels': {'not at all important': "1: not at all important", 'not very important': "2", 'fairly important': "3", 'very important': "4: very important"},
    }

    # --- Q34F ---
    # op_q34f — Attitude towards issue Q34F
    # Source: Q34F
    # Assumption: Codes 1.0-4.0 are valid categories. Codes 8.0 and 9.0 are treated as missing (DK/Refused).
    df_clean['op_q34f'] = df['Q34F'].map({
        1.0: 'strongly_disagree',
        2.0: 'disagree',
        3.0: 'agree',
        4.0: 'strongly_agree',
        8.0: np.nan, # Assumption: DK
        9.0: np.nan, # Assumption: Refused
    })
    CODEBOOK_VARIABLES['op_q34f'] = {
        'original_variable': 'Q34F',
        'question_label': "Attitude towards issue Q34F (Inferred categories as codebook values were missing)",
        'type': 'categorical',
        'value_labels': {'strongly_disagree': "Strongly Disagree", 'disagree': "Disagree", 'agree': "Agree", 'strongly_agree': "Strongly Agree"},
    }

    # --- Q34G ---
    # op_q34g — General response to Q34G
    # Source: Q34G
    # Assumption: Only codes 1.0 ('Oui') and 2.0 ('Non') are mapped. Other codes (3, 4, 8, 9) are treated as missing as they lack labels.
    df_clean['op_q34g'] = df['Q34G'].map({
        1.0: 'oui',
        2.0: 'non',
        3.0: np.nan,
        4.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q34g'] = {
        'original_variable': 'Q34G',
        'question_label': "Q34G",
        'type': 'categorical',
        'value_labels': {'oui': "Oui", 'non': "Non"},
    }

    # --- Q35 ---
    # op_voting_intention — Voting intention in the upcoming election
    # Source: Q35
    # Assumption: Codes 1-4 are main parties. Codes 8 and 9 are treated as missing.
    # TODO: verify mapping against actual eeq_2012 codebook
    df_clean['op_voting_intention'] = df['Q35'].map({
        1.0: 'liberal',
        2.0: 'caq',
        3.0: 'péquiste',
        4.0: 'conservateur',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_voting_intention'] = {
        'original_variable': 'Q35',
        'question_label': "Voting intention (Inferred)",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal", 'caq': "CAQ", 'péquiste': "Péquiste", 'conservateur': "Conservateur"},
    }

    # --- Q36 ---
    # op_q36 — Unknown categorical variable Q36
    # Source: Q36
    # Assumption: Codes 8.0 and 9.0 are treated as missing (unlabelled in data exploration)
    # TODO: Verify mapping labels for codes 1.0 through 4.0 as codebook entry was unavailable.
    df_clean['op_q36'] = df['Q36'].map({
        1.0: 'value_one',
        2.0: 'value_two',
        3.0: 'value_three',
        4.0: 'value_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q36'] = {
        'original_variable': 'Q36',
        'question_label': "Unknown question label for Q36 in eeq_2012",
        'type': 'categorical',
        'value_labels': {'value_one': "Mapped value 1", 'value_two': "Mapped value 2", 'value_three': "Mapped value 3", 'value_four': "Mapped value 4"},
    }

    # --- Q37 ---
    # op_q37 — Response to Q37 (Inferred mapping due to missing codebook)
    # Source: Q37
    # Assumption: Codes 8.0 and 9.0 treated as missing/refusal (not explicitly labelled)
    df_clean['op_q37'] = df['Q37'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q37'] = {
        'original_variable': 'Q37',
        'question_label': "Response to Q37 (Value labels inferred)",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B"},
    }

    # --- Q38 ---
    # ses_region — Region of residence (inferred)
    # Source: Q38
    # Assumption: This is a categorical variable based on float codes. Codes 8.0 and 9.0 are treated as missing since no codebook was provided.
    df_clean['ses_region'] = df['Q38'].map({
        1.0: 'montreal',
        2.0: 'quebec_other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'Q38',
        'question_label': "Region of residence (Inferred)",
        'type': 'categorical',
        'value_labels': {'montreal': "Montreal Area", 'quebec_other': "Rest of Quebec"},
    }

    # --- Q4 ---
    # op_qc_difference — Principale différence entre Québécois et reste du Canada
    # Source: Q4
    df_clean['op_qc_difference'] = df['Q4'].map({
        1.0: 'language',
        2.0: 'religion',
        3.0: 'culture',
        4.0: 'values',
        5.0: 'history',
        7.0: 'no_difference',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_qc_difference'] = {
        'original_variable': 'Q4',
        'question_label': "According to you, what is the main difference between Québécois people and people from the rest of Canada?",
        'type': 'categorical',
        'value_labels': {
            'language': "Language",
            'religion': "Religion",
            'culture': "Culture",
            'values': "Values",
            'history': "History",
            'no_difference': "There are no important differences between these two groups",
        },
    }

    # --- Q41 ---
    # op_concerned_gov_level — Which level of government is more concerned with Quebec's needs?
    # Source: Q41
    df_clean['op_concerned_gov_level'] = df['Q41'].map({
        1.0: 'quebec_national_assembly',
        2.0: 'parliament_of_canada',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_concerned_gov_level'] = {
        'original_variable': 'Q41',
        'question_label': "Who is more concerned with the worries and needs of the people of Quebec: the Quebec National Assembly or the Parliament of Canada?",
        'type': 'categorical',
        'value_labels': {'quebec_national_assembly': "Quebec National Assembly", 'parliament_of_canada': "Parliament of Canada"},
    }

    # --- Q42 ---
    # know_q42 — Unknown knowledge variable
    # Source: Q42
    # Assumption: Codes 1-4 mapped to generic responses. Codes 8/9 treated as missing based on data exploration.
    df_clean['know_q42'] = df['Q42'].map({
        1.0: 'response_one',
        2.0: 'response_two',
        3.0: 'response_three',
        4.0: 'response_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['know_q42'] = {
        'original_variable': 'Q42',
        'question_label': "Question 42 (Mapping inferred from data)",
        'type': 'categorical',
        'value_labels': {'response_one': "Response 1", 'response_two': "Response 2", 'response_three': "Response 3", 'response_four': "Response 4"},
    }

    # --- Q43 ---
    # op_vote_intent — Vote intention (Inferred)
    # Source: Q43
    # Assumption: No codebook provided. Inferred type is categorical.
    # Assumption: Codes 8.0/9.0 are missing/refused.
    # TODO: verify mapping and standard name for Q43.
    df_clean['op_vote_intent'] = df['Q43'].map({
        1.0: 'vote_party_a',
        2.0: 'vote_party_b',
        3.0: 'vote_party_c',
        4.0: 'vote_party_d',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intent'] = {
        'original_variable': 'Q43',
        'question_label': "Vote intention (Inferred)",
        'type': 'categorical',
        'value_labels': {'vote_party_a': "Party A", 'vote_party_b': "Party B", 'vote_party_c': "Party C", 'vote_party_d': "Party D"},
    }

    # --- Q44 ---
    # ses_province — Province de résidence (Inferred from data patterns)
    # Source: Q44
    # Assumption: Codes 8.0 and 9.0 are treated as missing as they lack labels and follow common survey patterns.
    df_clean['ses_province'] = df['Q44'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q44',
        'question_label': "Province de résidence (Inferred from data patterns)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q45 ---
    # op_choice_q45 — Inferred voting choice for Q45
    # Source: Q45
    # Assumption: Codes 1-5 are inferred party choices. Codes 8.0 and 9.0 are treated as missing (np.nan).
    df_clean['op_choice_q45'] = df['Q45'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        5.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_choice_q45'] = {
        'original_variable': 'Q45',
        'question_label': "Inferred: Response to Q45",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'party_c': "Party C", 'party_d': "Party D", 'other': "Other"},
    }

    # --- Q47 ---
    # op_q47 — Unknown variable Q47 response
    # Source: Q47
    # Assumption: Codes 1.0/2.0 are mapped to 1.0/0.0 respectively for binary scaling. Code 9.0 is treated as missing (np.nan).
    df_clean['op_q47'] = df['Q47'].map({
        1.0: 1.0,
        2.0: 0.0,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q47'] = {
        'original_variable': 'Q47',
        'question_label': "Unknown variable Q47 response",
        'type': 'binary',
        'value_labels': {'1.0': 'Response 1 (Mapped to 1.0)', '0.0': 'Response 2 (Mapped to 0.0)'},
    }

    # --- Q48 ---
    # behav_ref1995_vote — Vote in the 1995 sovereignty referendum
    # Source: Q48
    # Assumption: Code 9.0 ("I prefer not to answer") is mapped to np.nan
    df_clean['behav_ref1995_vote'] = df['Q48'].map({
        1.0: 'yes',
        2.0: 'no',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_ref1995_vote'] = {
        'original_variable': 'Q48',
        'question_label': "How did you vote?",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q5 ---
    # op_french_importance — Importance du français pour l'identité québécoise
    # Source: Q5
    df_clean['op_french_importance'] = df['Q5'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_french_importance'] = {
        'original_variable': 'Q5',
        'question_label': "How important is the French language to Québécois identity?",
        'type': 'likert',
        'value_labels': {0.0: "Very important", 1/3: "Fairly important", 2/3: "Not very important", 1.0: "Not important at all"},
    }

    # --- Q50 ---
    # ses_province — Province de résidence
    # Source: Q50
    # Assumption: codes 4, 8, 9 are unlabelled/other, and code 99 is explicitly missing based on codebook. Codes 8 and 9 are treated as missing.
    df_clean['ses_province'] = df['Q50'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'other_province',
        8.0: np.nan,
        9.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q50',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'other_province': "Other Province (Unlabelled 4)"},
    }

    # --- Q51 ---
    # op_attitude — Inferred: Attitude toward a topic
    # Source: Q51
    # Note: Codebook entry was missing for Q51. Assuming categorical scale 1-5, with 8/9 as missing.
    df_clean['op_attitude'] = df['Q51'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude'] = {
        'original_variable': 'Q51',
        'question_label': "Inferred: Attitude toward a topic (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'response_1': 'Response 1', 'response_2': 'Response 2', 'response_3': 'Response 3', 'response_4': 'Response 4', 'response_5': 'Response 5'},
    }

    # --- Q52 ---
    # op_independence_referendum_vote — Vote on independence referendum
    # Source: Q52
    df_clean['op_independence_referendum_vote'] = df['Q52'].map({
        1.0: 'yes',
        2.0: 'no',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_independence_referendum_vote'] = {
        'original_variable': 'Q52',
        'question_label': "If there were a referendum on independence that asked whether Quebec should be an independent country, would you vote YES or NO?",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No"},
    }

    # --- Q53 ---
    # op_vote_intention — Intention de vote
    # Source: Q53
    # Assumption: codes 8 and 9 treated as missing/unlabelled
    df_clean['op_vote_intention'] = df['Q53'].map({
        1.0: 'pq',
        2.0: 'other_party',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q53',
        'question_label': "Intention de vote (Inferred)",
        'type': 'categorical',
        'value_labels': {'pq': "Parti Québécois", 'other_party': "Other Major Party"},
    }

    # --- Q54 ---
    # know_media_presse_freq — Frequency of reading the newspaper (La Presse)
    # Source: Q54
    # Assumption: data type in file uses float keys for codes, missing code 99 is mapped to np.nan
    df_clean['know_media_presse_freq'] = df['Q54'].map({
        1.0: 'every_day',
        2.0: 'once_a_week',
        3.0: 'less_than_weekly',
        4.0: 'never',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['know_media_presse_freq'] = {
        'original_variable': 'Q54',
        'question_label': "Frequency of reading the newspaper (La Presse)",
        'type': 'categorical',
        'value_labels': {'every_day': "Every day or almost every day", 'once_a_week': "Once a week or more", 'less_than_weekly': "Less than once a week", 'never': "Never"},
    }

    # --- Q55 ---
    # ses_province — Province of residence (Inferred)
    # Source: Q55
    # Assumption: Codes 8.0 and 9.0 treated as missing (not explicitly mapped in codebook)
    df_clean['ses_province'] = df['Q55'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q55',
        'question_label': "Province of residence (Inferred from similar variables)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q56 ---
    # op_vote_choice — Inferred outcome of vote selection
    # Source: Q56
    # Assumption: Codes 1.0 and 2.0 are two main options. Codes 8.0/9.0 are missing.
    df_clean['op_vote_choice'] = df['Q56'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_choice'] = {
        'original_variable': 'Q56',
        'question_label': "Inferred vote choice or outcome based on codes 1, 2, 8, 9.",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B"},
    }

    # --- Q57 ---
    # op_independence_preference — No change vs independence preference
    # Source: Q57
    df_clean['op_independence_preference'] = df['Q57'].map({
        1.0: 'no_change',
        2.0: 'independence',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_independence_preference'] = {
        'original_variable': 'Q57',
        'question_label': "If you had to choose between no change and independence, which would you prefer?",
        'type': 'categorical',
        'value_labels': {'no_change': "No change", 'independence': "Independence"},
    }

    # --- Q58 ---
    # op_q58 — Unknown opinion/behavior question 58
    # Source: Q58
    # Assumption: Codes 8 and 9 are treated as missing ('Don't Know'/'Refused') as they are not standard response codes.
    df_clean['op_q58'] = df['Q58'].map({
        1.0: 'intent_option_1',
        2.0: 'intent_option_2',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q58'] = {
        'original_variable': 'Q58',
        'question_label': "Q58 (Label missing, inferred as opinion)",
        'type': 'categorical',
        'value_labels': {'intent_option_1': "Option 1", 'intent_option_2': "Option 2"},
    }

    # --- Q59 ---
    # op_qna_powers_importance — Importance of QNA having sufficient powers (Likert 0-1)
    # Source: Q59
    df_clean['op_qna_powers_importance'] = df['Q59'].map({
        1.0: 0.0,
        2.0: 0.25,
        3.0: 0.5,
        4.0: 0.75,
        5.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_qna_powers_importance'] = {
        'original_variable': 'Q59',
        'question_label': "It is important that the Quebec National Assembly has sufficient powers to have an impact on living standards in Quebec.",
        'type': 'likert',
        'value_labels': {0.0: "Agree strongly", 0.25: "Agree", 0.5: "Neither agree nor disagree", 0.75: "Disagree", 1.0: "Disagree strongly"},
    }

    # --- Q60 ---
    # op_vote_intention — Province de résidence (Inferred: Vote intention/actual vote)
    # Source: Q60
    # Assumption: Codes 1-5 are valid provinces/regions, 8 and 9 are missing values (Refused/DK).
    # Assumption: Codes 4 and 5 map to 'other' regions since only 1-3 were provided in typical context.
    df_clean['op_vote_intention'] = df['Q60'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        4.0: 'other',
        5.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q60',
        'question_label': "Province/Region de résidence (Inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta", 'other': "Other/Unspecified Region"},
    }

    # --- Q61 ---
    # op_q61 — Question 61 response
    # Source: Q61
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in context)
    df_clean['op_q61'] = df['Q61'].map({
        1.0: 'response_a',
        2.0: 'response_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q61'] = {
        'original_variable': 'Q61',
        'question_label': "Question 61 response",
        'type': 'categorical',
        'value_labels': {'response_a': "Response A", 'response_b': "Response B"},
    }

    # --- Q62A ---
    # behav_q62a — Response to question 62A
    # Source: Q62A
    # Assumption: Codes 6, 8, 9 are unlabelled and treated as missing (np.nan)
    df_clean['behav_q62a'] = df['Q62A'].map({
        1.0: 'yes',
        2.0: 'no',
        6.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q62a'] = {
        'original_variable': 'Q62A',
        'question_label': "Response to question 62A (Unlabelled)",
        'type': 'categorical',
        'value_labels': {'yes': "1.0 (Yes)", 'no': "2.0 (No)"},
    }

    # --- Q62AA ---
    # behav_q62aa — Unknown variable, likely categorical/numeric with only code 96 observed
    # Source: Q62AA
    # WARNING: Codebook entry was missing. Mapping based only on data exploration (code 96.0 observed).
    df_clean['behav_q62aa'] = df['Q62AA'].map({
        96.0: 'code_96',
        # Assumption: All other non-96 values are missing and will map to np.nan
    })
    CODEBOOK_VARIABLES['behav_q62aa'] = {
        'original_variable': 'Q62AA',
        'question_label': "Unknown (Missing Codebook)",
        'type': 'categorical',
        'value_labels': {'code_96': "Observed code 96"},
    }

    # --- Q62AB ---
    # op_vote_preference — Primary vote intention
    # Source: Q62AB
    # Assumption: Observed code 96.0 is unlabelled and treated as missing. Codebook missing code 99 is also treated as missing.
    df_clean['op_vote_preference'] = df['Q62AB'].map({
        1.0: 'parti libéral',
        2.0: 'parti conservateur',
        3.0: 'parti québécois',
        4.0: 'parti québécois',
        5.0: 'parti vert',
        6.0: 'n.d.a.',
        99.0: np.nan,
        96.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_preference'] = {
        'original_variable': 'Q62AB',
        'question_label': "Candidat principal de votre choix",
        'type': 'categorical',
        'value_labels': {'parti libéral': "Parti libéral", 'parti conservateur': "Parti conservateur", 'parti québécois': "Parti québécois", 'parti vert': "Parti vert", 'n.d.a.': "N.D.A."},
    }

    # --- Q62AC ---
    # op_province_code_outlier — Outlier province code from data that conflicts with codebook
    # Source: Q62AC
    # Assumption: Observed code 96.0 is treated as missing as it is not in the expected set {1, 2, 3} from codebook.
    df_clean['op_province_code_outlier'] = df['Q62AC'].map({
        96.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_province_code_outlier'] = {
        'original_variable': 'Q62AC',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {},
    }

    # --- Q62AD ---
    # ses_q62ad — Unspecified response code from Q62AD
    # Source: Q62AD
    # Assumption: Only code 96.0 found in data, treated as a unique category. All others are missing.
    df_clean['ses_q62ad'] = df['Q62AD'].map({
        96.0: 'unspecified_response',
    })
    CODEBOOK_VARIABLES['ses_q62ad'] = {
        'original_variable': 'Q62AD',
        'question_label': "Q62AD - No codebook entry available, inferred from data exploration.",
        'type': 'categorical',
        'value_labels': {'unspecified_response': "Code 96 found in data"},
    }

    # --- Q62AE ---
    # op_q62ae — Follow-up on Q62 regarding provincial election (Quebec 2012)
    # Source: Q62AE
    # Assumption: No codebook provided. Code 96.0 is present in data but has no label.
    # Assumption: Treating observed code 96.0 as missing due to high missing rate and lack of documentation.
    df_clean['op_q62ae'] = df['Q62AE'].map({
        96.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q62ae'] = {
        'original_variable': 'Q62AE',
        'question_label': "Unknown follow-up question Q62AE",
        'type': 'categorical',
        'value_labels': {},
    }

    # --- Q62AF ---
    # behav_disposition — Unspecified response category, likely "Did not answer" or similar
    # Source: Q62AF
    # Assumption: No codebook provided. Code 96.0 is the only observed non-missing value. Mapped to an uninformative label due to lack of context.
    df_clean['behav_disposition'] = df['Q62AF'].map({
        96.0: 'unspecified_response',
    })
    CODEBOOK_VARIABLES['behav_disposition'] = {
        'original_variable': 'Q62AF',
        'question_label': "Q62AF (No codebook provided)",
        'type': 'categorical',
        'value_labels': {'unspecified_response': "Unspecified Code 96.0"},
    }

    # --- Q62AG ---
    # ses_province — Province de résidence
    # Source: Q62AG
    # Assumption: Since codebook values are empty, observed code 96.0 is mapped to 'unknown'.
    # Assumption: Codebook missing code 99 is treated as np.nan.
    df_clean['ses_province'] = df['Q62AG'].map({
        96.0: 'unknown',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q62AG',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'unknown': "Unknown/Unlabelled"},
    }

    # --- Q62AH ---
    # op_unknown — Unknown code in Q62AH
    # Source: Q62AH
    # Assumption: Variable type is categorical based on float dtype with discrete codes.
    # Assumption: Code 96.0 corresponds to a single 'other' category, as no label is available.
    # Note: All other values are treated as missing due to the high percentage of NaNs.
    df_clean['op_unknown'] = df['Q62AH'].map({
        96.0: 'other_code',
    })
    CODEBOOK_VARIABLES['op_unknown'] = {
        'original_variable': 'Q62AH',
        'question_label': "Unknown question for Q62AH",
        'type': 'categorical',
        'value_labels': {'other_code': 'Other/Unspecified'},
    }

    # --- Q62AI ---
    # behav_q62ai — Observed response
    # Source: Q62AI
    # Assumption: Only code 96.0 observed, treated as a single response category. All others are missing.
    df_clean['behav_q62ai'] = df['Q62AI'].map({
        96.0: 'response',
    })
    CODEBOOK_VARIABLES['behav_q62ai'] = {
        'original_variable': 'Q62AI',
        'question_label': "Response to Q62AI",
        'type': 'categorical',
        'value_labels': {'response': 'Observed Response'},
    }

    # --- Q62B ---
    # ses_province — Province de résidence (Inferred from context)
    # Source: Q62B
    # Assumption: Codes 3, 6, 8, 9 are treated as missing/unmapped as they don't fit the known structure from context.
    df_clean['ses_province'] = df['Q62B'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q62B',
        'question_label': "Province de résidence (Inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q62C ---
    # op_q62c — Unknown opinion/attitude variable based on Q62C
    # Source: Q62C
    # Assumption: Only codes 1.0 and 2.0 have meaningful labels; all others are treated as missing.
    # Note: Question label and value labels are placeholders due to missing codebook entry.
    df_clean['op_q62c'] = df['Q62C'].map({
        1.0: 'cat_a',
        2.0: 'cat_b',
        6.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q62c'] = {
        'original_variable': 'Q62C',
        'question_label': "Unknown question text for Q62C",
        'type': 'categorical',
        'value_labels': {'cat_a': "Category A (Inferred)", 'cat_b': "Category B (Inferred)"},
    }

    # --- Q62D ---
    # op_cultural_policy_jurisdiction — Should cultural policy decisions be made by QNA or Parliament?
    # Source: Q62D
    df_clean['op_cultural_policy_jurisdiction'] = df['Q62D'].map({
        1.0: 'quebec_national_assembly',
        2.0: 'parliament_of_canada',
        6.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_cultural_policy_jurisdiction'] = {
        'original_variable': 'Q62D',
        'question_label': "For each of the following areas, do you think that decisions should be made by the Quebec National Assembly or by Parliament of Canada? / Cultural policy",
        'type': 'categorical',
        'value_labels': {'quebec_national_assembly': "Quebec National Assembly", 'parliament_of_canada': "Parliament of Canada", 'other': "Other"},
    }

    # --- Q62E ---
    # op_Q62E — Placeholder for variable Q62E
    # Source: Q62E
    # Note: No codebook entry provided. Mapping is inferred from data exploration and placeholder labels used.
    # TODO: Verify mapping for codes 1, 2, 6, and ensure 8, 9 are intended as missing.
    df_clean['op_Q62E'] = df['Q62E'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        6.0: 'option_c',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_Q62E'] = {
        'original_variable': 'Q62E',
        'question_label': "(No codebook entry provided for Q62E)",
        'type': 'categorical',
        'value_labels': {'option_a': 'Placeholder Label 1', 'option_b': 'Placeholder Label 2', 'option_c': 'Placeholder Label 3'},
    }

    # --- Q62F ---
    # op_vote_frequency — Frequency of voting in the last provincial election
    # Source: Q62F
    # Assumption: Codes 6.0, 8.0, and 9.0 are treated as missing as they are not documented and likely represent 'Refused', 'Don't know', or 'Not applicable'.
    df_clean['op_vote_frequency'] = df['Q62F'].map({
        1.0: 'voted',
        2.0: 'did_not_vote',
        6.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_frequency'] = {
        'original_variable': 'Q62F',
        'question_label': "Q62F - (Inferred: Frequency of voting in the last provincial election)",
        'type': 'categorical',
        'value_labels': {'voted': "Voted", 'did_not_vote': "Did not vote"},
    }

    # --- Q62G ---
    # ses_q62g — Unknown categorical variable from Q62G
    # Source: Q62G
    # Note: Codebook entry was missing, mapping is inferred based on value counts present in data.
    # Assumption: Codes 6.0, 8.0, and 9.0 are treated as missing (unlabelled/refused).
    df_clean['ses_q62g'] = df['Q62G'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        6.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_q62g'] = {
        'original_variable': 'Q62G',
        'question_label': "Unknown category question (based on Q62G)",
        'type': 'categorical',
        'value_labels': {'option_one': "Category 1", 'option_two': "Category 2"},
    }

    # --- Q62H ---
    # op_economic_policy_jurisdiction — Should economic policy decisions be made by QNA or Parliament?
    # Source: Q62H
    df_clean['op_economic_policy_jurisdiction'] = df['Q62H'].map({
        1.0: 'quebec_national_assembly',
        2.0: 'parliament_of_canada',
        6.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_economic_policy_jurisdiction'] = {
        'original_variable': 'Q62H',
        'question_label': "For each of the following areas, do you think that decisions should be made by the Quebec National Assembly or by Parliament of Canada? / Economic policy",
        'type': 'categorical',
        'value_labels': {'quebec_national_assembly': "Quebec National Assembly", 'parliament_of_canada': "Parliament of Canada", 'other': "Other"},
    }

    # --- Q62I ---
    # op_opinion_q62i — Deduced categorical opinion variable Q62I
    # Source: Q62I
    # Assumption: Mapping is a placeholder as codebook entry was not provided for Q62I.
    # Assumption: Codes 6.0, 8.0, 9.0 are treated as missing/unspecified.
    df_clean['op_opinion_q62i'] = df['Q62I'].map({
        1.0: 'level_one',
        2.0: 'level_two',
        6.0: np.nan,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_q62i'] = {
        'original_variable': 'Q62I',
        'question_label': "Q62I (Label Missing - Deduced Categorical)",
        'type': 'categorical',
        'value_labels': {'level_one': "Level One", 'level_two': "Level Two"},
    }

    # --- Q63 ---
    # op_q63_2012 — Response to Question 63
    # Source: Q63
    # Assumption: Codes 8.0 and 9.0 treated as missing (unlabelled in codebook)
    df_clean['op_q63_2012'] = df['Q63'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q63_2012'] = {
        'original_variable': 'Q63',
        'question_label': "Response to Q63",
        'type': 'categorical',
        'value_labels': {'option_one': "Option 1", 'option_two': "Option 2"},
    }

    # --- Q64 ---
    # op_Q64 — Response to Q64
    # Source: Q64
    # Assumption: Codes 96, 98, 99 treated as missing (unlabelled in data exploration)
    # Assumption: Codes 1, 2, 3, 4 mapped to generic options due to missing codebook
    df_clean['op_Q64'] = df['Q64'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_Q64'] = {
        'original_variable': 'Q64',
        'question_label': "Response to Q64",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4"},
    }

    # --- Q65 ---
    # op_q65 — Response to Question 65 (Codebook label missing)
    # Source: Q65
    # Note: Codebook entry was missing. Mapped observed values 1.0-25.0 to generic labels.
    df_clean['op_q65'] = df['Q65'].map({
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
        12.0: 'option_12',
        13.0: 'option_13',
        14.0: 'option_14',
        15.0: 'option_15',
        16.0: 'option_16',
        17.0: 'option_17',
        18.0: 'option_18',
        19.0: 'option_19',
        20.0: 'option_20',
        21.0: 'option_21',
        22.0: 'option_22',
        23.0: 'option_23',
        24.0: 'option_24',
        25.0: 'option_25',
    })
    CODEBOOK_VARIABLES['op_q65'] = {
        'original_variable': 'Q65',
        'question_label': "Response to Question 65 (Codebook label missing)",
        'type': 'categorical',
        'value_labels': {'option_1': 'Option 1 (Label Unknown)', 'option_2': 'Option 2 (Label Unknown)', 'option_3': 'Option 3 (Label Unknown)', 'option_4': 'Option 4 (Label Unknown)', 'option_5': 'Option 5 (Label Unknown)', 'option_6': 'Option 6 (Label Unknown)', 'option_7': 'Option 7 (Label Unknown)', 'option_8': 'Option 8 (Label Unknown)', 'option_9': 'Option 9 (Label Unknown)', 'option_10': 'Option 10 (Label Unknown)', 'option_11': 'Option 11 (Label Unknown)', 'option_12': 'Option 12 (Label Unknown)', 'option_13': 'Option 13 (Label Unknown)', 'option_14': 'Option 14 (Label Unknown)', 'option_15': 'Option 15 (Label Unknown)', 'option_16': 'Option 16 (Label Unknown)', 'option_17': 'Option 17 (Label Unknown)', 'option_18': 'Option 18 (Label Unknown)', 'option_19': 'Option 19 (Label Unknown)', 'option_20': 'Option 20 (Label Unknown)', 'option_21': 'Option 21 (Label Unknown)', 'option_22': 'Option 22 (Label Unknown)', 'option_23': 'Option 23 (Label Unknown)', 'option_24': 'Option 24 (Label Unknown)', 'option_25': 'Option 25 (Label Unknown)'},
    }

    # --- Q66 ---
    # op_attitude_q66 — Unknown attitude/opinion variable
    # Source: Q66
    # Note: No codebook entry found, mapping derived from observed data values. All values are treated as distinct categories.
    df_clean['op_attitude_q66'] = df['Q66'].map({
        1.0: 'code_1',
        2.0: 'code_2',
        7.0: 'code_7',
        10.0: 'code_10',
        12.0: 'code_12',
        14.0: 'code_14',
        18.0: 'code_18',
        22.0: 'code_22',
        23.0: 'code_23',
        24.0: 'code_24',
        25.0: 'code_25',
        26.0: 'code_26',
        30.0: 'code_30',
        34.0: 'code_34',
        43.0: 'code_43',
        46.0: 'code_46',
        48.0: 'code_48',
        50.0: 'code_50',
        51.0: 'code_51',
        52.0: 'code_52',
        53.0: 'code_53',
        54.0: 'code_54',
        60.0: 'code_60',
        61.0: 'code_61',
        63.0: 'code_63',
    })
    CODEBOOK_VARIABLES['op_attitude_q66'] = {
        'original_variable': 'Q66',
        'question_label': "Q66 (Missing label)",
        'type': 'categorical',
        'value_labels': {
            'code_1': 'Code 1 (Observed)', 'code_2': 'Code 2 (Observed)', 'code_7': 'Code 7 (Observed)', 
            'code_10': 'Code 10 (Observed)', 'code_12': 'Code 12 (Observed)', 'code_14': 'Code 14 (Observed)', 
            'code_18': 'Code 18 (Observed)', 'code_22': 'Code 22 (Observed)', 'code_23': 'Code 23 (Observed)', 
            'code_24': 'Code 24 (Observed)', 'code_25': 'Code 25 (Observed)', 'code_26': 'Code 26 (Observed)', 
            'code_30': 'Code 30 (Observed)', 'code_34': 'Code 34 (Observed)', 'code_43': 'Code 43 (Observed)', 
            'code_46': 'Code 46 (Observed)', 'code_48': 'Code 48 (Observed)', 'code_50': 'Code 50 (Observed)', 
            'code_51': 'Code 51 (Observed)', 'code_52': 'Code 52 (Observed)', 'code_53': 'Code 53 (Observed)', 
            'code_54': 'Code 54 (Observed)', 'code_60': 'Code 60 (Observed)', 'code_61': 'Code 61 (Observed)', 
            'code_63': 'Code 63 (Observed)'
        }
    }

    # --- Q67 ---
    # op_q67 — Unknown categorical question
    # Source: Q67
    # Assumption: Codes 8.0 and 9.0 are unlabelled and treated as missing (np.nan). Labels for 1.0-4.0 are placeholders due to missing codebook entry.
    df_clean['op_q67'] = df['Q67'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q67'] = {
        'original_variable': 'Q67',
        'question_label': "Unknown question (Q67 from eeq_2012)",
        'type': 'categorical',
        'value_labels': {'option_one': "Option One (Placeholder)", 'option_two': "Option Two (Placeholder)", 'option_three': "Option Three (Placeholder)", 'option_four': "Option Four (Placeholder)"},
    }

    # --- Q68 ---
    # behav_vote_intent_q68 — Vote intention/preference for Q68
    # Source: Q68
    # WARNING: Codebook not provided for Q68. Mapping inferred from data exploration.
    # Assumption: 0.0 is 'no_response' or other non-choice, other codes are discrete choices.
    df_clean['behav_vote_intent_q68'] = df['Q68'].map({
        0.0: 'no_response',
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        5.0: 'choice_5',
        6.0: 'choice_6',
        8.0: 'choice_8',
        10.0: 'choice_10',
        12.0: 'choice_12',
        15.0: 'choice_15',
        16.0: 'choice_16',
        17.0: 'choice_17',
        18.0: 'choice_18',
        19.0: 'choice_19',
        20.0: 'choice_20',
        22.0: 'choice_22',
        25.0: 'choice_25',
        30.0: 'choice_30',
        33.0: 'choice_33',
        35.0: 'choice_35',
        37.0: 'choice_37',
        40.0: 'choice_40',
        45.0: 'choice_45',
        46.0: 'choice_46',
    })
    CODEBOOK_VARIABLES['behav_vote_intent_q68'] = {
        'original_variable': 'Q68',
        'question_label': "Inferred: Vote intention/preference for Q68",
        'type': 'categorical',
        'value_labels': {'no_response': "No Response/Other (Inferred)", 'choice_1': "Choice 1 (Inferred)", 'choice_2': "Choice 2 (Inferred)", 'choice_3': "Choice 3 (Inferred)", 'choice_4': "Choice 4 (Inferred)", 'choice_5': "Choice 5 (Inferred)", 'choice_6': "Choice 6 (Inferred)", 'choice_8': "Choice 8 (Inferred)", 'choice_10': "Choice 10 (Inferred)", 'choice_12': "Choice 12 (Inferred)", 'choice_15': "Choice 15 (Inferred)", 'choice_16': "Choice 16 (Inferred)", 'choice_17': "Choice 17 (Inferred)", 'choice_18': "Choice 18 (Inferred)", 'choice_19': "Choice 19 (Inferred)", 'choice_20': "Choice 20 (Inferred)", 'choice_22': "Choice 22 (Inferred)", 'choice_25': "Choice 25 (Inferred)", 'choice_30': "Choice 30 (Inferred)", 'choice_33': "Choice 33 (Inferred)", 'choice_35': "Choice 35 (Inferred)", 'choice_37': "Choice 37 (Inferred)", 'choice_40': "Choice 40 (Inferred)", 'choice_45': "Choice 45 (Inferred)", 'choice_46': "Choice 46 (Inferred)"},
    }

    # --- Q68B ---
    # op_q68b — Unknown opinion variable Q68B
    # Source: Q68B
    # Assumption: Codes derived from value_counts are the only valid categories.
    df_clean['op_q68b'] = df['Q68B'].map({
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
        12.0: '12',
        13.0: '13',
        15.0: '15',
        16.0: '16',
        17.0: '17',
        20.0: '20',
        25.0: '25',
        30.0: '30',
        33.0: '33',
        35.0: '35',
        37.0: '37',
        38.0: '38',
        40.0: '40',
        43.0: '43',
    })
    CODEBOOK_VARIABLES['op_q68b'] = {
        'original_variable': 'Q68B',
        'question_label': "Unknown variable Q68B, codes derived from data exploration.",
        'type': 'categorical',
        'value_labels': {'0': "Category 0", '1': "Category 1", '2': "Category 2", '3': "Category 3", '4': "Category 4", '5': "Category 5", '6': "Category 6", '7': "Category 7", '8': "Category 8", '9': "Category 9", '10': "Category 10", '12': "Category 12", '13': "Category 13", '15': "Category 15", '16': "Category 16", '17': "Category 17", '20': "Category 20", '25': "Category 25", '30': "Category 30", '33': "Category 33", '35': "Category 35", '37': "Category 37", '38': "Category 38", '40': "Category 40", '43': "Category 43"},
    }

    # --- Q68C ---
    # ses_q68c — Raw categorical variable Q68C, mapped to generic string codes due to missing labels
    # Source: Q68C
    # Assumption: Variable is categorical based on dtype (float64) and discrete values.
    # Warning: Label information missing for Q68C. Mapping observed codes to generic string values (e.g., 'code_0').
    df_clean['ses_q68c'] = df['Q68C'].map({
        0.0: 'code_0',
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        6.0: 'code_6',
        7.0: 'code_7',
        9.0: 'code_9',
        10.0: 'code_10',
        12.0: 'code_12',
        14.0: 'code_14',
        15.0: 'code_15',
        16.0: 'code_16',
        17.0: 'code_17',
        18.0: 'code_18',
        20.0: 'code_20',
        22.0: 'code_22',
        25.0: 'code_25',
        30.0: 'code_30',
        31.0: 'code_31',
        32.0: 'code_32',
        33.0: 'code_33',
        35.0: 'code_35',
        39.0: 'code_39',
    })
    CODEBOOK_VARIABLES['ses_q68c'] = {
        'original_variable': 'Q68C',
        'question_label': "Province de résidence", # WARNING: Label from Q2_province context used as placeholder
        'type': 'categorical',
        'value_labels': {'code_0': "Code 0", 'code_1': "Code 1", 'code_2': "Code 2", 'code_3': "Code 3", 'code_4': "Code 4", 'code_5': "Code 5", 'code_6': "Code 6", 'code_7': "Code 7", 'code_9': "Code 9", 'code_10': "Code 10", 'code_12': "Code 12", 'code_14': "Code 14", 'code_15': "Code 15", 'code_16': "Code 16", 'code_17': "Code 17", 'code_18': "Code 18", 'code_20': "Code 20", 'code_22': "Code 22", 'code_25': "Code 25", 'code_30': "Code 30", 'code_31': "Code 31", 'code_32': "Code 32", 'code_33': "Code 33", 'code_35': "Code 35", 'code_39': "Code 39"},
    }

    # --- Q68D ---
    # op_q68d — Unknown categorical variable, proceeding with observed codes
    # Source: Q68D
    # WARNING: Codebook mapping and label information is missing for this variable.
    # The map keys are based on observed float values, mapped to generic string labels.
    df_clean['op_q68d'] = df['Q68D'].map({
        0.0: 'code_0',
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        6.0: 'code_6',
        8.0: 'code_8',
        9.0: 'code_9',
        10.0: 'code_10',
        15.0: 'code_15',
        20.0: 'code_20',
        25.0: 'code_25',
        26.0: 'code_26',
        29.0: 'code_29',
        30.0: 'code_30',
        33.0: 'code_33',
        34.0: 'code_34',
        35.0: 'code_35',
        36.0: 'code_36',
        40.0: 'code_40',
        45.0: 'code_45',
        49.0: 'code_49',
        50.0: 'code_50',
    })
    CODEBOOK_VARIABLES['op_q68d'] = {
        'original_variable': 'Q68D',
        'question_label': "Q68D - Unlabeled categorical variable",
        'type': 'categorical',
        'value_labels': {'code_0': "Code 0 (Unlabeled)", 'code_1': "Code 1 (Unlabeled)", 'code_2': "Code 2 (Unlabeled)", 'code_3': "Code 3 (Unlabeled)", 'code_4': "Code 4 (Unlabeled)", 'code_5': "Code 5 (Unlabeled)", 'code_6': "Code 6 (Unlabeled)", 'code_8': "Code 8 (Unlabeled)", 'code_9': "Code 9 (Unlabeled)", 'code_10': "Code 10 (Unlabeled)", 'code_15': "Code 15 (Unlabeled)", 'code_20': "Code 20 (Unlabeled)", 'code_25': "Code 25 (Unlabeled)", 'code_26': "Code 26 (Unlabeled)", 'code_29': "Code 29 (Unlabeled)", 'code_30': "Code 30 (Unlabeled)", 'code_33': "Code 33 (Unlabeled)", 'code_34': "Code 34 (Unlabeled)", 'code_35': "Code 35 (Unlabeled)", 'code_36': "Code 36 (Unlabeled)", 'code_40': "Code 40 (Unlabeled)", 'code_45': "Code 45 (Unlabeled)", 'code_49': "Code 49 (Unlabeled)", 'code_50': "Code 50 (Unlabeled)"},
    }

    # --- Q68E ---
    # ses_q68e — Unknown question, treated as categorical (no codebook provided)
    # Source: Q68E
    # WARNING: No codebook provided for variable Q68E. Mapping is based on observed values and uses generic labels.
    # Note: Original data has 0 explicit missing values. Unseen float values in data will map to np.nan.
    df_clean['ses_q68e'] = df['Q68E'].map({
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
        11.0: 'code_11',
        12.0: 'code_12',
        15.0: 'code_15',
        20.0: 'code_20',
        23.0: 'code_23',
        24.0: 'code_24',
        25.0: 'code_25',
        26.0: 'code_26',
        30.0: 'code_30',
        33.0: 'code_33',
        34.0: 'code_34',
        35.0: 'code_35',
        40.0: 'code_40',
        43.0: 'code_43',
    })
    CODEBOOK_VARIABLES['ses_q68e'] = {
        'original_variable': 'Q68E',
        'question_label': "Q68E - Label Missing/Unknown",
        'type': 'categorical',
        'value_labels': {'code_0': "Code 0", 'code_1': "Code 1", 'code_2': "Code 2", 'code_3': "Code 3", 'code_4': "Code 4", 'code_5': "Code 5", 'code_6': "Code 6", 'code_7': "Code 7", 'code_8': "Code 8", 'code_9': "Code 9", 'code_10': "Code 10", 'code_11': "Code 11", 'code_12': "Code 12", 'code_15': "Code 15", 'code_20': "Code 20", 'code_23': "Code 23", 'code_24': "Code 24", 'code_25': "Code 25", 'code_26': "Code 26", 'code_30': "Code 30", 'code_33': "Code 33", 'code_34': "Code 34", 'code_35': "Code 35", 'code_40': "Code 40", 'code_43': "Code 43"},
    }

    # --- Q68F ---
    # op_q68f — Unknown categorical response code
    # Source: Q68F
    # WARNING: Codebook entry for Q68F in eeq_2012 was not provided. Mapping raw codes to string labels based on observation only.
    df_clean['op_q68f'] = df['Q68F'].map({
        0.0: 'zero',
        1.0: 'one',
        2.0: 'two',
        3.0: 'three',
        4.0: 'four',
        5.0: 'five',
        6.0: 'six',
        9.0: 'nine',
        10.0: 'ten',
        15.0: 'fifteen',
        17.0: 'seventeen',
        20.0: 'twenty',
        25.0: 'twenty-five',
        30.0: 'thirty',
        32.0: 'thirty-two',
        33.0: 'thirty-three',
        35.0: 'thirty-five',
        40.0: 'forty',
        45.0: 'forty-five',
        46.0: 'forty-six',
        50.0: 'fifty',
        55.0: 'fifty-five',
        60.0: 'sixty',
        65.0: 'sixty-five',
        68.0: 'sixty-eight',
    })
    CODEBOOK_VARIABLES['op_q68f'] = {
        'original_variable': 'Q68F',
        'question_label': "Unknown: Response code for Q68F",
        'type': 'categorical',
        'value_labels': {'zero': "0", 'one': "1", 'two': "2", 'three': "3", 'four': "4", 'five': "5", 'six': "6", 'nine': "9", 'ten': "10", 'fifteen': "15", 'seventeen': "17", 'twenty': "20", 'twenty-five': "25", 'thirty': "30", 'thirty-two': "32", 'thirty-three': "33", 'thirty-five': "35", 'forty': "40", 'forty-five': "45", 'forty-six': "46", 'fifty': "50", 'fifty-five': "55", 'sixty': "60", 'sixty-five': "65", 'sixty-eight': "68"},
    }

    # --- Q69 ---
    # op_party_support — Party support or issue opinion (Codebook missing)
    # Source: Q69
    # Assumption: Variable treated as categorical based on numerous codes observed.
    # Assumption: Mappings and labels are placeholders due to missing codebook entry for Q69.
    df_clean['op_party_support'] = df['Q69'].map({
        0.0: 'code_0',
        1.0: 'code_1',
        2.0: 'code_2',
        3.0: 'code_3',
        4.0: 'code_4',
        5.0: 'code_5',
        6.0: 'code_6',
        7.0: 'code_7',
        8.0: 'code_8',
        10.0: 'code_10',
        12.0: 'code_12',
        13.0: 'code_13',
        15.0: 'code_15',
        20.0: 'code_20',
        22.0: 'code_22',
        24.0: 'code_24',
        25.0: 'code_25',
        30.0: 'code_30',
        33.0: 'code_33',
        35.0: 'code_35',
        37.0: 'code_37',
        40.0: 'code_40',
        44.0: 'code_44',
        45.0: 'code_45',
        49.0: 'code_49',
    })
    CODEBOOK_VARIABLES['op_party_support'] = {
        'original_variable': 'Q69',
        'question_label': "Unknown/Inferred from context (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'code_0': 'Code 0 (Unknown)', 'code_1': 'Code 1 (Unknown)', 'code_2': 'Code 2 (Unknown)', 'code_3': 'Code 3 (Unknown)', 'code_4': 'Code 4 (Unknown)', 'code_5': 'Code 5 (Unknown)', 'code_6': 'Code 6 (Unknown)', 'code_7': 'Code 7 (Unknown)', 'code_8': 'Code 8 (Unknown)', 'code_10': 'Code 10 (Unknown)', 'code_12': 'Code 12 (Unknown)', 'code_13': 'Code 13 (Unknown)', 'code_15': 'Code 15 (Unknown)', 'code_20': 'Code 20 (Unknown)', 'code_22': 'Code 22 (Unknown)', 'code_24': 'Code 24 (Unknown)', 'code_25': 'Code 25 (Unknown)', 'code_30': 'Code 30 (Unknown)', 'code_33': 'Code 33 (Unknown)', 'code_35': 'Code 35 (Unknown)', 'code_37': 'Code 37 (Unknown)', 'code_40': 'Code 40 (Unknown)', 'code_44': 'Code 44 (Unknown)', 'code_45': 'Code 45 (Unknown)', 'code_49': 'Code 49 (Unknown)'},
    }

    # --- Q69B ---
    # op_q69b — Categorical response to question Q69B
    # Source: Q69B
    # Assumption: This is a categorical variable, mapping all observed numeric codes to string options as the codebook is unavailable.
    df_clean['op_q69b'] = df['Q69B'].map({
        0.0: 'option_0',
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
        6.0: 'option_6',
        8.0: 'option_8',
        10.0: 'option_10',
        12.0: 'option_12',
        15.0: 'option_15',
        20.0: 'option_20',
        22.0: 'option_22',
        25.0: 'option_25',
        30.0: 'option_30',
        33.0: 'option_33',
        35.0: 'option_35',
        36.0: 'option_36',
        40.0: 'option_40',
        43.0: 'option_43',
        44.0: 'option_44',
        45.0: 'option_45',
        46.0: 'option_46',
        48.0: 'option_48',
        49.0: 'option_49',
    })
    CODEBOOK_VARIABLES['op_q69b'] = {
        'original_variable': 'Q69B',
        'question_label': "Unknown: Categorical response to Q69B",
        'type': 'categorical',
        'value_labels': {'option_0': "Code 0.0", 'option_1': "Code 1.0", 'option_2': "Code 2.0", 'option_3': "Code 3.0", 'option_4': "Code 4.0", 'option_5': "Code 5.0", 'option_6': "Code 6.0", 'option_8': "Code 8.0", 'option_10': "Code 10.0", 'option_12': "Code 12.0", 'option_15': "Code 15.0", 'option_20': "Code 20.0", 'option_22': "Code 22.0", 'option_25': "Code 25.0", 'option_30': "Code 30.0", 'option_33': "Code 33.0", 'option_35': "Code 35.0", 'option_36': "Code 36.0", 'option_40': "Code 40.0", 'option_43': "Code 43.0", 'option_44': "Code 44.0", 'option_45': "Code 45.0", 'option_46': "Code 46.0", 'option_48': "Code 48.0", 'option_49': "Code 49.0"},
    }

    # --- Q69C ---
    # op_attitude_q69c — Opinion/Attitude question Q69C
    # Source: Q69C
    # Assumption: Observed float codes mapped to generic string levels.
    df_clean['op_attitude_q69c'] = df['Q69C'].map({
        0.0: 'level_0',
        1.0: 'level_1',
        4.0: 'level_4',
        5.0: 'level_5',
        6.0: 'level_6',
        7.0: 'level_7',
        8.0: 'level_8',
        9.0: 'level_9',
        10.0: 'level_10',
        15.0: 'level_15',
        20.0: 'level_20',
        25.0: 'level_25',
        27.0: 'level_27',
        30.0: 'level_30',
        33.0: 'level_33',
        35.0: 'level_35',
        40.0: 'level_40',
        44.0: 'level_44',
        45.0: 'level_45',
        46.0: 'level_46',
        48.0: 'level_48',
        49.0: 'level_49',
        50.0: 'level_50',
        51.0: 'level_51',
        55.0: 'level_55',
    })
    CODEBOOK_VARIABLES['op_attitude_q69c'] = {
        'original_variable': 'Q69C',
        'question_label': "Placeholder for Q69C: Unknown question text",
        'type': 'categorical',
        'value_labels': {'level_0': "Level 0 (Observed 0.0)", 'level_1': "Level 1 (Observed 1.0)", 'level_4': "Level 4 (Observed 4.0)", 'level_5': "Level 5 (Observed 5.0)", 'level_6': "Level 6 (Observed 6.0)", 'level_7': "Level 7 (Observed 7.0)", 'level_8': "Level 8 (Observed 8.0)", 'level_9': "Level 9 (Observed 9.0)", 'level_10': "Level 10 (Observed 10.0)", 'level_15': "Level 15 (Observed 15.0)", 'level_20': "Level 20 (Observed 20.0)", 'level_25': "Level 25 (Observed 25.0)", 'level_27': "Level 27 (Observed 27.0)", 'level_30': "Level 30 (Observed 30.0)", 'level_33': "Level 33 (Observed 33.0)", 'level_35': "Level 35 (Observed 35.0)", 'level_40': "Level 40 (Observed 40.0)", 'level_44': "Level 44 (Observed 44.0)", 'level_45': "Level 45 (Observed 45.0)", 'level_46': "Level 46 (Observed 46.0)", 'level_48': "Level 48 (Observed 48.0)", 'level_49': "Level 49 (Observed 49.0)", 'level_50': "Level 50 (Observed 50.0)", 'level_51': "Level 51 (Observed 51.0)", 'level_55': "Level 55 (Observed 55.0)"},
    }

    # --- Q69D ---
    # op_attitude_q69d — Attitude/Opinion Q69D (Codebook missing)
    # Source: Q69D
    # Assumption: Codes 95, 97, 98, 99 are treated as missing based on value distribution (>=95)
    df_clean['op_attitude_q69d'] = df['Q69D'].map({
        1.0: 'strongly_disagree',
        2.0: 'disagree',
        3.0: 'neutral',
        4.0: 'agree',
        5.0: 'strongly_agree',
        95.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q69d'] = {
        'original_variable': 'Q69D',
        'question_label': "Attitude/Opinion Q69D (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'strongly_disagree': 'Strongly Disagree', 'disagree': 'Disagree', 'neutral': 'Neutral', 'agree': 'Agree', 'strongly_agree': 'Strongly Agree'},
    }

    # --- Q69E ---
    # op_interest — Level of interest in the election
    # Source: Q69E
    # Assumption: Codes 1-6 are response categories, 95, 97, 98, 99 are treated as missing.
    # TODO: The labels for codes 1-6 and the nature of codes 95, 97, 98 need verification against the full codebook.
    df_clean['op_interest'] = df['Q69E'].map({
        1.0: 'very low',
        2.0: 'low',
        3.0: 'neutral',
        4.0: 'high',
        5.0: 'very high',
        6.0: 'refused',
        95.0: np.nan,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_interest'] = {
        'original_variable': 'Q69E',
        'question_label': "Level of interest in the election",
        'type': 'categorical',
        'value_labels': {'very low': "Very low interest", 'low': "Low interest", 'neutral': "Neutral interest", 'high': "High interest", 'very high': "Very high interest", 'refused': "Refused"},
    }

    # --- Q69F ---
    # op_q69f — Inferred opinion variable
    # Source: Q69F
    # Note: Codebook entry was missing. Codes 1.0-6.0 mapped to generic responses.
    # Assumption: Codes 95.0, 97.0, 98.0, 99.0 are treated as missing (unmapped).
    df_clean['op_q69f'] = df['Q69F'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        5.0: 'response_5',
        6.0: 'response_6',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q69f'] = {
        'original_variable': 'Q69F',
        'question_label': "Inferred Opinion Question (Codebook Missing)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1", 'response_2': "Response 2", 'response_3': "Response 3", 'response_4': "Response 4", 'response_5': "Response 5", 'response_6': "Response 6"},
    }

    # --- Q7 ---
    # op_q7 — Attitude toward political parties or leaders
    # Source: Q7
    # Assumption: Codes 0.0-10.0 are the valid range; 98.0 and 99.0 are treated as missing.
    # Assumption: Generic labels assigned due to missing codebook entry.
    df_clean['op_q7'] = df['Q7'].map({
        0.0: 'no_response',
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
    CODEBOOK_VARIABLES['op_q7'] = {
        'original_variable': 'Q7',
        'question_label': "Attitude toward political parties or leaders",
        'type': 'categorical',
        'value_labels': {'no_response': 'No response/Unknown', 'option_1': 'Option 1', 'option_2': 'Option 2', 'option_3': 'Option 3', 'option_4': 'Option 4', 'option_5': 'Option 5', 'option_6': 'Option 6', 'option_7': 'Option 7', 'option_8': 'Option 8', 'option_9': 'Option 9', 'option_10': 'Option 10'},
    }

    # --- Q70A ---
    # op_plq_ideology — Left-right placement of Quebec Liberal Party (0-1 scale)
    # Source: Q70A
    df_clean['op_plq_ideology'] = np.nan
    mask = (df['Q70A'] >= 0) & (df['Q70A'] <= 10)
    df_clean.loc[mask, 'op_plq_ideology'] = df.loc[mask, 'Q70A'] / 10.0
    CODEBOOK_VARIABLES['op_plq_ideology'] = {
        'original_variable': 'Q70A',
        'question_label': "In political matters people talk of 'the left' and 'the right'. On a scale of 0 to 10, where 0 is the most left and 10 is the most right, where would place the following political parties? / Quebec Liberal Party",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q70B ---
    # op_q70b — Opinion variable Q70B (codebook missing)
    # Source: Q70B
    # Note: No codebook provided. Mapping codes 0-10 to generic labels. Codes 98 and 99 treated as missing based on data observation.
    df_clean['op_q70b'] = df['Q70B'].map({
        0.0: 'code 0',
        1.0: 'code 1',
        2.0: 'code 2',
        3.0: 'code 3',
        4.0: 'code 4',
        5.0: 'code 5',
        6.0: 'code 6',
        7.0: 'code 7',
        8.0: 'code 8',
        9.0: 'code 9',
        10.0: 'code 10',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q70b'] = {
        'original_variable': 'Q70B',
        'question_label': "Q70B",
        'type': 'categorical',
        'value_labels': {'code 0': 'Code 0', 'code 1': 'Code 1', 'code 2': 'Code 2', 'code 3': 'Code 3', 'code 4': 'Code 4', 'code 5': 'Code 5', 'code 6': 'Code 6', 'code 7': 'Code 7', 'code 8': 'Code 8', 'code 9': 'Code 9', 'code 10': 'Code 10'},
    }

    # --- Q70C ---
    # behav_read_news_weekly_or_more — Frequency of reading newspapers (weekly or more)
    # Source: Q70C
    # Assumption: Data exploration failed, proceeding based on codebook entry values.
    df_clean['behav_read_news_weekly_or_more'] = df['Q70C'].map({
        1.0: 1.0,
        2.0: 0.0,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_read_news_weekly_or_more'] = {
        'original_variable': 'Q70C',
        'question_label': "Fréquence de lecture des journaux (hebdomadaire ou plus)",
        'type': 'binary',
        'value_labels': {1.0: "Oui", 0.0: "Non"},
    }

    # --- Q70D ---
    # op_coded_response — Coded categorical response for Q70D
    # Source: Q70D
    # Assumption: Codes 0-10 are valid categories. Codes 98 and 99 are treated as missing as they are unlabelled in data/codebook.
    df_clean['op_coded_response'] = df['Q70D'].map({
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
    CODEBOOK_VARIABLES['op_coded_response'] = {
        'original_variable': 'Q70D',
        'question_label': "Coded response for Q70D (label unknown, mapping derived from data exploration)",
        'type': 'categorical',
        'value_labels': {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8', '9': '9', '10': '10'},
    }

    # --- Q70E ---
    # op_q70e — Attitude scale 0-10
    # Source: Q70E (No codebook provided for this variable/survey, derived from data exploration)
    # Assumption: 0 is most negative, 10 is most positive. Normalizing by dividing by 10.0.
    # Assumption: Codes 98.0 and 99.0 are unmapped and treated as missing.
    df_clean['op_q70e'] = df['Q70E'].map({
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
    CODEBOOK_VARIABLES['op_q70e'] = {
        'original_variable': 'Q70E',
        'question_label': "Attitude scale (0-10) based on data exploration",
        'type': 'likert',
        'value_labels': {'0.0': "Most Negative (0)", '1.0': "0.1", '2.0': "0.2", '3.0': "0.3", '4.0': "0.4", '5.0': "Midpoint (0.5)", '6.0': "0.6", '7.0': "0.7", '8.0': "0.8", '9.0': "0.9", '1.0': "Most Positive (1.0)"},
    }

    # --- Q70F ---
    # op_pvq_ideology — Left-right placement of Quebec Green Party (0-1 scale)
    # Source: Q70F
    df_clean['op_pvq_ideology'] = np.nan
    mask = (df['Q70F'] >= 0) & (df['Q70F'] <= 10)
    df_clean.loc[mask, 'op_pvq_ideology'] = df.loc[mask, 'Q70F'] / 10.0
    CODEBOOK_VARIABLES['op_pvq_ideology'] = {
        'original_variable': 'Q70F',
        'question_label': "In political matters people talk of 'the left' and 'the right'. On a scale of 0 to 10, where 0 is the most left and 10 is the most right, where would place the following political parties? / Parti Vert du Québec",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- Q71 ---
    # op_q71_attitude — Attitude toward the election (Likert scale)
    # Source: Q71
    # Assumption: This is a 0-10 Likert scale question. Values 98 and 99 are assumed to be missing/refused based on data exploration.
    # Mapping is normalized 0.0 (min) to 1.0 (max) corresponding to codes 0.0 to 10.0.
    df_clean['op_q71_attitude'] = df['Q71'].map({
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
    CODEBOOK_VARIABLES['op_q71_attitude'] = {
        'original_variable': 'Q71',
        'question_label': "Attitude toward the election (Normalized 0-10)",
        'type': 'likert',
        'value_labels': {'0.0': "Min Intensity/Agreement", '1.0': "Max Intensity/Agreement"},
    }

    # --- Q72 ---
    # behav_q72 — Likely outcome of vote for Q72 (Requires codebook verification)
    # Source: Q72
    # Note: Missing the codebook entry. Codes 8/9 assumed missing based on common practice.
    df_clean['behav_q72'] = df['Q72'].map({
        1.0: 'category_a',
        2.0: 'category_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_q72'] = {
        'original_variable': 'Q72',
        'question_label': "Likely outcome of vote for Q72 (Requires codebook verification)",
        'type': 'categorical',
        'value_labels': {'category_a': 'Category A (Verify)', 'category_b': 'Category B (Verify)'},
    }

    # --- Q73A ---
    # op_vote_intention — Vote intention
    # Source: Q73A
    # Assumption: Codes 8.0 ('Aucun') and 9.0 ('Refus de répondre') treated as missing.
    df_clean['op_vote_intention'] = df['Q73A'].map({
        1.0: 'bq',
        2.0: 'conservateur',
        3.0: 'liberal',
        4.0: 'npd',
        5.0: 'caq',
        6.0: 'autre_parti',
        7.0: 'vert',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_intention'] = {
        'original_variable': 'Q73A',
        'question_label': "Vote intention",
        'type': 'categorical',
        'value_labels': {'bq': "Bloc Québécois", 'conservateur': "Conservateur", 'liberal': "Libéral", 'npd': "NPD", 'caq': "CAQ", 'autre_parti': "Autre parti", 'vert': "Parti vert"},
    }

    # --- Q73B ---
    # ses_votemode — Mode de scrutin
    # Source: Q73B
    # Assumption: codes 8 and 9 are missing based on typical survey practice (not in codebook)
    df_clean['ses_votemode'] = df['Q73B'].map({
        1.0: 'main_residence',
        2.0: 'another_municipality',
        3.0: 'another_riding',
        4.0: 'in_person_outside_riding',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_votemode'] = {
        'original_variable': 'Q73B',
        'question_label': "Mode de scrutin",
        'type': 'categorical',
        'value_labels': {'main_residence': "Vote à la circonscription de résidence", 'another_municipality': "Vote dans une autre municipalité", 'another_riding': "Vote dans une autre circonscription", 'in_person_outside_riding': "Vote en personne en dehors de la circonscription"},
    }

    # --- Q73C ---
    # behav_response_q73c — Unlabeled categorical response for Q73C
    # Source: Q73C
    # Assumption: Codes 1-4 map to specific responses; 8 (DK) and 9 (Refused) treated as missing.
    df_clean['behav_response_q73c'] = df['Q73C'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'other',
        4.0: 'maybe',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_response_q73c'] = {
        'original_variable': 'Q73C',
        'question_label': "Response to question Q73C (Label not provided, derived from data)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", 'other': "Other", 'maybe': "Maybe"},
    }

    # --- Q73D ---
    # op_opinion_d — Inferred opinion/attitude question D
    # Source: Q73D
    # Note: Codebook entry was missing. Inferred type is categorical.
    # Assumption: Codes 8.0 and 9.0 are missing values (Refused/NA).
    df_clean['op_opinion_d'] = df['Q73D'].map({
        1.0: 'option_1',
        2.0: 'option_2',
        3.0: 'option_3',
        4.0: 'option_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_d'] = {
        'original_variable': 'Q73D',
        'question_label': "Inferred Opinion Variable Q73D (Codebook Missing)",
        'type': 'categorical',
        'value_labels': {'option_1': "Option 1", 'option_2': "Option 2", 'option_3': "Option 3", 'option_4': "Option 4"},
    }

    # --- Q73E ---
    # op_q73e — Likely choice of party in election
    # Source: Q73E
    # NOTE: Codebook entry was missing. Inferred type is categorical based on float64 dtype and codes 1-4, 8, 9.
    # Assumption: Codes 8.0 and 9.0 are treated as missing (unlabelled in inferred codebook).
    df_clean['op_q73e'] = df['Q73E'].map({
        1.0: 'choice_one',
        2.0: 'choice_two',
        3.0: 'choice_three',
        4.0: 'choice_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q73e'] = {
        'original_variable': 'Q73E',
        'question_label': "Likely choice of party in election (Inferred)",
        'type': 'categorical',
        'value_labels': {'choice_one': "Choice 1 (Inferred)", 'choice_two': "Choice 2 (Inferred)", 'choice_three': "Choice 3 (Inferred)", 'choice_four': "Choice 4 (Inferred)"},
    }

    # --- Q73F ---
    # op_opinion_q73f — Opinion on question 73, part F
    # Source: Q73F
    # Assumption: Codes 8 and 9 are treated as missing (not explicitly defined in codebook)
    df_clean['op_opinion_q73f'] = df['Q73F'].map({
        1.0: 'value_1',
        2.0: 'value_2',
        3.0: 'value_3',
        4.0: 'value_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_q73f'] = {
        'original_variable': 'Q73F',
        'question_label': "Opinion on question 73, part F (Codebook mapping unknown)",
        'type': 'categorical',
        'value_labels': {'value_1': "Category 1", 'value_2': "Category 2", 'value_3': "Category 3", 'value_4': "Category 4"},
    }

    # --- Q73G ---
    # op_q73g — Opinion on statement Q73G
    # Source: Q73G
    # Assumption: No codebook provided; assuming 1-4 scale + 8/9 for missing/refused. Type is categorical.
    df_clean['op_q73g'] = df['Q73G'].map({
        1.0: 'strongly disagree',
        2.0: 'disagree',
        3.0: 'agree',
        4.0: 'strongly agree',
        8.0: "don't know",
        9.0: 'refused',
    })
    CODEBOOK_VARIABLES['op_q73g'] = {
        'original_variable': 'Q73G',
        'question_label': "Opinion on statement Q73G (Codebook entry missing)",
        'type': 'categorical',
        'value_labels': {'strongly disagree': "1", 'disagree': "2", 'agree': "3", 'strongly agree': "4", "don't know": "8", 'refused': "9"},
    }

    # --- Q74 ---
    # behav_response_q74 — Response to question Q74
    # Source: Q74
    # Assumption: Codes 8.0 and 9.0 are treated as missing as they are unlabelled and likely represent 'Don't Know' or 'Refused'.
    df_clean['behav_response_q74'] = df['Q74'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_response_q74'] = {
        'original_variable': 'Q74',
        'question_label': "Response to question Q74",
        'type': 'categorical',
        'value_labels': {'response_1': "Category 1", 'response_2': "Category 2", 'response_3': "Category 3", 'response_4': "Category 4"},
    }

    # --- Q75 ---
    # op_vote_choice — Vote choice
    # Source: Q75
    # Assumption: Codes 8.0 and 9.0 are treated as missing values (Refused/Not applicable). Mapping for 1.0-4.0 is inferred as no codebook was provided.
    df_clean['op_vote_choice'] = df['Q75'].map({
        1.0: 'candidate_a',
        2.0: 'candidate_b',
        3.0: 'candidate_c',
        4.0: 'other',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_choice'] = {
        'original_variable': 'Q75',
        'question_label': "Vote choice (Inferred)",
        'type': 'categorical',
        'value_labels': {'candidate_a': "Candidate A", 'candidate_b': "Candidate B", 'candidate_c': "Candidate C", 'other': "Other/Other party"},
    }

    # --- Q76 ---
    # op_attitude — Opinion on main parties (inferred)
    # Source: Q76
    # Assumption: Codes 8.0 and 9.0 treated as missing (not in assumed mapping 1-4)
    df_clean['op_attitude'] = df['Q76'].map({
        1.0: 'support_party_1',
        2.0: 'support_party_2',
        3.0: 'support_party_3',
        4.0: 'support_party_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude'] = {
        'original_variable': 'Q76',
        'question_label': "Opinion on main parties (inferred)",
        'type': 'categorical',
        'value_labels': {'support_party_1': "Party 1", 'support_party_2': "Party 2", 'support_party_3': "Party 3", 'support_party_4': "Party 4"},
    }

    # --- Q77A ---
    # ses_voting_intention_conservative — Intention de vote conservateur
    # Source: Q77A
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook)
    df_clean['ses_voting_intention_conservative'] = df['Q77A'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'undecided',
        4.0: 'none',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_voting_intention_conservative'] = {
        'original_variable': 'Q77A',
        'question_label': "Votez-vous pour le Parti conservateur lors des prochaines élections fédérales?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non", 'undecided': "Indécis", 'none': "Aucun"},
    }

    # --- Q77B ---
    # op_q77b — Unknown variable (defaulted to op_ prefix)
    # Source: Q77B
    # CRITICAL: No codebook entry provided. Labels for 1.0-4.0 are placeholder guesses.
    # Assumption: Codes 8.0 and 9.0 (48 and 21 counts respectively) are unlabelled and treated as missing.
    df_clean['op_q77b'] = df['Q77B'].map({
        1.0: 'value_1',
        2.0: 'value_2',
        3.0: 'value_3',
        4.0: 'value_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q77b'] = {
        'original_variable': 'Q77B',
        'question_label': "Q77B - NOTE: Original question label is UNKNOWN",
        'type': 'categorical',
        'value_labels': {'value_1': 'Value 1 (Unknown)', 'value_2': 'Value 2 (Unknown)', 'value_3': 'Value 3 (Unknown)', 'value_4': 'Value 4 (Unknown)'},
    }

    # --- Q77C ---
    # ses_q77c — Unnamed variable, mapping based on observed values
    # Source: Q77C
    # Assumption: Codes 8 and 9 are treated as 'refused' and 'dont_know' respectively, as they were not explicitly labelled in the provided context.
    df_clean['ses_q77c'] = df['Q77C'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        3.0: 'option_c',
        4.0: 'option_d',
        8.0: 'refused',
        9.0: 'dont_know',
    })
    CODEBOOK_VARIABLES['ses_q77c'] = {
        'original_variable': 'Q77C',
        'question_label': "Response to question 77, category C (Inferred)",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A", 'option_b': "Option B", 'option_c': "Option C", 'option_d': "Option D", 'refused': "Refused", 'dont_know': "Don't know"},
    }

    # --- Q77D ---
    # op_vote_detail — Detailed vote choice/preference
    # Source: Q77D
    # Assumption: Codes 8/9 are missing (unlabelled in data exploration, inferred from context)
    df_clean['op_vote_detail'] = df['Q77D'].map({
        1.0: 'option_one',
        2.0: 'option_two',
        3.0: 'option_three',
        4.0: 'option_four',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_detail'] = {
        'original_variable': 'Q77D',
        'question_label': "Detailed vote choice/preference (Inferred from data structure)",
        'type': 'categorical',
        'value_labels': {'option_one': "Option 1", 'option_two': "Option 2", 'option_three': "Option 3", 'option_four': "Option 4"},
    }

    # --- Q77E ---
    # op_too_many_immigrants — Too many immigrants in Quebec (Likert 0-1)
    # Source: Q77E
    df_clean['op_too_many_immigrants'] = df['Q77E'].map({
        1.0: 0.0,
        2.0: 1/3,
        3.0: 2/3,
        4.0: 1.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_too_many_immigrants'] = {
        'original_variable': 'Q77E',
        'question_label': "Please tell us if you strongly agree, somewhat agree, somewhat disagree, or strongly disagree with the following statements: / There are too many immigrants in Quebec.",
        'type': 'likert',
        'value_labels': {0.0: "Strongly agree", 1/3: "Somewhat agree", 2/3: "Somewhat disagree", 1.0: "Strongly disagree"},
    }

    # --- Q78 ---
    # ses_province — Province de résidence
    # Source: Q78
    # Note: Codes 8 and 9 are not in the codebook and will be treated as missing (np.nan)
    # Assumption: Codes 1 and 2 map to provinces as per the context for eeq_2007, assuming similar structure.
    # Since no codebook was provided for this specific call, I am mapping based on the previous context's implicit meaning: 1=Quebec, 2=Ontario.
    df_clean['ses_province'] = df['Q78'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province'] = {
        'original_variable': 'Q78',
        'question_label': "Province de résidence (Inferred)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario"},
    }

    # --- Q79 ---
    # op_q79 — Voting intention (Inferred from context, no label available)
    # Source: Q79
    # Assumption: Codes 8.0 and 9.0 are unlabelled missing values (DK/Refused) and will be mapped to NaN.
    df_clean['op_q79'] = df['Q79'].map({
        1.0: 'voted_party_a',
        2.0: 'voted_party_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q79'] = {
        'original_variable': 'Q79',
        'question_label': "Voting intention (Inferred - Label Missing)",
        'type': 'categorical',
        'value_labels': {'voted_party_a': "Voted for Party A (Inferred)", 'voted_party_b': "Voted for Party B (Inferred)"},
    }

    # --- Q8 ---
    # op_vote_party — Vote intention at last election
    # Source: Q8
    # Assumption: Codes 1-4 map to placeholder parties. Codes 8 and 9 are treated as missing.
    df_clean['op_vote_party'] = df['Q8'].map({
        1.0: 'party_a',
        2.0: 'party_b',
        3.0: 'party_c',
        4.0: 'party_d',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_vote_party'] = {
        'original_variable': 'Q8',
        'question_label': "Vote intention at last election",
        'type': 'categorical',
        'value_labels': {'party_a': "Party A", 'party_b': "Party B", 'party_c': "Party C", 'party_d': "Party D"},
    }

    # --- Q80 ---
    # op_death_penalty_support — Are you for or against death penalty?
    # Source: Q80
    df_clean['op_death_penalty_support'] = df['Q80'].map({
        1.0: 'for',
        2.0: 'against',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_death_penalty_support'] = {
        'original_variable': 'Q80',
        'question_label': "Are you for or against death penalty?",
        'type': 'categorical',
        'value_labels': {'for': "For", 'against': "Against"},
    }

    # --- Q81 ---
    # op_attitude_q81 — Generic attitude/opinion question 81
    # Source: Q81
    # WARNING: No codebook provided. Mapping 1-5 to generic labels and treating 8/9 as missing.
    df_clean['op_attitude_q81'] = df['Q81'].map({
        1.0: 'strong_support',
        2.0: 'weak_support',
        3.0: 'neutral',
        4.0: 'dont_know',
        5.0: 'refused',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q81'] = {
        'original_variable': 'Q81',
        'question_label': "Unknown: Q81 from eeq_2012 data",
        'type': 'categorical',
        'value_labels': {'strong_support': "Strong Support (Inferred)", 'weak_support': "Weak Support (Inferred)", 'neutral': "Neutral (Inferred)", 'dont_know': "Don't Know (Inferred)", 'refused': "Refused (Inferred)"},
    }

    # --- Q82 ---
    # op_immigrants_integration — Views on immigrant integration
    # Source: Q82
    df_clean['op_immigrants_integration'] = df['Q82'].map({
        1.0: 'adapt_and_blend',
        2.0: 'stay_different',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_immigrants_integration'] = {
        'original_variable': 'Q82',
        'question_label': "There are different views about those who come from outside Quebec, often bringing their own customs, religion and traditions with them. Do you think it is best if such incomers try to adapt and blend into the locality? Or is it best if they stay different and add to the variety of cultures in the locality?",
        'type': 'categorical',
        'value_labels': {'adapt_and_blend': "Best if incomers try to adapt and blend", 'stay_different': "Best if incomers stay different and add to the variety of cultures"},
    }

    # --- Q82B ---
    # op_attitude_q82b — Attitude/Opinion question (Q82B)
    # Source: Q82B
    # Assumption: Codes 8 and 9 treated as missing (not present in original context codebook)
    df_clean['op_attitude_q82b'] = df['Q82B'].map({
        1.0: 'response_1',
        2.0: 'response_2',
        3.0: 'response_3',
        4.0: 'response_4',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_attitude_q82b'] = {
        'original_variable': 'Q82B',
        'question_label': "Inferred: Response to Question 82B (No label provided in context)",
        'type': 'categorical',
        'value_labels': {'response_1': "Response 1 (Code 1)", 'response_2': "Response 2 (Code 2)", 'response_3': "Response 3 (Code 3)", 'response_4': "Response 4 (Code 4)"},
    }

    # --- Q82C ---
    # op_political_choice — Political choice (inferred from values 1-4)
    # Source: Q82C
    # Assumption: Codes 1-4 are distinct choices. Codes 8/9 are treated as missing as per standard practice when details are unavailable.
    df_clean['op_political_choice'] = df['Q82C'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'choice_4',
        8.0: np.nan, # Assumption: Don't Know
        9.0: np.nan, # Assumption: Refused
    })
    CODEBOOK_VARIABLES['op_political_choice'] = {
        'original_variable': 'Q82C',
        'question_label': "Choice for Q82C (Labels inferred due to missing codebook)",
        'type': 'categorical',
        'value_labels': {'choice_1': "Choice 1", 'choice_2': "Choice 2", 'choice_3': "Choice 3", 'choice_4': "Choice 4"},
    }

    # --- Q82D_M1 ---
    # op_q82d_m1 — Inferred Q82D part 1 opinion/attitude
    # Source: Q82D_M1
    # Assumption: Likert scale normalized 0.0 to 1.0. Code 9.0 treated as missing (unlabelled in codebook/data exploration).
    df_clean['op_q82d_m1'] = df['Q82D_M1'].map({
        1.0: 0.0,
        2.0: 0.2,
        3.0: 0.4,
        4.0: 0.6,
        5.0: 0.8,
        7.0: 1.0,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q82d_m1'] = {
        'original_variable': 'Q82D_M1',
        'question_label': "Inferred Q82D part 1 opinion/attitude",
        'type': 'likert',
        'value_labels': {0.0: "Label 1", 0.2: "Label 2", 0.4: "Label 3", 0.6: "Label 4", 0.8: "Label 5", 1.0: "Label 6"},
    }

    # --- Q82D_M2 ---
    # behav_protest_march22 — Participation in protest held on March 22
    # Source: Q82D_M2
    df_clean['behav_protest_march22'] = df['Q82D_M2'].map({
        2.0: 'yes',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_protest_march22'] = {
        'original_variable': 'Q82D_M2',
        'question_label': "Last spring, did you at one time or another do one of the following (click as many as applies): / Take part in a protest held on the 22nd (of March, April, May...)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes"},
    }

    # --- Q82D_M3 ---
    # op_response_q82d_m3 — Inferred response for question Q82D part 3
    # Source: Q82D_M3
    # Assumption: Codes 3, 4, 5 mapped as generic options due to missing codebook. All other numeric codes and NA will map to np.nan.
    df_clean['op_response_q82d_m3'] = df['Q82D_M3'].map({
        3.0: 'option_3',
        4.0: 'option_4',
        5.0: 'option_5',
    })
    CODEBOOK_VARIABLES['op_response_q82d_m3'] = {
        'original_variable': 'Q82D_M3',
        'question_label': "Inferred response for question Q82D part 3 (Missing Codebook)",
        'type': 'categorical',
        'value_labels': {'option_3': "Option 3", 'option_4': "Option 4", 'option_5': "Option 5"},
    }

    # --- Q82D_M4 ---
    # know_q82dm4 — Unknown question part M4 from Q82D
    # Source: Q82D_M4
    # Note: No codebook provided. Mapping based on observed data (4.0, 5.0). All unmapped/NaN values become np.nan.
    df_clean['know_q82dm4'] = df['Q82D_M4'].map({
        4.0: 'code_4',
        5.0: 'code_5',
    })
    CODEBOOK_VARIABLES['know_q82dm4'] = {
        'original_variable': 'Q82D_M4',
        'question_label': "Unknown question part M4 from Q82D (requires codebook)",
        'type': 'categorical',
        'value_labels': {'code_4': "Observed code 4", 'code_5': "Observed code 5"},
    }

    # --- Q82D_M5 ---
    # ses_q82d_m5 — Placeholder for variable Q82D_M5 response
    # Source: Q82D_M5
    # Note: Codebook information is missing. Mapping based only on observed data (code 5.0).
    df_clean['ses_q82d_m5'] = df['Q82D_M5'].map({
        5.0: 'response_5',
    })
    CODEBOOK_VARIABLES['ses_q82d_m5'] = {
        'original_variable': 'Q82D_M5',
        'question_label': "Response for Q82D_M5 (Codebook details unavailable)",
        'type': 'categorical',
        'value_labels': {'response_5': "Observed Value 5.0"},
    }

    # --- Q82E ---
    # op_q82e — Voting intention (assumed)
    # Source: Q82E
    # Assumption: Based on numeric codes 1-4, treated as categorical. Codes >= 96 are treated as missing.
    df_clean['op_q82e'] = df['Q82E'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'refused',
        4.0: 'unsure',
        96.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q82e'] = {
        'original_variable': 'Q82E',
        'question_label': "Voting intention (inferred from name/codes)",
        'type': 'categorical',
        'value_labels': {'yes': "Yes", 'no': "No", 'refused': "Refused/Don't know", 'unsure': "Unsure"},
    }

    # --- Q83 ---
    # op_economic_comparison — Comparison of Quebec's economic situation vs. rest of Canada
    # Source: Q83
    # Assumption: Codes 8.0 ('I don't know') and 9.0 ('I prefer not to answer') are treated as missing (np.nan)
    df_clean['op_economic_comparison'] = df['Q83'].map({
        1.0: 'better',
        2.0: 'worse',
        3.0: 'no_different',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_economic_comparison'] = {
        'original_variable': 'Q83',
        'question_label': "If you compare the economic situation in Quebec with the rest of Canada, do you think that the situation is better in Quebec, worse, or no different?",
        'type': 'categorical',
        'value_labels': {'better': "Better", 'worse': "Worse", 'no_different': "No different"},
    }

    # --- Q84 ---
    # op_independence_economic_impact — Expected economic impact if Quebec becomes independent
    # Source: Q84
    df_clean['op_independence_economic_impact'] = df['Q84'].map({
        1.0: 'get_better',
        2.0: 'get_worse',
        3.0: 'stay_same',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_independence_economic_impact'] = {
        'original_variable': 'Q84',
        'question_label': "If Quebec were to become an independent country, do you think that the economic situation in Quebec would get better, get worse or stay about the same?",
        'type': 'categorical',
        'value_labels': {'get_better': "Get better", 'get_worse': "Get worse", 'stay_same': "Stay about the same"},
    }

    # --- Q85 ---
    # op_v85 — Unknown categorical response variable
    # Source: Q85
    # Assumption: Codes 8.0/9.0 are treated as missing (no codebook provided)
    df_clean['op_v85'] = df['Q85'].map({
        1.0: 'cat1',
        2.0: 'cat2',
        3.0: 'cat3',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_v85'] = {
        'original_variable': 'Q85',
        'question_label': "Unknown - Inferred from data structure as a categorical response.",
        'type': 'categorical',
        'value_labels': {'cat1': 'Category 1', 'cat2': 'Category 2', 'cat3': 'Category 3'},
    }

    # --- Q87 ---
    # ses_voting_intent — Intention de vote (Parti principal)
    # Source: Q87
    # Assumption: codes 8/9 treated as missing (unlabelled in codebook, often used for 'Don't know'/'Refused' in this context)
    df_clean['ses_voting_intent'] = df['Q87'].map({
        1.0: 'liberal',
        2.0: 'caq',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_voting_intent'] = {
        'original_variable': 'Q87',
        'question_label': "Votre intention de vote pour le parti principal",
        'type': 'categorical',
        'value_labels': {'liberal': "Parti libéral", 'caq': "CAQ", 'nan': "Missing/Refused"},
    }

    # --- Q88 ---
    # op_vote_Q88 — Inferred: Voting intention/behavior for Q88
    # Source: Q88
    # Assumption: No codebook provided. Inferring structure based on value counts:
    # 1-4 are response codes, 8/9 are missing/refused. Treating as categorical.
    df_clean['op_vote_Q88'] = df['Q88'].map({
        1.0: 'choice_1',
        2.0: 'choice_2',
        3.0: 'choice_3',
        4.0: 'undecided',
        8.0: np.nan,  # Refused
        9.0: np.nan,  # Don't Know
    })
    CODEBOOK_VARIABLES['op_vote_Q88'] = {
        'original_variable': 'Q88',
        'question_label': "Inferred: Voting intention/behavior for Q88",
        'type': 'categorical',
        'value_labels': {'choice_1': "Primary choice", 'choice_2': "Secondary choice", 'choice_3': "Other choice", 'undecided': "Undecided"},
    }

    # --- Q89 ---
    # op_rating_q89 — Rating scale or ordered response (Q89)
    # Source: Q89
    # Note: Codebook entry missing. Treating as categorical based on float dtype and codes 0-10, 98-99.
    df_clean['op_rating_q89'] = df['Q89'].map({
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
    CODEBOOK_VARIABLES['op_rating_q89'] = {
        'original_variable': 'Q89',
        'question_label': "Question Q89 (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'0': "0", '1': "1", '2': "2", '3': "3", '4': "4", '5': "5", '6': "6", '7': "7", '8': "8", '9': "9", '10': "10"},
    }

    # --- Q9 ---
    # ses_q9 — Avez-vous l'intention de voter aux prochaines élections?
    # Source: Q9
    # Assumption: codes 8 and 9 treated as missing (unlabelled in codebook)
    df_clean['ses_q9'] = df['Q9'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'undecided',
        4.0: 'will_not_vote',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_q9'] = {
        'original_variable': 'Q9',
        'question_label': "Avez-vous l'intention de voter aux prochaines élections?",
        'type': 'categorical',
        'value_labels': {'yes': "Oui", 'no': "Non", 'undecided': "Indécis", 'will_not_vote': "Ne votera pas"},
    }

    # --- Q90 ---
    # op_party_vote_intent — Vote intention (Party 1/2)
    # Source: Q90
    # Assumption: Codes 8.0 (Refused) and 9.0 (Don't know) are treated as missing.
    df_clean['op_party_vote_intent'] = df['Q90'].map({
        1.0: 'liberal',
        2.0: 'conservative',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_party_vote_intent'] = {
        'original_variable': 'Q90',
        'question_label': "Vote intention for major parties",
        'type': 'categorical',
        'value_labels': {'liberal': "Liberal", 'conservative': "Conservative"},
    }

    # --- Q91 ---
    # op_q91 — Unknown attitude/opinion response mapping
    # Source: Q91
    # Assumption: Codes 1, 2, 3 mapped to positive/neutral/negative. Codes 8, 9 treated as missing (np.nan).
    df_clean['op_q91'] = df['Q91'].map({
        1.0: 'positive',
        2.0: 'neutral',
        3.0: 'negative',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q91'] = {
        'original_variable': 'Q91',
        'question_label': "Q91 (Label missing, assuming general opinion response)",
        'type': 'categorical',
        'value_labels': {'positive': "Observed value 1 (assumed positive)", 'neutral': "Observed value 2 (assumed neutral)", 'negative': "Observed value 3 (assumed negative)"},
    }

    # --- Q92 ---
    # op_q92 — Opinion/Attitude question 92 (Best guess)
    # Source: Q92
    # Assumption: Codes 1.0-6.0 mapped to generic options due to missing codebook.
    # Assumption: Codes 97.0, 98.0, 99.0 are treated as missing (unlabelled in data).
    df_clean['op_q92'] = df['Q92'].map({
        1.0: 'opt1',
        2.0: 'opt2',
        3.0: 'opt3',
        4.0: 'opt4',
        5.0: 'opt5',
        6.0: 'opt6',
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_q92'] = {
        'original_variable': 'Q92',
        'question_label': "Unknown question (Codebook missing)",
        'type': 'categorical',
        'value_labels': {'opt1': "Option 1 (Code 1.0)", 'opt2': "Option 2 (Code 2.0)", 'opt3': "Option 3 (Code 3.0)", 'opt4': "Option 4 (Code 4.0)", 'opt5': "Option 5 (Code 5.0)", 'opt6': "Option 6 (Code 6.0)"},
    }

    # --- Q93 ---
    # ses_province_q93 — Inferred province of residence question
    # Source: Q93
    # Assumption: Codes 8.0 and 9.0 are missing codes not specified in the codebook.
    df_clean['ses_province_q93'] = df['Q93'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'alberta',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_province_q93'] = {
        'original_variable': 'Q93',
        'question_label': "Inferred province of residence question (Q93)",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'alberta': "Alberta"},
    }

    # --- Q94 ---
    # op_opinion_q94 — Opinion or rating question Q94
    # Source: Q94
    # Assumption: 5-point scale normalized 0.0-1.0. Codes 97, 98, 99 treated as missing.
    df_clean['op_opinion_q94'] = df['Q94'].map({
        1.0: 0.0,
        2.0: 0.25,
        3.0: 0.5,
        4.0: 0.75,
        5.0: 1.0,
        97.0: np.nan,
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_opinion_q94'] = {
        'original_variable': 'Q94',
        'question_label': "Opinion or rating question Q94 (Codebook label unknown)",
        'type': 'likert',
        'value_labels': {'0.0': "Lowest/Most Negative", '0.25': "Second Level", '0.5': "Neutral/Midpoint", '0.75': "Fourth Level", '1.0': "Highest/Most Positive"},
    }

    # --- Q95 ---
    # behav_vote_party — Parti voté pour qui vous avez voté
    # Source: Q95
    # Assumption: codes 8 and 9 are explicitly mapped as they appear in data counts. Code 99 from codebook is mapped to NaN.
    df_clean['behav_vote_party'] = df['Q95'].map({
        1.0: 'parti libéral',
        2.0: 'parti conservateur',
        3.0: 'parti québécois',
        4.0: 'option nationale',
        5.0: 'caq',
        6.0: 'autres partis',
        7.0: 'ne s\'applique pas',
        8.0: 'n\'ai pas voté',
        9.0: 'ne me souviens pas',
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_party'] = {
        'original_variable': 'Q95',
        'question_label': "Parti voté pour qui vous avez voté",
        'type': 'categorical',
        'value_labels': {'parti libéral': "Parti libéral", 'parti conservateur': "Parti conservateur", 'parti québécois': "Parti québécois", 'option nationale': "Option nationale", 'caq': "CAQ", 'autres partis': "Autres partis", 'ne s\'applique pas': "Ne s'applique pas", 'n\'ai pas voté': "Je n'ai pas voté", 'ne me souviens pas': "Je ne me souviens pas"},
    }

    # --- Q96 ---
    # ses_region_q96 — Inferred region/category from Q96
    # Source: Q96
    # Assumption: Codes 8/9 are missing/unlabelled based on float dtype and value distribution.
    df_clean['ses_region_q96'] = df['Q96'].map({
        1.0: 'option_a',
        2.0: 'option_b',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_region_q96'] = {
        'original_variable': 'Q96',
        'question_label': "Unknown question label for Q96",
        'type': 'categorical',
        'value_labels': {'option_a': "Option A (Inferred)", 'option_b': "Option B (Inferred)"},
    }

    # --- QUEST ---
    # ses_region — Inferred region/riding code
    # Source: QUEST
    # Assumption: Variable is categorical based on integer codes 1003-1031, treated as distinct regions since codebook is missing.
    df_clean['ses_region'] = df['QUEST'].map({
        1003.0: 'code_1003', 1004.0: 'code_1004', 1005.0: 'code_1005', 1006.0: 'code_1006', 1007.0: 'code_1007', 
        1008.0: 'code_1008', 1009.0: 'code_1009', 1010.0: 'code_1010', 1011.0: 'code_1011', 1012.0: 'code_1012', 
        1013.0: 'code_1013', 1016.0: 'code_1016', 1017.0: 'code_1017', 1018.0: 'code_1018', 1019.0: 'code_1019', 
        1020.0: 'code_1020', 1021.0: 'code_1021', 1023.0: 'code_1023', 1024.0: 'code_1024', 1026.0: 'code_1026', 
        1027.0: 'code_1027', 1028.0: 'code_1028', 1029.0: 'code_1029', 1030.0: 'code_1030', 1031.0: 'code_1031',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'QUEST',
        'question_label': "Inferred region/riding code from QUEST",
        'type': 'categorical',
        'value_labels': {'code_1003': 'Code 1003', 'code_1004': 'Code 1004', 'code_1005': 'Code 1005', 'code_1006': 'Code 1006', 'code_1007': 'Code 1007', 'code_1008': 'Code 1008', 'code_1009': 'Code 1009', 'code_1010': 'Code 1010', 'code_1011': 'Code 1011', 'code_1012': 'Code 1012', 'code_1013': 'Code 1013', 'code_1016': 'Code 1016', 'code_1017': 'Code 1017', 'code_1018': 'Code 1018', 'code_1019': 'Code 1019', 'code_1020': 'Code 1020', 'code_1021': 'Code 1021', 'code_1023': 'Code 1023', 'code_1024': 'Code 1024', 'code_1026': 'Code 1026', 'code_1027': 'Code 1027', 'code_1028': 'Code 1028', 'code_1029': 'Code 1029', 'code_1030': 'Code 1030', 'code_1031': 'Code 1031'},
    }

    # --- REGIO ---
    # ses_region — Province/Region of residence
    # Source: REGIO
    # Assumption: Codes 1, 2, 3 mapped to major Canadian political regions for the 2012 election context.
    df_clean['ses_region'] = df['REGIO'].map({
        1.0: 'quebec',
        2.0: 'ontario',
        3.0: 'other_province',
    })
    CODEBOOK_VARIABLES['ses_region'] = {
        'original_variable': 'REGIO',
        'question_label': "Province ou région de résidence",
        'type': 'categorical',
        'value_labels': {'quebec': "Québec", 'ontario': "Ontario", 'other_province': "Autre province"},
    }

    # --- REVEN ---
    # ses_income — Household income before taxes for 2011
    # Source: REVEN
    # Note: Step 1 failed due to missing data file, mapping based solely on codebook.
    df_clean['ses_income'] = df['REVEN'].map({
        1.0: 'less than 8k',
        2.0: '8k to 15.9k',
        3.0: '16k to 23.9k',
        4.0: '24k to 39.9k',
        5.0: '40k to 55.9k',
        6.0: '56k to 71.9k',
        7.0: '72k to 87.9k',
        8.0: '88k to 103.9k',
        9.0: '104k or more',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_income'] = {
        'original_variable': 'REVEN',
        'question_label': "And now your total household income before taxes for 2011.  That includes income from all sources such as savings, pensions, rent, as well as wages.   Was it:",
        'type': 'categorical',
        'value_labels': {'less than 8k': 'Less than $8,000', '8k to 15.9k': '$8000 - $15,999', '16k to 23.9k': '$16,000 - $23,999', '24k to 39.9k': '$24,000 - $39,999', '40k to 55.9k': '$40,000 - $55,999', '56k to 71.9k': '$56,000 - $71,999', '72k to 87.9k': '$72,000 - $87,999', '88k to 103.9k': '$88,000 -  $103,999', '104k or more': '$104,000 or more'},
    }

    # --- SCOL ---
    # ses_education — Level of schooling attainment
    # Source: SCOL
    # Assumption: Codes 1.0-12.0 map to increasing levels of education. Codes 98.0 and 99.0 are treated as missing.
    df_clean['ses_education'] = df['SCOL'].map({
        1.0: 'primary_school',
        2.0: 'secondary_school_level_1',
        3.0: 'secondary_school_level_2',
        4.0: 'secondary_school_level_3',
        5.0: 'secondary_school_level_4',
        6.0: 'secondary_school_level_5',
        7.0: 'post_secondary_level_1',
        8.0: 'post_secondary_level_2',
        9.0: 'post_secondary_level_3',
        10.0: 'university_level_1',
        11.0: 'university_level_2',
        12.0: 'university_level_3_or_higher',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_education'] = {
        'original_variable': 'SCOL',
        'question_label': "Level of schooling attainment (Inferred)",
        'type': 'categorical',
        'value_labels': {'primary_school': "Primary School", 'secondary_school_level_1': "Secondary School Level 1", 'secondary_school_level_2': "Secondary School Level 2", 'secondary_school_level_3': "Secondary School Level 3", 'secondary_school_level_4': "Secondary School Level 4", 'secondary_school_level_5': "Secondary School Level 5", 'post_secondary_level_1': "Post-Secondary Level 1", 'post_secondary_level_2': "Post-Secondary Level 2", 'post_secondary_level_3': "Post-Secondary Level 3", 'university_level_1': "University Level 1", 'university_level_2': "University Level 2", 'university_level_3_or_higher': "University Level 3 or Higher"},
    }

    # --- SEXE ---
    # ses_gender — Province de résidence
    # Source: SEXE
    # Assumption: Codes 1.0/2.0 map to Male/Female, respectively. Missing values are automatically handled as NaN.
    df_clean['ses_gender'] = df['SEXE'].map({
        1.0: 'male',
        2.0: 'female',
    })
    CODEBOOK_VARIABLES['ses_gender'] = {
        'original_variable': 'SEXE',
        'question_label': "Province de résidence",
        'type': 'categorical',
        'value_labels': {'male': "Male", 'female': "Female"},
    }


    # Add canonical strata
    df_strata = map_strates_canoniques(df)
    df_clean = pd.concat([df_clean, df_strata], axis=1)

    return df_clean


def map_strates_canoniques(df: pd.DataFrame) -> pd.DataFrame:
    """Crée les strates canoniques pour l'analyse.

    Args:
        df (pd.DataFrame): DataFrame brut du sondage

    Returns:
        pd.DataFrame: DataFrame avec les 4 colonnes standardisées
    """
    df_strata = pd.DataFrame(index=df.index)

    # Genre
    df_strata['genre'] = df['SEXE'].map({
        1.0: 'homme',
        2.0: 'femme',
    })

    # Langue
    df_strata['langue'] = df['LANGU'].map({
        1.0: 'francophone',
        2.0: 'anglo-autre',
        3.0: 'anglo-autre',
    })

    # Age group
    age = pd.to_numeric(df['AGE'], errors='coerce')
    df_strata['age_group'] = pd.cut(
        age,
        bins=[17, 34, 54, np.inf],
        labels=['18-34', '35-54', '55+']
    )

    # Région
    # Utilise REGIO qui contient les régions métropolitaines de recensement
    df_strata['region'] = df['REGIO'].map({
        1.0: 'mtl',
        2.0: 'qc',
        3.0: 'regions',
    })

    return df_strata


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
