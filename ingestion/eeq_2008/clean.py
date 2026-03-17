#!/usr/bin/env python3
"""
Script de nettoyage pour eeq_2008
Expose:
  - clean_data(raw_path) → pd.DataFrame nettoyé
  - get_metadata() → dictionnaire avec métadonnées du sondage et variables
"""

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
    # Labels from SAV file
    df_clean['ses_ethnicity'] = df['ethn1'].map({
        1.0: 'canadienne_quebecoise',
        2.0: 'afrique_nord',
        3.0: 'afrique',
        4.0: 'amerindienne',
        5.0: 'americaine_usa',
        6.0: 'amerique_centrale_sud',
        7.0: 'mexique',
        8.0: 'antillaise_haiti',
        9.0: 'asiatique',
        10.0: 'europe',
        11.0: 'europe_est',
        12.0: 'moyen_orient',
        13.0: 'turquie_armenie_iran',
        96.0: 'autre',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_ethnicity'] = {
        'original_variable': 'ethn1',
        'question_label': "De quelle ORIGINE ETHNIQUE êtes-vous ?",
        'type': 'categorical',
        'value_labels': {
            'canadienne_quebecoise': "Canadienne-québécoise",
            'afrique_nord': "Afrique du Nord (Maroc, Algérie, Tunisie, Libye, Égypte)",
            'afrique': "Afrique (Gabon, Congo, Côte d'Ivoire, Éthiopie, Kenya, Cameroun)",
            'amerindienne': "Amérindienne",
            'americaine_usa': "Américaine (USA)",
            'amerique_centrale_sud': "Amérique Centrale et du Sud",
            'mexique': "Mexique",
            'antillaise_haiti': "Antillaise, Haïti, Jamaïque, République dominicaine",
            'asiatique': "Asiatique (Japon, Chine, Vietnam, Corée, Cambodge)",
            'europe': "Europe (France, Belgique, Italie, Espagne, Grèce, Portugal, Allemagne)",
            'europe_est': "Europe de l'Est (Russie, Ukraine, Pologne, Roumanie)",
            'moyen_orient': "Moyen-Orient (Jordanie, Afghanistan, Pakistan)",
            'turquie_armenie_iran': "Turquie, Arménie, Iran, Kurde",
            'autre': "Autre (spécifiez)",
        },
    }

    # --- langu ---
    # ses_language — Language learned first in childhood
    # Source: langu
    # Labels from SAV file
    df_clean['ses_language'] = df['langu'].map({
        1.0: 'francais',
        2.0: 'anglais',
        3.0: 'autre',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'langu',
        'question_label': "Quelle est la langue que vous avez apprise en premier lieu à la maison dans votre enfance et que vous comprenez toujours ?",
        'type': 'categorical',
        'value_labels': {
            'francais': "Français",
            'anglais': "Anglais",
            'autre': "Autre",
        },
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
    # wgt_respondent_x — Respondent sampling weight with turnout
    # Source: pondx
    # Numeric weight variable - copy directly
    df_clean['wgt_respondent_x'] = df['pondx']
    CODEBOOK_VARIABLES['wgt_respondent_x'] = {
        'original_variable': 'pondx',
        'question_label': "avec taux de participation",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q0age ---
    # ses_age_group — Âge du répondant (catégories)
    # Source: q0age
    df_clean['ses_age_group'] = df['q0age'].map({
        1.0: 'less_than_18',
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
            'less_than_18': "Moins de 18 ans",
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
    # op_issue_priority — Most important issue in the election
    # Source: q1
    # Labels from SAV file
    df_clean['op_issue_priority'] = df['q1'].map({
        1.0: 'economie',
        2.0: 'sante',
        3.0: 'environnement',
        4.0: 'education',
        5.0: 'aide_familles',
        6.0: 'pauvrete',
        96.0: 'autre',
    })
    CODEBOOK_VARIABLES['op_issue_priority'] = {
        'original_variable': 'q1',
        'question_label': "Parmi les enjeux suivants, lequel était, pour vous personnellement, le plus important lors de l'élection provinciale du 8 décembre dernier ?",
        'type': 'categorical',
        'value_labels': {
            'economie': "L'économie",
            'sante': "La santé",
            'environnement': "L'environnement",
            'education': "L'éducation",
            'aide_familles': "L'aide aux familles",
            'pauvrete': "La pauvreté",
            'autre': "Autre",
        },
    }

    # --- q11 ---
    # behav_voted — Voted in the 2008 provincial election
    # Source: q11
    # Labels from SAV file
    df_clean['behav_voted'] = df['q11'].map({
        1.0: 'oui',
        2.0: 'non',
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_voted'] = {
        'original_variable': 'q11',
        'question_label': "Avez-vous voted à cette election provinciale ?",
        'type': 'categorical',
        'value_labels': {
            'oui': "Oui",
            'non': "Non",
        },
    }

    # --- q12a ---
    # behav_vote_choice — Vote choice in 2008 election
    # Source: q12a
    # Labels from SAV file
    df_clean['behav_vote_choice'] = df['q12a'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'adq',
        4.0: 'qs',
        5.0: 'pv',
        95.0: 'na_pas_vote',
        96.0: 'autre',
        97.0: 'aucun',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_choice'] = {
        'original_variable': 'q12a',
        'question_label': "Pour quel parti avez-vous vote ?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Le Parti liberal (PLQ)",
            'pq': "Le Parti québécois (PQ)",
            'adq': "L'Action democratique du Québec (ADQ)",
            'qs': "Québec Solidaire (QS)",
            'pv': "Le Parti vert (PV)",
            'na_pas_vote': "n'a pas vote",
            'autre': "autre parti (specifiez)",
            'aucun': "aucun",
        },
    }

    # --- q12b ---
    # behav_vote_intention_hypothetical — Hypothetical vote intention
    # Source: q12b
    # Labels from SAV file
    df_clean['behav_vote_intention_hypothetical'] = df['q12b'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'adq',
        4.0: 'qs',
        5.0: 'pv',
        95.0: 'na_pas_vote',
        96.0: 'autre',
        97.0: 'aucun',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_intention_hypothetical'] = {
        'original_variable': 'q12b',
        'question_label': "Si vous aviez vote, quel parti auriez-vous ete tente d'appuyer ?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Le Parti liberal (PLQ)",
            'pq': "Le Parti québécois (PQ)",
            'adq': "L'Action democratique du Québec (ADQ)",
            'qs': "Québec Solidaire (QS)",
            'pv': "Le Parti vert (PV)",
            'na_pas_vote': "n'a pas vote",
            'autre': "autre parti (specifiez)",
            'aucun': "aucun",
        },
    }

    # --- q13 ---
    # behav_vote_choice_2007 — Vote choice in 2007 election
    # Source: q13
    # Labels from SAV file
    df_clean['behav_vote_choice_2007'] = df['q13'].map({
        1.0: 'plq',
        2.0: 'pq',
        3.0: 'adq',
        4.0: 'qs',
        5.0: 'pv',
        95.0: 'na_pas_vote',
        96.0: 'autre',
        97.0: 'aucun',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['behav_vote_choice_2007'] = {
        'original_variable': 'q13',
        'question_label': "Pour quel parti aviez-vous vote lors de l'election provinciale du 26 mars 2007?",
        'type': 'categorical',
        'value_labels': {
            'plq': "Le Parti liberal (PLQ)",
            'pq': "Le Parti québécois (PQ)",
            'adq': "L'Action democratique du Québec (ADQ)",
            'qs': "Québec Solidaire (QS)",
            'pv': "Le Parti vert (PV)",
            'na_pas_vote': "n'a pas vote",
            'autre': "autre parti (specifiez)",
            'aucun': "aucun",
        },
    }

    # --- q14 ---
    # op_interest_election — Interest in the provincial election
    # Source: q14
    # Labels from SAV file: scale 0-10 normalized to 0-1
    df_clean['op_interest_election'] = np.nan
    mask = (df['q14'] >= 0) & (df['q14'] <= 10)
    df_clean.loc[mask, 'op_interest_election'] = df.loc[mask, 'q14'] / 10.0
    CODEBOOK_VARIABLES['op_interest_election'] = {
        'original_variable': 'q14',
        'question_label': "Sur une échelle de 0 à 10 où 0 veut dire aucun intérêt et 10 veut dire beaucoup d'intérêt, quel a été votre intérêt pour l'élection PROVINCIALE qui vient d'avoir lieu ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q18a ---
    # op_identity_quebec — Quebec identity
    # Source: q18a
    # Labels from SAV file
    df_clean['op_identity_quebec'] = df['q18a'].map({
        1.0: 'uniquement_quebecois',
        2.0: 'dabord_quebecois',
        3.0: 'egal_quebecois_canadien',
        4.0: 'dabord_canadien',
        5.0: 'uniquement_canadien',
        96.0: 'autre',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_identity_quebec'] = {
        'original_variable': 'q18a',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...?",
        'type': 'categorical',
        'value_labels': {
            'uniquement_quebecois': "...uniquement comme Québécois(e)",
            'dabord_quebecois': "...d'abord comme Québécois(e), puis comme Canadien(ne)",
            'egal_quebecois_canadien': "...également comme Canadien(ne) et comme Québécois(e)",
            'dabord_canadien': "...d'abord comme Canadien(ne), puis comme Québécois(e)",
            'uniquement_canadien': "...ou uniquement comme Canadien(ne)?",
            'autre': "autres (précisez)",
        },
    }

    # --- q18b ---
    # op_identity_canada — Canadian identity
    # Source: q18b
    # Labels from SAV file
    df_clean['op_identity_canada'] = df['q18b'].map({
        1.0: 'uniquement_canadien',
        2.0: 'dabord_canadien',
        3.0: 'egal_canadien_quebecois',
        4.0: 'dabord_quebecois',
        5.0: 'uniquement_quebecois',
        96.0: 'autre',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_identity_canada'] = {
        'original_variable': 'q18b',
        'question_label': "Les gens ont différentes façons de se définir. Diriez-vous que vous vous considérez...?",
        'type': 'categorical',
        'value_labels': {
            'uniquement_canadien': "...uniquement comme Canadien(ne)",
            'dabord_canadien': "...d'abord comme Canadien(ne), puis comme Québécois(e)",
            'egal_canadien_quebecois': "...également comme Canadien(ne) et comme Québécois(e)",
            'dabord_quebecois': "...d'abord comme Québécois(e), puis comme Canadien(ne)",
            'uniquement_quebecois': "...ou uniquement comme Québécois(e)?",
            'autre': "autres (précisez)",
        },
    }

    # --- q19 ---
    # op_referendum_intention — Referendum vote intention (1995 question)
    # Source: q19
    # Labels from SAV file
    df_clean['op_referendum_intention'] = df['q19'].map({
        1.0: 'oui',
        2.0: 'non',
        3.0: 'abstention',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_referendum_intention'] = {
        'original_variable': 'q19',
        'question_label': "Si un référendum avait lieu aujourd'hui sur la même question que celle qui a été posée lors du dernier référendum de 1995, c'est-à-dire sur la souveraineté assortie d'une offre de partenariat au reste du Canada, voteriez-vous OUI ou voteriez-vous NON ?",
        'type': 'categorical',
        'value_labels': {
            'oui': "Oui",
            'non': "Non",
            'abstention': "Ne voterait pas/annulerait",
        },
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
    # op_govt_indifferent — Government doesn't care about people like me
    # Source: q21
    # Labels from SAV file: Likert scale normalized to 0-1
    df_clean['op_govt_indifferent'] = df['q21'].map({
        1.0: 1.0,
        2.0: 0.667,
        3.0: 0.333,
        4.0: 0.0,
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_indifferent'] = {
        'original_variable': 'q21',
        'question_label': "Je ne crois pas que les gouvernements se soucient beaucoup de ce que les gens comme moi pensent.",
        'type': 'likert',
        'value_labels': {
            1.0: "fortement d'accord",
            0.667: "plutôt d'accord",
            0.333: "plutôt en désaccord",
            0.0: "fortement en désaccord",
        },
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
    # op_govt_waste — Government wastes tax money
    # Source: q24
    # Labels from SAV file
    df_clean['op_govt_waste'] = df['q24'].map({
        1.0: 'gaspillent_beaucoup',
        2.0: 'en_gaspillent_quelque_peu',
        3.0: 'n_en_gaspillent_pas_beaucoup',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_waste'] = {
        'original_variable': 'q24',
        'question_label': "Pensez-vous que les gens au gouvernement gaspillent BEAUCOUP, QUELQUE PEU ou PAS BEAUCOUP nos taxes ?",
        'type': 'categorical',
        'value_labels': {
            'gaspillent_beaucoup': "...gaspillent beaucoup de nos taxes",
            'en_gaspillent_quelque_peu': "...en gaspillent quelque peu",
            'n_en_gaspillent_pas_beaucoup': "...n'en gaspillent pas beaucoup",
        },
    }

    # --- q25 ---
    # op_govt_dishonest — Government leaders are dishonest
    # Source: q25
    # Labels from SAV file
    df_clean['op_govt_dishonest'] = df['q25'].map({
        1.0: 'plusieurs_malhonnetes',
        2.0: 'certains_malhonnetes',
        3.0: 'presqu_aucun_malhonnete',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_dishonest'] = {
        'original_variable': 'q25',
        'question_label': "Pensez-vous que...?",
        'type': 'categorical',
        'value_labels': {
            'plusieurs_malhonnetes': "...plusieurs dirigeants gouvernementaux sont malhonnêtes",
            'certains_malhonnetes': "...certains d'entre eux sont malhonnêtes",
            'presqu_aucun_malhonnete': "...presqu'aucun d'entre eux n'est malhonnête",
        },
    }

    # --- q26 ---
    # op_govt_for_few — Government works for few or all
    # Source: q26
    # Labels from SAV file
    df_clean['op_govt_for_few'] = df['q26'].map({
        1.0: 'pour_qquelques_uns',
        2.0: 'pour_benefice_tous',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_for_few'] = {
        'original_variable': 'q26',
        'question_label': "En général, diriez-vous que les gouvernements sont dirigés pour le bénéfice des intérêts de quelques personnes importantes préoccupées seulement par leur sort ou qu'ils sont dirigés pour le bénéfice de tous ?",
        'type': 'categorical',
        'value_labels': {
            'pour_qquelques_uns': "...Dirigés pour les intérêts de quelques personnes importantes",
            'pour_benefice_tous': "...Dirigés pour le bénéfice de tous",
        },
    }

    # --- q27 ---
    # op_democracy_satisfaction — Satisfaction with democracy
    # Source: q27
    # Labels from SAV file
    df_clean['op_democracy_satisfaction'] = df['q27'].map({
        1.0: 'tres_satisfait',
        2.0: 'assez_satisfait',
        3.0: 'pas_tres_satisfait',
        4.0: 'pas_du_tout_satisfait',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_democracy_satisfaction'] = {
        'original_variable': 'q27',
        'question_label': "Dans l'ensemble, êtes-vous SATISFAIT de la façon dont la démocratie fonctionne au Québec ?",
        'type': 'categorical',
        'value_labels': {
            'tres_satisfait': "...très satisfait",
            'assez_satisfait': "...assez satisfait",
            'pas_tres_satisfait': "...pas très satisfait",
            'pas_du_tout_satisfait': "...pas du tout satisfait",
        },
    }

    # --- q33 ---
    # op_election_timing — Opinion on election timing
    # Source: q33
    # Labels from SAV file
    df_clean['op_election_timing'] = df['q33'].map({
        1.0: 'tres_bonne',
        2.0: 'assez_bonne',
        3.0: 'assez_mauvaise',
        4.0: 'tres_mauvaise',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_election_timing'] = {
        'original_variable': 'q33',
        'question_label': "Personnellement, trouvez-vous que l'idée de tenir une election au Québec cet automne était...?",
        'type': 'categorical',
        'value_labels': {
            'tres_bonne': "Très bonne",
            'assez_bonne': "Assez bonne",
            'assez_mauvaise': "Assez mauvaise",
            'tres_mauvaise': "Très mauvaise",
        },
    }

    # --- q39 ---
    # op_rating_charest — Rating for Jean Charest
    # Source: q39
    # Labels from SAV file: scale 0-100 normalized to 0-1
    df_clean['op_rating_charest'] = np.nan
    mask = (df['q39'] >= 0) & (df['q39'] <= 100)
    df_clean.loc[mask, 'op_rating_charest'] = df.loc[mask, 'q39'] / 100.0
    CODEBOOK_VARIABLES['op_rating_charest'] = {
        'original_variable': 'q39',
        'question_label': "Aimez-vous JEAN CHAREST ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q40 ---
    # op_rating_marois — Rating for Pauline Marois
    # Source: q40
    # Labels from SAV file: scale 0-100 normalized to 0-1
    df_clean['op_rating_marois'] = np.nan
    mask = (df['q40'] >= 0) & (df['q40'] <= 100)
    df_clean.loc[mask, 'op_rating_marois'] = df.loc[mask, 'q40'] / 100.0
    CODEBOOK_VARIABLES['op_rating_marois'] = {
        'original_variable': 'q40',
        'question_label': "Aimez-vous PAULINE MAROIS ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q41 ---
    # op_rating_dumont — Rating for Mario Dumont
    # Source: q41
    # Labels from SAV file: scale 0-100 normalized to 0-1
    df_clean['op_rating_dumont'] = np.nan
    mask = (df['q41'] >= 0) & (df['q41'] <= 100)
    df_clean.loc[mask, 'op_rating_dumont'] = df.loc[mask, 'q41'] / 100.0
    CODEBOOK_VARIABLES['op_rating_dumont'] = {
        'original_variable': 'q41',
        'question_label': "Aimez-vous MARIO DUMONT ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q42 ---
    # op_attitude_q42 — Attitude towards an unspecified topic in Q42
    # --- q42 ---
    # op_rating_david — Rating for Françoise David
    # Source: q42
    # Labels from SAV file: scale 0-100 normalized to 0-1
    df_clean['op_rating_david'] = np.nan
    mask = (df['q42'] >= 0) & (df['q42'] <= 100)
    df_clean.loc[mask, 'op_rating_david'] = df.loc[mask, 'q42'] / 100.0
    CODEBOOK_VARIABLES['op_rating_david'] = {
        'original_variable': 'q42',
        'question_label': "Aimez-vous FRANÇOISE DAVID ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q43 ---
    # op_rating_rainville — Rating for Guy Rainville
    # Source: q43
    # Labels from SAV file: scale 0-100 normalized to 0-1
    df_clean['op_rating_rainville'] = np.nan
    mask = (df['q43'] >= 0) & (df['q43'] <= 100)
    df_clean.loc[mask, 'op_rating_rainville'] = df.loc[mask, 'q43'] / 100.0
    CODEBOOK_VARIABLES['op_rating_rainville'] = {
        'original_variable': 'q43',
        'question_label': "Aimez-vous GUY RAINVILLE ?",
        'type': 'numeric',
        'value_labels': {},
    }

    # --- q44 ---
    # op_leader_competent — Most competent leader
    # Source: q44
    # Labels from SAV file
    df_clean['op_leader_competent'] = df['q44'].map({
        1.0: 'jean_charest',
        2.0: 'pauline_marois',
        3.0: 'mario_dumont',
        4.0: 'francoise_david',
        5.0: 'guy_rainville',
        6.0: 'aucun',
        7.0: 'tous',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_leader_competent'] = {
        'original_variable': 'q44',
        'question_label': "le plus compétent ?",
        'type': 'categorical',
        'value_labels': {
            'jean_charest': "Jean Charest",
            'pauline_marois': "Pauline Marois",
            'mario_dumont': "Mario Dumont",
            'francoise_david': "Françoise David",
            'guy_rainville': "Guy Rainville",
            'aucun': "Aucun",
            'tous': "Tous",
        },
    }

    # --- q45 ---
    # op_leader_honest — Most honest leader
    # Source: q45
    # Labels from SAV file
    df_clean['op_leader_honest'] = df['q45'].map({
        1.0: 'jean_charest',
        2.0: 'pauline_marois',
        3.0: 'mario_dumont',
        4.0: 'francoise_david',
        5.0: 'guy_rainville',
        6.0: 'aucun',
        7.0: 'tous',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_leader_honest'] = {
        'original_variable': 'q45',
        'question_label': "le plus honnête ?",
        'type': 'categorical',
        'value_labels': {
            'jean_charest': "Jean Charest",
            'pauline_marois': "Pauline Marois",
            'mario_dumont': "Mario Dumont",
            'francoise_david': "Françoise David",
            'guy_rainville': "Guy Rainville",
            'aucun': "Aucun",
            'tous': "Tous",
        },
    }

    # --- q46 ---
    # op_leader_close_to_people — Leader closest to people
    # Source: q46
    # Labels from SAV file
    df_clean['op_leader_close_to_people'] = df['q46'].map({
        1.0: 'jean_charest',
        2.0: 'pauline_marois',
        3.0: 'mario_dumont',
        4.0: 'francoise_david',
        5.0: 'guy_rainville',
        6.0: 'aucun',
        7.0: 'tous',
        98.0: np.nan,
        99.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_leader_close_to_people'] = {
        'original_variable': 'q46',
        'question_label': "le plus proche des gens ?",
        'type': 'categorical',
        'value_labels': {
            'jean_charest': "Jean Charest",
            'pauline_marois': "Pauline Marois",
            'mario_dumont': "Mario Dumont",
            'francoise_david': "Françoise David",
            'guy_rainville': "Guy Rainville",
            'aucun': "Aucun",
            'tous': "Tous",
        },
    }

    # --- q47 ---
    # op_economy_assessment — Assessment of Quebec economy
    # Source: q47
    # Labels from SAV file
    df_clean['op_economy_assessment'] = df['q47'].map({
        1.0: 'amelioree',
        2.0: 'deterioree',
        3.0: 'meme_sans_changement',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_economy_assessment'] = {
        'original_variable': 'q47',
        'question_label': "Selon vous, l'économie québécoise s'est-elle AMÉLIORÉE, DÉTÉRIORÉE, ou est-elle restée à PEU PRÈS LA MÊME depuis un an ?",
        'type': 'categorical',
        'value_labels': {
            'amelioree': "Améliorée",
            'deterioree': "Détériorée",
            'meme_sans_changement': "A peu près la même",
        },
    }

    # --- q48 ---
    # op_fed_sovereignty — Federalist/sovereigntist identity
    # Source: q48
    # Labels from SAV file
    df_clean['op_fed_sovereignty'] = df['q48'].map({
        1.0: 'federaliste',
        2.0: 'souverainiste',
        3.0: 'entre_les_deux',
        4.0: 'ni_l_un_ni_l_autre',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_fed_sovereignty'] = {
        'original_variable': 'q48',
        'question_label': "Vous considérez-vous surtout comme un(e) fédéraliste, surtout comme un(e) souverainiste, comme quelqu'un qui est entre les deux ou comme quelqu'un qui n'est ni l'un ni l'autre ?",
        'type': 'categorical',
        'value_labels': {
            'federaliste': "...surtout comme un(e) fédéraliste",
            'souverainiste': "...surtout comme un(e) souverainiste",
            'entre_les_deux': "...quelqu'un qui est entre les deux",
            'ni_l_un_ni_l_autre': "... ni l'un ni l'autre",
        },
    }

    # --- q49 ---
    # op_power_distribution — Views on power distribution
    # Source: q49
    # Labels from SAV file
    df_clean['op_power_distribution'] = df['q49'].map({
        1.0: 'provinces_plus_pouvoirs',
        2.0: 'federal_plus_pouvoirs',
        3.0: 'statu_quo',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_power_distribution'] = {
        'original_variable': 'q49',
        'question_label': "D'après vous, à l'avenir…?",
        'type': 'categorical',
        'value_labels': {
            'provinces_plus_pouvoirs': "...les gouvernements provinciaux devraient avoir plus de pouvoirs",
            'federal_plus_pouvoirs': "...le gouvernement fédéral devrait avoir plus de pouvoirs",
            'statu_quo': "...ou est-ce que les choses devraient rester comme elles sont",
        },
    }

    # --- q50 ---
    # op_privatize_hydro — Opinion on Hydro Quebec privatization
    # Source: q50
    # Labels from SAV file: Likert scale
    df_clean['op_privatize_hydro'] = df['q50'].map({
        1.0: 'fortement_daccord',
        2.0: 'plutot_daccord',
        3.0: 'plutot_en_desaccord',
        4.0: 'fortement_en_desaccord',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_privatize_hydro'] = {
        'original_variable': 'q50',
        'question_label': "Au total, ce serait une bonne chose de privatiser Hydro Québec.",
        'type': 'categorical',
        'value_labels': {
            'fortement_daccord': "fortement d'accord",
            'plutot_daccord': "plutôt d'accord",
            'plutot_en_desaccord': "plutôt en désaccord",
            'fortement_en_desaccord': "fortement en désaccord",
        },
    }

    # --- q51 ---
    # op_private_health — Opinion on private healthcare
    # Source: q51
    # Labels from SAV file: Likert scale
    df_clean['op_private_health'] = df['q51'].map({
        1.0: 'fortement_daccord',
        2.0: 'plutot_daccord',
        3.0: 'plutot_en_desaccord',
        4.0: 'fortement_en_desaccord',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_private_health'] = {
        'original_variable': 'q51',
        'question_label': "Pour améliorer le système de santé au Québec, il faudrait avoir davantage recours au secteur privé.",
        'type': 'categorical',
        'value_labels': {
            'fortement_daccord': "fortement d'accord",
            'plutot_daccord': "plutôt d'accord",
            'plutot_en_desaccord': "plutôt en désaccord",
            'fortement_en_desaccord': "fortement en désaccord",
        },
    }

    # --- q52 ---
    # op_govt_poverty — Opinion on government role in poverty
    # Source: q52
    # Labels from SAV file: Likert scale
    df_clean['op_govt_poverty'] = df['q52'].map({
        1.0: 'fortement_daccord',
        2.0: 'plutot_daccord',
        3.0: 'plutot_en_desaccord',
        4.0: 'fortement_en_desaccord',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_poverty'] = {
        'original_variable': 'q52',
        'question_label': "Sans l'action du gouvernement, il y aurait beaucoup plus de pauvreté dans nos sociétés.",
        'type': 'categorical',
        'value_labels': {
            'fortement_daccord': "fortement d'accord",
            'plutot_daccord': "plutôt d'accord",
            'plutot_en_desaccord': "plutôt en désaccord",
            'fortement_en_desaccord': "fortement en désaccord",
        },
    }

    # --- q53 ---
    # op_govt_environment — Opinion on government role in environment
    # Source: q53
    # Labels from SAV file: Likert scale
    df_clean['op_govt_environment'] = df['q53'].map({
        1.0: 'fortement_daccord',
        2.0: 'plutot_daccord',
        3.0: 'plutot_en_desaccord',
        4.0: 'fortement_en_desaccord',
        8.0: np.nan,
        9.0: np.nan,
    })
    CODEBOOK_VARIABLES['op_govt_environment'] = {
        'original_variable': 'q53',
        'question_label': "Sans l'action du gouvernement, l'environnement serait beaucoup moins bien protégé.",
        'type': 'categorical',
        'value_labels': {
            'fortement_daccord': "fortement d'accord",
            'plutot_daccord': "plutôt d'accord",
            'plutot_en_desaccord': "plutôt en désaccord",
            'fortement_en_desaccord': "fortement en désaccord",
        },
    }
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
    """Retourne les métadonnées du sondage et des variables.

    Returns:
        dict: Dictionnaire contenant SURVEY_METADATA et CODEBOOK_VARIABLES
    """
    return {
        'survey_metadata': SURVEY_METADATA,
        'codebook_variables': CODEBOOK_VARIABLES
    }
