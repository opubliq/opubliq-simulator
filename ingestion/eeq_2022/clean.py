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

import os
import pandas as pd
import numpy as np

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

    # --- cps_EndDate ---
    # cps_EndDate — End date
    # Source: cps_EndDate
    df_clean['cps_EndDate'] = df['cps_EndDate']
    CODEBOOK_VARIABLES['cps_EndDate'] = {
        'original_variable': 'cps_EndDate',
        'question_label': "End date",
        'type': 'categorical',
        'value_labels': {}
    }

    # --- cps_ideoself_1 ---
    # cps_ideoself_1 — Auto-identification gauche/droite
    # Source: cps_ideoself_1
    df_clean['cps_ideoself_1'] = df['cps_ideoself_1']
    CODEBOOK_VARIABLES['cps_ideoself_1'] = {
        'original_variable': 'cps_ideoself_1',
        'question_label': "En politique, on parle parfois de gauche et de droite. Où vous placeriez-vous",
        'type': 'ordinal',
        'value_labels': {
            0: "Gauche",
            1: "1",
            2: "2",
            3: "3",
            4: "4",
            5: "5",
            6: "6",
            7: "7",
            8: "8",
            9: "9",
            10: "Droite"
        }
    }

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

    # --- cps_spendedu ---
    # cps_spendedu — Opinion sur les dépenses pour l'éducation
    # Source: cps_spendedu
    df_clean['cps_spendedu'] = df['cps_spendedu'].map({
        1.0: 'spend_less',
        2.0: 'spend_about_same',
        3.0: 'spend_more'
    })
    CODEBOOK_VARIABLES['cps_spendedu'] = {
        'original_variable': 'cps_spendedu',
        'question_label': "Combien le gouvernement provincial devrait-il dépense en éducation?",
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

    # --- cps_attractimm ---
    # cps_attractimm — Opinion sur l'attraction d'immigrants
    # Source: cps_attractimm
    df_clean['cps_attractimm'] = df['cps_attractimm'].map({
        1.0: 'attract_more',
        2.0: 'attract_less',
        3.0: 'maintain_level'
    })
    CODEBOOK_VARIABLES['cps_attractimm'] = {
        'original_variable': 'cps_attractimm',
        'question_label': "Pensez-vous que le Québec devrait essayer...",
        'type': 'categorical',
        'value_labels': {
            'attract_more': "D'attirer plus d'immigrants dans la province",
            'attract_less': "D'attirer moins d'immigrants dans la province",
            'maintain_level': "De maintenir le niveau d'immigrants comme il est actuellement"
        }
    }

    # --- cps_borncountry ---
    # cps_borncountry — Country of birth
    # Source: cps_borncountry
    df_clean['cps_borncountry'] = df['cps_borncountry']
    CODEBOOK_VARIABLES['cps_borncountry'] = {
        'original_variable': 'cps_borncountry',
        'question_label': "Dans quel pays êtes-vous né?",
        'type': 'categorical',
        'missing_codes': [],
        'value_labels': {
            1.0: "Afghanistan",
            2.0: "Åland Islands",
            3.0: "Albania",
            4.0: "Algeria",
            5.0: "American Samoa",
            6.0: "Andorra",
            7.0: "Angola",
            8.0: "Anguilla",
            9.0: "Antarctica",
            10.0: "Antigua and Barbuda",
            11.0: "Argentina",
            12.0: "Armenia",
            13.0: "Aruba",
            14.0: "Australia",
            15.0: "Austria",
            16.0: "Azerbaijan",
            17.0: "Bahamas",
            18.0: "Bahrain",
            19.0: "Bangladesh",
            20.0: "Barbados",
            21.0: "Belarus",
            22.0: "Belgium",
            23.0: "Belize",
            24.0: "Benin",
            25.0: "Bermuda",
            26.0: "Bhutan",
            27.0: "Bolivia, Plurinational State of",
            28.0: "Bonaire, Sint Eustatius and Saba",
            29.0: "Bosnia and Herzegovina",
            30.0: "Botswana",
            31.0: "Bouvet Island",
            32.0: "Brazil",
            33.0: "British Indian Ocean Territory",
            34.0: "Brunei Darussalam",
            35.0: "Bulgaria",
            36.0: "Burkina Faso",
            37.0: "Burundi",
            38.0: "Cambodia",
            39.0: "Cameroon",
            40.0: "Cape Verde",
            41.0: "Cayman Islands",
            42.0: "Central African Republic",
            43.0: "Chad",
            44.0: "Chile",
            45.0: "China",
            46.0: "Christmas Island",
            47.0: "Cocos (Keeling) Islands",
            48.0: "Colombia",
            49.0: "Comoros",
            50.0: "Congo",
            51.0: "Congo, the Democratic Republic of the",
            52.0: "Cook Islands",
            53.0: "Costa Rica",
            54.0: "Côte d'Ivoire",
            55.0: "Croatia",
            56.0: "Cuba",
            57.0: "Curaçao",
            58.0: "Cyprus",
            59.0: "Czech Republic",
            60.0: "Denmark",
            61.0: "Djibouti",
            62.0: "Dominica",
            63.0: "Dominican Republic",
            64.0: "Ecuador",
            65.0: "Egypt",
            66.0: "El Salvador",
            67.0: "Equatorial Guinea",
            68.0: "Eritrea",
            69.0: "Estonia",
            70.0: "Ethiopia",
            71.0: "Falkland Islands (Malvinas)",
            72.0: "Faroe Islands",
            73.0: "Fiji",
            74.0: "Finland",
            75.0: "France",
            76.0: "French Guiana",
            77.0: "French Polynesia",
            78.0: "French Southern Territories",
            79.0: "Gabon",
            80.0: "Gambia",
            81.0: "Georgia",
            82.0: "Germany",
            83.0: "Ghana",
            84.0: "Gibraltar",
            85.0: "Greece",
            86.0: "Greenland",
            87.0: "Grenada",
            88.0: "Guadeloupe",
            89.0: "Guam",
            90.0: "Guatemala",
            91.0: "Guernsey",
            92.0: "Guinea",
            93.0: "Guinea-Bissau",
            94.0: "Guyana",
            95.0: "Haiti",
            96.0: "Heard Island and McDonald Islands",
            97.0: "Holy See (Vatican City State)",
            98.0: "Honduras",
            99.0: "Hong Kong",
            100.0: "Hungary",
            101.0: "Iceland",
            102.0: "India",
            103.0: "Indonesia",
            104.0: "Iran, Islamic Republic of",
            105.0: "Iraq",
            106.0: "Ireland",
            107.0: "Isle of Man",
            108.0: "Israel",
            109.0: "Italy",
            110.0: "Jamaica",
            111.0: "Japan",
            112.0: "Jersey",
            113.0: "Jordan",
            114.0: "Kazakhstan",
            115.0: "Kenya",
            116.0: "Kiribati",
            117.0: "Korea, Democratic People's Republic of",
            118.0: "Korea, Republic of",
            119.0: "Kuwait",
            120.0: "Kyrgyzstan",
            121.0: "Lao People's Democratic Republic",
            122.0: "Latvia",
            123.0: "Lebanon",
            124.0: "Lesotho",
            125.0: "Liberia",
            126.0: "Libya",
            127.0: "Liechtenstein",
            128.0: "Lithuania",
            129.0: "Luxembourg",
            130.0: "Macao",
            131.0: "Macedonia, the Former Yugoslav Republic of",
            132.0: "Madagascar",
            133.0: "Malawi",
            134.0: "Malaysia",
            135.0: "Maldives",
            136.0: "Mali",
            137.0: "Malta",
            138.0: "Marshall Islands",
            139.0: "Martinique",
            140.0: "Mauritania",
            141.0: "Mauritius",
            142.0: "Mayotte",
            143.0: "Mexico",
            144.0: "Micronesia, Federated States of",
            145.0: "Moldova, Republic of",
            146.0: "Monaco",
            147.0: "Mongolia",
            148.0: "Montenegro",
            149.0: "Montserrat",
            150.0: "Morocco",
            151.0: "Mozambique",
            152.0: "Myanmar",
            153.0: "Namibia",
            154.0: "Nauru",
            155.0: "Nepal",
            156.0: "Netherlands",
            157.0: "New Caledonia",
            158.0: "New Zealand",
            159.0: "Nicaragua",
            160.0: "Niger",
            161.0: "Nigeria",
            162.0: "Niue",
            163.0: "Norfolk Island",
            164.0: "Northern Mariana Islands",
            165.0: "Norway",
            166.0: "Oman",
            167.0: "Pakistan",
            168.0: "Palau",
            169.0: "Palestinian Territory, Occupied",
            170.0: "Panama",
            171.0: "Papua New Guinea",
            172.0: "Paraguay",
            173.0: "Peru",
            174.0: "Philippines",
            175.0: "Pitcairn",
            176.0: "Poland",
            177.0: "Portugal",
            178.0: "Puerto Rico",
            179.0: "Qatar",
            180.0: "Réunion",
            181.0: "Romania",
            182.0: "Russian Federation",
            183.0: "Rwanda",
            184.0: "Saint Barthélemy",
            185.0: "Saint Helena, Ascension and Tristan da Cunha",
            186.0: "Saint Kitts and Nevis",
            187.0: "Saint Lucia",
            188.0: "Saint Martin (French Part)",
            189.0: "Saint Pierre and Miquelon",
            190.0: "Saint Vincent and the Grenadines",
            191.0: "Samoa",
            192.0: "San Marino",
            193.0: "Sao Tome and Principe",
            194.0: "Saudi Arabia",
            195.0: "Senegal",
            196.0: "Serbia",
            197.0: "Seychelles",
            198.0: "Sierra Leone",
            199.0: "Singapore",
            200.0: "Sint Maarten (Dutch Part)",
            201.0: "Slovakia",
            202.0: "Slovenia",
            203.0: "Solomon Islands",
            204.0: "Somalia",
            205.0: "South Africa",
            206.0: "South Georgia and the South Sandwich Islands",
            207.0: "South Sudan",
            208.0: "Spain",
            209.0: "Sri Lanka",
            210.0: "Sudan",
            211.0: "Suriname",
            212.0: "Svalbard and Jan Mayen",
            213.0: "Swaziland",
            214.0: "Sweden",
            215.0: "Switzerland",
            216.0: "Syrian Arab Republic",
            217.0: "Taiwan, Province of China",
            218.0: "Tajikistan",
            219.0: "Tanzania, United Republic of",
            220.0: "Thailand",
            221.0: "Timor-Leste",
            222.0: "Togo",
            223.0: "Tokelau",
            224.0: "Tonga",
            225.0: "Trinidad and Tobago",
            226.0: "Tunisia",
            227.0: "Turkey",
            228.0: "Turkmenistan",
            229.0: "Turks and Caicos Islands",
            230.0: "Tuvalu",
            231.0: "Uganda",
            232.0: "Ukraine",
            233.0: "United Arab Emirates",
            234.0: "United Kingdom",
            235.0: "United States",
            236.0: "United States Minor Outlying Islands",
            237.0: "Uruguay",
            238.0: "Uzbekistan",
            239.0: "Vanuatu",
            240.0: "Venezuela, Bolivarian Republic of",
            241.0: "Vietnam",
            242.0: "Virgin Islands, British",
            243.0: "Virgin Islands, U.S.",
            244.0: "Wallis and Futuna",
            245.0: "Western Sahara",
            246.0: "Yemen",
            247.0: "Zambia",
            248.0: "Zimbabwe",
            249.0: "Don't know/ Prefer not to say"
        }
    }

    # --- cps_income ---
    # cps_income — Revenu total du ménage avant impôts en 2021
    # Source: cps_income
    df_clean['cps_income'] = df['cps_income']
    CODEBOOK_VARIABLES['cps_income'] = {
        'original_variable': 'cps_income',
        'question_label': "Quel est le revenu total de votre ménage avant impôts en 2021? Cela doit inclure",
        'type': 'continuous',
        'value_labels': {}
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

    # --- cps_ideoparty_DO_7 ---
    # cps_ideoparty_DO_7 — Display order for Québec solidaire in cps_ideoparty
    # Source: cps_ideoparty_DO_7
    df_clean['cps_ideoparty_DO_7'] = df['cps_ideoparty_DO_7']
    CODEBOOK_VARIABLES['cps_ideoparty_DO_7'] = {
        'original_variable': 'cps_ideoparty_DO_7',
        'question_label': "Display order for Québec solidaire in cps_ideoparty",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_ideoparty_DO_5 ---
    # cps_ideoparty_DO_5 — Display order for Coalition avenir Québec in cps_ideoparty
    # Source: cps_ideoparty_DO_5
    df_clean['cps_ideoparty_DO_5'] = df['cps_ideoparty_DO_5'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_ideoparty_DO_5'] = {
        'original_variable': 'cps_ideoparty_DO_5',
        'question_label': "Display order for Coalition avenir Québec in cps_ideoparty",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_ideoparty_DO_8 ---
    # cps_ideoparty_DO_8 — Display order for Parti conservateur du Québec in cps_ideoparty
    # Source: cps_ideoparty_DO_8
    df_clean['cps_ideoparty_DO_8'] = df['cps_ideoparty_DO_8'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_ideoparty_DO_8'] = {
        'original_variable': 'cps_ideoparty_DO_8',
        'question_label': "Display order for Parti conservateur du Québec in cps_ideoparty",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
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
    df_clean['cps_impissue_matrix_DO_2'] = df['cps_impissue_matrix_DO_2'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_2'] = {
        'original_variable': 'cps_impissue_matrix_DO_2',
        'question_label': "Display order for second issue option (La santé) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
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
    # cps_impissue_matrix_DO_4 — Display order for fourth issue option (L'éducation) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_4
    df_clean['cps_impissue_matrix_DO_4'] = df['cps_impissue_matrix_DO_4'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_4'] = {
        'original_variable': 'cps_impissue_matrix_DO_4',
        'question_label': "Display order for fourth issue option (L'éducation) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
    }

    # --- cps_impissue_matrix_DO_5 ---
    # cps_impissue_matrix_DO_5 — Display order for fifth issue option (La pauvreté) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_5
    df_clean['cps_impissue_matrix_DO_5'] = df['cps_impissue_matrix_DO_5'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_5'] = {
        'original_variable': 'cps_impissue_matrix_DO_5',
        'question_label': "Display order for fifth issue option (La pauvreté) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
    }

    # --- cps_impissue_matrix_DO_6 ---
    # cps_impissue_matrix_DO_6 — Display order for sixth issue option (La crise du logement) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_6
    df_clean['cps_impissue_matrix_DO_6'] = df['cps_impissue_matrix_DO_6'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_6'] = {
        'original_variable': 'cps_impissue_matrix_DO_6',
        'question_label': "Display order for sixth issue option (La crise du logement) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
    }

    # --- cps_impissue_matrix_DO_7 ---
    # cps_impissue_matrix_DO_7 — Display order for seventh issue option (L'intégrité des politiciens et la corruption) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_7
    df_clean['cps_impissue_matrix_DO_7'] = df['cps_impissue_matrix_DO_7'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_7'] = {
        'original_variable': 'cps_impissue_matrix_DO_7',
        'question_label': "Display order for seventh issue option (L'intégrité des politiciens et la corruption) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
    }

    # --- cps_impissue_matrix_DO_8 ---
    # cps_impissue_matrix_DO_8 — Display order for eighth issue option (Les taxes et les finances publiques) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_8
    df_clean['cps_impissue_matrix_DO_8'] = df['cps_impissue_matrix_DO_8'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_8'] = {
        'original_variable': 'cps_impissue_matrix_DO_8',
        'question_label': "Display order for eighth issue option (Les taxes et les finances publiques) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
    }

    # --- cps_impissue_matrix_DO_9 ---
    # cps_impissue_matrix_DO_9 — Display order for ninth issue option (La souveraineté du Québec) in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_9
    df_clean['cps_impissue_matrix_DO_9'] = df['cps_impissue_matrix_DO_9'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth',
        6.0: 'sixth',
        7.0: 'seventh',
        8.0: 'eighth',
        9.0: 'ninth',
        10.0: 'tenth',
        11.0: 'eleventh',
        12.0: 'twelfth',
        13.0: 'thirteenth',
        14.0: 'fourteenth'
    })
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_9'] = {
        'original_variable': 'cps_impissue_matrix_DO_9',
        'question_label': "Display order for ninth issue option (La souveraineté du Québec) in cps_impissue_matrix",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième",
            'sixth': "sixième",
            'seventh': "septième",
            'eighth': "huitième",
            'ninth': "neuvième",
            'tenth': "dixième",
            'eleventh': "onzième",
            'twelfth': "douzième",
            'thirteenth': "treizième",
            'fourteenth': "quatorzième"
        }
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
    # cps_impissue_matrix_DO_12 — Display order for third Quebec City road link option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_12
    df_clean['cps_impissue_matrix_DO_12'] = df['cps_impissue_matrix_DO_12']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_12'] = {
        'original_variable': 'cps_impissue_matrix_DO_12',
        'question_label': "Display order for third Quebec City road link option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_13 ---
    # cps_impissue_matrix_DO_13 — Display order for French language option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_13
    df_clean['cps_impissue_matrix_DO_13'] = df['cps_impissue_matrix_DO_13']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_13'] = {
        'original_variable': 'cps_impissue_matrix_DO_13',
        'question_label': "Display order for French language option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_impissue_matrix_DO_14 ---
    # cps_impissue_matrix_DO_14 — Display order for gun violence option in cps_impissue_matrix
    # Source: cps_impissue_matrix_DO_14
    df_clean['cps_impissue_matrix_DO_14'] = df['cps_impissue_matrix_DO_14']
    CODEBOOK_VARIABLES['cps_impissue_matrix_DO_14'] = {
        'original_variable': 'cps_impissue_matrix_DO_14',
        'question_label': "Display order for gun violence option in cps_impissue_matrix",
        'type': 'continuous',
        'value_labels': {}
    }

    # --- cps_intelligent_1 ---
    # cps_intelligent_1 — Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en
    # Source: cps_intelligent
    df_clean['cps_intelligent_1'] = df['cps_intelligent_1'].map({
        1.0: 'strongly_disagree',
        2.0: 'rather_disagree',
        3.0: 'neutral',
        4.0: 'rather_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_intelligent_1'] = {
        'original_variable': 'cps_intelligent_1',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec cette personne: Dominique Anglade",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'rather_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'rather_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
     }

    # --- cps_intelligent_3 ---
    # cps_intelligent_3 — Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en
    # Source: cps_intelligent
    df_clean['cps_intelligent_3'] = df['cps_intelligent_3'].map({
        1.0: 'strongly_disagree',
        2.0: 'rather_disagree',
        3.0: 'neutral',
        4.0: 'rather_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_intelligent_3'] = {
        'original_variable': 'cps_intelligent_3',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec cette personne: Paul St-Pierre Plamondon",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'rather_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'rather_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
    }

    # --- cps_intelligent_4 ---
    # cps_intelligent_4 — Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en
    # Source: cps_intelligent
    df_clean['cps_intelligent_4'] = df['cps_intelligent_4'].map({
        1.0: 'strongly_disagree',
        2.0: 'rather_disagree',
        3.0: 'neutral',
        4.0: 'rather_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_intelligent_4'] = {
        'original_variable': 'cps_intelligent_4',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec cette personne: Gabriel Nadeau-Dubois",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'rather_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'rather_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
    }

    # --- cps_intelligent_DO_1 ---
    # cps_intelligent_DO_1 — Display order for first leader option in cps_intelligent
    # Source: cps_intelligent_DO_1
    df_clean['cps_intelligent_DO_1'] = df['cps_intelligent_DO_1'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_intelligent_DO_1'] = {
        'original_variable': 'cps_intelligent_DO_1',
        'question_label': "Display order for first leader option in cps_intelligent",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_intelligent_DO_2 ---
    # cps_intelligent_DO_2 — Display order for François Legault in cps_intelligent
    # Source: cps_intelligent_DO_2
    df_clean['cps_intelligent_DO_2'] = df['cps_intelligent_DO_2'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_intelligent_DO_2'] = {
        'original_variable': 'cps_intelligent_DO_2',
        'question_label': "Display order for François Legault in cps_intelligent",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_intelligent_DO_3 ---
    # cps_intelligent_DO_3 — Display order for Paul St-Pierre Plamondon in cps_intelligent
    # Source: cps_intelligent_DO_3
    df_clean['cps_intelligent_DO_3'] = df['cps_intelligent_DO_3'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_intelligent_DO_3'] = {
        'original_variable': 'cps_intelligent_DO_3',
        'question_label': "Display order for Paul St-Pierre Plamondon in cps_intelligent",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_intelligent_DO_4 ---
    # cps_intelligent_DO_4 — Display order for Gabriel Nadeau-Dubois in cps_intelligent
    # Source: cps_intelligent_DO_4
    df_clean['cps_intelligent_DO_4'] = df['cps_intelligent_DO_4'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_intelligent_DO_4'] = {
        'original_variable': 'cps_intelligent_DO_4',
        'question_label': "Display order for Gabriel Nadeau-Dubois in cps_intelligent",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_intelligent_DO_5 ---
    # cps_intelligent_DO_5 — Display order for Éric Duhaime in cps_intelligent
    # Source: cps_intelligent_DO_5
    df_clean['cps_intelligent_DO_5'] = df['cps_intelligent_DO_5'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_intelligent_DO_5'] = {
        'original_variable': 'cps_intelligent_DO_5',
        'question_label': "Display order for Éric Duhaime in cps_intelligent",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_cares_1 ---
    # cps_cares_1 — For each of the party leaders below, please indicate whether you agree or disagree
    # Source: cps_cares
    df_clean['cps_cares_1'] = df['cps_cares_1'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_cares_1'] = {
        'original_variable': 'cps_cares_1',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec l'énoncé suivant: Il(elle) se soucie vraiment des gens comme vous? (Dominique Anglade)",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
    }

    # --- cps_cares_2 ---
    # cps_cares_2 — For each of the party leaders below, please indicate whether you agree or disagree
    # Source: cps_cares (for François Legault)
    df_clean['cps_cares_2'] = df['cps_cares_2'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_cares_2'] = {
        'original_variable': 'cps_cares_2',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec l'énoncé suivant: Il(elle) se soucie vraiment des gens comme vous? (François Legault)",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
         }
     }

    # --- cps_cares_3 ---
    # cps_cares_3 — For each of the party leaders below, please indicate whether you agree or disagree
    # Source: cps_cares (for Paul St-Pierre Plamondon)
    df_clean['cps_cares_3'] = df['cps_cares_3'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_cares_3'] = {
        'original_variable': 'cps_cares_3',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec l'énoncé suivant: Il(elle) se soucie vraiment des gens comme vous? (Paul St-Pierre Plamondon)",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
    }

    # --- cps_cares_4 ---
    # cps_cares_4 — For each of the party leaders below, please indicate whether you agree or disagree
    # Source: cps_cares (for Gabriel Nadeau-Dubois)
    df_clean['cps_cares_4'] = df['cps_cares_4'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_cares_4'] = {
        'original_variable': 'cps_cares_4',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec l'énoncé suivant: Il(elle) se soucie vraiment des gens comme vous? (Gabriel Nadeau-Dubois)",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
     }

    # --- cps_cares_5 ---
    # cps_cares_5 — For each of the party leaders below, please indicate whether you agree or disagree
    # Source: cps_cares (for Éric Duhaime)
    df_clean['cps_cares_5'] = df['cps_cares_5'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_cares_5'] = {
        'original_variable': 'cps_cares_5',
        'question_label': "Pour chacun(e) des chef(fe)s de parti suivant(e)s, indiquez si vous êtes en accord ou en désaccord avec l'énoncé suivant: Il(elle) se soucie vraiment des gens comme vous? (Éric Duhaime)",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
    }

    # --- cps_income2 ---
    # cps_income2 — Revenu du ménage (catégories)
    # Source: cps_income2
    df_clean['cps_income2'] = df['cps_income2'].map({
        1.0: 'no_income',
        2.0: '1_to_30k',
        3.0: '30k_to_60k',
        4.0: '60k_to_90k',
        5.0: '90k_to_110k',
        6.0: '110k_to_150k',
         7.0: '150k_to_200k',
         8.0: 'over_200k'
    })
    CODEBOOK_VARIABLES['cps_income2'] = {
         'original_variable': 'cps_income2',
         'question_label': "Nous n'avons pas besoin du montant exact; le revenu de votre ménage se situet-il dans l'une des catégories suivantes?",
         'type': 'categorical',
         'value_labels': {
             'no_income': "Aucun revenu",
             '1_to_30k': "1$ à 30 000$",
             '30k_to_60k': "30 001$ à 60 000$",
             '60k_to_90k': "60 001$ à 90 000$",
             '90k_to_110k': "90 001$ à 110 000$",
             '110k_to_150k': "110 001$ à 150 000$",
             '150k_to_200k': "150 001$ à 200 000$",
             'over_200k': "Plus de 200 000$"
        }
    }

    # --- cps_interest_1 ---
    # cps_interest_1 — Quel est votre niveau d'intérêt pour la politique en général?
    # Source: cps_interest_1
    df_clean['cps_interest_1'] = df['cps_interest_1']
    CODEBOOK_VARIABLES['cps_interest_1'] = {
        'original_variable': 'cps_interest_1',
        'question_label': "Quel est votre niveau d'intérêt pour la politique en général? Veuillez glisser le curseur à l'endroit qui correspond le mieux à votre opinion.",
        'type': 'ordinal',
        'value_labels': {
            0: "Aucun intérêt",
            10: "Beaucoup d'intérêt"
        }
    }

    # --- cps_intelection_1 ---
    # cps_intelection_1 — Quel est votre niveau d'intérêt pour cette élection au Québec?
    # Source: cps_intelection_1
    df_clean['cps_intelection_1'] = df['cps_intelection_1']
    CODEBOOK_VARIABLES['cps_intelection_1'] = {
        'original_variable': 'cps_intelection_1',
        'question_label': "Quel est votre niveau d'intérêt pour cette élection au Québec? Veuillez",
        'type': 'ordinal',
        'value_labels': {}
    }

    # --- cps_jobsfirst ---
    # cps_jobsfirst — Lorsqu'il existe un conflit entre la protection de l'environnement et la création d'emplois
    # Source: cps_jobsfirst
    df_clean['cps_jobsfirst'] = df['cps_jobsfirst'].map({
        1.0: 'strongly_disagree',
        2.0: 'somewhat_disagree',
        3.0: 'neutral',
        4.0: 'somewhat_agree',
        5.0: 'strongly_agree'
    })
    CODEBOOK_VARIABLES['cps_jobsfirst'] = {
        'original_variable': 'cps_jobsfirst',
        'question_label': "Lorsqu'il existe un conflit entre la protection de l'environnement et la création d'emplois, laquelle des deux est la plus importante pour vous?",
        'type': 'categorical',
        'value_labels': {
            'strongly_disagree': "Fortement en désaccord",
            'somewhat_disagree': "Plutôt en désaccord",
            'neutral': "Ni en accord, ni en désaccord",
            'somewhat_agree': "Plutôt en accord",
            'strongly_agree': "Fortement en accord"
        }
     }

    # --- cps_langQC ---
    # cps_langQC — Selon vous, la langue française est-elle menacée au Québec?
    # Source: cps_langQC
    df_clean['cps_langQC'] = df['cps_langQC'].map({
        1.0: 'yes',
        2.0: 'no',
        3.0: 'dont_know'
    })
    CODEBOOK_VARIABLES['cps_langQC'] = {
        'original_variable': 'cps_langQC',
        'question_label': "Selon vous, la langue française est-elle menacée au Québec?",
        'type': 'categorical',
        'value_labels': {
            'yes': "Oui",
            'no': "Non",
            'dont_know': "Je ne sais pas"
        }
    }

    # --- cps_lang_1 ---
    # cps_lang_1 — Anglais language selection (sub-variable of cps_lang)
    # Source: cps_lang_1
    df_clean['cps_lang_1'] = df['cps_lang_1']
    CODEBOOK_VARIABLES['cps_lang_1'] = {
        'original_variable': 'cps_lang_1',
        'question_label': "Quelle est la/les première(s) langue(s) que vous avez apprise(s) et que vous parlez encore? (Anglais)",
        'type': 'categorical',
        'value_labels': {
            1: "Selected",
            -99: "Not selected"
        }
    }

    # --- cps_lang_2 ---
    # cps_lang_2 — Français language selection (sub-variable of cps_lang)
    # Source: cps_lang_2
    df_clean['cps_lang_2'] = df['cps_lang_2']
    CODEBOOK_VARIABLES['cps_lang_2'] = {
        'original_variable': 'cps_lang_2',
        'question_label': "Quelle est la/les première(s) langue(s) que vous avez apprise(s) et que vous parlez encore? (Français)",
        'type': 'categorical',
        'value_labels': {
            1: "Selected",
            -99: "Not selected"
        }
    }

    # --- cps_UserLanguage ---
    # cps_UserLanguage — Language the respondent answered the Campaign Period Survey in
    # Source: cps_UserLanguage
    df_clean['cps_UserLanguage'] = df['cps_UserLanguage'].map({
        'EN': 'english',
        'FR-CA': 'french'
    })
    CODEBOOK_VARIABLES['cps_UserLanguage'] = {
        'original_variable': 'cps_UserLanguage',
        'question_label': "Dans quelle langue avez-vous complété le sondage?",
        'type': 'categorical',
        'value_labels': {
            'english': "Anglais",
            'french': "Français"
        }
    }

    # --- cps_can_attach ---
    # cps_can_attach — Degree of attachment to Canada
    # Source: cps_can_attach
    df_clean['cps_can_attach'] = df['cps_can_attach'].map({
        1.0: 'very_attached',
        2.0: 'quite_attached',
        3.0: 'not_very_attached',
        4.0: 'not_at_all_attached',
        5.0: 'dont_know'
    })
    CODEBOOK_VARIABLES['cps_can_attach'] = {
        'original_variable': 'cps_can_attach',
        'question_label': "Quel est votre degré d’attachement au Canada?",
        'type': 'categorical',
        'value_labels': {
            'very_attached': "Très attaché",
            'quite_attached': "Assez attaché",
            'not_very_attached': "Peu attaché",
            'not_at_all_attached': "Pas du tout attaché",
            'dont_know': "Je ne sais pas"
        }
    }

    # --- cps_candtherm_23 ---
    # cps_candtherm_23 — Candidat(e) libéral(e) dans votre circonscription
    # Source: cps_candtherm_23 (feeling thermometer)
    df_clean['cps_candtherm_23'] = df['cps_candtherm_23']
    CODEBOOK_VARIABLES['cps_candtherm_23'] = {
        'original_variable': 'cps_candtherm_23',
        'question_label': "Sur la même échelle, que pensez-vous des candidat(e)s dans votre circonscription? (Candidat(e) libéral(e))",
        'type': 'ordinal',
        'value_labels': {},
        'missing_codes': {
            -99: "Je ne connais pas le/la candidat"
        },
        'range_min': 0,
        'range_max': 100
    }

    # --- cps_candtherm_DO_23 ---
    # cps_candtherm_DO_23 — Display order for Candidat(e) libéral(e) dans votre circonscription in cps_candtherm
    # Source: cps_candtherm_DO_23
    df_clean['cps_candtherm_DO_23'] = df['cps_candtherm_DO_23'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_candtherm_DO_23'] = {
        'original_variable': 'cps_candtherm_DO_23',
        'question_label': "Display order for Candidat(e) libéral(e) dans votre circonscription in cps_candtherm",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_candtherm_25 ---
    # cps_candtherm_25 — Candidat(e) péquiste dans votre circonscription
    # Source: cps_candtherm_25 (feeling thermometer)
    df_clean['cps_candtherm_25'] = df['cps_candtherm_25']
    CODEBOOK_VARIABLES['cps_candtherm_25'] = {
        'original_variable': 'cps_candtherm_25',
        'question_label': "Sur la même échelle, que pensez-vous des candidat(e)s dans votre circonscription? (Candidat(e) péquiste)",
        'type': 'ordinal',
        'value_labels': {},
        'missing_codes': {
            -99: "Je ne connais pas le/la candidat"
        },
         'range_min': 0,
         'range_max': 100
     }

    # --- cps_candtherm_DO_25 ---
    # cps_candtherm_DO_25 — Display order for Candidat(e) péquiste dans votre circonscription in cps_candtherm
    # Source: cps_candtherm_DO_25
    df_clean['cps_candtherm_DO_25'] = df['cps_candtherm_DO_25'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_candtherm_DO_25'] = {
        'original_variable': 'cps_candtherm_DO_25',
        'question_label': "Display order for Candidat(e) péquiste dans votre circonscription in cps_candtherm",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_candtherm_27 ---
    # cps_candtherm_27 — Candidat(e) caquiste dans votre circonscription
    # Source: cps_candtherm_27 (feeling thermometer)
    df_clean['cps_candtherm_27'] = df['cps_candtherm_27']
    CODEBOOK_VARIABLES['cps_candtherm_27'] = {
        'original_variable': 'cps_candtherm_27',
        'question_label': "Sur la même échelle, que pensez-vous des candidat(e)s dans votre circonscription? (Candidat(e) caquiste)",
        'type': 'ordinal',
        'value_labels': {},
        'missing_codes': {
            -99: "Je ne connais pas le/la candidat"
        },
        'range_min': 0,
        'range_max': 100
    }

    # --- cps_candtherm_DO_27 ---
    # cps_candtherm_DO_27 — Display order for Candidat(e) caquiste dans votre circonscription in cps_candtherm
    # Source: cps_candtherm_DO_27
    df_clean['cps_candtherm_DO_27'] = df['cps_candtherm_DO_27'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_candtherm_DO_27'] = {
        'original_variable': 'cps_candtherm_DO_27',
        'question_label': "Display order for Candidat(e) caquiste dans votre circonscription in cps_candtherm",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
     }

    # --- cps_candtherm_DO_29 ---
    # cps_candtherm_DO_29 — Display order for Candidat(e) solidaire dans votre circonscription in cps_candtherm
    # Source: cps_candtherm_DO_29
    df_clean['cps_candtherm_DO_29'] = df['cps_candtherm_DO_29'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_candtherm_DO_29'] = {
        'original_variable': 'cps_candtherm_DO_29',
        'question_label': "Display order for Candidat(e) solidaire dans votre circonscription in cps_candtherm",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_candtherm_29 ---
    # cps_candtherm_29 — Candidat(e) solidaire dans votre circonscription
    # Source: cps_candtherm_29 (feeling thermometer)
    df_clean['cps_candtherm_29'] = df['cps_candtherm_29']
    CODEBOOK_VARIABLES['cps_candtherm_29'] = {
        'original_variable': 'cps_candtherm_29',
        'question_label': "Sur la même échelle, que pensez-vous des candidat(e)s dans votre circonscription? (Candidat(e) solidaire)",
        'type': 'ordinal',
        'value_labels': {},
        'missing_codes': {
            -99: "Je ne connais pas le/la candidat"
        },
        'range_min': 0,
        'range_max': 100
    }

    # --- cps_candtherm_DO_30 ---
    # cps_candtherm_DO_30 — Display order for Candidat(e) conservateur(trice) in cps_candtherm
    # Source: cps_candtherm_DO_30
    df_clean['cps_candtherm_DO_30'] = df['cps_candtherm_DO_30'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third',
        4.0: 'fourth',
        5.0: 'fifth'
    })
    CODEBOOK_VARIABLES['cps_candtherm_DO_30'] = {
        'original_variable': 'cps_candtherm_DO_30',
        'question_label': "Display order for Candidat(e) conservateur(trice) dans votre circonscription in cps_candtherm",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième",
            'fourth': "quatrième",
            'fifth': "cinquième"
        }
    }

    # --- cps_candtherm_30 ---
    # cps_candtherm_30 — Candidat(e) conservateur(trice) dans votre circonscription
    # Source: cps_candtherm_30 (feeling thermometer)
    df_clean['cps_candtherm_30'] = df['cps_candtherm_30']
    CODEBOOK_VARIABLES['cps_candtherm_30'] = {
        'original_variable': 'cps_candtherm_30',
        'question_label': "Sur la même échelle, que pensez-vous des candidat(e)s dans votre circonscription? (Candidat(e) conservateur(trice))",
        'type': 'ordinal',
        'value_labels': {},
        'missing_codes': {
            -99: "Je ne connais pas le/la candidat"
        },
        'range_min': 0,
        'range_max': 100
     }

    # --- cps_cares_DO_1 ---
    # cps_cares_DO_1 — Display order for first leader option in cps_cares
    # Source: cps_cares_DO_1
    df_clean['cps_cares_DO_1'] = df['cps_cares_DO_1'].map({
        1.0: 'first',
        2.0: 'second',
        3.0: 'third'
    })
    CODEBOOK_VARIABLES['cps_cares_DO_1'] = {
        'original_variable': 'cps_cares_DO_1',
        'question_label': "Display order for first leader option in cps_cares",
        'type': 'ordinal',
        'value_labels': {
            'first': "premier",
            'second': "deuxième",
            'third': "troisième"
        },
        'missing_codes': []
    }

    # =========================================================================
    # VARIABLES SES DÉTAILLÉES
    # =========================================================================

    # ses_age — Âge en années
    # Source: cps_age_in_years
    df_clean['ses_age'] = pd.to_numeric(df['cps_age_in_years'], errors='coerce')
    df_clean.loc[df_clean['ses_age'] < 18, 'ses_age'] = np.nan
    CODEBOOK_VARIABLES['ses_age'] = {
        'original_variable': 'cps_age_in_years',
        'question_label': "Quel est votre âge?",
        'type': 'numeric',
        'value_labels': {},
    }

    # ses_gender — Genre
    # Source: cps_genderid
    df_clean['ses_gender'] = df['cps_genderid'].map({
        'A man':                            'homme',
        'A woman':                          'femme',
        'Non-binary':                       'non_binaire',
        'Another gender, please specify:':  'autre',
    })
    CODEBOOK_VARIABLES['ses_gender'] = {
        'original_variable': 'cps_genderid',
        'question_label': "Êtes-vous...",
        'type': 'categorical',
        'value_labels': {
            'homme':       "Homme",
            'femme':       "Femme",
            'non_binaire': "Non-binaire",
            'autre':       "Autre genre",
        },
    }

    # ses_language — Première(s) langue(s) apprise(s) (synthèse depuis cps_lang_1/2)
    lang_fr = df['cps_lang_2'] == 'French'
    lang_en = df['cps_lang_1'] == 'English'
    ses_language = pd.Series('autre', index=df.index)
    ses_language[lang_en & ~lang_fr] = 'english'
    ses_language[lang_fr] = 'french'
    df_clean['ses_language'] = ses_language
    CODEBOOK_VARIABLES['ses_language'] = {
        'original_variable': 'cps_lang_1 / cps_lang_2',
        'question_label': "Quelle est la/les première(s) langue(s) que vous avez apprise(s)?",
        'type': 'categorical',
        'value_labels': {'french': "Français", 'english': "Anglais", 'autre': "Autre"},
    }

    # ses_education — Niveau de scolarité complété
    # Source: cps_edu
    df_clean['ses_education'] = df['cps_edu'].map({
        'No schooling':                                                         'aucune_scolarite',
        'Some elementary school':                                               'primaire_incomplet',
        'Completed elementary school':                                          'primaire_complet',
        'Some secondary/ high school':                                          'secondaire_incomplet',
        'Completed secondary/ high school':                                     'secondaire_complet',
        'Some technical, community college, CEGEP, College Classique':         'cegep_incomplet',
        'Completed technical, community college, CEGEP, College Classique':    'cegep_complet',
        'Some university':                                                      'universite_incomplet',
        "Bachelor's degree":                                                    'baccalaureat',
        "Master's degree":                                                      'maitrise',
        'Professional degree or doctorate':                                     'doctorat_professionnel',
    })
    CODEBOOK_VARIABLES['ses_education'] = {
        'original_variable': 'cps_edu',
        'question_label': "Quel est votre plus haut niveau de scolarité complété?",
        'type': 'categorical',
        'value_labels': {
            'aucune_scolarite':      "Aucune scolarité",
            'primaire_incomplet':    "Quelques années primaire",
            'primaire_complet':      "École primaire terminée",
            'secondaire_incomplet':  "Quelques années secondaire",
            'secondaire_complet':    "École secondaire terminée",
            'cegep_incomplet':       "Quelques années cégep/collège",
            'cegep_complet':         "Études cégep/collège terminées",
            'universite_incomplet':  "Quelques années université",
            'baccalaureat':          "Baccalauréat",
            'maitrise':              "Maîtrise",
            'doctorat_professionnel':"Diplôme professionnel ou doctorat",
        },
    }

    # =========================================================================
    # STRATES CANONIQUES
    # =========================================================================

    # meta_strate_age_group — depuis cps_age_in_years (continu)
    age = pd.to_numeric(df['cps_age_in_years'], errors='coerce')
    df_clean['meta_strate_age_group'] = pd.cut(
        age,
        bins=[17, 34, 54, np.inf],
        labels=['18-34', '35-54', '55+']
    ).astype(object).where(age.notna())

    # meta_strate_genre — depuis cps_genderid (valeurs string dans le .dta)
    df_clean['meta_strate_genre'] = df['cps_genderid'].map({
        'A man':                            'homme',
        'A woman':                          'femme',
        'Non-binary':                       'non_binaire',
        'Another gender, please specify:':  'autre',
    })

    # meta_strate_langue — depuis cps_lang_2 (French sélectionné = 'French')
    lang_fr = df['cps_lang_2'] == 'French'
    df_clean['meta_strate_langue'] = lang_fr.map({True: 'francophone', False: 'anglo_autre'})

    # meta_strate_education — depuis cps_edu (valeurs string → 3 strates)
    df_clean['meta_strate_education'] = df['cps_edu'].map({
        'No schooling':                                                          'sans_diplome_sec',
        'Some elementary school':                                                'sans_diplome_sec',
        'Completed elementary school':                                           'sans_diplome_sec',
        'Some secondary/ high school':                                           'sans_diplome_sec',
        'Completed secondary/ high school':                                      'diplome_sec_cegep',
        'Some technical, community college, CEGEP, College Classique':          'diplome_sec_cegep',
        'Completed technical, community college, CEGEP, College Classique':     'diplome_sec_cegep',
        'Some university':                                                       'universite',
        "Bachelor's degree":                                                     'universite',
        "Master's degree":                                                       'universite',
        'Professional degree or doctorate':                                      'universite',
    })

    # meta_strate_region — depuis feduid (circonscription fédérale → région admin → strate)
    # cps_postal absent du .dta v1 (retiré pour confidentialité); feduid est dérivé
    # du PCCF par les auteurs de l'étude et couvre tous les répondants.
    # Référence : data/feduid_region_qc.csv (généré par jointure spatiale CEF × RA via ISQ)
    # Cohérent avec le mapping région admin → strate utilisé dans eeq_2018/clean.py
    _fed_csv = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'feduid_region_qc.csv')
    _fed_map = pd.read_csv(_fed_csv, usecols=['feduid', 'strate_region']).set_index('feduid')['strate_region'].to_dict()
    df_clean['meta_strate_region'] = pd.to_numeric(df['feduid'], errors='coerce').map(_fed_map)
    CODEBOOK_VARIABLES['meta_strate_region'] = {
        'original_variable': 'feduid',
        'question_label': "Région de résidence (strate canonique)",
        'type': 'categorical',
        'value_labels': {
            'montreal': "Montréal (île + Laval)",
            'couronne': "Couronne de Montréal",
            'quebec':   "Québec (Capitale-Nationale)",
            'regions':  "Régions du Québec",
        },
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
