# Validation des variables q50x (BEHAV) - EEQ 2018

## Variables analysées
- q50x1: `behav_vote_at_16_control` (contrôle)
- q50x2: `behav_vote_at_16_argument_participation` (argument participation)
- q50x3: `behav_vote_at_16_argument_rights` (argument droits)

## Source de vérité
Fichier source : `/home/hubcad25/opubliq/gdrive/_SharedFolder_data_produit/eeq_2018/Quebec Election Study 2018 FR with programmed answer values_converted.md`

Lignes codebook : 1893-1959

## Contexte de l'expérimentation
Il s'agit d'une expérimentation avec randomisation sur le droit de vote à 16 ans. Les trois variantes (Q50x1-3) testent différents cadres de présentation :

1. **Q50x1 (CONTROL)** : Question basique sans argument
2. **Q50x2 (PARTICIPATION ARGUMENT)** : Argument sur l'augmentation de la participation électorale chez les jeunes
3. **Q50x3 (RIGHTS ARGUMENT)** : Argument sur la cohérence des droits (conduire, travailler, payer impôts)

## Validation des formats

### Format
✅ **ORDINAL** - Correctement identifié comme ordinal dans le code (type: 'ordinal')

### Échelle de réponse
✅ **4-point Likert scale** (identique pour les trois variantes) :
- 1 = "Fortement d'accord" → mapped as 'strongly_agree'
- 2 = "Plutôt d'accord" → mapped as 'somewhat_agree'
- 3 = "Plutôt en désaccord" → mapped as 'somewhat_disagree'
- 4 = "Fortement en désaccord" → mapped as 'strongly_disagree'

### Valeurs manquantes
✅ Correctement mappées à NaN :
- 98.0 = "Je ne sais pas"
- 99.0 = "Je préfère ne pas répondre"

## Validation des labels

### Q50x1 - Contrôle
**Codebook (ligne 1895-1898):**
```
Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de 
voter à 16 ans?
```

**Code mapping (ligne 869):**
```
'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (contrôle)"
```
✅ Labels exacts correspondent au codebook (avec précision "(contrôle)" ajoutée pour clarté)

### Q50x2 - Argument Participation
**Codebook (ligne 1914-1922):**
```
Il y a déjà eu des discussions à propos de l'idée d'abaisser l'âge de
vote à 16 ans au Québec. Un argument avancé est que la participation aux
élections est en baisse, en particulier chez les jeunes, et que
l'abaissement de l'âge de vote pourrait contribuer à accroître la
participation électorale...
Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de
voter à 16 ans?
```

**Code mapping (ligne 892):**
```
'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (argument: participation)"
```
✅ Label correct (version condensée mais fidèle au contenu du codebook)

### Q50x3 - Argument Droits
**Codebook (ligne 1939-1947):**
```
Il y a déjà eu des discussions portant sur des changements au droit de
vote au Québec. Un argument avancé est qu'il devrait y avoir une
cohérence entre les droits sociaux, politiques et économiques des
citoyens...
Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de
voter à 16 ans?
```

**Code mapping (ligne 915):**
```
'question_label': "Êtes-vous en accord ou en désaccord avec l'idée d'abaisser l'âge de voter à 16 ans? (argument: droits)"
```
✅ Label correct (version condensée mais fidèle au contenu du codebook)

## Validation des valeurs

### Ordres de réponse
✅ L'ordre 1→4 correspond exactement au codebook (accord → désaccord)

### Labels des valeurs
✅ Tous les labels sont en français et correspondent exactement :
- 'strongly_agree' = "Fortement d'accord"
- 'somewhat_agree' = "Plutôt d'accord"
- 'somewhat_disagree' = "Plutôt en désaccord"
- 'strongly_disagree' = "Fortement en désaccord"

## Résumé de validation

| Critère | Statut | Notes |
|---------|--------|-------|
| Format (ordinal/binaire) | ✅ VALIDE | Ordinal confirmé |
| Échelle de réponse | ✅ VALIDE | 4-point Likert correct |
| Labels des réponses | ✅ VALIDE | Français exact, cohérent |
| Labels des variantes | ✅ VALIDE | Correspond au codebook |
| Valeurs 1-4 | ✅ VALIDE | Accord → désaccord |
| Valeurs manquantes | ✅ VALIDE | 98/99 → NaN |
| Correspondance codebook | ✅ VALIDE | Entièrement validé |

## Localisation du code
- Fichier: `/home/hubcad25/opubliq/repos/opubliq-simulator/ingestion/eeq_2018/clean.py`
- Lignes: 856-923
- Variables créées: `behav_vote_at_16_*`
- Codebook: `CODEBOOK_VARIABLES` (lignes 867-923)

## Statut
✅ **VALIDATION COMPLÈTE - AUCUNE CORRECTION NÉCESSAIRE**

Toutes les variables Q50x1-3 sont correctement mappées et documentées. Les formats, labels et valeurs correspondent exactement au codebook source.