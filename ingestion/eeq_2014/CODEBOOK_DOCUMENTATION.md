# EEQ 2014 - Documentation du Codebook

**Sondage:** Quebec Election Study 2014 (Sondage québécois sur les élections 2014)  
**Année:** 2014  
**Langue:** Français  
**Source du codebook:** Quebec Election Study 2014 FR.doc  
**Date de documentation:** 2024-2025

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Variables démographiques (SES)](#variables-démographiques)
3. [Variables d'opinion (OP)](#variables-dopinion)
4. [Variables de comportement (BEHAV)](#variables-de-comportement)
5. [Cas ambigus et problèmes identifiés](#cas-ambigus)
6. [Index des variables](#index-des-variables)

---

## Vue d'ensemble

Ce document documente les variables du sondage "Quebec Election Study 2014" (EEQ 2014). 

- **Nombre total de questions:** 63
- **Nombre de variables source:** 137
- **Nombre de variables nettoyées:** 117

### Structure des noms de variables nettoyées

Les variables nettoyées suivent la convention de nommage suivante:
- `ses_*` : Variables socio-économiques (sexe, âge, région, etc.)
- `op_*` : Variables d'opinion (attitudes, préférences politiques, etc.)
- `behav_*` : Variables de comportement (vote, participation, etc.)
- `fin_*` : Variables financières (placements, propriétés, etc.)

---

## Variables démographiques (SES)

### Q2 → ses_province

**Libellé original:** Avez-vous voté à cette élection provinciale?


### Q8 → ses_province

**Libellé original:** Vos préférences partisanes mises à part, quel parti a selon vous mené la


### Q11 → ses_province

**Libellé original:** Et à quel point êtes-vous satisfait(e) de la performance du gouvernement


### Q16 → ses_region

**Libellé original:** Avez-vous voté lors du référendum de 1995 sur la souveraineté du Québec?

**Options:**
- Oui [aller à Q17]
- Non [aller à Q18]
- Je préfère ne pas répondre [aller à Q18]

### Q18 → ses_province

**Libellé original:** À quel point l’enjeu de l’indépendance politique du Québec est-il important pour


### Q25 → ses_province

**Libellé original:** Parmi les trois questions suivantes, laquelle préféreriez-vous voir posée dans un


### Q38 → ses_q38_response

**Libellé original:** Êtes-vous pour ou contre le mariage entre personnes de même sexe?


### Q39 → ses_party_preference

**Libellé original:** Êtes-vous pour ou contre la peine de mort?


### Q48 → ses_q48

**Libellé original:** Si le Québec devenait un pays indépendant, croyez-vous qu’il devrait garder le


### Q51 → ses_province

**Libellé original:** Dans l’avenir, quelle serait la meilleure option pour l’économie du Québec:


### Q53 → ses_region

**Libellé original:** Sur une échelle de ZÉRO à DIX, où zéro veut dire qu'il n'est PAS DU TOUT


### Q56 → ses_province

**Libellé original:** Vous sentez-vous très fortement [insérer réponse de Q55], assez fortement, ou


### Q68 → ses_marital_status

**Libellé original:** Quel est votre statut civil officiel?

**Options:**
- Marié(e)
- Marié(e), mais séparé(e)
- Célibataire
- Divorcé(e)
- Veuf/veuve
- ... (+2 autres options)


---

## Variables d'opinion (OP)

### Q1 → op_vote_intent

**Libellé original:** Parmi les enjeux suivants, lequel était, pour vous personnellement, le plus


### Q3 → op_q3

**Libellé original:** Pour quel parti avez-vous voté?


### Q4 → op_attitude_q4

**Libellé original:** Est-ce que ce parti était votre premier choix?

**Options:**
- Oui [aller à Q6]
- Non [aller à Q5]
- Je préfère ne pas répondre [aller à Q6]

### Q5 → op_first_choice_party

**Libellé original:** Quel parti était votre premier choix?


### Q6 → op_voting_intention

**Libellé original:** Pour quel parti aviez-vous voté lors de l’élection provinciale du 4 septembre


### Q9 → op_worst_campaign

**Libellé original:** Et quel parti a selon vous mené la moins bonne campagne?


### Q10 → op_vote_Q10

**Libellé original:** À quel point êtes-vous satisfait(e) de la performance du gouvernement péquiste


### Q12 → op_support_q12

**Libellé original:** Quel est votre degré d’attachement au Québec?


### Q15 → op_q15

**Libellé original:** Certaines personnes croient que les Québécois ont des valeurs et priorités qui


### Q17 → op_vote_intention

**Libellé original:** Pour quelle option avez-vous voté?


### Q19 → op_vote_intention_independentist

**Libellé original:** Si un référendum sur l’indépendance avait lieu vous demandant si vous voulez


### Q20 → op_vote_intention

**Libellé original:** Et si un référendum avait lieu vous demandant si vous voulez que l’Assemblée


### Q22 → op_q22

**Libellé original:** Et s’il y avait un référendum vous demandant de choisir parmi les trois options


### Q23 → op_vote_intention

**Libellé original:** Si l’option [RAPPEL DE LA RÉPONSE DONNÉE À Q22] était rejetée par une


### Q33 → op_response_q33

**Libellé original:** Diriez-vous que l’on peut faire confiance à la plupart des gens ou qu’on n’est



---

## Cas ambigus et problèmes identifiés

### 1. Variables mappées plusieurs fois

Les variables source suivantes sont mappées à plusieurs variables nettoyées:


### 2. Variables avec noms génériques

Les variables suivantes ont des noms génériques qui ne décrivent pas clairement leur contenu:

- Q15 → **op_q15**
- Q21 → **op_q21**
- Q22 → **op_q22**
- Q3 → **op_q3**
- Q30B → **op_q30b**
- Q31B → **op_q31b**
- Q31D → **behav_q31d**
- Q37 → **op_q37**
- Q40 → **op_q40**
- Q42B → **behav_q42b**
- Q42C → **op_q42c**
- Q42D → **op_q42d**
- Q42E → **op_q42e**
- Q42F → **op_q42f**
- Q42G → **op_q42g**
- Q42H → **ses_q42h**
- Q42I → **ses_q42i**
- Q43A → **op_q43a**
- Q43B → **op_q43b**
- Q44A → **op_q44a**
- Q45 → **op_q45**
- Q48 → **ses_q48**
- Q49 → **op_q49**
- Q55 → **op_q55**
- Q58 → **op_q58**
- Q60C → **behav_q60c**
- Q65 → **op_q65**
- Q7F → **op_q7f**
- Q7G → **behav_q7g**

### 3. Variables source non mappées

Les 17 variables suivantes du codebook n'ont pas d'équivalent nettoyé:

- Q7
- Q24
- Q29a
- Q30b
- Q31
- Q34
- Q36
- Q42a
- Q42b
- Q42d
- Q42e
- Q42f
- Q42g
- Q42h
- Q42i
- Q43b
- Q60

---

## Index des variables

| Source | Variable nettoyée(s) |
|--------|----------------------|
| CLAGE | ses_age_group |
| CODE1 | ses_code |
| CODE2 | ses_code2 |
| CODE3 | ses_code3 |
| GREET | ses_simple_binary |
| LANG | ses_lang |
| POND | ses_weight |
| Q1 | op_vote_intent |
| Q10 | op_vote_Q10 |
| Q10_vote | behav_vote_choice |
| Q11 | ses_province |
| Q12 | op_support_q12 |
| Q13 | behav_vote_intent |
| Q14A | identity_q14a |
| Q14B | identity_q14b |
| Q15 | op_q15 |
| Q16 | ses_region |
| Q17 | op_vote_intention |
| Q18 | ses_province |
| Q19 | op_vote_intention_independentist |
| Q1_gender | ses_gender |
| Q2 | ses_province |
| Q20 | op_vote_intention |
| Q21 | op_q21 |
| Q22 | op_q22 |
| Q23 | op_vote_intention |
| Q24A | op_constitutional_rank_1 |
| Q24B | op_constitutional_rank_2 |
| Q24C | op_constitutional_rank_3 |
| Q25 | ses_province |
| Q26A | ses_frequency |
| Q26B | op_question_26b |
| Q27A | op_choice_party |
| Q27B | op_Q27B |
| Q28 | behav_vote_party |
| Q2_province | ses_province |
| Q3 | op_q3 |
| Q30A | ses_voting_intention_party |
| Q30B | op_q30b |
| Q30C | behav_voting_intent_party |
| Q31A | behav_political_donation_last12m |
| Q31B | op_q31b |
| Q31C | behav_Q31C |
| Q31D | behav_q31d |
| Q31E | op_scale_q31e |
| Q31F | behav_vote_frequency |
| Q32 | op_attitude_q32 |
| Q33 | op_response_q33 |
| Q34A | op_liberal_approval |
| Q34B | op_vote_intention_b |
| Q34C | op_q34c_response |
| Q34D | op_opinion_d |
| Q35 | behav_vote_choice |
| Q36A | op_opinion_social_a |
| Q36B | op_opinion_social_b |
| Q36C | op_opinion_social_c |
| Q36D | op_opinion_social_d |
| Q36E | op_opinion_social_e |
| Q37 | op_q37 |
| Q38 | ses_q38_response |
| Q39 | ses_party_preference |
| Q4 | op_attitude_q4 |
| Q40 | op_q40 |
| Q41 | op_response_q41 |
| Q42A | op_vote_intention_party_a |
| Q42B | behav_q42b |
| Q42C | op_q42c |
| Q42D | op_q42d |
| Q42E | op_q42e |
| Q42F | op_q42f |
| Q42G | op_q42g |
| Q42H | ses_q42h |
| Q42I | ses_q42i |
| Q43A | op_q43a |
| Q43B | op_q43b |
| Q44A | op_q44a |
| Q44B | op_response_b |
| Q45 | op_q45 |
| Q46 | op_independence_economic_comparison |
| Q47 | op_opinion_q47 |
| Q48 | ses_q48 |
| Q49 | op_q49 |
| Q5 | op_first_choice_party |
| Q50 | op_attitude_q50 |
| Q51 | ses_province |
| Q52 | behav_vote_intent |
| Q53 | ses_region |
| Q54 | op_charter_importance |
| Q55 | op_q55 |
| Q56 | ses_province |
| Q57 | behav_voter_choice |
| Q58 | op_q58 |
| Q59A | fin_savings_bank |
| Q59B | fin_trust_account |
| Q59C | fin_rrsp_tfsa |
| Q59D | fin_stocks_shares |
| Q59E | fin_bonds |
| Q59F | fin_financial_assets_portfolio |
| Q59G | fin_retirement_savings_plan |
| Q5_satisfaction | op_satisfaction_gov |
| Q6 | op_voting_intention |
| Q60A | behav_q60a_response |
| Q60B | behav_vote_q60b |
| Q60C | behav_q60c |
| Q60D | ses_Q60D |
| Q60E | ses_region |
| Q61 | op_vote_intention |
| Q62 | var_q62 |
| Q63 | op_vote_intention |
| Q64 | op_vote_intention |
| Q65 | op_q65 |
| Q66 | behav_response_q66 |
| Q67 | op_attitude_q67 |
| Q68 | ses_marital_status |
| Q7A | behav_party_vote_actual |
| Q7B | ses_province |
| Q7C | behav_vote_intention_q7c |
| Q7D | op_response |
| Q7E | ses_province |
| Q7F | op_q7f |
| Q7G | behav_q7g |
| Q8 | ses_province |
| Q9 | op_worst_campaign |
| QAGE | ses_birth_year |
| QENFAN | ses_child_status |
| QLANG | ses_language |
| QREGION | ses_region |
| QSCOL | op_col_support |
| QSEXE | ses_sex |
| REGIO | ses_region |
| SDAT | ses_date |
| SEL1 | ses_sel |
| SEL2 | ses_sel2 |
| SEL3 | ses_sel3 |
| SEL4 | ses_self_rating |
| SMAGE | ses_age |
| y | x |
