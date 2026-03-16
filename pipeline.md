INPUTS:

- l'utilisateur pose une question de sondage fictive dans une boite de texte. Exemple: Les immigrants faisant déjà partie du programme devraient avoir une clause grand-père. Êtes-vous d'accord? (likert)

- L'utilisateur ajoute du contexte pertinent sur cette question (articles médiatiques, rapports de sondages récents pertinents, etc.)

PIPELINE

1. On filtre les questions dans notre BD qui sont pertinentes à cet enjeu, faite par un LLM (ou pt un modele d'embedding plus simple? Dépend de la performance.)
    1.1. Il faudrait également déjà quantifier la "ressemblance" de cette question par rapport à notre question fictive. Ce score sera utile plus tard pour notre index de certitude.

2. On construit des "priors" basés sur ces réponses + l'actualité.

OU

2. On passe déjà à la simulation de LLM, plus simple. Détails:

- On se fait une liste de strates de population selon SES pertinents (exemple age X genre X langue X region X education)
- On crée un system prompt du genre:
    """ tu es un [genre] de [age] ans, [langue], [region]. Voici tes attitudes sur ces questions:
            - Plus ou moins d'immigrants: Plus (en 2012)
            - Immigrants devraient apprendre le français: Pas nécessairement (en 2018)
        [Résumé du contexte] Le programme PEQ est blabla par la CAQ depuis 5 mois. On parle de la clause grand-père blabla qui aurait X comme impact. C'est relativement médiatisé dans des sources comme le devoir, la presse, etc.
        Répond à cette question de sondage: Les immigrants faisant déjà partie du programme devraient avoir une clause grand-père. Êtes-vous d'accord? (likert) """

On passe ça à travers toutes nos strates.

Le problème est l'aspect d'intervalle: en ayant un LLM par strate, on a seulement un point de donnée par strate. À moins qu'on demande au LLM de quantifier l'intervalle? Dans le prompt, les réponses à des questions antérieures pourraient inclure des marges d'erreur aussi. C'est là qu'avoir plusieurs répondants par strates serait intéressant par contre, mais pour proto je crois que l'option "LLM donne une ME lui-même" est pt correct?
**Les attitudes sur anciennes questions pertinentes pourraient simplement être la prédiction d'un modèle de régression simple sur les SES.