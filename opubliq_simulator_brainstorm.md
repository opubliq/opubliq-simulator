# Opubliq Simulator — Notes de brainstorming

## Contexte

Session de réflexion sur l'architecture technique et le positionnement produit d'un simulateur d'opinion publique québécois.

---

## Produit : le simulateur

### Cas d'usage principal

L'utilisateur input une question de sondage fictive. Le modèle estime la distribution des réponses à cette question en simulant une population québécoise.

### Logique du modèle (3 étapes)

1. **RAG sur les données de sondage** — recherche sémantique dans les questions ET réponses de sondages existants pour identifier les variables et répondants les plus proches de la question de l'utilisateur.

2. **Construction des priors bayésiens** — les priors sont construits à partir des distributions statistiques des réponses trouvées à l'étape 1, puis ajustés par du contexte textuel injecté (articles médiatiques, communiqués) pour refléter l'état actuel de l'enjeu.

3. **Silicon sampling** — un LLM se met dans la peau de vrais répondants (profil SES + opinions historiques sur questions similaires), calibré par les priors bayésiens mis à jour à l'étape 2, et simule leurs réponses à la question de l'utilisateur.

**Output :** distribution d'opinion simulée, décomposée par sous-groupes socio-démographiques.

### Différenciation vs Aaru / Simile

| | Aaru | Simile | Opubliq |
|---|---|---|---|
| Source | Populations synthétiques | Jumeaux numériques (interviews) | Vraies données québécoises |
| Géographie | Global | Global | Québec-spécifique |
| Validation | Limitée | GSS (USA) | Calibrée sur sondages réels |
| Contexte temps réel | Non | Non | Oui (articles médiatiques) |

**Avantage clé :** prior vient des données réelles, pas du LLM. Scientifiquement défendable.

---

## Architecture technique

### Pipeline de données (long terme)

```
Google Drive (raw files — xlsx, csv, sav)
    ↓ déclenché manuellement via GitHub Actions
S3 (data.parquet + codebook.json par sondage)
```

Structure S3 :
```
s3://opubliq-surveys/
    survey-001/
        data.parquet
        codebook.json
```

### Prototype (court terme — 5 sondages)

```
React (Netlify)
    ↓ direct
Supabase (métadonnées + pgvector pour recherche sémantique)
    ↓ direct
Gemini Flash API (clé fournie par l'user dans le browser)
```

**Zéro backend pour le proto.** Backend FastAPI ajouté plus tard pour cacher une clé maître en prod.

### Pourquoi pas AWS / GCP pour le proto

- Supabase free tier suffisant pour 5 sondages (~1000x100 répondants)
- pgvector natif dans Supabase = recherche sémantique sans infra additionnelle
- Gemini Flash free tier (15 req/min, 1M tokens/jour) suffisant pour 2-3 users

### Stack long terme (quand DevOps rejoindra)

```
S3          → Parquet répondants (données brutes)
Supabase    → métadonnées + embeddings (pgvector)
FastAPI     → backend (Railway ou AWS)
React       → frontend (Netlify)
```

---

## Repo

Nouveau repo distinct : **`opubliq-simulator`**

Séparé de `survey-cleaner` (pipeline nettoyage) et de `mvp_moteur_recherche_sondages` (moteur recherche).

---

## Marché et positionnement

### Clients cibles prioritaires

- **Firmes de conseil stratégique** (besoin de stress-tester des décisions sur la population québécoise)
- **Agences de communication/affaires publiques** (valider un message avant lancement)
- **Gouvernements** (enjeux émergents — réponse en 24h avant de commander un vrai sondage à $50k)
- **Candidats politiques / associations de circonscription** (2026)

### Pitch central

> "Réponse en 30 secondes sur l'opinion québécoise vs 6 semaines et $15 000 pour un sondage. Calibré sur de vraies données, mis à jour par l'actualité."

### Données de marché (gouvernement fédéral)

Le gouvernement fédéral a attribué 111 contrats de recherche sur mesure en opinion publique en 2024-2025, pour **14,2 millions de dollars**. Chaque contrat peut aller jusqu'à $300 000.

### Modèle de revenu envisagé

- Accès libre-service : $200-500/requête ou abonnement mensuel
- Contrats enterprise (gouvernements, grandes firmes) : $50 000-150 000/an

---

## Prochaines étapes

1. Créer repo `opubliq-simulator`
2. Définir schéma Supabase (métadonnées + vecteurs)
3. Charger 5 sondages cleanés dans Supabase
4. Construire prototype React — input question → simulation → distribution output
