# opubliq-simulator

Simulateur d'opinion publique québécoise. Estime la distribution des réponses à des questions de sondage fictives en combinant données historiques, context temps réel et simulation LLM.

## 📋 Architecture générale

```
┌─────────────┐
│   React     │  Frontend (Netlify)
│  + Vite     │
└──────┬──────┘
       │
       ├─────────────────────┐
       │                     │
┌──────▼──────────┐  ┌──────▼────────────┐
│   Supabase      │  │  Gemini Flash     │
│  - Métadonnées  │  │     API           │
│  - Embeddings   │  │  (Simulation)     │
│  - pgvector     │  │                   │
└─────────────────┘  └───────────────────┘
```

**Stack prototype :** React + Supabase + Gemini Flash  
**Zéro backend** pour le prototype (clé Gemini côté client)

---

## 🎯 Logique du modèle (3 étapes)

### 1️⃣ Recherche sémantique (RAG)
- L'utilisateur pose une question de sondage
- Recherche dans la base (pgvector) : questions + réponses similaires
- Identifie les variables et répondants les plus proches

### 2️⃣ Construction des priors bayésiens
- Extrait les distributions statistiques des résultats similaires
- Ajuste avec du contexte temps réel (articles médiatiques, communiqués)
- Crée des priors reflétant l'état actuel de l'enjeu

### 3️⃣ Silicon Sampling (LLM)
- Gemini Flash simule des répondants réels (profil SES + historique opinions)
- Calibré par les priors bayésiens
- Output : distribution simulée par sous-groupe socio-démographique

---

## 📁 Structure du repo

```
opubliq-simulator/
├── frontend/              # React + Vite app
│   ├── src/
│   ├── public/
│   └── vite.config.ts
├── ingestion/             # Scripts de chargement données
│   ├── load_surveys.py
│   └── embeddings_generator.py
├── supabase/
│   ├── migrations/        # Schéma SQL (5 tables)
│   └── functions/         # Edge Functions (optionnel)
├── .gitignore
├── .env.example
└── README.md
```

### `/frontend`
React + Vite + TypeScript  
Configuration Netlify pour déploiement CI/CD

### `/ingestion`
Scripts Python pour :
- Charger les sondages cleanés dans Supabase
- Générer les embeddings (pgvector)

### `/supabase/migrations`
Schéma SQL :
- `surveys` : métadonnées sondage
- `questions` : questions du sondage
- `options` : options de réponse
- `respondents` : profils des répondants
- `responses` : réponses historiques + embeddings

---

## 🚀 Quick Start

### Prérequis
- Node.js 18+ (frontend)
- Python 3.10+ (ingestion)
- Compte Supabase (free tier)
- Clé API Gemini Flash

### Setup

#### 1. **Cloner et install dépendances**
```bash
git clone <repo>
cd opubliq-simulator
```

#### 2. **Supabase Setup** 🔧
1. Go to [supabase.com](https://supabase.com) and create a new **free tier** project
2. Once project is created, go to **Settings → API** and copy:
   - **SUPABASE_URL** (starts with `https://...supabase.co`)
   - **SUPABASE_ANON_KEY** (public key for client-side use)
3. Enable `pgvector` extension:
   - In Supabase dashboard, go to **SQL Editor**
   - Run this query:
     ```sql
     CREATE EXTENSION IF NOT EXISTS vector;
     ```
4. Load the database schema:
   - Copy migrations from `/supabase/migrations` and run them in **SQL Editor**
   - Or use Supabase CLI: `supabase db push` (if using local Supabase)

#### 3. **Environment Variables**
```bash
cp .env.example .env.local
```
Then edit `.env.local` and fill in:
```env
VITE_SUPABASE_URL=https://your-project.supabase.co
VITE_SUPABASE_ANON_KEY=your-anon-key-from-settings
VITE_GEMINI_API_KEY=your-gemini-api-key
```

Get Gemini API key: [ai.google.dev](https://ai.google.dev)

#### 4. **Frontend**
```bash
cd frontend
npm install
npm run dev
```
Visit http://localhost:5173

#### 5. **Ingestion** (Load Survey Data)
```bash
cd ingestion
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python load_surveys.py
```

---

## 📊 Données

**Prototype :** 5 sondages québécois cleanés (~1000 répondants × 100 variables par sondage)

Structure S3 (long terme) :
```
s3://opubliq-surveys/
  survey-001/
    data.parquet
    codebook.json
```

---

## 🔄 Pipeline (MVP)

1. **Q1 2026** : Repo + frontend + Supabase schema ✅ (en cours)
2. **Q1 2026** : Chargement 5 sondages + embeddings
3. **Q1 2026** : Prototype React fonctionnel
4. **Q2 2026** : Backend FastAPI (masquer clé Gemini)
5. **Q2 2026** : S3 + pipeline AWS (données réelles)

---

## 📚 Ressources

- [Supabase Docs](https://supabase.com/docs)
- [pgvector Guide](https://github.com/pgvector/pgvector)
- [Gemini API](https://ai.google.dev)
- [Vite Docs](https://vitejs.dev)

---

## 👥 Équipe

Géré par l'équipe opubliq  
Pour questions : voir AGENTS.md
