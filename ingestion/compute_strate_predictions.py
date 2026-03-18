#!/usr/bin/env python3
"""
Régression linéaire par question × strate → strate_predictions

Pour chaque question de chaque sondage (hors préfixe 'meta', N >= 30),
ajuste un modèle de régression sur les microdata des répondants et prédit
la distribution de réponses pour chacune des 48 strates canoniques.

Choix de modèle par scale_type:
  - categorical, binary, ordinal, likert → LogisticRegression (softmax)
    → distribution = {"valeur": probabilité, ...}
  - numeric, continuous → LinearRegression
    → distribution = {"mean": x, "sd": y}

Variables explicatives: age_group, langue, region, genre (effets principaux,
one-hot encoding, drop_first=True). Les dimensions manquantes dans un sondage
sont exclues du modèle pour ce sondage.

Exception eeq_2008: strate_region ne distingue pas 'montreal' et 'couronne';
on utilise la prédiction de 'montreal' comme proxy pour 'couronne'.

Usage:
  python compute_strate_predictions.py --all
  python compute_strate_predictions.py --survey eeq_2022
"""

import argparse
import itertools
import json
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sklearn.linear_model import LinearRegression, LogisticRegression
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INGESTION_DIR = Path(__file__).parent
load_dotenv(INGESTION_DIR.parent / ".env.local")

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY")

# Strates canoniques: 3 × 2 × 4 × 2 = 48
STRATE_VALUES = {
    "age_group": ["18-34", "35-54", "55+"],
    "langue":    ["francophone", "anglo_autre"],
    "region":    ["montreal", "couronne", "quebec", "regions"],
    "genre":     ["homme", "femme"],
}

# Colonnes strate dans strate_canonical JSONB (avec préfixe meta_strate_)
STRATE_COLS = {
    "age_group": "meta_strate_age_group",
    "langue":    "meta_strate_langue",
    "region":    "meta_strate_region",
    "genre":     "meta_strate_genre",
}

CATEGORICAL_TYPES = {"categorical", "binary", "ordinal", "likert"}
NUMERIC_TYPES     = {"numeric", "continuous"}

MIN_N = 30  # seuil minimal de répondants par question

BATCH_PREDICTIONS = 200

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def all_strates():
    """Retourne la liste des 48 strates canoniques comme dicts."""
    keys = list(STRATE_VALUES.keys())
    for combo in itertools.product(*STRATE_VALUES.values()):
        yield dict(zip(keys, combo))


def build_X(df_resp: pd.DataFrame, dims: list[str]) -> pd.DataFrame:
    """
    Construit la matrice X (one-hot, drop_first=True) à partir des colonnes
    de strate présentes dans df_resp.
    """
    cols = [STRATE_COLS[d] for d in dims]
    X = pd.get_dummies(df_resp[cols], drop_first=True)
    return X.astype(float)


def strate_to_X_row(strate: dict, dims: list[str], X_ref: pd.DataFrame, region_remap: dict | None = None) -> pd.DataFrame:
    """
    Construit une ligne X pour une strate canonique donnée, en alignant sur
    les colonnes de X_ref (issues du get_dummies du jeu d'entraînement).

    region_remap: optionnel, ex. {"couronne": "montreal"} pour eeq_2008.
    """
    row = {STRATE_COLS[d]: strate[d] for d in dims}
    if region_remap and "region" in dims:
        col = STRATE_COLS["region"]
        row[col] = region_remap.get(row[col], row[col])
    df_row = pd.DataFrame([row])
    X_row = pd.get_dummies(df_row, drop_first=False).reindex(columns=X_ref.columns, fill_value=0)
    return X_row.astype(float)


def fit_predict_categorical(y_series: pd.Series, X: pd.DataFrame, strates: list[dict], dims: list[str], region_remap: dict | None = None) -> list[dict]:
    """
    Ajuste une régression logistique multinomiale et retourne la distribution
    prédite pour chaque strate.
    """
    # Encoder les valeurs de y comme strings pour stabilité
    y = y_series.astype(str)
    classes = sorted(y.unique())

    if len(classes) == 1:
        # Une seule classe: distribution triviale
        dist = {classes[0]: 1.0}
        return [dist for _ in strates]

    model = LogisticRegression(
        solver="lbfgs",
        max_iter=500,
        C=1.0,
    )
    model.fit(X, y)

    results = []
    for strate in strates:
        X_row = strate_to_X_row(strate, dims, X, region_remap=region_remap)
        proba = model.predict_proba(X_row)[0]
        dist = {cls: round(float(p), 6) for cls, p in zip(model.classes_, proba)}
        results.append(dist)
    return results


def fit_predict_numeric(y_series: pd.Series, X: pd.DataFrame, strates: list[dict], dims: list[str], region_remap: dict | None = None) -> list[dict]:
    """
    Ajuste une régression linéaire et retourne {mean, sd} pour chaque strate.
    Le sd est l'écart-type des résidus (homoscédastique).
    """
    y = y_series.astype(float)
    model = LinearRegression()
    model.fit(X, y)

    residuals = y - model.predict(X)
    sd = float(residuals.std(ddof=1)) if len(residuals) > 1 else 0.0

    results = []
    for strate in strates:
        X_row = strate_to_X_row(strate, dims, X, region_remap=region_remap)
        mean_pred = float(model.predict(X_row)[0])
        results.append({"mean": round(mean_pred, 6), "sd": round(sd, 6)})
    return results


def active_dims(df_resp: pd.DataFrame) -> list[str]:
    """
    Retourne les dimensions SES disponibles dans le sondage (celles qui ont
    au moins une valeur non-null dans df_resp).
    """
    dims = []
    for dim, col in STRATE_COLS.items():
        if col in df_resp.columns and df_resp[col].notna().any():
            dims.append(dim)
    return dims


def batched(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i: i + size]


# ---------------------------------------------------------------------------
# Core: compute predictions for one survey
# ---------------------------------------------------------------------------

def compute_for_survey(client, survey_id: str, eeq_2008_region_proxy: bool = False):
    print(f"\n[{survey_id}] Chargement des données...")

    # 1. Récupérer l'id de la survey
    r = client.table("surveys").select("id").eq("source", survey_id).execute()
    if not r.data:
        print(f"  Survey '{survey_id}' introuvable en DB. Skipping.")
        return False
    survey_db_id = r.data[0]["id"]

    # 2. Récupérer les questions (hors meta et ses)
    # On ne modélise que les opinions/comportements, pas les variables sociodémographiques.
    r = client.table("questions") \
        .select("id, var_name, prefix, scale_type") \
        .eq("survey_id", survey_db_id) \
        .neq("prefix", "meta") \
        .neq("prefix", "ses") \
        .execute()
    questions = r.data
    print(f"  {len(questions)} questions (hors meta/ses) trouvées")

    # 3. Récupérer les répondants (strate_canonical + responses)
    respondents_raw = []
    page_size = 1000
    offset = 0
    while True:
        r = client.table("respondents") \
            .select("strate_canonical, responses") \
            .eq("survey_id", survey_db_id) \
            .range(offset, offset + page_size - 1) \
            .execute()
        if not r.data:
            break
        respondents_raw.extend(r.data)
        if len(r.data) < page_size:
            break
        offset += page_size
    print(f"  {len(respondents_raw)} répondants chargés")

    if not respondents_raw:
        print("  Aucun répondant, skipping.")
        return False

    # 4. Construire le DataFrame des répondants
    rows = []
    for resp in respondents_raw:
        strate = resp.get("strate_canonical") or {}
        responses = resp.get("responses") or {}
        if isinstance(strate, str):
            strate = json.loads(strate)
        if isinstance(responses, str):
            responses = json.loads(responses)
        row = {**strate, **responses}
        rows.append(row)
    df_resp = pd.DataFrame(rows)

    # Exception eeq_2008: couronne et montreal sont confondus dans les données brutes.
    # On remplace 'couronne' → 'montreal' dans les répondants AVANT de fitter le modèle,
    # pour que le modèle apprenne montreal comme proxy de montreal+couronne.
    # À la prédiction, on enverra 'montreal' pour les deux strates.
    if eeq_2008_region_proxy and "meta_strate_region" in df_resp.columns:
        df_resp["meta_strate_region"] = df_resp["meta_strate_region"].replace("couronne", "montreal")
        print("  eeq_2008: couronne → montreal dans les répondants (proxy)")

    # 5. Déterminer les dimensions disponibles
    dims = active_dims(df_resp)
    print(f"  Dimensions actives: {dims}")

    if not dims:
        print("  Aucune dimension SES disponible, skipping.")
        return False

    # 6. Supprimer les prédictions existantes pour ce sondage (via question_ids)
    q_ids = [q["id"] for q in questions]
    if q_ids:
        for batch in batched(q_ids, 200):
            client.table("strate_predictions") \
                .delete() \
                .in_("question_id", batch) \
                .execute()
        print("  Prédictions existantes supprimées")

    # 7. Construire la liste des strates à prédire
    strates_list = list(all_strates())

    # 8. eeq_2008: au moment de prédire pour 'couronne', on envoie 'montreal' au modèle
    #    (les répondants ont déjà été remappés montreal+couronne → montreal plus haut).
    region_remap = {"couronne": "montreal"} if eeq_2008_region_proxy else None

    # 9. Modéliser chaque question
    n_success = 0
    n_skip = 0
    all_prediction_rows = []

    for q in questions:
        q_id = q["id"]
        var_name = q.get("var_name") or ""
        scale_type = q.get("scale_type") or ""

        if not var_name:
            n_skip += 1
            continue

        # Récupérer la colonne de réponse
        if var_name not in df_resp.columns:
            n_skip += 1
            continue

        y_raw = df_resp[var_name]

        # Listwise deletion: exclure les lignes où y ou une dim SES est null
        strate_dim_cols = [STRATE_COLS[d] for d in dims]
        mask = y_raw.notna()
        for col in strate_dim_cols:
            if col in df_resp.columns:
                mask = mask & df_resp[col].notna()

        y_series = y_raw[mask]
        df_sub = df_resp[mask]

        if len(y_series) < MIN_N:
            n_skip += 1
            continue

        X = build_X(df_sub, dims)

        try:
            if scale_type in CATEGORICAL_TYPES:
                predictions = fit_predict_categorical(y_series, X, strates_list, dims, region_remap=region_remap)
            elif scale_type in NUMERIC_TYPES:
                predictions = fit_predict_numeric(y_series, X, strates_list, dims, region_remap=region_remap)
            else:
                # Type inconnu: traiter comme categorical si peu de valeurs uniques, sinon numeric
                n_unique = y_series.nunique()
                if n_unique <= 10:
                    predictions = fit_predict_categorical(y_series, X, strates_list, dims, region_remap=region_remap)
                else:
                    predictions = fit_predict_numeric(y_series, X, strates_list, dims, region_remap=region_remap)
        except Exception as e:
            print(f"    WARN: {var_name} ({scale_type}) — modèle échoué: {e}")
            n_skip += 1
            continue

        # Construire les lignes à insérer
        for strate, dist in zip(strates_list, predictions):
            row = {
                "question_id": q_id,
                "strate_age_group": strate["age_group"],
                "strate_langue":    strate["langue"],
                "strate_region":    strate["region"],
                "strate_genre":     strate["genre"],
                "distribution":     json.dumps(dist),
            }
            all_prediction_rows.append(row)

        n_success += 1

    print(f"  Modèles: {n_success} succès, {n_skip} skippés")

    # 11. Upsert en batch
    total = 0
    for batch in batched(all_prediction_rows, BATCH_PREDICTIONS):
        client.table("strate_predictions").upsert(batch, on_conflict="question_id,strate_age_group,strate_langue,strate_region,strate_genre").execute()
        total += len(batch)

    print(f"  {total} prédictions upsertées")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SURVEYS = ["eeq_2007", "eeq_2008", "eeq_2012", "eeq_2014", "eeq_2018", "eeq_2022"]


def main():
    parser = argparse.ArgumentParser(description="Compute strate predictions from respondent microdata")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Process all surveys")
    group.add_argument("--survey", metavar="SURVEY_ID", help="Process a single survey")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env.local")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    surveys_to_run = SURVEYS if args.all else [args.survey]

    results = {}
    for survey_id in surveys_to_run:
        is_eeq_2008 = survey_id == "eeq_2008"
        results[survey_id] = compute_for_survey(client, survey_id, eeq_2008_region_proxy=is_eeq_2008)

    print("\n--- Résumé ---")
    for sid, ok in results.items():
        status = "OK" if ok else "FAILED/SKIPPED"
        print(f"  {sid}: {status}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
