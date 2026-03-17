#!/usr/bin/env python3
"""
Charge ingestion/weights/strate_weights_final.csv dans la table strate_weights de Supabase.

Usage:
    python ingestion/weights/load_strate_weights.py

Variables d'environnement requises (depuis .env ou .env.local) :
    SUPABASE_URL  (ou VITE_SUPABASE_URL)
    SUPABASE_KEY  (ou VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY)

Le script fait un TRUNCATE + INSERT de toutes les lignes du CSV.
Colonnes attendues dans le CSV :
    strate_age_group | strate_langue | strate_region | strate_genre | weight
"""

import os
import sys
import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Charger .env.local si présent
# ---------------------------------------------------------------------------
repo_root = Path(__file__).resolve().parents[2]
env_file = repo_root / ".env.local"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.environ.get("VITE_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL et SUPABASE_KEY (ou variantes VITE_) doivent être définis.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Lire le CSV
# ---------------------------------------------------------------------------
csv_path = Path(__file__).parent / "strate_weights_final.csv"
if not csv_path.exists():
    print(f"ERROR: fichier introuvable: {csv_path}")
    sys.exit(1)

rows = []
with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append({
            "strate_age_group": row["strate_age_group"],
            "strate_langue":    row["strate_langue"],
            "strate_region":    row["strate_region"],
            "strate_genre":     row["strate_genre"],
            "weight":           float(row["weight"]),
        })

print(f"Lu {len(rows)} lignes depuis {csv_path.relative_to(repo_root)}")

# ---------------------------------------------------------------------------
# Push vers Supabase : truncate + insert
# ---------------------------------------------------------------------------
client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("Truncate table strate_weights...")
# Supabase REST API ne supporte pas TRUNCATE directement — on delete tout
client.table("strate_weights").delete().neq("strate_age_group", "__never__").execute()

print(f"Insertion de {len(rows)} lignes...")
result = client.table("strate_weights").insert(rows).execute()
print(f"OK — {len(result.data)} lignes insérées.")

# Vérification rapide
check = client.table("strate_weights").select("*").execute()
print(f"Total dans Supabase : {len(check.data)} lignes.")
