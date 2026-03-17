#!/usr/bin/env python3
"""
Orchestration script: runs all clean.py modules and pushes data to Supabase.

Usage:
  python run_all.py --all
  python run_all.py --survey eeq_2022
"""

import argparse
import importlib.util
import inspect
import json
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INGESTION_DIR = Path(__file__).parent

# Load .env.local from repo root
load_dotenv(INGESTION_DIR.parent / ".env.local")

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY")
SHARED_FOLDER_PATH = Path(os.environ["SHARED_FOLDER_PATH"])

# Raw file map: survey_id → filename inside SHARED_FOLDER_PATH/{survey_id}/
RAW_FILE_MAP = {
    "eeq_2007": "Quebec Election Study 2007 (SPSS).sav",
    "eeq_2008": "Quebec Election Study 2008 (SPSS).sav",
    "eeq_2012": "Quebec Election Study 2012 (SPSS).sav",
    "eeq_2014": "Quebec Election Study 2014.sav",
    "eeq_2018": "Quebec Election Study 2018.dta",
    "eeq_2022": "2022 Quebec Election Study v1.dta",
}

BATCH_QUESTIONS = 100
BATCH_RESPONDENTS = 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_module(survey_id: str):
    """Dynamically import ingestion/{survey_id}/clean.py."""
    module_path = INGESTION_DIR / survey_id / "clean.py"
    if not module_path.exists():
        raise FileNotFoundError(f"clean.py not found: {module_path}")
    spec = importlib.util.spec_from_file_location(f"{survey_id}.clean", module_path)
    module = importlib.util.module_from_spec(spec)
    # Add survey dir to sys.path so relative imports inside clean.py work
    survey_dir = str(module_path.parent)
    if survey_dir not in sys.path:
        sys.path.insert(0, survey_dir)
    spec.loader.exec_module(module)
    return module


def call_clean_data(module, raw_path: Path) -> pd.DataFrame:
    """Call clean_data() with either raw_path or df depending on signature."""
    sig = inspect.signature(module.clean_data)
    param_names = list(sig.parameters.keys())
    first_param = param_names[0] if param_names else ""

    if raw_path.suffix == ".dta" or first_param == "df":
        df_raw = pd.read_stata(raw_path)
        return module.clean_data(df_raw)
    else:
        return module.clean_data(str(raw_path))


def _serialize(obj):
    """JSON-serialize pandas/numpy types."""
    import numpy as np
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if hasattr(obj, "item"):  # catch-all for other numpy scalars
        return obj.item()
    return obj


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of JSON-safe dicts."""
    records = []
    for row in df.itertuples(index=False):
        rec = {}
        for col, val in zip(df.columns, row):
            rec[col] = _serialize(val)
        records.append(rec)
    return records


def batched(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def upsert_survey(client, survey_id: str, df: pd.DataFrame, metadata: dict):
    survey_meta = metadata["survey_metadata"]
    codebook = metadata.get("codebook") or metadata.get("variables", {})

    # 1. Delete existing survey (cascades to questions + respondents)
    existing = client.table("surveys").select("id").eq("source", survey_id).execute()
    if existing.data:
        existing_id = existing.data[0]["id"]
        client.table("surveys").delete().eq("id", existing_id).execute()
        print(f"  Deleted existing survey id={existing_id}")

    # 2. Insert survey
    year = survey_meta.get("year") or int(survey_id.split("_")[-1])
    survey_row = {
        "source": survey_id,
        "title": survey_meta.get("title", survey_id),
        "year": year,
        "n_respondents": len(df),
    }
    result = client.table("surveys").insert(survey_row).execute()
    survey_db_id = result.data[0]["id"]
    print(f"  Inserted survey id={survey_db_id} ({len(df)} respondents)")

    # 3. Batch insert questions
    question_rows = []
    for var_name, var_meta in codebook.items():
        question_rows.append({
            "survey_id": survey_db_id,
            "text": var_meta.get("question_label", var_name),
            "scale_type": var_meta.get("type"),
        })

    inserted_questions = 0
    for batch in batched(question_rows, BATCH_QUESTIONS):
        client.table("questions").insert(batch).execute()
        inserted_questions += len(batch)
    print(f"  Inserted {inserted_questions} questions")

    # 4. Batch insert respondents
    # strate_* columns → strate_canonical JSONB; rest → responses JSONB
    strate_cols = [c for c in df.columns if c.startswith("strate_")]
    other_cols = [c for c in df.columns if not c.startswith("strate_")]

    records = df_to_records(df)
    respondent_rows = []
    for rec in records:
        strate = {k: rec[k] for k in strate_cols if k in rec}
        responses = {k: rec[k] for k in other_cols if k in rec}
        respondent_rows.append({
            "survey_id": survey_db_id,
            "strate_canonical": json.dumps(strate),
            "responses": json.dumps(responses),
        })

    inserted_resp = 0
    for batch in batched(respondent_rows, BATCH_RESPONDENTS):
        client.table("respondents").insert(batch).execute()
        inserted_resp += len(batch)
    print(f"  Inserted {inserted_resp} respondents")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_survey(client, survey_id: str):
    print(f"\n[{survey_id}] Starting...")

    raw_filename = RAW_FILE_MAP.get(survey_id)
    if not raw_filename:
        print(f"  ERROR: no raw file mapping for {survey_id}")
        return False

    raw_path = SHARED_FOLDER_PATH / survey_id / raw_filename
    if not raw_path.exists():
        print(f"  ERROR: raw file not found: {raw_path}")
        return False

    try:
        module = load_module(survey_id)
        df = call_clean_data(module, raw_path)
        metadata = module.get_metadata()
        upsert_survey(client, survey_id, df, metadata)
        print(f"[{survey_id}] Done.")
        return True
    except Exception as e:
        print(f"  ERROR processing {survey_id}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="Push survey data to Supabase")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Process all surveys")
    group.add_argument("--survey", metavar="SURVEY_ID", help="Process a single survey")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY (or VITE_ variants) must be set in .env.local")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    surveys_to_run = list(RAW_FILE_MAP.keys()) if args.all else [args.survey]

    results = {}
    for survey_id in surveys_to_run:
        results[survey_id] = process_survey(client, survey_id)

    print("\n--- Summary ---")
    for sid, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  {sid}: {status}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
