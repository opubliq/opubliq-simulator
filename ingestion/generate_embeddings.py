#!/usr/bin/env python3
"""
Generate Gemini embeddings for questions that don't have one yet.

Uses text-embedding-004 (768 dimensions) from Google Gemini.
Idempotent: only processes questions where embedding IS NULL.

Usage:
  python generate_embeddings.py
  python generate_embeddings.py --survey eeq_2022
  python generate_embeddings.py --dry-run
  python generate_embeddings.py --batch-size 50 --sleep 1.0
"""

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INGESTION_DIR = Path(__file__).parent
load_dotenv(INGESTION_DIR.parent / ".env.local")

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIMS = 1536  # Matryoshka truncation — near-full quality, fits within pgvector HNSW 2000-dim limit
DEFAULT_BATCH_SIZE = 100
DEFAULT_SLEEP = 1.0  # seconds between batches


# ---------------------------------------------------------------------------
# Gemini client
# ---------------------------------------------------------------------------

def make_gemini_client(api_key: str):
    from google import genai
    return genai.Client(api_key=api_key)


def embed_batch(client, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts using Gemini gemini-embedding-001."""
    from google.genai import types
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=texts,
        config=types.EmbedContentConfig(
            task_type="SEMANTIC_SIMILARITY",
            output_dimensionality=EMBEDDING_DIMS,
        ),
    )
    return [e.values for e in result.embeddings]


# ---------------------------------------------------------------------------
# Fetch questions without embeddings
# ---------------------------------------------------------------------------

def fetch_questions_without_embeddings(client, survey_source: str | None) -> list[dict]:
    """
    Returns list of {id, text} for questions where embedding IS NULL.
    Optionally filtered to a single survey by source identifier.
    """
    query = (
        client.table("questions")
        .select("id, text, survey_id")
        .is_("embedding", "null")
    )

    if survey_source:
        # Resolve survey_id from source string
        survey_result = (
            client.table("surveys")
            .select("id")
            .eq("source", survey_source)
            .execute()
        )
        if not survey_result.data:
            print(f"ERROR: No survey found with source='{survey_source}'")
            sys.exit(1)
        survey_db_id = survey_result.data[0]["id"]
        query = query.eq("survey_id", survey_db_id)

    result = query.execute()
    return result.data or []


# ---------------------------------------------------------------------------
# Update embeddings in Supabase
# ---------------------------------------------------------------------------

def update_embeddings(client, question_ids: list[int], embeddings: list[list[float]], dry_run: bool):
    """Write embeddings back to Supabase for the given question IDs."""
    for qid, embedding in zip(question_ids, embeddings):
        if dry_run:
            print(f"  [dry-run] Would update question id={qid} (dims={len(embedding)})")
            continue
        client.table("questions").update({"embedding": embedding}).eq("id", qid).execute()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Gemini embeddings for questions")
    parser.add_argument("--survey", metavar="SURVEY_ID", help="Only process questions from this survey (e.g. eeq_2022)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Questions per Gemini API call (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help=f"Seconds to sleep between batches (default: {DEFAULT_SLEEP})")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and embed but don't write to Supabase")
    args = parser.parse_args()

    # Validate env
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env.local")
        sys.exit(1)
    if not GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY must be set in .env.local")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    gemini = make_gemini_client(GEMINI_API_KEY)

    # Fetch questions
    print(f"Fetching questions without embeddings" + (f" (survey={args.survey})" if args.survey else "") + "...")
    questions = fetch_questions_without_embeddings(client, args.survey)
    total = len(questions)

    if total == 0:
        print("No questions found without embeddings. Nothing to do.")
        return

    print(f"Found {total} questions to embed.")
    if args.dry_run:
        print("[dry-run mode] No writes to Supabase.")

    # Process in batches
    processed = 0
    errors = 0

    for i in range(0, total, args.batch_size):
        batch = questions[i : i + args.batch_size]
        ids = [q["id"] for q in batch]
        texts = [q["text"] for q in batch]

        batch_num = i // args.batch_size + 1
        total_batches = (total + args.batch_size - 1) // args.batch_size
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} questions...", end=" ", flush=True)

        try:
            embeddings = embed_batch(gemini, texts)
            update_embeddings(client, ids, embeddings, args.dry_run)
            processed += len(batch)
            print(f"OK ({processed}/{total})")
        except Exception as e:
            errors += len(batch)
            print(f"ERROR: {e}")

        if i + args.batch_size < total:
            time.sleep(args.sleep)

    print(f"\nDone. {processed} embedded, {errors} errors.")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
