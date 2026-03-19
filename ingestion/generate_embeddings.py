#!/usr/bin/env python3
"""
Generate embeddings for questions that don't have one yet.

Uses intfloat/multilingual-e5-large-instruct (1024 dimensions) via OpenRouter.
Idempotent: only processes questions where embedding IS NULL.
Use --force to re-embed questions that already have an embedding.

Usage:
  python generate_embeddings.py
  python generate_embeddings.py --survey eeq_2022
  python generate_embeddings.py --dry-run
  python generate_embeddings.py --force
  python generate_embeddings.py --batch-size 50 --sleep 1.0
"""

import argparse
import os
import sys
import time
import json
import urllib.request
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
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

EMBEDDING_MODEL = "intfloat/multilingual-e5-large-instruct"
EMBEDDING_DIMS = 1024  # multilingual-e5-large-instruct native output dimensionality
OPENROUTER_EMBEDDINGS_URL = "https://openrouter.ai/api/v1/embeddings"
DEFAULT_BATCH_SIZE = 100
DEFAULT_SLEEP = 1.0  # seconds between batches


# ---------------------------------------------------------------------------
# OpenRouter embeddings client
# ---------------------------------------------------------------------------

def embed_batch(api_key: str, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts using OpenRouter (multilingual-e5-large-instruct)."""
    payload = json.dumps({
        "model": EMBEDDING_MODEL,
        "input": texts,
    }).encode("utf-8")

    req = urllib.request.Request(
        OPENROUTER_EMBEDDINGS_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    # OpenAI-compatible response: data[i].embedding
    embeddings = [item["embedding"] for item in data["data"]]

    # Verify dimensionality
    for emb in embeddings:
        if len(emb) != EMBEDDING_DIMS:
            raise ValueError(
                f"Expected {EMBEDDING_DIMS}-dim embedding, got {len(emb)}-dim from {EMBEDDING_MODEL}"
            )

    return embeddings


# ---------------------------------------------------------------------------
# Fetch questions without embeddings
# ---------------------------------------------------------------------------

def fetch_questions(client, survey_source: str | None, force: bool) -> list[dict]:
    """
    Returns list of {id, text, survey_id} for questions to embed.
    If force=False (default): only questions where embedding IS NULL.
    If force=True: all questions (or all in the given survey).
    Optionally filtered to a single survey by source identifier.
    """
    query = client.table("questions").select("id, text, survey_id")

    if not force:
        query = query.is_("embedding", "null")

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
    parser = argparse.ArgumentParser(description="Generate OpenRouter embeddings for questions")
    parser.add_argument("--survey", metavar="SURVEY_ID", help="Only process questions from this survey (e.g. eeq_2022)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Questions per API call (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help=f"Seconds to sleep between batches (default: {DEFAULT_SLEEP})")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and embed but don't write to Supabase")
    parser.add_argument("--force", action="store_true", help="Re-embed questions that already have an embedding")
    args = parser.parse_args()

    # Validate env
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env.local")
        sys.exit(1)
    if not OPENROUTER_API_KEY:
        print("ERROR: OPENROUTER_API_KEY must be set in .env.local")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fetch questions
    mode = "all questions" if args.force else "questions without embeddings"
    survey_suffix = f" (survey={args.survey})" if args.survey else ""
    print(f"Fetching {mode}{survey_suffix}...")
    questions = fetch_questions(client, args.survey, args.force)
    total = len(questions)

    if total == 0:
        print("No questions found to embed. Nothing to do.")
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
            embeddings = embed_batch(OPENROUTER_API_KEY, texts)
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
