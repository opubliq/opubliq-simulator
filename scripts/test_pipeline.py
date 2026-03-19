import os
import json
import requests
from dotenv import load_dotenv

# Load .env.local if present
load_dotenv(".env.local")

# Supabase config
SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
# For calling edge functions, we usually use the ANON_KEY or SERVICE_ROLE_KEY
SUPABASE_KEY = (
    os.environ.get("VITE_SUPABASE_ANON_KEY") or 
    os.environ.get("VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY") or
    os.environ.get("VITE_SUPABASE_PUBLISHABLE_KEY") or 
    os.environ.get("SUPABASE_ANON_KEY") or 
    os.environ.get("SUPABASE_KEY")
)

# OpenRouter API key — used as the auth bearer for the semantic-search Edge Function.
# NOTE: the semantic-search Edge Function still passes this as gemini_api_key in the
# request body; migrating that function to OpenRouter is tracked in issue 1i7.
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

def test_pipeline():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL (VITE_SUPABASE_URL or SUPABASE_URL)")
    if not SUPABASE_KEY: missing.append("SUPABASE_KEY (VITE_SUPABASE_PUBLISHABLE_KEY or SUPABASE_ANON_KEY)")
    if not OPENROUTER_API_KEY: missing.append("OPENROUTER_API_KEY")
    
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        return

    # Fictional question
    question = "Les immigrants faisant déjà partie du programme devraient avoir une clause grand-père. Êtes-vous d'accord? (likert)"
    context = "Le programme PEQ est débattu à l'Assemblée Nationale suite à une proposition de la CAQ."
    choices = ["Tout à fait d'accord", "Plutôt d'accord", "Plutôt en désaccord", "Tout à fait en désaccord", "Ne sait pas"]

    # On utilise les mêmes headers partout car l'étape 1 a prouvé qu'ils fonctionnent
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "apikey": SUPABASE_KEY
    }

    # --- Step 1: Semantic Search ---
    print("\n--- Step 1: Semantic Search ---")
    step1_url = f"{SUPABASE_URL}/functions/v1/semantic-search"
    resp = requests.post(step1_url, headers=headers, json={"question": question, "top_k": 3})
    
    if resp.status_code != 200:
        print(f"FAILED Step 1: {resp.text}")
        return
    
    step1_data = resp.json()
    print(f"Found {len(step1_data.get('results', []))} similar questions.")
    for res in step1_data.get('results', []):
        print(f"  - [{res['id']}] {res['text']} (Points: {res['llm_points']})")

    # --- Step 2: Fetch Strate Predictions ---
    print("\n--- Step 2: Fetch Strate Predictions ---")
    step2_url = f"{SUPABASE_URL}/functions/v1/fetch-strate-predictions"
    
    # Try WITHOUT Authorization header first, only apikey
    headers_step2 = {
        "Content-Type": "application/json",
        "apikey": SUPABASE_KEY
    }
    resp = requests.post(step2_url, headers=headers_step2, json={"results": step1_data["results"]})
    
    if resp.status_code == 401:
        print("Step 2 failed with 401, retrying with SUPABASE_KEY in Authorization...")
        headers_step2["Authorization"] = f"Bearer {SUPABASE_KEY}"
        resp = requests.post(step2_url, headers=headers_step2, json={"results": step1_data["results"]})

    if resp.status_code != 200:
        print(f"FAILED Step 2: {resp.text}")
        return
    
    step2_data = resp.json()
    predictions = step2_data.get("predictions", [])
    print(f"Fetched predictions for {len(predictions)} questions.")
    for pred in predictions:
        n = len(pred.get("strates", []))
        print(f"  - question_id={pred['question_id']} ({pred['llm_points']} pts): {n} strates in DB")
        if n > 0:
            s = pred["strates"][0]
            print(f"    e.g. strate_age_group={s['strate_age_group']!r}, langue={s['strate_langue']!r}, region={s['strate_region']!r}, genre={s['strate_genre']!r}")

    # Pick a strate that actually has prior data (first strate of first question with strates)
    limit_to_strate = None
    for pred in predictions:
        if pred.get("strates"):
            s = pred["strates"][0]
            limit_to_strate = {
                "strate_age_group": s["strate_age_group"],
                "strate_langue":    s["strate_langue"],
                "strate_region":    s["strate_region"],
                "strate_genre":     s["strate_genre"],
            }
            break

    if not limit_to_strate:
        print("ERROR: No strates found in predictions — cannot run Step 3.")
        return

    print(f"\nUsing strate with prior data: {limit_to_strate}")

    # --- Step 3: Silicon Sampling (Limited to 1 strate) ---
    print("\n--- Step 3: Silicon Sampling (1 strate) ---")

    step3_url = f"{SUPABASE_URL}/functions/v1/llm-strate-sampling"

    # Try DRY RUN first to see the prompt
    print("Testing DRY RUN...")
    dry_run_payload = {
        "predictions": step2_data["predictions"],
        "question": question,
        "context": context,
        "choices": choices,
        "dry_run": True,
        "dry_run_strate": limit_to_strate
    }
    resp = requests.post(step3_url, headers=headers, json=dry_run_payload)
    if resp.status_code == 200:
        dr_data = resp.json()
        if dr_data.get("strate_prompts"):
            print("Prompt for the strate:")
            print("-" * 40)
            print(dr_data["strate_prompts"][0]["prompt"])
            print("-" * 40)
        else:
            print(f"Dry run succeeded but no prompts returned. Meta: {dr_data.get('meta')}")
    else:
        print(f"Dry run failed: {resp.text}")

    print("\nExecuting actual LLM call...")
    step3_payload = {
        "predictions": step2_data["predictions"],
        "question": question,
        "context": context,
        "choices": choices,
        "limit_to_strate": limit_to_strate
    }
    
    resp = requests.post(step3_url, headers=headers, json=step3_payload)
    
    if resp.status_code != 200:
        print(f"FAILED Step 3: {resp.text}")
        return
    
    step3_data = resp.json()
    print(f"Step 3 meta: {step3_data.get('meta')}")
    strate_results = step3_data.get("strate_results", [])
    
    if not strate_results:
        print("No results returned for the strate.")
        return

    res = strate_results[0]
    print(f"Strate: {res['strate_age_group']}, {res['strate_langue']}, {res['strate_region']}, {res['strate_genre']}")
    if res.get("error"):
        print(f"  Error: {res['error']}")
    else:
        print(f"  LLM Response: {json.dumps(res['llm_response'], indent=2, ensure_ascii=False)}")

    # --- Step 4: Aggregate Final Distribution ---
    print("\n--- Step 4: Aggregate Final Distribution ---")
    step4_url = f"{SUPABASE_URL}/functions/v1/aggregate-final-distribution"
    step4_payload = {
        "question": question,
        "strate_results": strate_results
    }
    resp = requests.post(step4_url, headers=headers, json=step4_payload)

    if resp.status_code != 200:
        print(f"FAILED Step 4: {resp.text}")
        return

    step4_data = resp.json()
    print(f"Question type: {step4_data.get('question_type')}")
    print(f"National distribution: {json.dumps(step4_data.get('national_distribution'), indent=2, ensure_ascii=False)}")
    print(f"National margin of error: {step4_data.get('national_margin_of_error')}")
    print(f"Meta: {step4_data.get('meta')}")

if __name__ == "__main__":
    test_pipeline()
