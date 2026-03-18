import { createClient } from "jsr:@supabase/supabase-js@2";

// ---------------------------------------------------------------------------
// Step 2 Edge Function — fetch strate_predictions for top-K similar questions
//
// Input  (from Step 1 / semantic-search):
//   {
//     "results": [
//       { "id": 42, "llm_points": 35, ... },   // top-K scored questions
//       ...
//     ]
//   }
//
// Output (passed directly to Step 3 / silicon-sampling):
//   {
//     "predictions": [
//       {
//         "question_id": 42,
//         "llm_points": 35,
//         "strates": [
//           {
//             "strate_age_group": "18_34",
//             "strate_langue": "francophone",
//             "strate_region": "montreal",
//             "strate_genre": "homme",
//             "distribution": {"party a": 0.049962, "party b": 0.715158, ...}
//           },
//           ...  // up to 48 rows per question_id
//         ]
//       },
//       ...
//     ]
//   }
// ---------------------------------------------------------------------------

interface StepOneResult {
  id: number;
  llm_points: number;
  text?: string;
  scale_type?: string | null;
  var_name?: string | null;
  prefix?: string | null;
  survey_id?: number;
  cosine_similarity?: number;
}

interface FetchStratePredictionsRequest {
  results: StepOneResult[];
}

interface StrateRow {
  strate_age_group: string;
  strate_langue: string;
  strate_region: string;
  strate_genre: string;
  distribution: Record<string, number>;
}

interface QuestionPredictions {
  question_id: number;
  llm_points: number;
  strates: StrateRow[];
}

Deno.serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body: FetchStratePredictionsRequest;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { results } = body;

  if (!Array.isArray(results) || results.length === 0) {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'results' array" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  // Validate each result has the required fields
  for (const r of results) {
    if (typeof r.id !== "number" || typeof r.llm_points !== "number") {
      return new Response(
        JSON.stringify({
          error: "Each result must have numeric 'id' and 'llm_points' fields",
        }),
        {
          status: 400,
          headers: { "Content-Type": "application/json" },
        },
      );
    }
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const questionIds = results.map((r) => r.id);

    // Build a lookup from question_id → llm_points for the output
    const llmPointsMap: Record<number, number> = {};
    for (const r of results) {
      llmPointsMap[r.id] = r.llm_points;
    }

    // Fetch all strate_predictions rows for the given question_ids in one query.
    // The table has up to 48 rows per question_id (3 age × 2 langue × 4 region × 2 genre).
    const { data: rows, error: fetchError } = await supabase
      .from("strate_predictions")
      .select(
        "question_id, strate_age_group, strate_langue, strate_region, strate_genre, distribution",
      )
      .in("question_id", questionIds);

    if (fetchError) {
      throw new Error(`Supabase fetch error: ${fetchError.message}`);
    }

    if (!rows || rows.length === 0) {
      // No predictions found — return empty predictions array with llm_points preserved
      const emptyPredictions: QuestionPredictions[] = results.map((r) => ({
        question_id: r.id,
        llm_points: r.llm_points,
        strates: [],
      }));
      return new Response(
        JSON.stringify({ predictions: emptyPredictions }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        },
      );
    }

    // Group strate rows by question_id
    const grouped: Record<number, StrateRow[]> = {};
    for (const row of rows) {
      const qid = row.question_id as number;
      if (!grouped[qid]) {
        grouped[qid] = [];
      }
      grouped[qid].push({
        strate_age_group: row.strate_age_group as string,
        strate_langue: row.strate_langue as string,
        strate_region: row.strate_region as string,
        strate_genre: row.strate_genre as string,
        distribution: row.distribution as Record<string, number>,
      });
    }

    // Build the output in the same order as the input results (preserving LLM ranking)
    const predictions: QuestionPredictions[] = results.map((r) => ({
      question_id: r.id,
      llm_points: r.llm_points,
      strates: grouped[r.id] ?? [],
    }));

    return new Response(
      JSON.stringify({ predictions }),
      {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      },
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
});
