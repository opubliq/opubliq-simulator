// supabase/functions/aggregate-final-distribution/index.ts
// --------------------------------------------------------------------------
// Step 4 Edge Function — Weighted aggregation -> final distribution for UI
//
// Receives results from Step 3 (LLM silicon sampling per strate) and:
//   - Fetches demographic weights (strate_weights) from Supabase.
//   - Aggregates strate-level LLM responses (distributions or means)
//     using these weights to produce a national distribution.
//   - Calculates a national margin of error.
//   - Returns the national distribution, individual strate results,
//     and uncertainty scores in a structured JSON format for the UI.
//
// Input (from Step 3 / llm-strate-sampling):
//   {
//     "question": "...",
//     "strate_results": [
//       {
//         "strate_age_group": "18_34",
//         "strate_langue": "francophone",
//         "strate_region": "montreal",
//         "strate_genre": "homme",
//         "llm_response": {
//           // For multinomial questions:
//           "distribution": { "Oui": 0.65, "Non": 0.35 },
//           "margin_of_error": 0.08
//           // For numeric questions:
//           // "mean": 4.2,
//           // "margin_of_error": 0.6
//         },
//         "had_prior": true,
//         "error": null
//       },
//       ... // 48 entries total
//     ]
//   }
//
// Output (to frontend):
//   {
//     "question": "...",
//     "question_type": "multinomial" | "numeric",
//     "national_distribution": { "Oui": 0.60, "Non": 0.40 }, // or { "mean": 4.5 }
//     "national_margin_of_error": 0.05,
//     "strate_results": [
//       // Same as input, but potentially augmented with the weight for UI display
//       // and potentially error handling for individual strate calls
//       {
//         "strate_age_group": "18_34",
//         "strate_langue": "francophone",
//         "strate_region": "montreal",
//         "strate_genre": "homme",
//         "weight": 0.025, // Added for clarity
//         "llm_response": { ... },
//         "had_prior": true,
//         "error": null
//       },
//       ...
//     ],
//     "meta": {
//       "total_strates": 48,
//       "successful_strates": 45,
//       "failed_strates": 3
//     }
//   }
// --------------------------------------------------------------------------

import { createClient, SupabaseClient } from "jsr:@supabase/supabase-js@2";

// --- Input Types (from Step 3) ---

interface MultinomialLlmResponse {
  distribution: Record<string, number>;
  margin_of_error: number;
}

interface NumericLlmResponse {
  mean: number;
  margin_of_error: number;
}

type LlmResponse = MultinomialLlmResponse | NumericLlmResponse;

interface Strate {
  strate_age_group: string;
  strate_langue: string;
  strate_region: string;
  strate_genre: string;
}

interface StrateResult extends Strate {
  llm_response: LlmResponse | null;
  had_prior: boolean;
  error: string | null;
}

interface AggregateFinalDistributionRequest {
  question: string;
  strate_results: StrateResult[];
}

// --- Supabase Types ---

interface StrateWeightRow extends Strate {
  weight: number;
}

// --- Output Types (to Frontend) ---

interface NationalAggregatedResult {
  question: string;
  question_type: "multinomial" | "numeric";
  national_distribution: Record<string, number> | { mean: number };
  national_margin_of_error: number;
  strate_results: (StrateResult & { weight: number | null })[];
  meta: {
    total_strates: number;
    successful_strates: number;
    failed_strates: number;
  };
}

// --- Helper to detect question type ---
function detectQuestionType(
  strateResults: StrateResult[],
): "multinomial" | "numeric" | null {
  for (const result of strateResults) {
    if (result.llm_response) {
      if ("distribution" in result.llm_response) {
        return "multinomial";
      }
      if ("mean" in result.llm_response) {
        return "numeric";
      }
    }
  }
  return null; // Should not happen if there are valid results
}

// --- Main aggregation logic ---
async function performAggregation(
  request: AggregateFinalDistributionRequest,
  supabase: SupabaseClient,
): Promise<NationalAggregatedResult> {
  const { question, strate_results } = request;

  // 1. Fetch strate_weights
  const { data: weightsData, error: weightsError } = await supabase
    .from("strate_weights")
    .select(
      "strate_age_group, strate_langue, strate_region, strate_genre, weight",
    );

  if (weightsError) {
    throw new Error(`Failed to fetch strate weights: ${weightsError.message}`);
  }
  if (!weightsData || weightsData.length === 0) {
    throw new Error("No strate weights found in database.");
  }

  console.log("Fetched weightsData:", JSON.stringify(weightsData)); // DEBUG LOG

  const strateWeightsMap = new Map<string, number>();
  for (const row of weightsData) {
    const key = `${row.strate_age_group}|${row.strate_langue}|${row.strate_region}|${row.strate_genre}`;
    strateWeightsMap.set(key, row.weight);
  }

  console.log("Populated strateWeightsMap:", JSON.stringify(Object.fromEntries(strateWeightsMap))); // DEBUG LOG

  const questionType = detectQuestionType(strate_results);
  if (!questionType) {
    throw new Error("Could not determine question type from strate results.");
  }

  let totalWeightedSum = 0; // For numeric mean or total weight for multinomial
  let totalWeightUsed = 0;
  let nationalMarginOfErrorSumSq = 0; // For weighted sum of squares of MOE

  const augmentedStrateResults: (StrateResult & { weight: number | null })[] =
    [];

  if (questionType === "multinomial") {
    const aggregatedDistribution: Record<string, number> = {};

    for (const result of strate_results) {
      const key = `${result.strate_age_group}|${result.strate_langue}|${result.strate_region}|${result.strate_genre}`;
      const weight = strateWeightsMap.get(key) ?? null;

      console.log(`Strate: ${key}, Retrieved Weight: ${weight}`); // DEBUG LOG

      augmentedStrateResults.push({ ...result, weight });

      if (result.llm_response && "distribution" in result.llm_response && weight !== null) {
        const { distribution, margin_of_error } = result.llm_response;
        totalWeightUsed += weight;
        nationalMarginOfErrorSumSq += (weight * margin_of_error) ** 2;

        for (const [option, prob] of Object.entries(distribution)) {
          aggregatedDistribution[option] = (aggregatedDistribution[option] ?? 0) + (prob * weight);
        }
      }
    }

    // Normalize the aggregated distribution
    if (totalWeightUsed === 0) {
      throw new Error("No valid strate results with weights for aggregation.");
    }
    for (const option in aggregatedDistribution) {
      aggregatedDistribution[option] /= totalWeightUsed;
    }

    // Calculate national margin of error (weighted average, assuming independence)
    const nationalMarginOfError = Math.sqrt(nationalMarginOfErrorSumSq) / totalWeightUsed;

    return {
      question,
      question_type: questionType,
      national_distribution: aggregatedDistribution,
      national_margin_of_error: nationalMarginOfError,
      strate_results: augmentedStrateResults,
      meta: {
        total_strates: strate_results.length,
        successful_strates: strate_results.filter((s) => s.error === null).length,
        failed_strates: strate_results.filter((s) => s.error !== null).length,
      },
    };
  } else {
    // Numeric question type
    let weightedMean = 0;

    for (const result of strate_results) {
      const key = `${result.strate_age_group}|${result.strate_langue}|${result.strate_region}|${result.strate_genre}`;
      const weight = strateWeightsMap.get(key) ?? null;

      augmentedStrateResults.push({ ...result, weight });

      if (result.llm_response && "mean" in result.llm_response && weight !== null) {
        const { mean, margin_of_error } = result.llm_response;
        totalWeightUsed += weight;
        weightedMean += mean * weight;
        nationalMarginOfErrorSumSq += (weight * margin_of_error) ** 2;
      }
    }

    if (totalWeightUsed === 0) {
      throw new Error("No valid strate results with weights for aggregation.");
    }

    const nationalMean = weightedMean / totalWeightUsed;
    const nationalMarginOfError = Math.sqrt(nationalMarginOfErrorSumSq) / totalWeightUsed;


    return {
      question,
      question_type: questionType,
      national_distribution: { mean: nationalMean },
      national_margin_of_error: nationalMarginOfError,
      strate_results: augmentedStrateResults,
      meta: {
        total_strates: strate_results.length,
        successful_strates: strate_results.filter((s) => s.error === null).length,
        failed_strates: strate_results.filter((s) => s.error !== null).length,
      },
    };
  }
}

// --- Main handler ---

Deno.serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
    });
  }

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body: AggregateFinalDistributionRequest;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { question, strate_results } = body;

  if (!question || typeof question !== "string" || question.trim() === "") {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'question' field" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  if (!Array.isArray(strate_results) || strate_results.length === 0) {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'strate_results' array" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    );

    const aggregatedOutput = await performAggregation(body, supabase);

    return new Response(
      JSON.stringify(aggregatedOutput),
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
