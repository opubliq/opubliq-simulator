// ---------------------------------------------------------------------------
// Step 3 Edge Function — LLM silicon sampling per strate
//
// For each of the 48 canonical strates, builds a persona prompt including:
//   - The strate's SES profile
//   - Historical questions with LLM weights and per-strate distributions
//   - Raw context and fictional question from the user
//
// Calls Gemini Flash per strate (2-3 in parallel with a 500ms delay between
// batches to stay within free-tier rate limits).
//
// Input (from Step 2 / fetch-strate-predictions):
//   {
//     "predictions": [...],           // Step 2 output
//     "question": "...",              // fictional question from user
//     "context": "...",               // optional raw context from user
//     "question_text_from_step1": ... // optional: full question text (forwarded from Step 1)
//   }
//
// Output (passed to Step 4 / aggregation):
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
//         "had_prior": true,           // false = national marginal was used
//         "error": null                // non-null if this strate's call failed
//       },
//       ...  // 48 entries total
//     ]
//   }
// ---------------------------------------------------------------------------

const GEMINI_LLM_MODEL = "gemini-2.5-flash";
const PARALLEL_BATCH_SIZE = 2;    // number of strates sampled simultaneously
const BATCH_DELAY_MS = 500;        // delay between batches (free-tier rate limit)

// ---------------------------------------------------------------------------
// All 48 canonical strates
// ---------------------------------------------------------------------------

const AGE_GROUPS = ["18_34", "35_54", "55_plus"] as const;
const LANGUES = ["francophone", "anglo_autre"] as const;
const REGIONS = ["montreal", "couronne", "quebec", "autre"] as const;
const GENRES = ["homme", "femme"] as const;

type AgeGroup = typeof AGE_GROUPS[number];
type Langue = typeof LANGUES[number];
type Region = typeof REGIONS[number];
type Genre = typeof GENRES[number];

interface Strate {
  strate_age_group: AgeGroup;
  strate_langue: Langue;
  strate_region: Region;
  strate_genre: Genre;
}

const ALL_STRATES: Strate[] = [];
for (const age of AGE_GROUPS) {
  for (const langue of LANGUES) {
    for (const region of REGIONS) {
      for (const genre of GENRES) {
        ALL_STRATES.push({
          strate_age_group: age,
          strate_langue: langue,
          strate_region: region,
          strate_genre: genre,
        });
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Types matching Step 2 output
// ---------------------------------------------------------------------------

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
  // optional fields forwarded from Step 1
  text?: string;
  scale_type?: string | null;
  var_name?: string | null;
}

interface LlmStrateSamplingRequest {
  predictions: QuestionPredictions[];
  question: string;       // fictional question from user
  context?: string;       // optional raw context
}

// ---------------------------------------------------------------------------
// LLM response types
// ---------------------------------------------------------------------------

interface MultinomialLlmResponse {
  distribution: Record<string, number>;
  margin_of_error: number;
}

interface NumericLlmResponse {
  mean: number;
  margin_of_error: number;
}

type LlmResponse = MultinomialLlmResponse | NumericLlmResponse;

interface StrateResult extends Strate {
  llm_response: LlmResponse | null;
  had_prior: boolean;
  error: string | null;
}

// ---------------------------------------------------------------------------
// Helpers: human-readable strate labels for prompts
// ---------------------------------------------------------------------------

function describeAge(age: AgeGroup): string {
  const map: Record<AgeGroup, string> = {
    "18_34": "18 à 34 ans",
    "35_54": "35 à 54 ans",
    "55_plus": "55 ans et plus",
  };
  return map[age];
}

function describeLangue(langue: Langue): string {
  const map: Record<Langue, string> = {
    "francophone": "francophone",
    "anglo_autre": "anglophone ou allophone",
  };
  return map[langue];
}

function describeRegion(region: Region): string {
  const map: Record<Region, string> = {
    "montreal": "l'île de Montréal",
    "couronne": "la couronne de Montréal (banlieues)",
    "quebec": "la région de Québec",
    "autre": "une autre région du Québec",
  };
  return map[region];
}

function describeGenre(genre: Genre): string {
  const map: Record<Genre, string> = {
    "homme": "un homme",
    "femme": "une femme",
  };
  return map[genre];
}

// ---------------------------------------------------------------------------
// Detect question type from priors: multinomial vs. numeric
// ---------------------------------------------------------------------------

type QuestionType = "multinomial" | "numeric";

function detectQuestionType(predictions: QuestionPredictions[]): QuestionType {
  for (const pred of predictions) {
    for (const strate of pred.strates) {
      const keys = Object.keys(strate.distribution);
      // If distribution has a "mean" key or only one numeric key that isn't
      // a typical categorical label, treat as numeric.
      // Simple heuristic: if any distribution has > 2 meaningful label keys
      // that aren't "mean"/"std", it's multinomial.
      if (keys.length >= 2 && !keys.includes("mean")) {
        return "multinomial";
      }
      if (keys.includes("mean")) {
        return "numeric";
      }
    }
  }
  // Default to multinomial if we can't tell
  return "multinomial";
}

// ---------------------------------------------------------------------------
// Get all response options from priors (for multinomial questions)
// ---------------------------------------------------------------------------

function getResponseOptions(predictions: QuestionPredictions[]): string[] {
  const optionSet = new Set<string>();
  for (const pred of predictions) {
    for (const strate of pred.strates) {
      for (const key of Object.keys(strate.distribution)) {
        if (key !== "mean" && key !== "std") {
          optionSet.add(key);
        }
      }
    }
  }
  return Array.from(optionSet);
}

// ---------------------------------------------------------------------------
// Format distribution for prompt readability
// ---------------------------------------------------------------------------

function formatDistribution(dist: Record<string, number>): string {
  return Object.entries(dist)
    .map(([k, v]) => `"${k}": ${(v * 100).toFixed(1)}%`)
    .join(", ");
}

// ---------------------------------------------------------------------------
// Compute national marginal distribution from all strates across all questions
// (weighted mean — each question weighted by llm_points, each strate equal weight)
// ---------------------------------------------------------------------------

function computeNationalMarginal(
  predictions: QuestionPredictions[],
  qtype: QuestionType,
): Record<string, number> | null {
  const totalPoints = predictions.reduce((s, p) => s + p.llm_points, 0);
  if (totalPoints === 0) return null;

  if (qtype === "numeric") {
    let weightedMean = 0;
    let count = 0;
    for (const pred of predictions) {
      const w = pred.llm_points / totalPoints;
      for (const strate of pred.strates) {
        const mean = strate.distribution["mean"];
        if (typeof mean === "number") {
          weightedMean += w * mean;
          count++;
        }
      }
    }
    if (count === 0) return null;
    // Normalize by strate count per question
    return { mean: weightedMean };
  }

  // Multinomial: accumulate weighted distributions
  const accum: Record<string, number> = {};
  let totalWeight = 0;

  for (const pred of predictions) {
    const w = pred.llm_points / totalPoints;
    for (const strate of pred.strates) {
      for (const [k, v] of Object.entries(strate.distribution)) {
        accum[k] = (accum[k] ?? 0) + w * v;
      }
      totalWeight += w;
    }
  }

  if (totalWeight === 0) return null;

  // Normalize so values sum to 1
  const normalized: Record<string, number> = {};
  for (const [k, v] of Object.entries(accum)) {
    normalized[k] = v / totalWeight;
  }
  return normalized;
}

// ---------------------------------------------------------------------------
// Build the per-strate persona prompt
// ---------------------------------------------------------------------------

function buildStratePrompt(
  strate: Strate,
  predictions: QuestionPredictions[],
  nationalMarginal: Record<string, number> | null,
  question: string,
  context: string | undefined,
  qtype: QuestionType,
  responseOptions: string[],
): string {
  const { strate_age_group, strate_langue, strate_region, strate_genre } = strate;

  // Build historical questions section
  const historicalLines: string[] = [];

  for (const pred of predictions) {
    // Find this strate's distribution in the prediction
    const strateRow = pred.strates.find(
      (s) =>
        s.strate_age_group === strate_age_group &&
        s.strate_langue === strate_langue &&
        s.strate_region === strate_region &&
        s.strate_genre === strate_genre,
    );

    const dist = strateRow?.distribution ?? nationalMarginal;
    const distStr = dist ? formatDistribution(dist) : "(données non disponibles)";
    const usedFallback = !strateRow && dist ? " [estimation nationale]" : "";

    const questionText = pred.text ?? `Question ID ${pred.question_id}`;
    historicalLines.push(
      `- [poids: ${(pred.llm_points / 100).toFixed(2)}${usedFallback}] ${questionText}\n  Réponses typiques de ce profil: {${distStr}}`,
    );
  }

  const historicalSection = historicalLines.length > 0
    ? `\n\nDonnées historiques pour ce profil démographique :\n${historicalLines.join("\n")}`
    : "\n\n(Aucune donnée historique disponible pour ce profil.)";

  const contextSection = context?.trim()
    ? `\n\nContexte fourni :\n${context.trim()}`
    : "";

  // Response format instructions
  let formatInstructions: string;
  if (qtype === "multinomial") {
    const optionsList = responseOptions.map((o) => `"${o}"`).join(", ");
    formatInstructions = `Réponds UNIQUEMENT avec un objet JSON avec exactement deux clés :
- "distribution": objet avec les options de réponse comme clés (parmi : ${optionsList}) et des probabilités décimales comme valeurs (devant sommer à 1.0)
- "margin_of_error": nombre décimal entre 0 et 1 représentant ton incertitude (ex: 0.08 = ±8%)

Exemple : {"distribution": {"Oui": 0.65, "Non": 0.35}, "margin_of_error": 0.08}`;
  } else {
    formatInstructions = `Réponds UNIQUEMENT avec un objet JSON avec exactement deux clés :
- "mean": nombre décimal représentant la réponse moyenne typique pour ce profil
- "margin_of_error": nombre décimal représentant l'incertitude autour de cette moyenne

Exemple : {"mean": 4.2, "margin_of_error": 0.6}`;
  }

  return `Tu es ${describeGenre(strate_genre)}, ${describeAge(strate_age_group)}, ${describeLangue(strate_langue)}, vivant à ${describeRegion(strate_region)}. Tu réponds à un sondage d'opinion au Québec.${historicalSection}${contextSection}

Question du sondage : "${question}"

En te basant sur ton profil démographique et les données historiques ci-dessus qui montrent comment des personnes similaires ont répondu à des questions reliées, comment répondrais-tu à cette question ?

${formatInstructions}`;
}

// ---------------------------------------------------------------------------
// Call Gemini Flash for a single strate
// ---------------------------------------------------------------------------

async function callGeminiForStrate(
  prompt: string,
  geminiApiKey: string,
): Promise<LlmResponse> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_LLM_MODEL}:generateContent?key=${geminiApiKey}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.3,   // slight variance to reflect group-level uncertainty
        maxOutputTokens: 512,
        responseMimeType: "application/json",
      },
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Gemini error ${response.status}: ${err}`);
  }

  const data = await response.json();
  const rawText: string = data.candidates?.[0]?.content?.parts?.[0]?.text ?? "{}";

  // Strip markdown code fences if present (defensive — responseMimeType should prevent this)
  const cleaned = rawText
    .replace(/```json\s*/gi, "")
    .replace(/```\s*/gi, "")
    .trim();

  const parsed = JSON.parse(cleaned);

  // Validate and normalise the response
  if ("mean" in parsed) {
    // Numeric response
    const mean = Number(parsed.mean);
    const moe = Number(parsed.margin_of_error ?? 0);
    if (isNaN(mean)) throw new Error(`Invalid numeric mean: ${rawText}`);
    return { mean, margin_of_error: isNaN(moe) ? 0 : moe };
  } else if ("distribution" in parsed) {
    // Multinomial response
    const dist = parsed.distribution as Record<string, unknown>;
    const moe = Number(parsed.margin_of_error ?? 0);

    // Normalise to ensure sum = 1
    const values: Record<string, number> = {};
    let sum = 0;
    for (const [k, v] of Object.entries(dist)) {
      const n = Number(v);
      if (isNaN(n) || n < 0) throw new Error(`Invalid distribution value for "${k}": ${v}`);
      values[k] = n;
      sum += n;
    }
    if (sum === 0) throw new Error(`Distribution sums to 0: ${rawText}`);
    const normalised: Record<string, number> = {};
    for (const [k, v] of Object.entries(values)) {
      normalised[k] = v / sum;
    }
    return { distribution: normalised, margin_of_error: isNaN(moe) ? 0 : moe };
  } else {
    throw new Error(`Unrecognised LLM response format: ${rawText}`);
  }
}

// ---------------------------------------------------------------------------
// Process all 48 strates in batches of PARALLEL_BATCH_SIZE
// ---------------------------------------------------------------------------

async function sampleAllStrates(
  predictions: QuestionPredictions[],
  question: string,
  context: string | undefined,
  geminiApiKey: string,
): Promise<StrateResult[]> {
  const qtype = detectQuestionType(predictions);
  const responseOptions = qtype === "multinomial" ? getResponseOptions(predictions) : [];
  const nationalMarginal = computeNationalMarginal(predictions, qtype);

  const results: StrateResult[] = [];

  // Determine which strates actually have prior data
  const stratesWithPrior = new Set<string>();
  for (const pred of predictions) {
    for (const s of pred.strates) {
      stratesWithPrior.add(
        `${s.strate_age_group}|${s.strate_langue}|${s.strate_region}|${s.strate_genre}`,
      );
    }
  }

  // Process in batches
  for (let i = 0; i < ALL_STRATES.length; i += PARALLEL_BATCH_SIZE) {
    const batch = ALL_STRATES.slice(i, i + PARALLEL_BATCH_SIZE);

    const batchPromises = batch.map(async (strate): Promise<StrateResult> => {
      const key = `${strate.strate_age_group}|${strate.strate_langue}|${strate.strate_region}|${strate.strate_genre}`;
      const hadPrior = stratesWithPrior.has(key);

      const prompt = buildStratePrompt(
        strate,
        predictions,
        nationalMarginal,
        question,
        context,
        qtype,
        responseOptions,
      );

      try {
        const llmResponse = await callGeminiForStrate(prompt, geminiApiKey);
        return { ...strate, llm_response: llmResponse, had_prior: hadPrior, error: null };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { ...strate, llm_response: null, had_prior: hadPrior, error: message };
      }
    });

    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults);

    // Delay between batches (skip after last batch)
    if (i + PARALLEL_BATCH_SIZE < ALL_STRATES.length) {
      await new Promise((resolve) => setTimeout(resolve, BATCH_DELAY_MS));
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

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

  // Extract Gemini API key from Authorization header
  const authHeader = req.headers.get("Authorization") ?? "";
  const geminiApiKey = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : authHeader.trim();

  if (!geminiApiKey) {
    return new Response(
      JSON.stringify({ error: "Missing Gemini API key in Authorization header" }),
      {
        status: 401,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  let body: LlmStrateSamplingRequest;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { predictions, question, context } = body;

  if (!Array.isArray(predictions) || predictions.length === 0) {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'predictions' array" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  if (!question || typeof question !== "string" || question.trim() === "") {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'question' field" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  try {
    const strateResults = await sampleAllStrates(
      predictions,
      question.trim(),
      context,
      geminiApiKey,
    );

    const failedCount = strateResults.filter((r) => r.error !== null).length;
    const successCount = strateResults.length - failedCount;

    return new Response(
      JSON.stringify({
        question: question.trim(),
        strate_results: strateResults,
        meta: {
          total_strates: strateResults.length,
          success_count: successCount,
          failed_count: failedCount,
          strates_with_prior: strateResults.filter((r) => r.had_prior).length,
          strates_using_national_marginal: strateResults.filter(
            (r) => !r.had_prior,
          ).length,
        },
      }),
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
