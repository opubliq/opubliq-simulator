import { createClient } from "jsr:@supabase/supabase-js@2";

const GEMINI_EMBEDDING_MODEL = "gemini-embedding-001";
const GEMINI_LLM_MODEL = "gemini-2.5-flash";
const EMBEDDING_DIMENSIONS = 1536; // Matryoshka truncation — matches questions.embedding vector(1536)
const TOP_CANDIDATES = 15;
const DEFAULT_TOP_K = 5;

interface SemanticSearchRequest {
  question: string;
  top_k?: number;
}

interface QuestionCandidate {
  id: number;
  text: string;
  scale_type: string | null;
  var_name: string | null;
  prefix: string | null;
  survey_id: number;
  cosine_similarity: number;
}

interface ScoredQuestion extends QuestionCandidate {
  llm_points: number; // integer points out of 100 — only assigned to relevant questions; unassigned points = uncovered attitudes
}

async function generateEmbedding(
  text: string,
  geminiApiKey: string,
): Promise<number[]> {
  const url =
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_EMBEDDING_MODEL}:embedContent?key=${geminiApiKey}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: `models/${GEMINI_EMBEDDING_MODEL}`,
        content: { parts: [{ text }] },
        taskType: "SEMANTIC_SIMILARITY",
        outputDimensionality: EMBEDDING_DIMENSIONS,
      }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Gemini embedding error ${response.status}: ${err}`);
  }

  const data = await response.json();
  return data.embedding.values as number[];
}

async function scoreCandidatesWithLLM(
  userQuestion: string,
  candidates: QuestionCandidate[],
  geminiApiKey: string,
): Promise<Record<number, number>> {
  const url =
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_LLM_MODEL}:generateContent?key=${geminiApiKey}`;

  const candidateList = candidates
    .map((c, i) => `${i + 1}. [ID:${c.id}] "${c.text}"`)
    .join("\n");

  const prompt = `You are helping assess how relevant historical survey questions are to a new fictional survey question.

New question: "${userQuestion}"

You have 100 relevance points to distribute across the ${candidates.length} candidate historical questions below. Assign points to questions that would genuinely help predict how people answer the new question — based on:
- Topical similarity (same issue/policy area)
- Conceptual overlap (similar attitudes being measured)
- Predictive value (knowing someone's answer to this question helps predict their answer to the new one)

IMPORTANT: Do NOT assign all 100 points if the questions are not strongly relevant. If the candidates collectively cover the new question poorly, the total should reflect that — assign only as many points as the evidence warrants. Unassigned points represent attitudes not captured by the available data.

Return ONLY a JSON object mapping each question ID to its integer point allocation. Only include questions with points > 0. The values must sum to at most 100.
Example: {"<id>": <points>, ...}

Candidate questions:
${candidateList}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.1,
          maxOutputTokens: 4096,
          responseMimeType: "application/json",
        },
      }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Gemini LLM error ${response.status}: ${err}`);
  }

  const data = await response.json();
  const rawText: string =
    data.candidates?.[0]?.content?.parts?.[0]?.text ?? "{}";

  // Strip markdown code fences if present
  const cleaned = rawText
    .replace(/```json\s*/gi, "")
    .replace(/```\s*/gi, "")
    .trim();

  try {
    const parsed = JSON.parse(cleaned);
    // Parse integer points (0-100 budget), clamp to valid range
    const scores: Record<number, number> = {};
    for (const [k, v] of Object.entries(parsed)) {
      const id = parseInt(k, 10);
      const points = Math.round(Math.max(0, Math.min(100, Number(v))));
      if (!isNaN(id) && points > 0) {
        scores[id] = points;
      }
    }
    return scores;
  } catch {
    throw new Error(`Failed to parse LLM scoring response: ${rawText}`);
  }
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

  // Extract Gemini API key from Authorization header
  const authHeader = req.headers.get("Authorization") ?? "";
  const geminiApiKey = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : authHeader.trim();

  if (!geminiApiKey) {
    return new Response(
      JSON.stringify({
        error: "Missing Gemini API key in Authorization header",
      }),
      {
        status: 401,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  let body: SemanticSearchRequest;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const { question, top_k = DEFAULT_TOP_K } = body;

  if (!question || typeof question !== "string" || question.trim() === "") {
    return new Response(
      JSON.stringify({ error: "Missing or empty 'question' field" }),
      {
        status: 400,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    // Step 1a: Generate embedding for user question
    const embedding = await generateEmbedding(question.trim(), geminiApiKey);

    // Step 1b: Vector similarity search — top-15 candidates
    // Exclude meta-prefixed variables (sampling weights, identifiers)
    const { data: candidates, error: searchError } = await supabase.rpc(
      "match_questions",
      {
        query_embedding: embedding,
        match_count: TOP_CANDIDATES,
      },
    );

    if (searchError) {
      throw new Error(`pgvector search error: ${searchError.message}`);
    }

    if (!candidates || candidates.length === 0) {
      return new Response(
        JSON.stringify({ question, results: [], top_k }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        },
      );
    }

    const typedCandidates = candidates as QuestionCandidate[];

    // Step 1c: Single LLM call to score all candidates simultaneously
    const llmScores = await scoreCandidatesWithLLM(
      question.trim(),
      typedCandidates,
      geminiApiKey,
    );

    // Merge cosine similarity + LLM points; only include questions with points > 0
    const scored: ScoredQuestion[] = typedCandidates
      .filter((c) => (llmScores[c.id] ?? 0) > 0)
      .map((c) => ({
        ...c,
        llm_points: llmScores[c.id],
      }));

    // Sort by llm_points descending, take top_k
    scored.sort((a, b) => b.llm_points - a.llm_points);
    const topK = scored.slice(0, Math.max(1, top_k));

    const total_points_assigned = scored.reduce((sum, c) => sum + c.llm_points, 0);

    return new Response(
      JSON.stringify({
        question,
        top_k,
        total_points_assigned, // signal de couverture: < 100 = attitudes non couvertes par nos données
        results: topK,
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
