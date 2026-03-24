import { createClient } from "jsr:@supabase/supabase-js@2";
import {
  parseAndMapLLMScores,
  type ParsedLLMScores,
  type QuestionCandidate,
} from "./scoring.ts";

const OPENROUTER_EMBEDDING_MODEL = "openai/text-embedding-3-small";
const OPENROUTER_LLM_MODEL = "meta-llama/llama-3.1-8b-instruct";
const OPENROUTER_EMBEDDINGS_URL = "https://openrouter.ai/api/v1/embeddings";
const OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions";
const EMBEDDING_DIMENSIONS = 1536; // text-embedding-3-small native output — matches questions.embedding vector(1536)
const TOP_CANDIDATES = 15;
const DEFAULT_TOP_K = 5;

interface SemanticSearchRequest {
  question: string;
  top_k?: number;
}

interface ScoredQuestion extends QuestionCandidate {
  llm_points: number; // integer points out of 100 — only assigned to relevant questions; unassigned points = uncovered attitudes
}

async function generateEmbedding(
  text: string,
  openRouterApiKey: string,
): Promise<number[]> {
  const response = await fetch(OPENROUTER_EMBEDDINGS_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${openRouterApiKey}`,
    },
    body: JSON.stringify({
      model: OPENROUTER_EMBEDDING_MODEL,
      input: text,
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`OpenRouter embedding error ${response.status}: ${err}`);
  }

  const data = await response.json();
  const embedding: number[] = data.data?.[0]?.embedding;

  if (!embedding || embedding.length !== EMBEDDING_DIMENSIONS) {
    throw new Error(
      `Expected ${EMBEDDING_DIMENSIONS}-dim embedding, got ${
        embedding?.length ?? 0
      } from ${OPENROUTER_EMBEDDING_MODEL}`,
    );
  }

  return embedding;
}

async function scoreCandidatesWithLLM(
  userQuestion: string,
  candidates: QuestionCandidate[],
  openRouterApiKey: string,
): Promise<ParsedLLMScores> {
  const candidateList = candidates
    .map((c, i) => `${i + 1}. [ID:${c.id}] "${c.text}"`)
    .join("\n");

  const prompt =
    `You are helping assess how relevant historical survey questions are to a new fictional survey question.

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

  const response = await fetch(OPENROUTER_CHAT_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${openRouterApiKey}`,
    },
    body: JSON.stringify({
      model: OPENROUTER_LLM_MODEL,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.1,
      max_tokens: 4096,
      response_format: { type: "json_object" },
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`OpenRouter LLM error ${response.status}: ${err}`);
  }

  const data = await response.json();
  const rawText: string = data.choices?.[0]?.message?.content ?? "{}";

  try {
    return parseAndMapLLMScores(rawText, candidates);
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

  // Read OpenRouter API key from Supabase Secrets (server-side only)
  const openRouterApiKey = Deno.env.get("OPENROUTER_API_KEY") ?? "";

  if (!openRouterApiKey) {
    return new Response(
      JSON.stringify({
        error: "Missing OPENROUTER_API_KEY environment variable",
      }),
      {
        status: 500,
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

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    // Step 1a: Generate embedding for user question
    const embedding = await generateEmbedding(
      question.trim(),
      openRouterApiKey,
    );

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
    const llmResult = await scoreCandidatesWithLLM(
      question.trim(),
      typedCandidates,
      openRouterApiKey,
    );

    // Merge cosine similarity + LLM points; only include questions with points > 0
    const scored: ScoredQuestion[] = typedCandidates
      .filter((c) => (llmResult.scoresById[c.id] ?? 0) > 0)
      .map((c) => ({
        ...c,
        llm_points: llmResult.scoresById[c.id],
      }));

    console.log("[semantic-search] LLM scoring diagnostics", {
      requested_candidates: typedCandidates.length,
      scored_candidates: llmResult.mappedCandidateCount,
      returned_keys: llmResult.returnedKeyCount,
      key_format: llmResult.keyFormat,
    });

    // Sort by llm_points descending, take top_k
    scored.sort((a, b) => b.llm_points - a.llm_points);
    const topK = scored.slice(0, Math.max(1, top_k));

    const total_points_assigned = scored.reduce(
      (sum, c) => sum + c.llm_points,
      0,
    );

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
