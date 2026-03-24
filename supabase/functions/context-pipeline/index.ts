const OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions";
const DEFAULT_CONTEXT_MODEL = "meta-llama/llama-3.1-8b-instruct";

const MAX_URLS = 12;
const MAX_FILES = 8;
const MAX_BYTES_PER_SOURCE = 4_000_000;
const MAX_COMBINED_CHARS = 120_000;
const MAX_SUMMARY_INPUT_CHARS = 32_000;
const MAX_SUMMARY_OUTPUT_CHARS = 2_000;
const FETCH_TIMEOUT_MS = 12_000;

interface ContextFileInput {
  name: string;
  mime_type?: string;
  content_base64: string;
  size_bytes?: number;
}

interface ContextPipelineRequest {
  raw_text?: string;
  urls?: string[];
  files?: ContextFileInput[];
}

interface SourceResult {
  source_type: "raw_text" | "url" | "file";
  source_id: string;
  content_type: string | null;
  chars_extracted: number;
  truncated: boolean;
  error: string | null;
}

interface PartialError {
  source_type: "raw_text" | "url" | "file" | "summary";
  source_id: string;
  message: string;
}

interface ExtractedSource {
  text: string;
  result: SourceResult;
}

function jsonResponse(status: number, payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

function normalizeExtractedText(text: string): string {
  return text
    .normalize("NFC")
    .replace(/\r\n?/g, "\n")
    .replace(/\u0000/g, "")
    .replace(/[\t\f\v ]+\n/g, "\n")
    .replace(/[\t\f\v ]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function stripHtmlToText(html: string): string {
  const withoutScript = html
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, " ")
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, " ");

  const withLineBreaks = withoutScript
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/\s*(p|div|section|article|li|h[1-6])\s*>/gi, "\n");

  const noTags = withLineBreaks
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"');

  return normalizeExtractedText(noTags);
}

function inferMimeType(name: string, declared?: string): string {
  const normalizedDeclared = (declared ?? "").trim().toLowerCase();
  if (normalizedDeclared) return normalizedDeclared;

  const lower = name.toLowerCase();
  if (lower.endsWith(".pdf")) return "application/pdf";
  if (lower.endsWith(".html") || lower.endsWith(".htm")) return "text/html";
  if (lower.endsWith(".md") || lower.endsWith(".markdown")) {
    return "text/markdown";
  }
  if (lower.endsWith(".json")) return "application/json";
  if (lower.endsWith(".xml")) return "application/xml";
  if (lower.endsWith(".csv")) return "text/csv";
  if (lower.endsWith(".txt") || lower.endsWith(".rtf")) return "text/plain";
  return "application/octet-stream";
}

function decodeBase64(base64: string): Uint8Array {
  const cleaned = base64.replace(/\s+/g, "");
  const binary = atob(cleaned);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

async function readResponseBytesLimited(
  response: Response,
  maxBytes: number,
): Promise<{ bytes: Uint8Array; truncated: boolean }> {
  if (!response.body) {
    return { bytes: new Uint8Array(), truncated: false };
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let truncated = false;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (!value) continue;

    if (total + value.length > maxBytes) {
      const remaining = Math.max(0, maxBytes - total);
      if (remaining > 0) {
        chunks.push(value.slice(0, remaining));
        total += remaining;
      }
      truncated = true;
      await reader.cancel();
      break;
    }

    chunks.push(value);
    total += value.length;
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }

  return { bytes: merged, truncated };
}

async function extractPdfText(bytes: Uint8Array): Promise<string> {
  const pdfjs = await import("npm:pdfjs-dist@4.10.38/legacy/build/pdf.mjs");
  const loadingTask = pdfjs.getDocument({
    data: bytes,
    disableWorker: true,
    useSystemFonts: true,
  });
  const pdf = await loadingTask.promise;

  const pages: string[] = [];
  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const textContent = await page.getTextContent();
    const segments = textContent.items
      .map((item: { str?: string }) => item.str ?? "")
      .filter((segment: string) => segment.trim().length > 0);
    if (segments.length > 0) {
      pages.push(segments.join(" "));
    }
  }

  return normalizeExtractedText(pages.join("\n\n"));
}

function decodeBytesAsText(bytes: Uint8Array): string {
  const decoder = new TextDecoder("utf-8", { fatal: false });
  return normalizeExtractedText(decoder.decode(bytes));
}

function clampText(text: string, maxChars: number): { text: string; truncated: boolean } {
  if (text.length <= maxChars) {
    return { text, truncated: false };
  }
  return { text: text.slice(0, maxChars), truncated: true };
}

function buildFallbackSummary(extractedText: string): string {
  const normalized = normalizeExtractedText(extractedText);
  if (!normalized) return "";

  const sentences = normalized
    .split(/(?<=[.!?])\s+/)
    .map((sentence) => sentence.trim())
    .filter((sentence) => sentence.length > 0);

  const selected = sentences.slice(0, 6).join(" ");
  const basis = selected || normalized;
  return clampText(basis, MAX_SUMMARY_OUTPUT_CHARS).text;
}

function extractSummaryFactualField(rawText: string): string | null {
  const match = rawText.match(/"summary_factual"\s*:\s*"((?:\\.|[^"\\])*)"/s);
  if (!match) return null;

  try {
    const decoded = JSON.parse(`"${match[1]}"`);
    if (typeof decoded === "string" && decoded.trim()) {
      return normalizeExtractedText(decoded);
    }
  } catch {
    return null;
  }

  return null;
}

function normalizeModelSummaryOutput(rawText: string, extractedText: string): string {
  const withoutFence = rawText
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/i, "")
    .trim();

  try {
    const parsed = JSON.parse(withoutFence) as unknown;
    if (typeof parsed === "object" && parsed !== null) {
      const summary = (parsed as { summary_factual?: unknown }).summary_factual;
      if (typeof summary === "string" && summary.trim()) {
        return clampText(normalizeExtractedText(summary), MAX_SUMMARY_OUTPUT_CHARS).text;
      }
    }
  } catch {
    const extracted = extractSummaryFactualField(withoutFence);
    if (extracted) {
      return clampText(extracted, MAX_SUMMARY_OUTPUT_CHARS).text;
    }
  }

  const looksLikeJson =
    withoutFence.startsWith("{") ||
    withoutFence.includes('"summary_factual"') ||
    withoutFence.includes('"key_facts"') ||
    withoutFence.includes('"caveats"');

  if (looksLikeJson) {
    return buildFallbackSummary(extractedText);
  }

  return clampText(normalizeExtractedText(withoutFence), MAX_SUMMARY_OUTPUT_CHARS).text;
}

async function extractFromUrl(url: string): Promise<ExtractedSource> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "opubliq-context-pipeline/1.0",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const contentTypeHeader = response.headers.get("content-type") ?? "";
    const contentType = contentTypeHeader.split(";")[0].trim().toLowerCase();

    const { bytes, truncated } = await readResponseBytesLimited(
      response,
      MAX_BYTES_PER_SOURCE,
    );

    let text = "";
    if (contentType === "application/pdf" || url.toLowerCase().endsWith(".pdf")) {
      text = await extractPdfText(bytes);
    } else if (contentType === "text/html" || contentType === "application/xhtml+xml") {
      text = stripHtmlToText(decodeBytesAsText(bytes));
    } else {
      text = decodeBytesAsText(bytes);
    }

    const clamped = clampText(text, MAX_COMBINED_CHARS);

    return {
      text: clamped.text,
      result: {
        source_type: "url",
        source_id: url,
        content_type: contentType || null,
        chars_extracted: clamped.text.length,
        truncated: truncated || clamped.truncated,
        error: null,
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function extractFromFile(file: ContextFileInput): Promise<ExtractedSource> {
  const mimeType = inferMimeType(file.name, file.mime_type);
  const bytes = decodeBase64(file.content_base64);

  const sourceBytes = bytes.length > MAX_BYTES_PER_SOURCE
    ? bytes.slice(0, MAX_BYTES_PER_SOURCE)
    : bytes;
  const truncatedByBytes = bytes.length > sourceBytes.length;

  let text = "";
  if (mimeType === "application/pdf" || file.name.toLowerCase().endsWith(".pdf")) {
    text = await extractPdfText(sourceBytes);
  } else if (mimeType === "text/html" || mimeType === "application/xhtml+xml") {
    text = stripHtmlToText(decodeBytesAsText(sourceBytes));
  } else {
    text = decodeBytesAsText(sourceBytes);
  }

  const clamped = clampText(text, MAX_COMBINED_CHARS);

  return {
    text: clamped.text,
    result: {
      source_type: "file",
      source_id: file.name,
      content_type: mimeType,
      chars_extracted: clamped.text.length,
      truncated: truncatedByBytes || clamped.truncated,
      error: null,
    },
  };
}

async function summarizeContext(
  extractedText: string,
  openRouterApiKey: string,
): Promise<string> {
  const model = Deno.env.get("CONTEXT_OPENROUTER_MODEL") ?? DEFAULT_CONTEXT_MODEL;

  const prompt = [
    "Tu crées un contexte factuel compact pour un pipeline de simulation.",
    "Retourne uniquement un objet JSON avec ces clés:",
    '- "summary_factual": un résumé factuel compact en français (4-8 phrases).',
    '- "key_facts": tableau de points factuels (max 10).',
    '- "caveats": tableau court des limites/incertitudes détectées (max 5).',
    "Règles:",
    "- Pas de spéculation.",
    "- Garde les chiffres, noms, dates, positions explicites.",
    "- Supprime le bruit et les répétitions.",
    "- Si le contenu est pauvre ou ambigu, le dire clairement dans caveats.",
    "Contenu source:",
    extractedText,
  ].join("\n");

  const response = await fetch(OPENROUTER_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${openRouterApiKey}`,
    },
    body: JSON.stringify({
      model,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.1,
      max_tokens: 900,
      response_format: { type: "json_object" },
    }),
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`OpenRouter error ${response.status}: ${err}`);
  }

  const data = await response.json();
  const rawText: string = data.choices?.[0]?.message?.content ?? "";
  if (!rawText) {
    throw new Error("OpenRouter returned empty summary response");
  }

  return normalizeModelSummaryOutput(rawText, extractedText);
}

function mergeExtractedSources(sources: ExtractedSource[]): {
  text: string;
  truncated: boolean;
} {
  const chunks: string[] = [];
  let totalChars = 0;
  let truncated = false;

  for (const source of sources) {
    if (!source.text.trim()) continue;
    const chunk = source.text.trim();
    if (totalChars + chunk.length > MAX_COMBINED_CHARS) {
      const remaining = Math.max(0, MAX_COMBINED_CHARS - totalChars);
      if (remaining > 0) {
        chunks.push(chunk.slice(0, remaining));
      }
      truncated = true;
      break;
    }

    chunks.push(chunk);
    totalChars += chunk.length;
  }

  return {
    text: normalizeExtractedText(chunks.join("\n\n---\n\n")),
    truncated,
  };
}

async function runContextPipeline(
  body: ContextPipelineRequest,
) {
  const partialErrors: PartialError[] = [];
  const sourceResults: SourceResult[] = [];
  const extractedSources: ExtractedSource[] = [];

  const rawText = normalizeExtractedText(body.raw_text ?? "");
  if (rawText) {
    const clamped = clampText(rawText, MAX_COMBINED_CHARS);
    const extracted = {
      text: clamped.text,
      result: {
        source_type: "raw_text" as const,
        source_id: "raw_text",
        content_type: "text/plain",
        chars_extracted: clamped.text.length,
        truncated: clamped.truncated,
        error: null,
      },
    };
    extractedSources.push(extracted);
    sourceResults.push(extracted.result);
  }

  const urls = Array.isArray(body.urls) ? body.urls.slice(0, MAX_URLS) : [];
  for (const url of urls) {
    try {
      const extracted = await extractFromUrl(url);
      extractedSources.push(extracted);
      sourceResults.push(extracted.result);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      sourceResults.push({
        source_type: "url",
        source_id: url,
        content_type: null,
        chars_extracted: 0,
        truncated: false,
        error: message,
      });
      partialErrors.push({ source_type: "url", source_id: url, message });
    }
  }

  const files = Array.isArray(body.files) ? body.files.slice(0, MAX_FILES) : [];
  for (const file of files) {
    try {
      const extracted = await extractFromFile(file);
      extractedSources.push(extracted);
      sourceResults.push(extracted.result);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      sourceResults.push({
        source_type: "file",
        source_id: file.name,
        content_type: inferMimeType(file.name, file.mime_type),
        chars_extracted: 0,
        truncated: false,
        error: message,
      });
      partialErrors.push({ source_type: "file", source_id: file.name, message });
    }
  }

  const merged = mergeExtractedSources(extractedSources);
  const summaryInput = clampText(merged.text, MAX_SUMMARY_INPUT_CHARS);
  const openRouterApiKey = Deno.env.get("OPENROUTER_API_KEY") ?? "";

  let summaryFactual = "";
  if (summaryInput.text) {
    if (!openRouterApiKey) {
      summaryFactual = summaryInput.text;
      partialErrors.push({
        source_type: "summary",
        source_id: "openrouter",
        message: "Missing OPENROUTER_API_KEY. Falling back to extracted text.",
      });
    } else {
      try {
        summaryFactual = await summarizeContext(summaryInput.text, openRouterApiKey);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        summaryFactual = summaryInput.text;
        partialErrors.push({
          source_type: "summary",
          source_id: "openrouter",
          message,
        });
      }
    }
  }

  return {
    extracted_text: merged.text,
    summary_factual: summaryFactual,
    source_results: sourceResults,
    partial_errors: partialErrors,
    metrics: {
      raw_text_chars: rawText.length,
      url_count: urls.length,
      file_count: files.length,
      source_success_count: sourceResults.filter((r) => r.error === null).length,
      source_error_count: sourceResults.filter((r) => r.error !== null).length,
      extracted_chars: merged.text.length,
      summary_chars: summaryFactual.length,
      extracted_truncated: merged.truncated,
      summary_input_truncated: summaryInput.truncated,
      max_combined_chars: MAX_COMBINED_CHARS,
      max_summary_input_chars: MAX_SUMMARY_INPUT_CHARS,
    },
  };
}

Deno.serve(async (req: Request) => {
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
    return jsonResponse(405, { error: "Method not allowed" });
  }

  let body: ContextPipelineRequest;
  try {
    body = await req.json();
  } catch {
    return jsonResponse(400, { error: "Invalid JSON body" });
  }

  try {
    const result = await runContextPipeline(body);
    return jsonResponse(200, result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }
});
