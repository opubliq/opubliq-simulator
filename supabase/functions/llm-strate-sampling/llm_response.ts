export interface MultinomialLlmResponse {
  raisonnement: string;
  distribution: Record<string, number>;
  margin_of_error: number;
}

export interface NumericLlmResponse {
  raisonnement: string;
  mean: number;
  margin_of_error: number;
}

export type LlmResponse = MultinomialLlmResponse | NumericLlmResponse;

export function parseLlmResponse(
  rawText: string,
  choices?: string[],
): LlmResponse {
  const cleaned = rawText
    .replace(/```json\s*/gi, "")
    .replace(/```\s*/gi, "")
    .trim();

  let parsed: unknown;
  try {
    parsed = JSON.parse(cleaned);
  } catch (e) {
    throw new Error(
      `JSON parse failed: ${
        e instanceof Error ? e.message : String(e)
      }. Raw LLM response: ${rawText.slice(0, 500)}`,
    );
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error(`Unrecognised LLM response format: ${rawText}`);
  }

  const parsedObj = parsed as Record<string, unknown>;

  const raisonnement: string = typeof parsedObj.raisonnement === "string"
    ? parsedObj.raisonnement as string
    : "";

  if ("mean" in parsedObj) {
    const mean = Number(parsedObj.mean);
    const moe = Number(parsedObj.margin_of_error ?? 0);
    if (isNaN(mean)) throw new Error(`Invalid numeric mean: ${rawText}`);
    return { raisonnement, mean, margin_of_error: isNaN(moe) ? 0 : moe };
  }

  if (!("distribution" in parsedObj)) {
    throw new Error(`Unrecognised LLM response format: ${rawText}`);
  }

  const dist = parsedObj.distribution;
  if (!dist || typeof dist !== "object") {
    throw new Error(`Invalid distribution object: ${rawText}`);
  }

  const values: Record<string, number> = {};
  for (const [k, v] of Object.entries(dist as Record<string, unknown>)) {
    const n = Number(v);
    if (isNaN(n) || n < 0) {
      throw new Error(`Invalid distribution value for "${k}": ${v}`);
    }
    values[k] = n;
  }

  const shouldFilterByChoices = Array.isArray(choices) && choices.length > 0;
  const allowedChoices = shouldFilterByChoices ? new Set(choices) : null;
  const selectedValues: Record<string, number> = shouldFilterByChoices
    ? Object.fromEntries(
      Object.entries(values).filter(([k]) => allowedChoices?.has(k)),
    )
    : values;

  const sum = Object.values(selectedValues).reduce(
    (acc, curr) => acc + curr,
    0,
  );
  if (sum === 0) {
    if (shouldFilterByChoices) {
      throw new Error(
        `Distribution has no matching allowed choices: ${rawText}`,
      );
    }
    throw new Error(`Distribution sums to 0: ${rawText}`);
  }

  const normalised: Record<string, number> = {};
  for (const [k, v] of Object.entries(selectedValues)) {
    normalised[k] = v / sum;
  }

  const moe = Number(parsedObj.margin_of_error ?? 0);
  return {
    raisonnement,
    distribution: normalised,
    margin_of_error: isNaN(moe) ? 0 : moe,
  };
}
