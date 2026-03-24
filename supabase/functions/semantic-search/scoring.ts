export interface QuestionCandidate {
  id: number;
  text: string;
  scale_type: string | null;
  var_name: string | null;
  prefix: string | null;
  survey_id: number;
  choices: Record<string, string> | null;
  cosine_similarity: number;
}

export type LLMKeyFormat =
  | "question_ids"
  | "candidate_indices_1_based"
  | "candidate_indices_0_based"
  | "mixed"
  | "unknown";

export interface ParsedLLMScores {
  scoresById: Record<number, number>;
  returnedKeyCount: number;
  mappedCandidateCount: number;
  keyFormat: LLMKeyFormat;
}

type KeyType =
  | "question_ids"
  | "candidate_indices_1_based"
  | "candidate_indices_0_based";

function clampPoints(value: unknown): number {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return 0;
  }

  return Math.round(Math.max(0, Math.min(100, numericValue)));
}

function canMapKeyWithType(
  key: number,
  keyType: KeyType,
  candidateIds: Set<number>,
  candidateCount: number,
): boolean {
  if (keyType === "question_ids") {
    return candidateIds.has(key);
  }
  if (keyType === "candidate_indices_1_based") {
    return key >= 1 && key <= candidateCount;
  }
  return key >= 0 && key < candidateCount;
}

function resolveTargetId(
  key: number,
  keyType: KeyType,
  candidates: QuestionCandidate[],
): number {
  if (keyType === "question_ids") {
    return key;
  }

  if (keyType === "candidate_indices_1_based") {
    return candidates[key - 1].id;
  }

  return candidates[key].id;
}

function aggregateKeyFormat(
  keyTypes: Set<KeyType>,
  unknownKeyCount: number,
): LLMKeyFormat {
  if (keyTypes.size === 0) {
    return "unknown";
  }

  if (keyTypes.size > 1 || unknownKeyCount > 0) {
    return "mixed";
  }

  return Array.from(keyTypes)[0];
}

function inferPrimaryKeyType(
  keys: number[],
  candidateIds: Set<number>,
  candidateCount: number,
): KeyType | null {
  if (keys.length === 0) {
    return null;
  }

  const keyTypes: KeyType[] = [
    "question_ids",
    "candidate_indices_1_based",
    "candidate_indices_0_based",
  ];

  const mappedCounts = new Map<KeyType, number>();
  for (const keyType of keyTypes) {
    const count = keys.filter((key) =>
      canMapKeyWithType(key, keyType, candidateIds, candidateCount)
    ).length;
    mappedCounts.set(keyType, count);
  }

  const hasZeroKey = keys.includes(0);
  if (hasZeroKey && mappedCounts.get("candidate_indices_0_based")! > 0) {
    return "candidate_indices_0_based";
  }

  const allMapAsOneBased =
    mappedCounts.get("candidate_indices_1_based") === keys.length;
  const allMapAsQuestionIds = mappedCounts.get("question_ids") === keys.length;

  if (allMapAsOneBased && !allMapAsQuestionIds) {
    return "candidate_indices_1_based";
  }

  if (allMapAsQuestionIds && !allMapAsOneBased) {
    return "question_ids";
  }

  const preferenceOrder: KeyType[] = [
    "question_ids",
    "candidate_indices_1_based",
    "candidate_indices_0_based",
  ];

  let bestType: KeyType | null = null;
  let bestCount = -1;

  for (const keyType of preferenceOrder) {
    const count = mappedCounts.get(keyType)!;
    if (count > bestCount) {
      bestType = keyType;
      bestCount = count;
    }
  }

  return bestCount > 0 ? bestType : null;
}

export function parseAndMapLLMScores(
  rawText: string,
  candidates: QuestionCandidate[],
): ParsedLLMScores {
  const parsed = JSON.parse(rawText);

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`Failed to parse LLM scoring response: ${rawText}`);
  }

  const candidateIds = new Set(candidates.map((candidate) => candidate.id));
  const keyTypes = new Set<KeyType>();

  const scoresById: Record<number, number> = {};
  let unknownKeyCount = 0;

  const entries = Object.entries(parsed)
    .map(([rawKey, rawValue]) => {
      const key = Number.parseInt(rawKey, 10);
      const points = clampPoints(rawValue);
      return { key, points };
    })
    .filter(({ key, points }) => !Number.isNaN(key) && points > 0);

  const primaryKeyType = inferPrimaryKeyType(
    entries.map((entry) => entry.key),
    candidateIds,
    candidates.length,
  );

  const fallbackOrder: KeyType[] = [
    "question_ids",
    "candidate_indices_1_based",
    "candidate_indices_0_based",
  ];

  const keyTypePriority = primaryKeyType
    ? [
      primaryKeyType,
      ...fallbackOrder.filter((type) => type !== primaryKeyType),
    ]
    : fallbackOrder;

  for (const { key, points } of entries) {
    const keyType = keyTypePriority.find((type) =>
      canMapKeyWithType(key, type, candidateIds, candidates.length)
    );

    if (!keyType) {
      unknownKeyCount += 1;
      continue;
    }

    keyTypes.add(keyType);
    const targetId = resolveTargetId(key, keyType, candidates);
    scoresById[targetId] = Math.max(scoresById[targetId] ?? 0, points);
  }

  return {
    scoresById,
    returnedKeyCount: Object.keys(parsed).length,
    mappedCandidateCount: Object.keys(scoresById).length,
    keyFormat: aggregateKeyFormat(keyTypes, unknownKeyCount),
  };
}
