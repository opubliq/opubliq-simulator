import { assertEquals } from "jsr:@std/assert@1";

import { parseAndMapLLMScores, type QuestionCandidate } from "./scoring.ts";

const CANDIDATES: QuestionCandidate[] = [
  {
    id: 101,
    text: "Candidate A",
    scale_type: null,
    var_name: null,
    prefix: null,
    survey_id: 1,
    choices: null,
    cosine_similarity: 0.9,
  },
  {
    id: 205,
    text: "Candidate B",
    scale_type: null,
    var_name: null,
    prefix: null,
    survey_id: 1,
    choices: null,
    cosine_similarity: 0.85,
  },
  {
    id: 309,
    text: "Candidate C",
    scale_type: null,
    var_name: null,
    prefix: null,
    survey_id: 1,
    choices: null,
    cosine_similarity: 0.8,
  },
];

Deno.test("maps 1-based candidate index keys to question IDs", () => {
  const result = parseAndMapLLMScores('{"1": 70, "3": 20}', CANDIDATES);

  assertEquals(result.scoresById, {
    101: 70,
    309: 20,
  });
  assertEquals(result.keyFormat, "candidate_indices_1_based");
  assertEquals(result.returnedKeyCount, 2);
  assertEquals(result.mappedCandidateCount, 2);
});

Deno.test("maps string question IDs directly", () => {
  const result = parseAndMapLLMScores('{"205": 55, "309": 35}', CANDIDATES);

  assertEquals(result.scoresById, {
    205: 55,
    309: 35,
  });
  assertEquals(result.keyFormat, "question_ids");
  assertEquals(result.returnedKeyCount, 2);
  assertEquals(result.mappedCandidateCount, 2);
});

Deno.test("prefers index mapping when it explains more keys", () => {
  const ambiguousCandidates: QuestionCandidate[] = [
    {
      id: 1,
      text: "Candidate 1",
      scale_type: null,
      var_name: null,
      prefix: null,
      survey_id: 1,
      choices: null,
      cosine_similarity: 0.9,
    },
    {
      id: 40,
      text: "Candidate 40",
      scale_type: null,
      var_name: null,
      prefix: null,
      survey_id: 1,
      choices: null,
      cosine_similarity: 0.85,
    },
    {
      id: 2,
      text: "Candidate 2",
      scale_type: null,
      var_name: null,
      prefix: null,
      survey_id: 1,
      choices: null,
      cosine_similarity: 0.8,
    },
  ];

  const result = parseAndMapLLMScores(
    '{"1": 60, "2": 25, "3": 10}',
    ambiguousCandidates,
  );

  assertEquals(result.scoresById, {
    1: 60,
    40: 25,
    2: 10,
  });
  assertEquals(result.keyFormat, "candidate_indices_1_based");
  assertEquals(result.returnedKeyCount, 3);
  assertEquals(result.mappedCandidateCount, 3);
});

Deno.test("maps 0-based candidate index keys", () => {
  const result = parseAndMapLLMScores('{"0": 45, "2": 35}', CANDIDATES);

  assertEquals(result.scoresById, {
    101: 45,
    309: 35,
  });
  assertEquals(result.keyFormat, "candidate_indices_0_based");
  assertEquals(result.returnedKeyCount, 2);
  assertEquals(result.mappedCandidateCount, 2);
});

Deno.test("ignores non-numeric point values", () => {
  const result = parseAndMapLLMScores(
    '{"1": "not-a-number", "2": 30}',
    CANDIDATES,
  );

  assertEquals(result.scoresById, {
    205: 30,
  });
  assertEquals(result.returnedKeyCount, 2);
  assertEquals(result.mappedCandidateCount, 1);
});
