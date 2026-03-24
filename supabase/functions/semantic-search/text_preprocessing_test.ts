import { assertEquals } from "jsr:@std/assert@1";

import {
  buildEmbeddingInput,
  extractContentTerms,
} from "./text_preprocessing.ts";

Deno.test("extractContentTerms removes framing stopwords", () => {
  const terms = extractContentTerms(
    "Etes-vous pour ou contre la hausse des taxes sur les carburants ?",
  );

  assertEquals(terms, ["hausse", "taxes", "carburants"]);
});

Deno.test("buildEmbeddingInput appends key terms when available", () => {
  const embeddingInput = buildEmbeddingInput(
    "Etes-vous pour ou contre la hausse des taxes sur les carburants ?",
  );

  assertEquals(
    embeddingInput,
    "Etes-vous pour ou contre la hausse des taxes sur les carburants ?\n\nTermes_cles: hausse taxes carburants",
  );
});

Deno.test("buildEmbeddingInput keeps original text if only stopwords", () => {
  const embeddingInput = buildEmbeddingInput("pour ou contre");

  assertEquals(embeddingInput, "pour ou contre");
});

Deno.test("extractContentTerms removes common EN framing words", () => {
  const terms = extractContentTerms(
    "Did you vote in the last Quebec provincial election?",
  );

  assertEquals(terms, ["vote", "last", "quebec", "provincial", "election"]);
});
