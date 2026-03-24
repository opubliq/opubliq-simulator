const STOPWORDS = new Set([
  "a",
  "au",
  "aux",
  "avec",
  "ce",
  "cet",
  "cette",
  "ces",
  "comme",
  "contre",
  "dans",
  "doit",
  "doivent",
  "de",
  "des",
  "did",
  "do",
  "does",
  "du",
  "elle",
  "en",
  "entre",
  "est",
  "ete",
  "etes",
  "et",
  "etre",
  "eux",
  "fait",
  "faites",
  "il",
  "je",
  "la",
  "le",
  "les",
  "leur",
  "leurs",
  "lui",
  "ma",
  "mais",
  "me",
  "meme",
  "mes",
  "moins",
  "moi",
  "mon",
  "ne",
  "ni",
  "nos",
  "notre",
  "nous",
  "on",
  "ou",
  "par",
  "pas",
  "plus",
  "plutot",
  "pour",
  "qu",
  "que",
  "quel",
  "quelle",
  "quelles",
  "quels",
  "qui",
  "sa",
  "sans",
  "se",
  "ses",
  "si",
  "son",
  "sont",
  "sur",
  "ta",
  "te",
  "tes",
  "tout",
  "tous",
  "toutes",
  "toi",
  "ton",
  "tres",
  "un",
  "une",
  "vos",
  "votre",
  "vous",
  "what",
  "which",
  "who",
  "whom",
  "why",
  "when",
  "where",
  "you",
  "your",
  "y",
  "against",
  "an",
  "and",
  "are",
  "as",
  "at",
  "avait",
  "avoir",
  "be",
  "can",
  "could",
  "by",
  "for",
  "from",
  "had",
  "has",
  "have",
  "in",
  "is",
  "it",
  "of",
  "on",
  "or",
  "should",
  "that",
  "the",
  "these",
  "this",
  "those",
  "to",
  "will",
  "with",
  "would",
  "favor",
  "favour",
  "oppose",
]);

function stripDiacritics(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function extractContentTerms(text: string): string[] {
  const normalized = stripDiacritics(text.toLowerCase());
  const tokens = normalized.split(/[^a-z0-9]+/);
  const seen = new Set<string>();
  const terms: string[] = [];

  for (const token of tokens) {
    if (token.length < 2 || STOPWORDS.has(token) || seen.has(token)) {
      continue;
    }
    seen.add(token);
    terms.push(token);
  }

  return terms;
}

export function buildEmbeddingInput(text: string): string {
  const trimmed = text.trim();
  const terms = extractContentTerms(trimmed).slice(0, 24);

  if (terms.length === 0) {
    return trimmed;
  }

  return `${trimmed}\n\nTermes_cles: ${terms.join(" ")}`;
}
