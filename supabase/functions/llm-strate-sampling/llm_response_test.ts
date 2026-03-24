import { assertEquals, assertThrows } from "jsr:@std/assert@1";

import { parseLlmResponse } from "./llm_response.ts";

Deno.test("filters distribution keys to provided choices and renormalizes", () => {
  const response = parseLlmResponse(
    '{"raisonnement":"test","distribution":{"Oui":0.5,"Non":0.25,"Historique":0.25},"margin_of_error":0.1}',
    ["Oui", "Non"],
  );

  if (!("distribution" in response)) {
    throw new Error("Expected multinomial response");
  }

  assertEquals(response.distribution, {
    Oui: 2 / 3,
    Non: 1 / 3,
  });
});

Deno.test("keeps all distribution keys when choices are not provided", () => {
  const response = parseLlmResponse(
    '{"raisonnement":"test","distribution":{"Oui":2,"Non":1},"margin_of_error":0.1}',
  );

  if (!("distribution" in response)) {
    throw new Error("Expected multinomial response");
  }

  assertEquals(response.distribution, {
    Oui: 2 / 3,
    Non: 1 / 3,
  });
});

Deno.test("throws when filtering removes all returned keys", () => {
  assertThrows(
    () =>
      parseLlmResponse(
        '{"raisonnement":"test","distribution":{"Historique":1},"margin_of_error":0.1}',
        ["Oui", "Non"],
      ),
    Error,
    "Distribution has no matching allowed choices",
  );
});
