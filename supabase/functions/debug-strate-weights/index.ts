import { createClient } from "jsr:@supabase/supabase-js@2";

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  if (req.method !== "GET") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    );

    const { data: weightsData, error: weightsError } = await supabase
      .from("strate_weights")
      .select(
        "strate_age_group, strate_langue, strate_region, strate_genre, weight",
      );

    if (weightsError) {
      console.error("Error fetching strate_weights:", weightsError.message);
      return new Response(
        JSON.stringify({ error: `Failed to fetch strate weights: ${weightsError.message}` }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    console.log("Fetched strate_weights data:", JSON.stringify(weightsData, null, 2));

    return new Response(
      JSON.stringify({ message: "Strate weights logged to console", data: weightsData }),
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
    console.error("Unhandled error:", message);
    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
});
