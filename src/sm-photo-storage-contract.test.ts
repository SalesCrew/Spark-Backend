import assert from "node:assert/strict";
import test from "node:test";
import { createClient } from "@supabase/supabase-js";

test("installed Supabase SDK signs a batch with the response shape consumed by SM Fotoarchiv", async () => {
  const calls: Array<{ url: string; body: unknown }> = [];
  const client = createClient("http://127.0.0.1:1", "synthetic-service-key", {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
    global: { fetch: async (input, init) => {
      calls.push({ url: String(input), body: JSON.parse(String(init?.body)) });
      return new Response(JSON.stringify([{ path: "synthetic/photo one.png", signedURL: "/object/sign/sm-visit-photos/synthetic/photo one.png?token=synthetic", error: null },
        { path: "synthetic/missing.png", signedURL: null, error: "Object not found" }]), { headers: { "Content-Type": "application/json" } });
    } },
  });
  const result = await client.storage.from("sm-visit-photos").createSignedUrls(["synthetic/photo one.png", "synthetic/missing.png"], 600);
  assert.equal(result.error, null); assert.equal(calls.length, 1);
  assert.equal(calls[0]!.url, "http://127.0.0.1:1/storage/v1/object/sign/sm-visit-photos");
  assert.deepEqual(calls[0]!.body, { expiresIn: 600, paths: ["synthetic/photo one.png", "synthetic/missing.png"] });
  assert.equal(result.data![0]!.path, "synthetic/photo one.png");
  assert.equal(result.data![0]!.signedUrl, "http://127.0.0.1:1/storage/v1/object/sign/sm-visit-photos/synthetic/photo%20one.png?token=synthetic");
  assert.equal(result.data![1]!.signedUrl, null);
});
