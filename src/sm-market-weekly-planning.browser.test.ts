import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type { Express } from "express";
import type { PGlite } from "@electric-sql/pglite";
import { build } from "esbuild";

/** Optional real-browser regression test, using only the local HTTP/PGlite fixture. */
export async function verifySmWeeklyPlanningBrowser(app: Express, pg: PGlite, marketId: string) {
  const { chromium } = createRequire(import.meta.url)("playwright");
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const pagePath = fileURLToPath(new URL("../../src/app/admin/sm/maerkte/page.tsx", import.meta.url));
  const apiSource = await readFile(new URL("../../src/lib/api/backend.ts", import.meta.url), "utf8");
  const updateApi = apiSource.match(/export async function updateSmMarket\([\s\S]*?\n\}/)?.[0];
  assert.ok(updateApi, "use the application's actual updateSmMarket request implementation");
  const bundle = await build({
    absWorkingDir: root, bundle: true, write: false, platform: "browser", format: "iife", jsx: "automatic",
    stdin: { contents: `import React from 'react'; import {createRoot} from 'react-dom/client'; import Page from ${JSON.stringify(pagePath)}; createRoot(document.getElementById('root')).render(<Page/>);`, loader: "tsx", resolveDir: root },
    plugins: [{
      name: "local-weekly-plan-fixture",
      setup(builder) {
        builder.onResolve({ filter: /^@\/lib\/api\/backend$/ }, () => ({ path: "api", namespace: "fixture" }));
        builder.onResolve({ filter: /^@\/components\/admin\/sm\/SmMarket(?:ImportModal|UserSyncModal|DeactivationModal)$/ }, (args) => ({ path: args.path.split("/").at(-1)!, namespace: "fixture" }));
        builder.onLoad({ filter: /.*/, namespace: "fixture" }, (args) => ({
          contents: args.path === "api" ? `
            async function authedFetch(path, init = {}) {
              const response = await fetch(path, {...init, headers:{'Content-Type':'application/json'}});
              const data = await response.json(); if (!response.ok) throw new Error(data.error); return data;
            }
            export async function fetchSmMarkets(){ return (await authedFetch('/admin/sm-markets')).markets; }
            export async function fetchSmUsers(){return [];}
            export async function fetchGmUsers(){return [];}
            export async function createSmMarket(){throw new Error('Not part of this fixture');}
            export async function importSmMarkets(){throw new Error('Not part of this fixture');}
            export async function softDeleteSmMarket(){throw new Error('Not part of this fixture');}
            ${updateApi}
          ` : `export function ${args.path}(){return null;}`,
          loader: "ts",
        }));
        builder.onLoad({ filter: /admin[\\/]sm[\\/]maerkte[\\/]page\.tsx$/ }, async (args) => ({
          // Next's global styled-jsx output is ordinary CSS; preserve it in this isolated renderer.
          contents: (await readFile(args.path, "utf8")).replace(/<style jsx global>/g, "<style>"),
          loader: "tsx", resolveDir: fileURLToPath(new URL("../../src/app/admin/sm/maerkte/", import.meta.url)),
        }));
      },
    }],
  });
  let fontCss = "";
  try {
    const layoutCss = await readFile(new URL("../../.next/dev/static/css/app/layout.css", import.meta.url), "utf8");
    const face = layoutCss.match(/\/\* latin \*\/\s*(@font-face\s*\{[^}]+\})/)?.[1];
    const filename = face?.match(/\/media\/([^/)]+\.woff2)/)?.[1];
    if (face && filename) {
      const font = await readFile(new URL(`../../.next/dev/static/media/${filename}`, import.meta.url));
      app.get("/fixture-inter.woff2", (_req, res) => res.type("font/woff2").send(font));
      fontCss = face.replace(/url\([^)]+\)/, "url('/fixture-inter.woff2')");
    }
  } catch {
    // A fresh checkout without Next dev assets still verifies the flow with a system font.
  }
  app.get("/fixture.js", (_req, res) => res.type("js").send(bundle.outputFiles[0]!.text));
  app.get("/weekly-plan", (_req, res) => res.type("html").send(`<!doctype html><html lang="de"><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Lokaler SM Wochenplan-Test</title><style>${fontCss}body{margin:0;background:#f5f5f7;font-family:Inter,ui-sans-serif,system-ui,sans-serif;}#root{padding:28px;}button:disabled{cursor:default;}</style></head><body><div id="root"></div><script src="/fixture.js"></script></body></html>`));
  await pg.query("update sm_markets set monday_hours=null,tuesday_hours=null,wednesday_hours=null,thursday_hours=2,friday_hours=null,updated_at='2026-09-01T00:00:00Z' where id=$1", [marketId]);
  const assignmentsBefore = (await pg.query("select * from sm_assignments")).rows;
  // Allow a preinstalled browser, without installing anything or using the user's live Chrome session.
  const browser = await chromium.launch({ headless: true, ...(process.env.SM_TEST_BROWSER_EXECUTABLE ? { executablePath: process.env.SM_TEST_BROWSER_EXECUTABLE } : {}) });
  const server = app.listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const origin = `http://127.0.0.1:${address.port}`;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 800 } });
    const errors: string[] = [];
    await context.route("**/*", (route: { request(): { url(): string }; continue(): Promise<void>; abort(): Promise<void> }) => {
      if (!route.request().url().startsWith(`${origin}/`)) {
        errors.push(`Forbidden external request: ${route.request().url()}`);
        return route.abort();
      }
      return route.continue();
    });
    const page = await context.newPage();
    page.on("pageerror", (error: Error) => errors.push(error.message));
    await page.goto(`${origin}/weekly-plan`);
    await page.evaluate("document.fonts.ready");
    const openPlan = async () => {
      await page.locator(".sm-market-row").click();
      await page.getByRole("button", { name: "Einsätze", exact: true }).click();
    };
    await openPlan();
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).click();
    await page.getByRole("button", { name: "Donnerstag als Betreuungstag" }).click();
    await page.getByRole("button", { name: "Dienstag als Betreuungstag" }).click();
    const save = page.getByRole("button", { name: "Wochenplanung speichern" });
    assert.equal(await save.isDisabled(), true, "selected day requires valid hours");
    await page.getByLabel("Stunden am Dienstag").fill("2,5");
    await page.getByRole("button", { name: "Freitag als Betreuungstag" }).click();
    await page.getByLabel("Stunden am Freitag").fill("1,25");
    await page.getByLabel("Stunden am Freitag").blur();
    await page.locator("aside").screenshot({ path: fileURLToPath(new URL("../../outputs/sm-market-weekly-planning-polished-edit.png", import.meta.url)) });
    const savedRequest = page.waitForResponse((response: { url(): string; request(): { method(): string } }) => response.url().endsWith(`/admin/sm-markets/${marketId}`) && response.request().method() === "PATCH");
    await save.click();
    const response = await savedRequest;
    assert.equal(response.status(), 200);
    assert.deepEqual(response.request().postDataJSON().weekdayHours, { mo: null, di: 2.5, mi: null, do: null, fr: 1.25 });
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).waitFor();
    assert.match(await page.locator(".sm-planning-metrics").innerText(), /3,75 h/);
    await page.locator("aside").screenshot({ path: fileURLToPath(new URL("../../outputs/sm-market-weekly-planning-polished-view.png", import.meta.url)) });
    await page.reload();
    await openPlan();
    assert.match(await page.locator(".sm-planning-metrics").innerText(), /3,75 h/);
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).click();
    await page.getByLabel("Stunden am Dienstag").fill("25");
    assert.equal(await save.isDisabled(), true);
    assert.match(await page.getByRole("alert").innerText(), /Dienstag/);
    await page.getByRole("button", { name: "Abbrechen", exact: true }).click();
    assert.match(await page.locator(".sm-planning-metrics").innerText(), /3,75 h/);
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).click();
    await page.getByLabel("Stunden am Dienstag").fill("3");
    // Simulate another admin/import changing this local record while the drawer is open.
    await pg.query("update sm_markets set updated_at=updated_at+interval '1 second' where id=$1", [marketId]);
    await save.click();
    await page.getByText(/Der Markt wurde inzwischen geändert/).waitFor();
    assert.equal(await page.getByLabel("Stunden am Dienstag").inputValue(), "3", "conflict keeps the unsaved draft");
    await page.getByRole("button", { name: "Abbrechen", exact: true }).click();
    await page.reload();
    await openPlan();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).click();
    assert.equal(await page.evaluate("Array.from(document.querySelectorAll('.sm-planning-metric div > span')).every(label => label.scrollWidth <= label.clientWidth)"), true, "mobile metric labels are not truncated");
    await page.locator("aside").screenshot({ path: fileURLToPath(new URL("../../outputs/sm-market-weekly-planning-polished-mobile.png", import.meta.url)) });
    await page.getByRole("button", { name: "Dienstag als Betreuungstag" }).click();
    await page.getByRole("button", { name: "Freitag als Betreuungstag" }).click();
    await save.click();
    await page.getByRole("button", { name: "Wochenplanung bearbeiten", exact: true }).waitFor();
    assert.match(await page.locator(".sm-planning-status").innerText(), /Kein Plan/);
    assert.match(await page.locator(".sm-planning-metrics").innerText(), /0 h/);
    assert.deepEqual((await pg.query("select * from sm_assignments")).rows, assignmentsBefore);
    assert.deepEqual(errors, [], "no page errors or production requests");
    await context.close();
  } finally {
    await browser.close();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}
