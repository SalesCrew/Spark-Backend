import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import request from "supertest";
import { createPraemienDashboardReadRouter } from "./routes/praemien-dashboard-read.js";
import type { ModelDatabase } from "./lib/praemien-workspace.js";

test("bonus dashboard detects missing configuration without writes or editor routes", async () => {
  let reads = 0;
  const database: ModelDatabase = {
    async query<T>() {
      reads += 1;
      return [{ ready: false }] as T[];
    },
    async transaction() {
      throw new Error("Dashboard must not open a mutation transaction");
    },
  };
  const app = express();
  app.use("/workspace", createPraemienDashboardReadRouter(database));
  const status = await request(app).get("/workspace/status");
  assert.equal(status.status, 200);
  assert.deepEqual(status.body, { ready: false });
  assert.equal(status.headers["cache-control"], "private, no-store");
  assert.equal((await request(app).get("/workspace/waves")).status, 503);
  assert.equal((await request(app).get("/workspace/waves/invalid")).status, 400);
  assert.equal((await request(app).get("/workspace/waves/00000000-0000-4000-8000-000000000001")).status, 503);
  assert.equal((await request(app).post("/workspace/waves").send({})).status, 404);
  assert.equal((await request(app).put("/workspace/waves/00000000-0000-4000-8000-000000000001/rules").send({})).status, 404);
  assert.equal(reads, 3);
});

test("configured bonus dashboard lists waves using read queries only", async () => {
  const waves = [{ id: "00000000-0000-4000-8000-000000000001", name: "Q3", status: "active" }];
  let reads = 0;
  const database: ModelDatabase = {
    async query<T>() {
      return (++reads === 1 ? [{ ready: true }] : waves) as T[];
    },
    async transaction() {
      throw new Error("Dashboard must not mutate data");
    },
  };
  const app = express();
  app.use("/workspace", createPraemienDashboardReadRouter(database));
  const response = await request(app).get("/workspace/waves");
  assert.equal(response.status, 200);
  assert.deepEqual(response.body, { waves });
  assert.equal(reads, 2);
});
