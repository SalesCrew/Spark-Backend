import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("SM menu inbox uses real message routes and keeps read receipts explicit", async t => {
  const f = await createSMDurcharbeitFixture();
  try {
    const get = (path: string) => request(f.app).get(path).auth("synthetic-sm", { type: "bearer" });
    const read = (id: string) => request(f.app).post(`/sm/messages/${id}/read`).auth("synthetic-sm", { type: "bearer" });
    const other = randomUUID();
    await f.database.insert(f.schema.users).values({ id: other, role: "sm", firstName: "Private", lastName: "Synthetic", email: "private@preview.test" });
    const send = async (subject: string, days: number, recipient = f.employee) => {
      const reply = await request(f.app).post("/admin/sm-messages").auth("synthetic-sm-admin", { type: "bearer" })
        .send({ subject, body: `Synthetic body: ${subject}`, recipientIds: [recipient], idempotencyKey: randomUUID(), visibleAfterReadDays: days }).expect(201);
      return reply.body.messageId as string;
    };
    const oneTime = await send("One time", 0), retained = await send("Retained", 7), foreign = await send("PRIVATE SENTINEL", 0, other);
    const rows = () => Promise.all([f.database.select().from(f.schema.smMessages), f.database.select().from(f.schema.smMessageRecipients)]);
    await t.test("closed-menu badge returns only a count; opening, pagination and repeated GETs never mark read", async () => {
      const before = await rows();
      assert.deepEqual((await get("/sm/messages/unread-count").expect(200)).body, { unreadCount: 2 });
      const inbox = (await get("/sm/messages/inbox?limit=1").expect(200)).body;
      assert.equal(inbox.messages.length, 1); assert.ok(inbox.nextCursor);
      const next = (await get(`/sm/messages/inbox?limit=1&cursor=${encodeURIComponent(inbox.nextCursor)}`).expect(200)).body;
      assert.notEqual(next.messages[0].id, inbox.messages[0].id); assert.equal(next.nextCursor, null);
      assert.equal((await get("/sm/messages").expect(200)).body.messages.length, 2, "Legacy inbox remains available");
      assert.deepEqual(await rows(), before, "No read receipt, message, deletion or delivery timestamp changes on GET");
    });
    await t.test("employee role and recipient ownership remain authoritative, including cursor and read requests", async () => {
      for (const endpoint of ["/sm/messages/unread-count", "/sm/messages/inbox"]) {
        await request(f.app).get(endpoint).expect(401);
        for (const token of ["synthetic-sm-admin", "synthetic-gm", "synthetic-admin"]) {
          await request(f.app).get(endpoint).auth(token, { type: "bearer" }).expect(403);
        }
        await get(`${endpoint}?smUserId=${other}`).expect(400);
      }
      await get("/sm/messages/inbox?limit=101").expect(400);
      await get("/sm/messages/inbox?cursor=not-json").expect(400);
      const foreignCursor = Buffer.from(JSON.stringify({ read: false, sentAt: new Date().toISOString(), id: foreign })).toString("base64url");
      const page = (await get(`/sm/messages/inbox?cursor=${foreignCursor}`).expect(200)).body;
      assert.ok(page.messages.every((row: any) => row.id !== foreign && !row.body.includes("PRIVATE SENTINEL")));
      await read(foreign).expect(404);
      const [receipt] = await f.database.select().from(f.schema.smMessageRecipients).where(eq(f.schema.smMessageRecipients.messageId, foreign));
      assert.equal(receipt!.readAt, null);
    });
    await t.test("only explicit read changes a receipt; one-time removal and retries preserve the first timestamp", async () => {
      const original = (await f.database.select().from(f.schema.smMessages).where(eq(f.schema.smMessages.id, oneTime)))[0];
      const first = (await read(oneTime).expect(200)).body;
      assert.equal(first.alreadyRead, false); assert.ok(first.readAt);
      const replay = (await read(oneTime).expect(200)).body;
      assert.equal(replay.alreadyRead, true); assert.equal(replay.readAt, first.readAt);
      assert.deepEqual((await get("/sm/messages/unread-count").expect(200)).body, { unreadCount: 1 });
      assert.equal((await get("/sm/messages/inbox").expect(200)).body.messages.some((row: any) => row.id === oneTime), false);
      assert.deepEqual((await f.database.select().from(f.schema.smMessages).where(eq(f.schema.smMessages.id, oneTime)))[0], original);
      await read(retained).expect(200);
      assert.deepEqual((await get("/sm/messages/unread-count").expect(200)).body, { unreadCount: 0 });
      const message = (await get("/sm/messages/inbox").expect(200)).body.messages[0];
      assert.equal(message.id, retained); assert.ok(message.readAt); assert.ok(message.visibleUntil);
    });
    await t.test("expired, deleted and historical NULL-retention messages obey existing visibility", async () => {
      const deliveredAt = new Date(Date.now() - 10 * 86_400_000), readAt = new Date(Date.now() - 8 * 86_400_000);
      for (const [subject, visibleAfterReadDays] of [["Expired", 1], ["Historical NULL", null]] as const) {
        const [message] = await f.database.insert(f.schema.smMessages).values({ idempotencyKey: randomUUID(), subject, body: "Historic synthetic body", senderUserId: f.admin, senderNameSnapshot: "Local Admin", sentAt: deliveredAt, visibleAfterReadDays }).returning();
        await f.database.insert(f.schema.smMessageRecipients).values({ messageId: message!.id, smUserId: f.employee, recipientNameSnapshot: "Local SM", recipientEmailSnapshot: "local@preview.test", deliveredAt, readAt });
      }
      const deleted = await send("Deleted delivery", 0);
      await f.database.update(f.schema.smMessageRecipients).set({ isDeleted: true, deletedAt: new Date() }).where(eq(f.schema.smMessageRecipients.messageId, deleted));
      await read(deleted).expect(404);
      const subjects = (await get("/sm/messages/inbox").expect(200)).body.messages.map((row: any) => row.subject);
      assert.deepEqual(subjects, ["Retained", "Historical NULL"]);
      assert.deepEqual((await get("/sm/messages/unread-count").expect(200)).body, { unreadCount: 0 });
    });
    await t.test("all messages beyond the old 100-row limit are reachable; unread priority and timestamp ties are stable", async () => {
      const sentAt = new Date(), deliveredAt = new Date();
      const messages = Array.from({ length: 137 }, (_, index) => ({ id: randomUUID(), idempotencyKey: randomUUID(), subject: `Bulk ${index}`, body: "Synthetic pagination body", senderUserId: f.admin, senderNameSnapshot: "Local Admin", sentAt, visibleAfterReadDays: 7 }));
      await f.database.insert(f.schema.smMessages).values(messages);
      await f.database.insert(f.schema.smMessageRecipients).values(messages.map(message => ({ messageId: message.id, smUserId: f.employee, recipientNameSnapshot: "Local SM", recipientEmailSnapshot: "local@preview.test", deliveredAt })));
      const before = await rows();
      assert.deepEqual((await get("/sm/messages/unread-count").expect(200)).body, { unreadCount: 137 });
      let cursor: string | null = null; const all: any[] = [];
      do {
        const page = (await get(`/sm/messages/inbox?limit=30${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`).expect(200)).body;
        assert.ok(page.messages.length <= 30); all.push(...page.messages); cursor = page.nextCursor;
      } while (cursor);
      assert.equal(all.length, 139); assert.equal(new Set(all.map(row => row.id)).size, 139);
      assert.deepEqual(all.slice(0, 137).map(row => row.id), messages.map(row => row.id).sort().reverse());
      assert.equal(all[137].subject, "Retained"); assert.equal(all[138].subject, "Historical NULL");
      assert.deepEqual(await rows(), before, "Paging the complete inbox does not write any data");
    });
  } finally { await f.pg.close(); }
});
