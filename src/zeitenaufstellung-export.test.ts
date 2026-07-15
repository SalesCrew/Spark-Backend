import assert from "node:assert/strict";
import test from "node:test";
import {
  buildZeitenaufstellungRows,
  formatZeitenaufstellungTargetObject,
  resolveZeitenaufstellungCustomerNumber,
  type ZeitenaufstellungMarket,
} from "./lib/zeitenaufstellung-export.js";

const market: ZeitenaufstellungMarket = {
  name: "SPAR TESTMARKT",
  dbName: "Sparmarkt",
  address: "HAUPTSTRASSE 001",
  postalCode: "1010",
  city: "WIEN",
  region: "Ost",
  cokeMasterNumber: "1200000001",
  standardMarketNumber: "S-1",
  flexNumber: "F-1",
  kuehlerStammnr: "K-1",
  universeMarket: true,
};

test("Zeitenaufstellung formats the target object and keeps the customer-number priority", () => {
  assert.equal(resolveZeitenaufstellungCustomerNumber(market), "1200000001");
  assert.match(formatZeitenaufstellungTargetObject(market), /^Spar; O;/);
  assert.match(formatZeitenaufstellungTargetObject(market), /Universums-Markt, Ja/);
});

test("Zeitenaufstellung emits one row per questionnaire, hides pauses and assigns travel once", () => {
  const dayStart = new Date("2026-07-13T06:00:00.000Z");
  const dayEnd = new Date("2026-07-13T15:00:00.000Z");
  const visitStart = new Date("2026-07-13T07:00:00.000Z");
  const visitEnd = new Date("2026-07-13T08:00:00.000Z");
  const rows = buildZeitenaufstellungRows({
    timezone: "UTC",
    days: [{ id: "day-1", gmId: "gm-1", workDate: "2026-07-13", startedAt: dayStart, endedAt: dayEnd }],
    visits: [{
      id: "visit-1",
      gmId: "gm-1",
      workDate: "2026-07-13",
      startedAt: visitStart,
      submittedAt: visitEnd,
      person: "Test GM",
      market,
    }],
    sections: [
      {
        id: "section-standard",
        visitSessionId: "visit-1",
        section: "standard",
        orderIndex: 0,
        fragebogenName: "2026_Coca-Cola_Standard Q3",
        campaignName: "Standard 07",
        createdAt: visitStart,
        updatedAt: new Date("2026-07-13T07:50:00.000Z"),
      },
      {
        id: "section-flex",
        visitSessionId: "visit-1",
        section: "flex",
        orderIndex: 1,
        fragebogenName: "2026_Coca-Cola_Flex Q3",
        campaignName: "Flex 07",
        createdAt: new Date("2026-07-13T07:20:00.000Z"),
        updatedAt: new Date("2026-07-13T07:40:00.000Z"),
      },
    ],
    extras: [{
      id: "extra-1",
      gmId: "gm-1",
      workDate: "2026-07-13",
      startedAt: new Date("2026-07-13T08:30:00.000Z"),
      endedAt: new Date("2026-07-13T09:00:00.000Z"),
      activityType: "homeoffice",
      comment: "Nachbereitung",
      person: "Test GM",
    }],
    pauses: [{
      id: "pause-1",
      gmId: "gm-1",
      workDate: "2026-07-13",
      startedAt: new Date("2026-07-13T09:00:00.000Z"),
      endedAt: new Date("2026-07-13T09:15:00.000Z"),
    }],
    sectionMetrics: new Map([
      ["section-standard", { photoCount: 2, fillDurationMin: 5, comments: ["Alles erledigt"] }],
      ["section-flex", { photoCount: 1, fillDurationMin: 3, comments: [] }],
    ]),
    personByGmId: new Map([["gm-1", "Test GM"]]),
  });

  assert.equal(rows.length, 4);
  const standard = rows.find((row) => row.questionnaire === "2026_Coca-Cola_Standard Q3");
  const flex = rows.find((row) => row.questionnaire === "2026_Coca-Cola_Flex Q3");
  const homeOffice = rows.find((row) => row.targetObject === "Home-Office");
  const homeDrive = rows.find((row) => row.targetObject === "Heimfahrt");
  assert.ok(standard);
  assert.ok(flex);
  assert.ok(homeOffice);
  assert.ok(homeDrive);
  assert.equal(standard.travelDurationMin, 60);
  assert.equal(flex.travelDurationMin, null);
  assert.equal(standard.visitDurationMin, 50);
  assert.equal(flex.visitDurationMin, 20);
  assert.equal(standard.imageCount, 2);
  assert.equal(standard.calculatedFillDurationMin, 5);
  assert.equal(homeOffice.travelDurationMin, 30);
  assert.equal(homeOffice.visitDurationMin, 30);
  assert.equal(homeDrive.travelDurationMin, 345);
  assert.equal(rows.some((row) => row.targetObject.toLocaleLowerCase("de").includes("pause")), false);
});
