import assert from "node:assert/strict";
import test from "node:test";
import { resolveBillaGmFilterEnrollment } from "./billa-gm-market-filter.shared.js";

const arthur = {
  id: "f154ed72-0f70-4d00-86df-50da233217cb",
  firstName: "Arthur",
  lastName: "Neuhold",
};

const market = {
  name: "Billa",
  dbName: "Billa",
  employee: "Neuhold Arthur",
  currentGmName: null,
  marketType: "universum" as const,
  isActive: true,
  isDeleted: false,
  kuehlerStammnr: null,
  cokeMasterNumber: " 1201131840 ",
  flexNumber: "S0969",
};

test("enrolls one exact active Billa GM assignment using the preferred market identifier", () => {
  assert.deepEqual(resolveBillaGmFilterEnrollment(market, [arthur]), {
    gmUserId: arthur.id,
    matchValue: "1201131840",
  });
});

test("accepts the normal first-name/last-name order", () => {
  assert.deepEqual(
    resolveBillaGmFilterEnrollment({ ...market, employee: null, currentGmName: "Arthur   Neuhold" }, [arthur]),
    { gmUserId: arthur.id, matchValue: "1201131840" },
  );
});

test("does not enroll an ambiguous assignment", () => {
  assert.equal(
    resolveBillaGmFilterEnrollment(market, [arthur, { ...arthur, id: "another-id" }]),
    null,
  );
});

test("does not create the first whitelist row for an unfiltered GM", () => {
  assert.equal(resolveBillaGmFilterEnrollment(market, []), null);
});

test("does not enroll ineligible or unrelated markets", () => {
  assert.equal(resolveBillaGmFilterEnrollment({ ...market, name: "Spar", dbName: "Spar" }, [arthur]), null);
  assert.equal(resolveBillaGmFilterEnrollment({ ...market, marketType: "kuehler" }, [arthur]), null);
  assert.equal(resolveBillaGmFilterEnrollment({ ...market, isActive: false }, [arthur]), null);
  assert.equal(resolveBillaGmFilterEnrollment({ ...market, isDeleted: true }, [arthur]), null);
  assert.equal(resolveBillaGmFilterEnrollment({ ...market, employee: "Someone Else" }, [arthur]), null);
});

test("falls back to the Flex number only when no canonical identifier exists", () => {
  assert.deepEqual(
    resolveBillaGmFilterEnrollment(
      { ...market, kuehlerStammnr: null, cokeMasterNumber: null, flexNumber: " s 0969 " },
      [arthur],
    ),
    { gmUserId: arthur.id, matchValue: "S0969" },
  );
});
