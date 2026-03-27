// k6 scenario: sustained read traffic against /api/v1/events and
// /api/v1/odds. Models the gateway snapshotting events and the
// betting-service polling odds during slip validation.
//
// Run:
//   k6 run --vus 200 --duration 1m load-test/scenarios/odds_read.js
//
// Requires a running service on localhost:8085 with the mock profile
// (so the EventCatalog has populated entries).

import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "10s", target: 50 },   // ramp to 50 VUs
    { duration: "30s", target: 200 },  // sustain 200 VUs
    { duration: "10s", target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_failed: ["rate<0.001"],
    http_req_duration: ["p(99)<50"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8085";

export default function () {
  const listResponse = http.get(`${BASE_URL}/api/v1/events?size=20`);
  check(listResponse, {
    "events list 200": (r) => r.status === 200,
    "events list has items": (r) => r.json("items").length > 0,
  });

  // Subsequent request: fetch one event's odds. The mock seeds three
  // events, each with one MATCH_RESULT_1X2 market and three selections,
  // so we just hit the first event from the list response.
  const firstEvent = listResponse.json("items.0");
  if (firstEvent) {
    // The odds path needs marketId + selectionId, which the read API
    // doesn't currently expose; this scenario therefore exercises the
    // events list path only. A future commit can extend the API with
    // a markets-of-event endpoint and this script will cover it.
  }

  sleep(0.1);
}
