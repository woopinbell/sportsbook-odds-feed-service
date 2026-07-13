import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8085";
const ENDPOINT = requiredEndpoint(__ENV.ENDPOINT);
const PHASE = requiredPhase(__ENV.PHASE);
const EVENT_ID = requiredUuid("EVENT_ID");
const EXPECTED_EVENTS = ENDPOINT === "events" ? requiredJson("EXPECTED_EVENTS_JSON") : null;
const EXPECTED_EVENTS_CANONICAL =
  ENDPOINT === "events" ? canonicalJson(EXPECTED_EVENTS) : "";
const MARKET_ID = ENDPOINT === "odds" ? requiredUuid("MARKET_ID") : "";
const SELECTION_ID = ENDPOINT === "odds" ? requiredUuid("SELECTION_ID") : "";
const EXPECTED_ODDS = ENDPOINT === "odds" ? requiredPositiveNumber("EXPECTED_ODDS") : 0;

export const options = {
  scenarios: {
    endpoint_gate: {
      executor: "constant-arrival-rate",
      exec: ENDPOINT === "events" ? "readEvents" : "readOdds",
      rate: 1000,
      timeUnit: "1s",
      duration: "60s",
      preAllocatedVUs: 200,
      maxVUs: 500,
      gracefulStop: "5s",
    },
  },
  thresholds:
    PHASE === "measure"
      ? {
          http_req_duration: ["p(99)<50"],
          http_req_failed: ["rate<0.001"],
          checks: ["rate>0.999"],
          dropped_iterations: ["count==0"],
        }
      : {},
  summaryTrendStats: ["min", "avg", "p(50)", "p(95)", "p(99)", "max"],
};

export function readEvents() {
  const response = http.get(`${BASE_URL}/api/v1/events?size=20`, {
    tags: { endpoint: "events" },
  });

  check(response, {
    "events response matches the preconditioned contract": (result) => {
      if (result.status !== 200) {
        return false;
      }
      try {
        return canonicalJson(result.json()) === EXPECTED_EVENTS_CANONICAL;
      } catch (_error) {
        return false;
      }
    },
  });
}

export function readOdds() {
  const response = http.get(
    `${BASE_URL}/api/v1/odds/${EVENT_ID}/${MARKET_ID}/${SELECTION_ID}`,
    { tags: { endpoint: "odds" } },
  );

  check(response, {
    "odds response exactly matches the frozen Redis value": (result) => {
      if (result.status !== 200) {
        return false;
      }
      try {
        const body = result.json();
        return (
          body !== null &&
          typeof body === "object" &&
          !Array.isArray(body) &&
          Object.keys(body).sort().join(",") === "eventId,marketId,odds,selectionId" &&
          body.eventId === EVENT_ID &&
          body.marketId === MARKET_ID &&
          body.selectionId === SELECTION_ID &&
          typeof body.odds === "number" &&
          body.odds === EXPECTED_ODDS
        );
      } catch (_error) {
        return false;
      }
    },
  });
}

function requiredEndpoint(value) {
  if (value !== "events" && value !== "odds") {
    throw new Error("ENDPOINT must be events or odds");
  }
  return value;
}

function requiredPhase(value) {
  if (value !== "warmup" && value !== "measure" && value !== "observe") {
    throw new Error("PHASE must be warmup, measure, or observe");
  }
  return value;
}

function requiredUuid(name) {
  const value = __ENV[name];
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  if (!value || !uuid.test(value)) {
    throw new Error(`${name} must be a canonical UUID from the running service`);
  }
  return value;
}

function requiredPositiveNumber(name) {
  const number = Number(__ENV[name]);
  if (!Number.isFinite(number) || number <= 0) {
    throw new Error(`${name} must be a positive decimal from the frozen Redis key`);
  }
  return number;
}

function requiredJson(name) {
  const value = __ENV[name];
  if (!value) {
    throw new Error(`${name} must contain the frozen canonical events response`);
  }
  try {
    return JSON.parse(value);
  } catch (_error) {
    throw new Error(`${name} must be valid JSON`);
  }
}

function canonicalJson(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}
