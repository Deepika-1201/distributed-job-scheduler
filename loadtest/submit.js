// Offers the load-test gate's submissions (LLD §19.1): BASE_RATE jobs/s, a burst at BURST_RATE,
// then BASE_RATE again. Each iteration posts one load.noop job for the next tenant in turn.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const api = __ENV.API || 'http://localhost:8080';
const keys = JSON.parse(open(__ENV.KEYS));
const run = __ENV.RUN || String(Date.now());
const base = Number(__ENV.BASE_RATE || 500);
const burst = Number(__ENV.BURST_RATE || 5000);
const warmup = Number(__ENV.WARMUP || 30);
const burstFor = Number(__ENV.BURST || 60);
const cooldown = Number(__ENV.COOLDOWN || 30);
const offered = base * (warmup + cooldown) + burst * burstFor + (base + burst) * 2;

// Over every 100 iterations: 2 CRITICAL, 10 HIGH, 68 NORMAL and 20 LOW, interleaved.
const mix = [['CRITICAL', 2], ['HIGH', 10], ['NORMAL', 68], ['LOW', 20]];

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'p(50)', 'p(99)', 'max'],
  scenarios: {
    submissions: {
      executor: 'ramping-arrival-rate',
      startRate: base,
      timeUnit: '1s',
      preAllocatedVUs: Number(__ENV.VUS || 300),
      maxVUs: Number(__ENV.MAX_VUS || 2000),
      stages: [
        { target: base, duration: `${warmup}s` },
        { target: burst, duration: '2s' },
        { target: burst, duration: `${burstFor}s` },
        { target: base, duration: '2s' },
        { target: base, duration: `${cooldown}s` },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    dropped_iterations: [`count<${Math.ceil(offered / 100)}`],
  },
};

function priority(i) {
  let n = (i * 37) % 100;
  for (const [name, share] of mix) {
    if (n < share) return name;
    n -= share;
  }
  return 'NORMAL';
}

export default function () {
  const i = exec.scenario.iterationInTest;
  const res = http.post(`${api}/v1/jobs`, JSON.stringify({ type: 'load.noop', priority: priority(i), payload: { i } }), {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${keys[i % keys.length]}`,
      'Idempotency-Key': `${run}-${i}`,
    },
  });
  check(res, { 'job created': (r) => r.status === 201 });
}
