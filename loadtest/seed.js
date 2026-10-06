// Seeds COUNT jobs due in a day, so a scenario runs against a backlog of future-dated jobs like
// NFR-2's, scaled down (LLD §23.1).
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const api = __ENV.API || 'http://localhost:8080';
const keys = JSON.parse(open(__ENV.KEYS));
const run = __ENV.RUN || String(Date.now());

export const options = {
  discardResponseBodies: true,
  scenarios: {
    seed: { executor: 'shared-iterations', vus: 64, iterations: Number(__ENV.COUNT || 100000), maxDuration: '20m' },
  },
  thresholds: { checks: ['rate>0.999'] },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const res = http.post(`${api}/v1/jobs`, JSON.stringify({ type: 'load.noop', delay: '24h', payload: { i } }), {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${keys[i % keys.length]}`,
      'Idempotency-Key': `${run}-seed-${i}`,
    },
  });
  check(res, { 'job created': (r) => r.status === 201 });
}
