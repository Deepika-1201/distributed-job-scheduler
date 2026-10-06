// Creates COUNT schedules that fire at every minute boundary, spread over the tenants: the cron
// boundary scenario (LLD §23.1).
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const api = __ENV.API || 'http://localhost:8080';
const keys = JSON.parse(open(__ENV.KEYS));
const run = __ENV.RUN || String(Date.now());

export const options = {
  discardResponseBodies: true,
  scenarios: {
    create: { executor: 'shared-iterations', vus: 32, iterations: Number(__ENV.COUNT || 5000), maxDuration: '10m' },
  },
  thresholds: { checks: ['rate==1'] },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const res = http.post(`${api}/v1/schedules`, JSON.stringify({
    name: `cron-${run}-${i}`,
    job_type: 'load.noop',
    payload: { i },
    trigger: { kind: 'cron', cron: '* * * * *' },
  }), { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${keys[i % keys.length]}` } });
  check(res, { 'schedule created': (r) => r.status === 201 });
}
