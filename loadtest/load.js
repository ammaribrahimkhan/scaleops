import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 15 },
    { duration: '2m', target: 40 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.10'],
  },
};

export default function () {
  const res = http.get('http://localhost:8080/api/work?ms=40');
  check(res, { 'status is 200': (r) => r.status === 200 });
  sleep(0.2);
}