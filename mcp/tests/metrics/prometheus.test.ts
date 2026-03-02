import { describe, it, expect, beforeEach } from 'vitest';
import express from 'express';
import request from 'supertest';
import { Registry, collectDefaultMetrics, Histogram, Counter } from 'prom-client';

function createMetricsApp() {
    const register = new Registry();
    collectDefaultMetrics({ register });

    const httpRequestDuration = new Histogram({
        name: 'mcp_http_request_duration_seconds',
        help: 'Duration of HTTP requests in seconds',
        labelNames: ['method', 'route', 'status_code', 'auth_type'] as const,
        buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
        registers: [register],
    });

    const httpRequestsTotal = new Counter({
        name: 'mcp_http_requests_total',
        help: 'Total number of HTTP requests',
        labelNames: ['method', 'route', 'status_code', 'auth_type'] as const,
        registers: [register],
    });

    const app = express();

    app.get('/metrics', async (_req, res) => {
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    });

    app.post('/mcp', (req, res) => {
        const startTime = Date.now();
        const authType = (req.headers['x-api-key'] ? 'api-key' : 'none') as string;
        res.on('finish', () => {
            const duration = (Date.now() - startTime) / 1000;
            const labels = { method: 'POST', route: '/mcp', status_code: String(res.statusCode), auth_type: authType };
            httpRequestDuration.observe(labels, duration);
            httpRequestsTotal.inc(labels);
        });
        res.json({ ok: true });
    });

    return { app, register, httpRequestsTotal, httpRequestDuration };
}

describe('Prometheus metrics', () => {
    let app: express.Application;
    let register: Registry;
    let httpRequestsTotal: Counter;

    beforeEach(() => {
        const ctx = createMetricsApp();
        app = ctx.app;
        register = ctx.register;
        httpRequestsTotal = ctx.httpRequestsTotal;
    });

    it('GET /metrics returns 200 with prometheus content type', async () => {
        const res = await request(app).get('/metrics');
        expect(res.status).toBe(200);
        expect(res.headers['content-type']).toMatch(/text\/(plain|openmetrics)/);
    });

    it('GET /metrics includes default Node.js process metrics', async () => {
        const res = await request(app).get('/metrics');
        expect(res.text).toContain('process_cpu_');
        expect(res.text).toContain('nodejs_');
    });

    it('counter increments after a request to /mcp', async () => {
        await request(app).post('/mcp').send({});
        const res = await request(app).get('/metrics');
        expect(res.text).toContain('mcp_http_requests_total');
        expect(res.text).toMatch(/mcp_http_requests_total\{.*method="POST".*\}\s+1/);
    });

    it('histogram records duration after a request to /mcp', async () => {
        await request(app).post('/mcp').send({});
        const res = await request(app).get('/metrics');
        expect(res.text).toContain('mcp_http_request_duration_seconds_bucket');
        expect(res.text).toContain('mcp_http_request_duration_seconds_count');
    });

    it('labels include auth_type dimension', async () => {
        await request(app).post('/mcp').set('X-API-Key', 'test-key').send({});
        const res = await request(app).get('/metrics');
        expect(res.text).toMatch(/auth_type="api-key"/);
    });

    it('counter accumulates across multiple requests', async () => {
        await request(app).post('/mcp').send({});
        await request(app).post('/mcp').send({});
        await request(app).post('/mcp').send({});
        const res = await request(app).get('/metrics');
        expect(res.text).toMatch(/mcp_http_requests_total\{.*\}\s+3/);
    });
});
