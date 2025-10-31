import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Trend } from 'k6/metrics';

const baselineLatency = new Trend('baseline_serial_latency', true);
const mixedLatency = new Trend('parallel_mixed_filters_latency', true);
const paginationLatency = new Trend('pagination_window_latency', true);
const invalidLatency = new Trend('invalid_parameter_latency', true);
const loadLatency = new Trend('load_user_parallel_latency', true);

export const options = {
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(99)<1500'],
        baseline_serial_latency: ['p(95)<200'],
        parallel_mixed_filters_latency: ['p(95)<500'],
        pagination_window_latency: ['p(95)<400'],
        invalid_parameter_latency: ['p(95)<300'],
        load_user_parallel_latency: ['p(95)<600'],
    },
    scenarios: {
        baseline_serial: scenarioConfig('BASELINE', {
            exec: 'scenarioBaselineSerial',
            rate: 40,
            duration: '5m',
            preAllocatedVUs: 4,
            maxVUs: 20,
        }),
        parallel_mixed_filters: scenarioConfig('MIXED', {
            exec: 'scenarioParallelMixedFilters',
            rate: 120,
            duration: '10m',
            preAllocatedVUs: 24,
            maxVUs: 120,
        }),
        pagination_window_scan: scenarioConfig('PAGINATION', {
            exec: 'scenarioPaginationWindowScan',
            rate: 48,
            duration: '6m',
            preAllocatedVUs: 8,
            maxVUs: 48,
        }),
        invalid_parameter_resilience: scenarioConfig('INVALID', {
            exec: 'scenarioInvalidParameterResilience',
            rate: 20,
            duration: '4m',
            preAllocatedVUs: 6,
            maxVUs: 24,
        }),
        load_user_parallel: scenarioConfig('LOAD', {
            exec: 'scenarioLoadUserParallel',
            rate: 160,
            duration: '10m',
            preAllocatedVUs: 32,
            maxVUs: 160,
        }),
    },
};

export function setup() {
    const baseUrl = sanitizeBaseUrl(requireEnv('BASE_URL'));
    const dataset = parseDataset(requireEnv('LIST_DATASET'));
    const token = obtainToken(baseUrl);
    return { baseUrl, token, dataset };
}

export function scenarioBaselineSerial(cfg) {
    const context = ensureSetup(cfg);
    const query = buildQuery({ name: context.dataset.primaryName, limit: 1 });
    const res = sendListRequest(context, query, { scenario: 'baseline_serial' });
    baselineLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    check(res, {
        baseline_status_200: r => r.status === 200,
        baseline_code_success: () => parsed.code === 100001,
        baseline_single_result: () => parsed.users.length === 1,
        baseline_username_match: () => parsed.users.length === 1 && parsed.users[0].username === context.dataset.primaryName,
    });
    sleep(0.1);
}

export function scenarioParallelMixedFilters(cfg) {
    const context = ensureSetup(cfg);
    const profiles = buildMixedProfiles(context.dataset);
    const profile = profiles[Number(__ITER) % profiles.length];
    const res = sendListRequest(context, profile.query, { scenario: 'parallel_mixed_filters', profile: profile.label });
    mixedLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const ok = parsed.code === 100001 && profile.validator(parsed.users, parsed);
    check(res, {
        [`${profile.label}_http_200`]: r => r.status === 200,
        [`${profile.label}_code_success`]: () => parsed.code === 100001,
        [`${profile.label}_validation`]: () => ok,
    });
    sleep(0.05);
}

export function scenarioPaginationWindowScan(cfg) {
    const context = ensureSetup(cfg);
    const { dataset } = context;
    const idx = Number(__ITER) % dataset.paginationExpected.length;
    const query = buildQuery({
        status: dataset.paginationStatus,
        ['email[like]']: dataset.paginationEmailPrefix,
        limit: 1,
        offset: idx,
    });
    const res = sendListRequest(context, query, { scenario: 'pagination_window_scan', offset: String(idx) });
    paginationLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    check(res, {
        pagination_http_200: r => r.status === 200,
        pagination_code_success: () => parsed.code === 100001,
        pagination_single_result: () => parsed.users.length === 1,
        pagination_expected_user: () => parsed.users.length === 1 && parsed.users[0].username === dataset.paginationExpected[idx],
    });
    sleep(0.1);
}

export function scenarioInvalidParameterResilience(cfg) {
    const context = ensureSetup(cfg);
    const variants = buildInvalidVariants();
    const variant = variants[Number(__ITER) % variants.length];
    const res = sendListRequest(context, variant.query, { scenario: 'invalid_parameter_resilience', variant: variant.label });
    invalidLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    check(res, {
        [`${variant.label}_http_400`]: r => r.status === 400,
        [`${variant.label}_code_invalid_param`]: () => parsed.code === 110004 || parsed.code === 100004,
    });
    sleep(0.1);
}

export function scenarioLoadUserParallel(cfg) {
    const context = ensureSetup(cfg);
    const { dataset } = context;
    const idx = Number(__ITER) % dataset.loadUserNames.length;
    const username = dataset.loadUserNames[idx];
    const query = buildQuery({ name: username, status: '0,1', limit: 1 });
    const res = sendListRequest(context, query, { scenario: 'load_user_parallel' });
    loadLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    check(res, {
        load_http_200: r => r.status === 200,
        load_code_success: () => parsed.code === 100001,
        load_match_username: () => parsed.users.length === 1 && parsed.users[0].username === username,
    });
    sleep(0.05);
}

function scenarioConfig(prefix, defaults) {
    const rate = parsePositiveIntEnv(`${prefix}_RATE`, defaults.rate);
    const duration = __ENV[`${prefix}_DURATION`] || defaults.duration;
    const preAllocatedVUs = parsePositiveIntEnv(`${prefix}_VUS`, defaults.preAllocatedVUs);
    const maxVUs = parsePositiveIntEnv(`${prefix}_MAX_VUS`, defaults.maxVUs);
    return {
        executor: 'constant-arrival-rate',
        exec: defaults.exec,
        rate,
        timeUnit: '1s',
        duration,
        preAllocatedVUs,
        maxVUs,
    };
}

function ensureSetup(cfg) {
    if (!cfg || !cfg.baseUrl || !cfg.token || !cfg.dataset) {
        fail('setup data missing or incomplete');
    }
    return cfg;
}

function buildMixedProfiles(dataset) {
    return [
        {
            label: 'profile_by_name',
            query: buildQuery({ name: dataset.primaryName, limit: 1 }),
            validator: users => users.length === 1 && users[0].username === dataset.primaryName,
        },
        {
            label: 'profile_disabled_status',
            query: buildQuery({ name: dataset.multiDisabledName, status: '0', limit: 1 }),
            validator: users => users.length === 1 && users[0].username === dataset.multiDisabledName,
        },
        {
            label: 'profile_email_like',
            query: buildQuery({ ['email[like]']: dataset.multiEmailPrefix, status: '0,1' }),
            validator: users => users.length >= 1,
        },
        {
            label: 'profile_phone_like',
            query: buildQuery({ ['phone[like]']: dataset.contactPhonePrefix, limit: 5 }),
            validator: users => users.some(user => user.username === dataset.contactUsername),
        },
    ];
}

function buildInvalidVariants() {
    return [
        { label: 'invalid_status', query: buildQuery({ status: 'abc' }) },
        { label: 'invalid_time', query: buildQuery({ ['createdAt[gte]']: '2024-13-01' }) },
        { label: 'invalid_extend', query: buildQuery({ 'extend..illegal': 'foo' }) },
        { label: 'invalid_offset', query: buildQuery({ offset: -1 }) },
    ];
}

function sendListRequest(context, query, tags) {
    const queryString = typeof query === 'string' ? query : queryStringFromObject(query);
    const url = `${context.baseUrl}/v1/users${queryString ? `?${queryString}` : ''}`;
    return http.get(url, { headers: buildHeaders(context.token), tags });
}

function buildHeaders(token) {
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers.Authorization = `Bearer ${token}`;
    }
    return headers;
}

function parseResponse(res) {
    let payload = null;
    try {
        payload = res.json();
    } catch (err) {
        return { code: null, users: [], message: String(err) };
    }
    const code = payload && payload.code !== undefined ? payload.code : null;
    const message = payload && payload.message ? payload.message : '';
    const users = extractUsers(payload ? payload.data : null);
    return { code, users, message, payload };
}

function extractUsers(data) {
    if (!data) {
        return [];
    }
    if (Array.isArray(data)) {
        return data.map(normalizeUserRecord);
    }
    if (typeof data === 'string') {
        try {
            const parsed = JSON.parse(data);
            return Array.isArray(parsed) ? parsed.map(normalizeUserRecord) : [];
        } catch (err) {
            return [];
        }
    }
    if (Array.isArray(data.items)) {
        return data.items.map(normalizeUserRecord);
    }
    return [];
}

function normalizeUserRecord(item) {
    if (!item || typeof item !== 'object') {
        return item;
    }
    if (item.username && item.email) {
        return item;
    }
    const normalized = Object.assign({}, item);
    for (const [key, value] of Object.entries(item)) {
        const lower = key.toLowerCase();
        if (!(lower in normalized)) {
            normalized[lower] = value;
        }
        const camel = key.length > 0 ? key[0].toLowerCase() + key.slice(1) : key;
        if (!(camel in normalized)) {
            normalized[camel] = value;
        }
    }
    return normalized;
}

function buildQuery(values) {
    return values;
}

function queryStringFromObject(params) {
    const segments = [];
    for (const [key, value] of Object.entries(params)) {
        if (value === undefined || value === null || value === '') {
            continue;
        }
        segments.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
    }
    return segments.join('&');
}

function parseDataset(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (err) {
        fail(`LIST_DATASET 解析失败: ${err.message}`);
    }

    const requiredStringFields = ['primaryName', 'multiDisabledName', 'multiEmailPrefix', 'contactPhonePrefix', 'contactUsername', 'paginationEmailPrefix'];
    for (const field of requiredStringFields) {
        if (!parsed[field] || typeof parsed[field] !== 'string') {
            fail(`LIST_DATASET 缺少或非法字段: ${field}`);
        }
        parsed[field] = parsed[field].trim();
    }

    if (!Array.isArray(parsed.paginationExpected) || parsed.paginationExpected.length === 0) {
        fail('LIST_DATASET.paginationExpected 必须为非空数组');
    }
    parsed.paginationExpected = parsed.paginationExpected.map(v => String(v));

    if (!Array.isArray(parsed.loadUserNames) || parsed.loadUserNames.length === 0) {
        fail('LIST_DATASET.loadUserNames 必须为非空数组');
    }
    parsed.loadUserNames = parsed.loadUserNames.map(v => String(v));

    parsed.paginationStatus = parsed.paginationStatus ? String(parsed.paginationStatus) : '1';

    return parsed;
}

function sanitizeBaseUrl(raw) {
    const trimmed = raw.replace(/\s+/g, '');
    if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://')) {
        fail('BASE_URL 必须包含 http:// 或 https:// 前缀');
    }
    return trimmed.replace(/\/+$/, '');
}

function obtainToken(baseUrl) {
    if (__ENV.ADMIN_TOKEN && __ENV.ADMIN_TOKEN.trim() !== '') {
        return __ENV.ADMIN_TOKEN.trim();
    }
    const username = (__ENV.ADMIN_USERNAME || '').trim();
    const password = (__ENV.ADMIN_PASSWORD || '').trim();
    if (!username || !password) {
        fail('请通过 ADMIN_TOKEN 或 ADMIN_USERNAME/ADMIN_PASSWORD 提供管理员凭据');
    }
    const payload = JSON.stringify({ username, password });
    const res = http.post(`${baseUrl}/login`, payload, { headers: { 'Content-Type': 'application/json' } });
    if (res.status !== 200) {
        fail(`管理员登录失败，状态码: ${res.status}`);
    }
    let body;
    try {
        body = res.json();
    } catch (err) {
        fail(`解析登录响应失败: ${err.message}`);
    }
    const data = body && body.data ? body.data : null;
    const token = data ? data.access_token || data.accessToken || data.token : null;
    if (!token) {
        fail('登录响应中缺少 access_token');
    }
    return token;
}

function requireEnv(name) {
    const value = (__ENV[name] || '').trim();
    if (!value) {
        fail(`环境变量 ${name} 未设置`);
    }
    return value;
}

function parsePositiveIntEnv(name, fallback) {
    const raw = (__ENV[name] || '').trim();
    if (!raw) {
        return fallback;
    }
    const value = parseInt(raw, 10);
    if (!Number.isFinite(value) || value <= 0) {
        fail(`${name} 必须为正整数`);
    }
    return value;
}
