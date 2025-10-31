import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { randomSeed } from 'k6';

randomSeed(Date.now());

const CODE_SUCCESS = 100001;
const CODE_BIND_ERROR = 100003;
const CODE_VALIDATION = 100004;
const CODE_KAFKA_DEGRADED = 100401;
const CODE_INVALID_PARAMETER = 110004;
const CODE_DUPLICATE = 110002;
const CODE_RESOURCE_CONFLICT = 110006;

const HTTP_CREATED = 201;
const HTTP_BAD_REQUEST = 400;
const HTTP_CONFLICT = 409;

const baselineLatency = new Trend('create_baseline_latency', true);
const ingestLatency = new Trend('create_parallel_latency', true);
const duplicateLatency = new Trend('create_duplicate_latency', true);
const validationLatency = new Trend('create_validation_latency', true);

const baselineSuccessRate = new Rate('create_baseline_success_rate');
const ingestSuccessRate = new Rate('create_parallel_success_rate');
const duplicateConflictRate = new Rate('create_duplicate_conflict_rate');
const validationFailureRate = new Rate('create_validation_failure_rate');
const degradedRate = new Rate('create_degraded_rate');

const HIGH_CARDINALITY_TAG_KEYS = new Set(['username', 'request_id', 'x_request_id', 'x-request-id']);
const DEFAULT_TAG_VALUE = '_default';

export const options = {
    setupTimeout: '180s',
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(99)<2000'],
        create_baseline_latency: ['p(95)<350'],
        create_parallel_latency: ['p(95)<900'],
        create_duplicate_latency: ['p(95)<250'],
        create_validation_latency: ['p(95)<200'],
        create_baseline_success_rate: ['rate>0.98'],
        create_parallel_success_rate: ['rate>0.95'],
        create_duplicate_conflict_rate: ['rate>0.95'],
        create_validation_failure_rate: ['rate>0.95'],
        create_degraded_rate: ['rate<0.02'],
    },
    scenarios: {
        baseline_serial: scenarioConfig('BASELINE', {
            exec: 'scenarioBaselineSerial',
            rate: 40,
            duration: '5m',
            preAllocatedVUs: 4,
            maxVUs: 20,
        }),
        parallel_ingest: scenarioConfig('INGEST', {
            exec: 'scenarioParallelIngest',
            rate: 120,
            duration: '10m',
            preAllocatedVUs: 24,
            maxVUs: 120,
        }),
        duplicate_guard: scenarioConfig('DUPLICATE', {
            exec: 'scenarioDuplicateGuard',
            rate: 32,
            duration: '6m',
            preAllocatedVUs: 6,
            maxVUs: 32,
        }),
        validation_wall: scenarioConfig('VALIDATION', {
            exec: 'scenarioValidationFailures',
            rate: 24,
            duration: '4m',
            preAllocatedVUs: 4,
            maxVUs: 24,
        }),
    },
};

export function setup() {
    const baseUrl = sanitizeBaseUrl(requireEnv('BASE_URL'));
    const dataset = parseDataset(requireEnv('CREATE_DATASET'));
    const token = obtainToken(baseUrl);
    const context = { baseUrl, token, dataset };
    context.duplicateUser = createOrEnsureDuplicate(context);
    return context;
}

export function scenarioBaselineSerial(cfg) {
    const context = ensureSetup(cfg);
    const result = generateUserPayload(context.dataset, { includeOptional: false });
    const res = sendCreateRequest(context, result.payload, { scenario: 'baseline_serial', username: result.username });
    baselineLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const success = res.status === HTTP_CREATED && parsed.code === CODE_SUCCESS && !degraded;
    baselineSuccessRate.add(success);
    degradedRate.add(degraded);
    check(res, {
        baseline_http_201: r => r.status === HTTP_CREATED,
        baseline_code_success: () => parsed.code === CODE_SUCCESS,
        baseline_created_username: () => matchCreatedUsername(parsed.data, result.username),
    });
    sleep(context.dataset.sleepBaseline);
}

export function scenarioParallelIngest(cfg) {
    const context = ensureSetup(cfg);
    const variants = parallelVariants();
    const variant = variants[Number(__ITER) % variants.length];
    const result = generateUserPayload(context.dataset, { includeOptional: true });
    variant.mutate(result, context.dataset);
    const res = sendCreateRequest(context, result.payload, { scenario: 'parallel_ingest', variant: variant.label, username: result.username });
    ingestLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const success = res.status === HTTP_CREATED && parsed.code === CODE_SUCCESS && !degraded;
    ingestSuccessRate.add(success);
    degradedRate.add(degraded);
    check(res, {
        [`${variant.label}_http_201`]: r => r.status === HTTP_CREATED,
        [`${variant.label}_code_success`]: () => parsed.code === CODE_SUCCESS,
    });
    sleep(context.dataset.sleepParallel);
}

export function scenarioDuplicateGuard(cfg) {
    const context = ensureSetup(cfg);
    const duplicate = context.duplicateUser;
    const payload = createDuplicatePayload(context.dataset, duplicate);
    const res = sendCreateRequest(context, payload, { scenario: 'duplicate_guard', username: duplicate.username });
    duplicateLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const conflict = res.status === HTTP_CONFLICT && (parsed.code === CODE_DUPLICATE || parsed.code === CODE_RESOURCE_CONFLICT);
    duplicateConflictRate.add(conflict);
    degradedRate.add(degraded);
    check(res, {
        duplicate_http_409: r => r.status === HTTP_CONFLICT,
        duplicate_code_conflict: () => parsed.code === CODE_DUPLICATE || parsed.code === CODE_RESOURCE_CONFLICT,
    });
    sleep(context.dataset.sleepDuplicate);
}

export function scenarioValidationFailures(cfg) {
    const context = ensureSetup(cfg);
    const invalidCases = buildInvalidPayloads(context.dataset);
    const variant = invalidCases[Number(__ITER) % invalidCases.length];
    const res = sendCreateRequest(context, variant.payload, { scenario: 'validation_wall', variant: variant.label });
    validationLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const isValidation = res.status === HTTP_BAD_REQUEST && isValidationCode(parsed.code);
    validationFailureRate.add(isValidation);
    degradedRate.add(degraded);
    check(res, {
        [`${variant.label}_http_400`]: r => r.status === HTTP_BAD_REQUEST,
        [`${variant.label}_code_validation`]: () => isValidation,
    });
    sleep(context.dataset.sleepValidation);
}

function scenarioConfig(prefix, defaults) {
    const rate = parsePositiveIntEnv(`${prefix}_RATE`, defaults.rate);
    const duration = (__ENV[`${prefix}_DURATION`] || defaults.duration).trim();
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
        fail('setup data missing or invalid');
    }
    return cfg;
}

function sendCreateRequest(context, payload, tags) {
    const finalTags = mergeTags({ endpoint: 'create_user' }, tags);
    const headers = buildHeaders(context.token, finalTags);
    const params = { headers, tags: finalTags };
    if (context.dataset.requestTimeout) {
        params.timeout = context.dataset.requestTimeout;
    }
    return http.post(`${context.baseUrl}/v1/users`, JSON.stringify(payload), params);
}

function buildHeaders(token, tags) {
    const headers = {
        'Content-Type': 'application/json',
    };
    if (token) {
        headers.Authorization = `Bearer ${token}`;
    }
    const scenario = tags && typeof tags.scenario === 'string' ? tags.scenario : '';
    if (scenario) {
        headers['X-Scenario-ID'] = buildScenarioId(scenario);
    }
    return headers;
}

function parseResponse(res) {
    let payload = null;
    try {
        payload = res.json();
    } catch (err) {
        return { code: null, message: String(err), data: null, payload: null };
    }
    if (!payload || typeof payload !== 'object') {
        return { code: null, message: '', data: null, payload };
    }
    const code = Object.prototype.hasOwnProperty.call(payload, 'code') ? payload.code : null;
    const message = payload.message ? String(payload.message) : '';
    const data = normalizeData(payload.data);
    return { code, message, data, payload };
}

function normalizeData(raw) {
    if (raw === null || raw === undefined) {
        return null;
    }
    if (typeof raw === 'string') {
        try {
            return JSON.parse(raw);
        } catch (err) {
            return { value: raw };
        }
    }
    return raw;
}

function matchCreatedUsername(data, username) {
    if (!data) {
        return false;
    }
    if (typeof data === 'string') {
        try {
            const parsed = JSON.parse(data);
            return matchCreatedUsername(parsed, username);
        } catch (err) {
            return false;
        }
    }
    if (data.create_user && data.create_user === username) {
        return true;
    }
    if (data.createUser && data.createUser === username) {
        return true;
    }
    if (data.username && data.username === username) {
        return true;
    }
    return false;
}

function generateUserPayload(dataset, options) {
    const opts = options || {};
    const includeOptional = opts.includeOptional !== false;
    const username = buildUsername(dataset);
    const payload = {
        metadata: { name: username },
        password: dataset.basePassword,
        email: buildEmailForUsername(dataset, username),
    };
    if (includeOptional && dataset.nicknamePool.length > 0) {
        payload.nickname = randomPick(dataset.nicknamePool);
    }
    if (includeOptional && dataset.phonePrefix) {
        payload.phone = dataset.phonePrefix + randomDigits(dataset.phoneSuffixLength);
    }
    payload.status = includeOptional ? dataset.defaultStatus : 1;
    let isAdmin = dataset.defaultIsAdmin;
    if (includeOptional && dataset.isAdminRatio > 0 && Math.random() < dataset.isAdminRatio) {
        isAdmin = 1;
    }
    payload.isAdmin = isAdmin;
    if (includeOptional && dataset.extendTemplates.length > 0) {
        const extendTemplate = cloneObject(randomPick(dataset.extendTemplates));
        payload.metadata.extend = extendTemplate;
    }
    if (includeOptional && dataset.labels) {
        payload.labels = cloneObject(dataset.labels);
    }
    if (includeOptional && dataset.extras) {
        payload.extras = cloneObject(dataset.extras);
    }
    if (opts.forceStatus !== undefined) {
        payload.status = opts.forceStatus;
    }
    if (opts.forceIsAdmin !== undefined) {
        payload.isAdmin = opts.forceIsAdmin;
    }
    if (opts.additionalMetadata) {
        payload.metadata = Object.assign({}, payload.metadata, opts.additionalMetadata);
    }
    if (opts.additionalPayload) {
        Object.assign(payload, opts.additionalPayload);
    }
    return { payload, username };
}

function buildUsername(dataset) {
    const suffix = uniqueSuffix();
    let username = `${dataset.usernamePrefix}-${suffix}`;
    if (username.length > dataset.maxUsernameLength) {
        username = username.slice(0, dataset.maxUsernameLength);
    }
    return username;
}

function buildEmailForUsername(dataset, username) {
    const maxLocalLength = 64;
    let local = username;
    if (local.length > maxLocalLength) {
        local = local.slice(0, maxLocalLength);
    }
    return `${local}@${dataset.emailDomain}`;
}

function parallelVariants() {
    return [
        {
            label: 'variant_full_profile',
            mutate: () => { },
        },
        {
            label: 'variant_disabled_user',
            mutate: result => {
                result.payload.status = 0;
                result.payload.nickname = `disabled_${result.username.slice(0, 12)}`;
            },
        },
        {
            label: 'variant_admin_user',
            mutate: result => {
                result.payload.isAdmin = 1;
                result.payload.status = 1;
            },
        },
        {
            label: 'variant_extended_metadata',
            mutate: (result, dataset) => {
                const extendMix = dataset.extendTemplates.length > 0 ? cloneObject(randomPick(dataset.extendTemplates)) : {};
                extendMix.trace = uniqueSuffix().slice(-6);
                result.payload.metadata.extend = extendMix;
                result.payload.extras = Object.assign({ source: 'k6_parallel' }, result.payload.extras || {});
            },
        },
    ];
}

function createDuplicatePayload(dataset, duplicate) {
    const payload = {
        metadata: { name: duplicate.username },
        password: duplicate.password,
        email: duplicate.email,
    };
    if (duplicate.phone) {
        payload.phone = duplicate.phone;
    }
    if (duplicate.isAdmin !== undefined) {
        payload.isAdmin = duplicate.isAdmin;
    }
    if (duplicate.status !== undefined) {
        payload.status = duplicate.status;
    }
    return payload;
}

function buildInvalidPayloads(dataset) {
    const cases = [];
    const baseEmail = label => buildEmailForUsername(dataset, `${dataset.usernamePrefix}-${label}-${uniqueSuffix()}`);
    const nameFor = label => sanitizeNameCandidate(`${dataset.usernamePrefix}-${label}-${uniqueSuffix()}`, dataset.maxUsernameLength);

    cases.push({
        label: 'missing_password',
        payload: {
            metadata: { name: nameFor('nopass') },
            email: baseEmail('nopass'),
        },
    });

    cases.push({
        label: 'invalid_email',
        payload: {
            metadata: { name: nameFor('badmail') },
            password: dataset.basePassword,
            email: 'not-an-email',
        },
    });

    cases.push({
        label: 'short_password',
        payload: {
            metadata: { name: nameFor('shortpwd') },
            password: 'Aa1!',
            email: baseEmail('shortpwd'),
        },
    });

    cases.push({
        label: 'missing_name',
        payload: {
            password: dataset.basePassword,
            email: baseEmail('noname'),
        },
    });

    if (dataset.phonePrefix) {
        cases.push({
            label: 'invalid_phone',
            payload: {
                metadata: { name: nameFor('badphone') },
                password: dataset.basePassword,
                email: baseEmail('badphone'),
                phone: 'abc123',
            },
        });
    }

    return cases;
}

function isValidationCode(code) {
    return code === CODE_VALIDATION || code === CODE_INVALID_PARAMETER || code === CODE_BIND_ERROR;
}

function parseDataset(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (err) {
        fail(`CREATE_DATASET 解析失败: ${err.message}`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        fail('CREATE_DATASET 必须是 JSON 对象');
    }
    enforceString(parsed, 'usernamePrefix');
    enforceString(parsed, 'basePassword');
    enforceString(parsed, 'emailDomain');

    parsed.usernamePrefix = sanitizeNameCandidate(parsed.usernamePrefix, 32);
    parsed.basePassword = parsed.basePassword.trim();
    parsed.emailDomain = parsed.emailDomain.trim().toLowerCase();

    parsed.phonePrefix = typeof parsed.phonePrefix === 'string' ? parsed.phonePrefix.trim() : '';
    parsed.phoneSuffixLength = typeof parsed.phoneSuffixLength === 'number' && parsed.phoneSuffixLength > 0 ? Math.floor(parsed.phoneSuffixLength) : 6;

    parsed.nicknamePool = Array.isArray(parsed.nicknamePool) ? parsed.nicknamePool.map(v => String(v).trim()).filter(v => v !== '') : [];

    parsed.extendTemplates = Array.isArray(parsed.extendTemplates) ? parsed.extendTemplates.filter(isPlainObject) : [];

    parsed.labels = isPlainObject(parsed.labels) ? parsed.labels : null;
    parsed.extras = isPlainObject(parsed.extras) ? parsed.extras : null;

    parsed.defaultStatus = parseIntSafe(parsed.defaultStatus, 1);
    parsed.defaultIsAdmin = parseIntSafe(parsed.defaultIsAdmin, 0);

    parsed.isAdminRatio = typeof parsed.isAdminRatio === 'number' ? clamp(parsed.isAdminRatio, 0, 1) : 0;

    parsed.sleepBaseline = parseFloatSafe(parsed.sleepBaseline, 0.1);
    parsed.sleepParallel = parseFloatSafe(parsed.sleepParallel, 0.05);
    parsed.sleepDuplicate = parseFloatSafe(parsed.sleepDuplicate, 0.08);
    parsed.sleepValidation = parseFloatSafe(parsed.sleepValidation, 0.1);

    parsed.maxUsernameLength = parseIntSafe(parsed.maxUsernameLength, 63);
    if (parsed.maxUsernameLength < 8) {
        parsed.maxUsernameLength = 8;
    }

    parsed.requestTimeout = typeof parsed.requestTimeout === 'string' && parsed.requestTimeout.trim() !== '' ? parsed.requestTimeout.trim() : '60s';

    parsed.duplicateUser = normalizeDuplicate(parsed, parsed.duplicateUser);

    return parsed;
}

function normalizeDuplicate(dataset, duplicateRaw) {
    const duplicate = isPlainObject(duplicateRaw) ? Object.assign({}, duplicateRaw) : {};
    duplicate.username = duplicate.username ? sanitizeNameCandidate(String(duplicate.username), dataset.maxUsernameLength) : `${dataset.usernamePrefix}-duplicate`;
    duplicate.password = typeof duplicate.password === 'string' && duplicate.password.trim() !== '' ? duplicate.password.trim() : dataset.basePassword;
    duplicate.email = typeof duplicate.email === 'string' && duplicate.email.trim() !== '' ? duplicate.email.trim() : buildEmailForUsername(dataset, duplicate.username);
    if (!duplicate.phone && dataset.phonePrefix) {
        duplicate.phone = dataset.phonePrefix + padZeros(dataset.phoneSuffixLength);
    }
    if (duplicate.status !== undefined) {
        duplicate.status = parseIntSafe(duplicate.status, dataset.defaultStatus);
    }
    if (duplicate.isAdmin !== undefined) {
        duplicate.isAdmin = parseIntSafe(duplicate.isAdmin, dataset.defaultIsAdmin);
    }
    return duplicate;
}

function createOrEnsureDuplicate(context) {
    const duplicate = context.dataset.duplicateUser || normalizeDuplicate(context.dataset, null);
    const payload = createDuplicatePayload(context.dataset, duplicate);
    const res = sendCreateRequest(context, payload, { scenario: 'setup_duplicate', username: duplicate.username });
    const parsed = parseResponse(res);
    if (parsed.code === CODE_KAFKA_DEGRADED) {
        fail(`预创建重复用户触发降级: ${parsed.message}`);
    }
    if (res.status !== HTTP_CREATED && res.status !== HTTP_CONFLICT) {
        fail(`预创建重复用户失败: status=${res.status} code=${parsed.code} message=${parsed.message}`);
    }
    context.dataset.duplicateUser = duplicate;
    return duplicate;
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
    const data = body && typeof body === 'object' ? body.data : null;
    const token = data ? data.access_token || data.accessToken || data.token : null;
    if (!token) {
        fail('登录响应中缺少 access_token');
    }
    return String(token).trim();
}

function requireEnv(name) {
    const value = (__ENV[name] || '').trim();
    if (!value) {
        fail(`环境变量 ${name} 未设置`);
    }
    return value;
}

function sanitizeBaseUrl(raw) {
    const trimmed = raw.replace(/\s+/g, '');
    if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://')) {
        fail('BASE_URL 必须包含 http:// 或 https:// 前缀');
    }
    return trimmed.replace(/\/+$/, '');
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

function buildScenarioId(label) {
    const normalized = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, '_')
        .replace(/_+/g, '_')
        .slice(0, 48);
    return `k6-${normalized || DEFAULT_TAG_VALUE}`;
}

function uniqueSuffix() {
    const timestamp = Date.now().toString(36);
    const vu = typeof __VU === 'number' ? __VU.toString(36) : '0';
    const iter = typeof __ITER === 'number' ? __ITER.toString(36) : '0';
    const randomPart = Math.floor(Math.random() * 60466176).toString(36);
    return `${timestamp}${vu}${iter}${randomPart}`;
}

function randomDigits(length) {
    let out = '';
    for (let i = 0; i < length; i += 1) {
        out += Math.floor(Math.random() * 10);
    }
    return out;
}

function padZeros(length) {
    let out = '';
    for (let i = 0; i < length; i += 1) {
        out += '0';
    }
    return out;
}

function randomPick(items) {
    if (!items || items.length === 0) {
        return null;
    }
    const idx = Math.floor(Math.random() * items.length);
    return items[idx];
}

function sanitizeNameCandidate(name, maxLength) {
    let value = String(name).toLowerCase();
    value = value.replace(/[^a-z0-9-]/g, '-');
    value = value.replace(/-+/g, '-');
    value = value.replace(/^-+/, '').replace(/-+$/, '');
    if (!value) {
        value = 'user';
    }
    if (value.length > maxLength) {
        value = value.slice(0, maxLength);
    }
    return value;
}

function enforceString(obj, field) {
    if (typeof obj[field] !== 'string' || obj[field].trim() === '') {
        fail(`CREATE_DATASET.${field} 必须为非空字符串`);
    }
    obj[field] = obj[field].trim();
}

function isPlainObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value);
}

function parseIntSafe(value, fallback) {
    const num = Number.isFinite(Number(value)) ? parseInt(Number(value), 10) : NaN;
    if (Number.isNaN(num)) {
        return fallback;
    }
    return num;
}

function parseFloatSafe(value, fallback) {
    const num = Number(value);
    if (!Number.isFinite(num) || num < 0) {
        return fallback;
    }
    return num;
}

function clamp(value, min, max) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
        return min;
    }
    if (v < min) {
        return min;
    }
    if (v > max) {
        return max;
    }
    return v;
}

function cloneObject(value) {
    return JSON.parse(JSON.stringify(value));
}

function mergeTags(base, extra) {
    const merged = {};
    const apply = source => {
        if (!source) {
            return;
        }
        const keys = Object.keys(source);
        for (let i = 0; i < keys.length; i += 1) {
            const key = keys[i];
            merged[key] = source[key];
        }
    };
    apply(base);
    apply(extra);
    return sanitizeMetricTags(merged);
}

function isDegraded(parsed) {
    return parsed && parsed.code === CODE_KAFKA_DEGRADED;
}

function sanitizeMetricTags(tags) {
    const sanitized = {};
    const keys = Object.keys(tags || {});
    for (let i = 0; i < keys.length; i += 1) {
        const key = keys[i];
        const value = tags[key];
        if (value === null || value === undefined || value === '') {
            continue;
        }
        const normalizedKey = String(key).toLowerCase();
        if (HIGH_CARDINALITY_TAG_KEYS.has(normalizedKey)) {
            sanitized[key] = DEFAULT_TAG_VALUE;
            continue;
        }
        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (!trimmed) {
                continue;
            }
            sanitized[key] = trimmed.length > 80 ? trimmed.slice(0, 80) : trimmed;
            continue;
        }
        if (typeof value === 'number' || typeof value === 'boolean') {
            sanitized[key] = value;
            continue;
        }
        sanitized[key] = DEFAULT_TAG_VALUE;
    }
    if (!Object.prototype.hasOwnProperty.call(sanitized, 'group')) {
        sanitized.group = DEFAULT_TAG_VALUE;
    }
    return sanitized;
}
