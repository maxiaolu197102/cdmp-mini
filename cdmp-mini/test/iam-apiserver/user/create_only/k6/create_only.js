import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { randomSeed } from 'k6';

randomSeed(Date.now());

const CODE_SUCCESS = 100001;
const CODE_DUPLICATE = 110002;
const CODE_RESOURCE_CONFLICT = 110006;
const CODE_KAFKA_DEGRADED = 100401;

const HTTP_OK = 200;
const HTTP_CREATED = 201;
const HTTP_CONFLICT = 409;
const HTTP_UNAUTHORIZED = 401;
const HTTP_NOT_FOUND = 404;

const createLatency = new Trend('create_only_request_latency', true);
const visibilityLatency = new Trend('create_only_visibility_latency', true);
const pendingRetryTrend = new Trend('create_only_pending_retries', true);

const successRate = new Rate('create_only_success_rate');
const degradedRate = new Rate('create_only_degraded_rate');
const pendingTimeoutRate = new Rate('create_only_pending_timeout_rate');
const duplicateConflictRate = new Rate('create_only_duplicate_conflict_rate');

const HIGH_CARDINALITY_TAG_KEYS = new Set(['username', 'request_id', 'x_request_id', 'x-request-id']);
const DEFAULT_TAG_VALUE = '_default';
const DEFAULT_ADMIN_USERNAME = 'admin';
const DEFAULT_ADMIN_PASSWORD = 'Admin@2021';

const scenarioStateRegistry = {};
const scenarioFilters = createScenarioFilters();

const scenarioDefinitions = [
    {
        name: 'create_only_baseline',
        prefix: 'CREATE_ONLY_BASELINE',
        config: {
            exec: 'scenarioCreateBaseline',
            rate: 40,
            duration: '30m',
            preAllocatedVUs: 6,
            maxVUs: 24,
        },
    },
    {
        name: 'create_only_parallel',
        prefix: 'CREATE_ONLY_PARALLEL',
        config: {
            exec: 'scenarioCreateParallel',
            rate: 140,
            duration: '45m',
            preAllocatedVUs: 36,
            maxVUs: 160,
        },
    },
    {
        name: 'create_only_pending_probe',
        prefix: 'CREATE_ONLY_PENDING',
        config: {
            exec: 'scenarioPendingProbe',
            rate: 24,
            duration: '20m',
            preAllocatedVUs: 4,
            maxVUs: 20,
        },
    },
];

const GLOBAL_SCENARIO_DURATION = resolveGlobalScenarioDuration();
if (GLOBAL_SCENARIO_DURATION) {
    console.log(`[config] 使用全局场景时长覆盖: ${GLOBAL_SCENARIO_DURATION}`);
}

const RATE_MULTIPLIER = resolveMultiplier(['CREATE_ONLY_RATE_MULTIPLIER', 'K6_RATE_MULTIPLIER'], 1);
const VUS_MULTIPLIER = resolveMultiplier(['CREATE_ONLY_VUS_MULTIPLIER', 'K6_VUS_MULTIPLIER'], 1);
const MAX_VUS_MULTIPLIER = resolveMultiplier(['CREATE_ONLY_MAX_VUS_MULTIPLIER', 'K6_MAX_VUS_MULTIPLIER'], VUS_MULTIPLIER);
if (RATE_MULTIPLIER !== 1) {
    console.log(`[config] 场景请求速率整体放大倍数: x${RATE_MULTIPLIER}`);
}
if (VUS_MULTIPLIER !== 1) {
    console.log(`[config] 预分配 VU 整体放大倍数: x${VUS_MULTIPLIER}`);
}
if (MAX_VUS_MULTIPLIER !== 1) {
    console.log(`[config] 最大 VU 整体放大倍数: x${MAX_VUS_MULTIPLIER}`);
}

export const options = {
    setupTimeout: '240s',
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(99)<2000'],
        create_only_request_latency: ['p(95)<450'],
        create_only_visibility_latency: ['p(95)<2500'],
        create_only_pending_retries: ['p(95)<6'],
        create_only_success_rate: ['rate>0.96'],
        create_only_degraded_rate: ['rate<0.02'],
        create_only_pending_timeout_rate: ['rate<0.01'],
        create_only_duplicate_conflict_rate: ['rate<0.02'],
    },
    scenarios: buildScenarioOptions(scenarioDefinitions),
};

export function setup() {
    const baseUrl = sanitizeBaseUrl(requireEnv('BASE_URL'));
    const dataset = parseDataset(requireEnv('CREATE_ONLY_DATASET'));
    const token = obtainToken(baseUrl);
    return { baseUrl, dataset, token };
}

export function scenarioCreateBaseline(cfg) {
    const context = ensureSetup(cfg);
    runCreateScenario({
        context,
        scenarioName: 'create_only_baseline',
        baseTags: { scenario: 'create_only_baseline' },
        variantSelector: baselineVariant,
        waitForVisibility: context.dataset.waitForVisibilityBaseline,
        sleepSeconds: context.dataset.sleepBaseline,
    });
}

export function scenarioCreateParallel(cfg) {
    const context = ensureSetup(cfg);
    const variants = parallelVariants(context.dataset);
    runCreateScenario({
        context,
        scenarioName: 'create_only_parallel',
        baseTags: { scenario: 'create_only_parallel' },
        variantSelector: state => {
            const pick = randomPick(variants);
            state.parallelCounter = (state.parallelCounter || 0) + 1;
            return pick;
        },
        waitForVisibility: context.dataset.waitForVisibilityParallel,
        sleepSeconds: context.dataset.sleepParallel,
    });
}

export function scenarioPendingProbe(cfg) {
    const context = ensureSetup(cfg);
    runCreateScenario({
        context,
        scenarioName: 'create_only_pending_probe',
        baseTags: { scenario: 'create_only_pending_probe' },
        variantSelector: baselineVariant,
        waitForVisibility: true,
        sleepSeconds: context.dataset.sleepPending,
        logSlowVisibility: true,
    });
}

function runCreateScenario({ context, scenarioName, baseTags, variantSelector, waitForVisibility, sleepSeconds, logSlowVisibility }) {
    const state = ensureVuState(scenarioName);
    const dataset = context.dataset;
    const username = buildUsername(dataset, scenarioName, state);
    const payload = buildCreatePayload(dataset, username, state);
    const variant = typeof variantSelector === 'function' ? variantSelector(state, dataset, username) : null;
    if (variant && typeof variant.mutate === 'function') {
        variant.mutate(payload, dataset, state, username);
    }

    const requestTags = {
        scenario: baseTags && baseTags.scenario ? baseTags.scenario : scenarioName,
        variant: variant && variant.label ? variant.label : 'baseline',
        username,
    };

    const res = sendCreateUser(context, payload, requestTags);
    createLatency.add(res.timings.duration);

    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    degradedRate.add(degraded);

    let success = false;
    let duplicate = false;
    let visibility = { visible: false, elapsedMs: 0, retries: 0 };

    if (res.status === HTTP_CREATED && parsed.code === CODE_SUCCESS) {
        success = true;
        if (waitForVisibility !== false) {
            visibility = observeUserVisibility(context, username, dataset, {
                scenario: requestTags.scenario,
                variant: requestTags.variant,
                phase: 'visibility_probe',
            });
            if (!visibility.visible) {
                success = false;
                pendingTimeoutRate.add(1);
                logPendingTimeout(scenarioName, username, visibility);
            } else {
                pendingTimeoutRate.add(0);
                if (logSlowVisibility && dataset.pendingLogThresholdMs > 0 && visibility.elapsedMs > dataset.pendingLogThresholdMs) {
                    logSlowVisibilityEvent(scenarioName, username, visibility);
                }
            }
        } else {
            pendingTimeoutRate.add(0);
        }
    } else if (res.status === HTTP_CONFLICT && (parsed.code === CODE_DUPLICATE || parsed.code === CODE_RESOURCE_CONFLICT)) {
        duplicate = true;
        pendingTimeoutRate.add(0);
    } else {
        pendingTimeoutRate.add(0);
    }

    duplicateConflictRate.add(duplicate);
    successRate.add(success);
    visibilityLatency.add(visibility.elapsedMs);
    pendingRetryTrend.add(visibility.retries);

    recordChecks(res, {
        create_http_201: r => r.status === HTTP_CREATED,
        create_code_success: () => parsed.code === CODE_SUCCESS,
    });

    state.seedCounter = (state.seedCounter || 0) + 1;

    if (sleepSeconds && sleepSeconds > 0) {
        sleep(sleepSeconds);
    }
}

function baselineVariant() {
    return {
        label: 'baseline',
        mutate: () => { },
    };
}

function parallelVariants(dataset) {
    return [
        {
            label: 'variant_full_profile',
            mutate: () => { },
        },
        {
            label: 'variant_disabled_user',
            mutate: (payload, ds, _state, username) => {
                payload.status = 0;
                payload.nickname = `disabled_${username.slice(0, 10)}`;
            },
        },
        {
            label: 'variant_admin_user',
            mutate: payload => {
                payload.isAdmin = 1;
                payload.status = 1;
            },
        },
        {
            label: 'variant_extended_metadata',
            mutate: (payload, ds) => {
                const extendMix = ds.extendTemplates && ds.extendTemplates.length > 0 ? cloneObject(randomPick(ds.extendTemplates)) : {};
                extendMix.trace = uniqueSuffix().slice(-6);
                payload.metadata.extend = extendMix;
                payload.extras = Object.assign({ source: 'k6_create_only' }, payload.extras || {});
            },
        },
    ];
}

function observeUserVisibility(context, username, dataset, extraTags) {
    const timeoutMs = Number.isFinite(dataset.userReadyTimeoutMs) && dataset.userReadyTimeoutMs > 0 ? dataset.userReadyTimeoutMs : 20000;
    const tags = mergeTags({ endpoint: 'get_user_visibility', username }, extraTags);
    const pollInterval = Number.isFinite(dataset.visibilityPollIntervalMs) && dataset.visibilityPollIntervalMs > 0 ? dataset.visibilityPollIntervalMs : 200;
    const start = Date.now();
    let attempts = 0;

    while (Date.now() - start < timeoutMs) {
        const res = sendWithAuthRetry(context, tags, params => {
            const headers = Object.assign({}, params.headers, { 'X-Consistency-Mode': 'strong' });
            const reqParams = Object.assign({}, params, {
                headers,
                responseCallback: http.expectedStatuses(HTTP_OK, HTTP_NOT_FOUND),
            });
            return http.get(`${context.baseUrl}/v1/users/${encodeURIComponent(username)}?consistency=strong`, reqParams);
        });
        attempts += 1;
        if (res.status === HTTP_OK) {
            const elapsed = Date.now() - start;
            return { visible: true, elapsedMs: elapsed, retries: Math.max(0, attempts - 1) };
        }
        const retryDelay = computePendingRetryDelayMs(dataset, attempts - 1);
        const delayMs = retryDelay > 0 ? retryDelay : pollInterval;
        sleep(delayMs / 1000);
    }

    const fallbackVisible = attemptLoginProbe(context, username, dataset, extraTags);
    const elapsedMs = Date.now() - start;
    if (fallbackVisible) {
        return { visible: true, elapsedMs, retries: attempts };
    }
    return { visible: false, elapsedMs, retries: attempts };
}

function attemptLoginProbe(context, username, dataset, extraTags) {
    if (dataset.loginFallbackOnVisibility === false) {
        return false;
    }
    const timeoutMs = Number.isFinite(dataset.loginFallbackTimeoutMs) && dataset.loginFallbackTimeoutMs > 0 ? dataset.loginFallbackTimeoutMs : 4000;
    const tags = mergeTags({ endpoint: 'login_visibility_probe', username }, extraTags);
    const deadline = Date.now() + timeoutMs;
    const payload = JSON.stringify({ username, password: dataset.basePassword });
    while (Date.now() < deadline) {
        const res = http.post(`${context.baseUrl}/login`, payload, {
            headers: { 'Content-Type': 'application/json' },
            tags,
        });
        if (res.status === HTTP_OK) {
            return true;
        }
        sleep(0.3);
    }
    return false;
}

function logPendingTimeout(scenarioName, username, visibility) {
    console.warn('[pending-timeout] scenario=%s user=%s waited_ms=%d retries=%d', scenarioName, username, visibility.elapsedMs, visibility.retries);
}

function logSlowVisibilityEvent(scenarioName, username, visibility) {
    console.log('[pending-slow] scenario=%s user=%s waited_ms=%d retries=%d', scenarioName, username, visibility.elapsedMs, visibility.retries);
}

function sendCreateUser(context, payload, extraTags) {
    const tags = mergeTags({ endpoint: 'create_user' }, extraTags);
    return sendWithAuthRetry(context, tags, params =>
        http.post(`${context.baseUrl}/v1/users`, JSON.stringify(payload), params)
    );
}

function buildCreatePayload(dataset, username, state) {
    const index = state.seedCounter || 0;
    const payload = {
        metadata: { name: username },
        password: dataset.basePassword,
        email: `${username}@${dataset.emailDomain}`,
        status: 1,
        isAdmin: 0,
    };
    if (dataset.seedNicknamePrefix) {
        payload.nickname = `${dataset.seedNicknamePrefix}-${index}`.slice(0, 32);
    }
    if (dataset.seedExtras) {
        payload.extras = cloneObject(dataset.seedExtras);
    } else if (dataset.extras) {
        payload.extras = cloneObject(dataset.extras);
    }
    if (dataset.seedLabels) {
        payload.labels = cloneObject(dataset.seedLabels);
    } else if (dataset.labels) {
        payload.labels = cloneObject(dataset.labels);
    }
    if (dataset.seedExtendTemplate) {
        payload.metadata.extend = cloneObject(dataset.seedExtendTemplate);
    }
    return payload;
}

function ensureVuState(scenarioName) {
    const key = `${scenarioName || 'default'}#${__VU || 0}`;
    if (!scenarioStateRegistry[key]) {
        scenarioStateRegistry[key] = {
            usernameCounter: 0,
            seedCounter: 0,
        };
    }
    return scenarioStateRegistry[key];
}

function buildUsername(dataset, scenarioName, state) {
    const counter = (state.usernameCounter || 0) + 1;
    state.usernameCounter = counter;
    const maxLength = Number.isFinite(dataset.maxUsernameLength) ? dataset.maxUsernameLength : 45;

    const prefixBase = normalizeSegment(`${dataset.usernamePrefix}-${scenarioName}`);
    const vuPart = normalizeSegment(`vu${__VU || 0}`);
    const counterPart = normalizeSegment(`c${counter}`);
    let uniquePart = normalizeSegment(uniqueSuffix());
    if (uniquePart.length > 12) {
        uniquePart = uniquePart.slice(-12);
    }

    const suffixParts = [vuPart, counterPart, uniquePart].filter(Boolean);
    let suffix = suffixParts.join('-');
    if (suffix.length > Math.max(12, Math.floor(maxLength / 2))) {
        suffix = suffix.slice(-Math.max(12, Math.floor(maxLength / 2)));
    }
    suffix = normalizeSegment(suffix);
    if (!suffix) {
        suffix = normalizeSegment(`${counter}`);
    }

    const suffixBudget = Math.max(6, Math.min(maxLength - 4, suffix.length));
    if (suffix) {
        suffix = suffix.slice(-suffixBudget);
    }

    let prefixBudget = maxLength - suffix.length - (suffix ? 1 : 0);
    if (prefixBudget < 0) {
        prefixBudget = 0;
    }
    let prefix = prefixBase;
    if (prefixBudget > 0 && prefix.length > prefixBudget) {
        prefix = prefix.slice(0, prefixBudget).replace(/-+$/, '');
    }
    if (!prefix) {
        prefix = normalizeSegment(dataset.usernamePrefix).slice(0, prefixBudget).replace(/-+$/, '');
    }

    let candidate = prefix;
    if (suffix) {
        candidate = candidate ? `${candidate}-${suffix}` : suffix;
    }
    candidate = normalizeSegment(candidate);
    if (!candidate) {
        candidate = suffix ? suffix.slice(-Math.min(maxLength, suffix.length)) : 'user';
    }
    if (candidate.length > maxLength) {
        candidate = candidate.slice(0, maxLength).replace(/-+$/, '');
    }
    if (!candidate) {
        candidate = 'user';
    }
    return candidate;
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

function isDegraded(parsed) {
    return parsed && parsed.code === CODE_KAFKA_DEGRADED;
}

function computePendingRetryDelayMs(dataset, retryIndex) {
    const base = Number(dataset.pendingRetryInitialSleepMs) || 0;
    const max = Number(dataset.pendingRetryMaxSleepMs) || 0;
    const multiplierRaw = Number(dataset.pendingRetryBackoffMultiplier);
    const multiplier = Number.isFinite(multiplierRaw) && multiplierRaw >= 1 ? multiplierRaw : 1;
    const index = Math.max(0, retryIndex);
    if (base <= 0) {
        return 0;
    }
    let delay = base * Math.pow(multiplier, index);
    if (max > 0 && delay > max) {
        delay = max;
    }
    if (!Number.isFinite(delay) || delay < 0) {
        return 0;
    }
    return delay;
}

function recordChecks(res, expectations) {
    const entries = Object.entries(expectations || {});
    const checks = {};
    for (let i = 0; i < entries.length; i += 1) {
        const [label, fn] = entries[i];
        if (typeof fn === 'function') {
            checks[label] = () => fn(res);
        }
    }
    if (Object.keys(checks).length > 0) {
        check(res, checks);
    }
}

function sendWithAuthRetry(context, tags, executor) {
    const sanitizedTags = tags || {};
    const attempt = () => {
        const params = buildRequestParams(context, sanitizedTags);
        return executor(params);
    };
    let res = attempt();
    if (shouldRefreshAdminToken(res) && refreshAdminToken(context)) {
        res = attempt();
    }
    return res;
}

function buildRequestParams(context, tags) {
    const headers = buildHeaders(context.token, tags);
    const params = { headers, tags };
    if (context.dataset.requestTimeout) {
        params.timeout = context.dataset.requestTimeout;
    }
    return params;
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

function shouldRefreshAdminToken(res) {
    if (!res || res.status !== HTTP_UNAUTHORIZED) {
        return false;
    }
    const rawBody = typeof res.body === 'string' ? res.body : '';
    if (rawBody) {
        const lowered = rawBody.toLowerCase();
        if (lowered.indexOf('token is expired') !== -1) {
            return true;
        }
        try {
            const parsed = JSON.parse(rawBody);
            const message = extractMessageField(parsed);
            return typeof message === 'string' && message.toLowerCase().indexOf('token is expired') !== -1;
        } catch (err) {
            // ignore parse error
        }
    }
    return false;
}

function extractMessageField(payload) {
    if (!payload || typeof payload !== 'object') {
        return '';
    }
    if (typeof payload.message === 'string') {
        return payload.message;
    }
    if (payload.error && typeof payload.error.message === 'string') {
        return payload.error.message;
    }
    if (payload.data && typeof payload.data.message === 'string') {
        return payload.data.message;
    }
    return '';
}

function refreshAdminToken(context) {
    try {
        const token = obtainToken(context.baseUrl, { forceLogin: true });
        if (token) {
            context.token = token;
            console.log(`[auth] refreshed admin token at ${new Date().toISOString()}`);
            return true;
        }
    } catch (err) {
        console.error(`[auth] failed to refresh admin token: ${err && err.message ? err.message : err}`);
    }
    return false;
}

function obtainToken(baseUrl, options = {}) {
    const forceLogin = Boolean(options.forceLogin);
    if (!forceLogin && __ENV.ADMIN_TOKEN && __ENV.ADMIN_TOKEN.trim() !== '') {
        return __ENV.ADMIN_TOKEN.trim();
    }
    let username = (__ENV.ADMIN_USERNAME || '').trim();
    let password = (__ENV.ADMIN_PASSWORD || '').trim();
    if ((!username || !password) && shouldUseDefaultAdminCredentials(baseUrl)) {
        username = DEFAULT_ADMIN_USERNAME;
        password = DEFAULT_ADMIN_PASSWORD;
        console.warn('[auth] ADMIN_USERNAME/ADMIN_PASSWORD 未配置，将尝试默认管理员凭据');
    }
    if (!username || !password) {
        if (forceLogin) {
            console.error('[auth] ADMIN_USERNAME 或 ADMIN_PASSWORD 未配置，无法刷新 Token');
            return null;
        }
        fail('请通过 ADMIN_TOKEN 或 ADMIN_USERNAME/ADMIN_PASSWORD 提供管理员凭据');
    }
    const payload = JSON.stringify({ username, password });
    const res = http.post(`${baseUrl}/login`, payload, { headers: { 'Content-Type': 'application/json' } });
    if (res.status !== HTTP_OK) {
        if (forceLogin) {
            console.error(`[auth] 管理员登录失败，状态码: ${res.status}`);
            return null;
        }
        fail(`管理员登录失败，状态码: ${res.status}`);
    }
    let body;
    try {
        body = res.json();
    } catch (err) {
        if (forceLogin) {
            console.error('[auth] 解析登录响应失败: ' + err.message);
            return null;
        }
        fail('解析登录响应失败: ' + err.message);
    }
    const data = body && typeof body === 'object' ? body.data : null;
    const token = data ? data.access_token || data.accessToken || data.token : null;
    if (!token) {
        if (forceLogin) {
            console.error('[auth] 登录响应缺少 access_token');
            return null;
        }
        fail('登录响应中缺少 access_token');
    }
    return String(token).trim();
}

function shouldUseDefaultAdminCredentials(baseUrl) {
    const flag = (__ENV.ALLOW_DEFAULT_ADMIN || '').trim().toLowerCase();
    if (flag === '0' || flag === 'false' || flag === 'no' || flag === 'off') {
        return false;
    }
    if (flag === '1' || flag === 'true' || flag === 'yes' || flag === 'on') {
        return true;
    }
    if (!baseUrl || typeof baseUrl !== 'string') {
        return true;
    }
    const normalized = baseUrl.toLowerCase();
    return normalized.indexOf('192.168.10.8') !== -1 || normalized.indexOf('localhost') !== -1 || normalized.indexOf('127.0.0.1') !== -1;
}

function buildScenarioId(label) {
    const normalized = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, '_')
        .replace(/_+/g, '_')
        .slice(0, 48);
    return `k6-${normalized || DEFAULT_TAG_VALUE}`;
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

function createScenarioFilters() {
    return {
        enabled: parseScenarioList(__ENV.ENABLED_SCENARIOS || __ENV.K6_ENABLED_SCENARIOS || ''),
        disabled: parseScenarioList(__ENV.DISABLED_SCENARIOS || __ENV.K6_DISABLED_SCENARIOS || ''),
    };
}

function parseScenarioList(raw) {
    if (!raw || typeof raw !== 'string') {
        return [];
    }
    return raw
        .split(/[\s,;]+/)
        .map(item => item.trim().toLowerCase())
        .filter(Boolean);
}

function buildScenarioOptions(definitions) {
    const result = {};
    for (let i = 0; i < definitions.length; i += 1) {
        const definition = definitions[i];
        if (!definition || !shouldEnableScenario(definition)) {
            continue;
        }
        const defaults = Object.assign({}, definition.config);
        result[definition.name] = scenarioConfig(definition.prefix, defaults);
    }
    if (Object.keys(result).length === 0) {
        fail('没有启用任何场景，请检查 ENABLED_SCENARIOS 或 DISABLED_SCENARIOS 设置');
    }
    return result;
}

function shouldEnableScenario(definition) {
    if (!definition || !definition.config) {
        return false;
    }
    if (scenarioFilters.enabled.length > 0) {
        return matchesScenarioSelector(definition, scenarioFilters.enabled);
    }
    if (scenarioFilters.disabled.length > 0) {
        return !matchesScenarioSelector(definition, scenarioFilters.disabled);
    }
    return true;
}

function matchesScenarioSelector(definition, selectors) {
    for (let i = 0; i < selectors.length; i += 1) {
        if (scenarioMatches(definition, selectors[i])) {
            return true;
        }
    }
    return false;
}

function scenarioMatches(definition, selector) {
    if (!selector) {
        return false;
    }
    const candidate = selector.toLowerCase();
    if (definition.name && definition.name.toLowerCase() === candidate) {
        return true;
    }
    if (definition.prefix && definition.prefix.toLowerCase() === candidate) {
        return true;
    }
    if (definition.config.exec && definition.config.exec.toLowerCase() === candidate) {
        return true;
    }
    return false;
}

function scenarioConfig(prefix, defaults) {
    const rate = scalePositiveNumber(parsePositiveIntEnv(`${prefix}_RATE`, defaults.rate), RATE_MULTIPLIER, 1);
    const perScenarioOverride = (__ENV[`${prefix}_DURATION`] || '').trim();
    const durationOverride = perScenarioOverride || GLOBAL_SCENARIO_DURATION;
    const duration = (durationOverride || defaults.duration).trim();
    const preAllocatedVUs = scalePositiveNumber(parsePositiveIntEnv(`${prefix}_VUS`, defaults.preAllocatedVUs), VUS_MULTIPLIER, 1);
    const maxVUs = scalePositiveNumber(parsePositiveIntEnv(`${prefix}_MAX_VUS`, defaults.maxVUs), MAX_VUS_MULTIPLIER, preAllocatedVUs);
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

function resolveGlobalScenarioDuration() {
    const candidates = [
        (__ENV.K6_DURATION_OVERRIDE || '').trim(),
        (__ENV.K6_SCENARIO_DURATION || '').trim(),
        (__ENV.K6_DURATION || '').trim(),
    ];
    for (let i = 0; i < candidates.length; i += 1) {
        if (candidates[i]) {
            return candidates[i];
        }
    }
    return '';
}

function resolveMultiplier(keys, fallback) {
    const candidates = Array.isArray(keys) ? keys : [keys];
    for (let i = 0; i < candidates.length; i += 1) {
        const key = candidates[i];
        if (!key) {
            continue;
        }
        const raw = (__ENV[key] || '').trim();
        if (!raw) {
            continue;
        }
        const value = Number(raw);
        if (!Number.isFinite(value) || value <= 0) {
            fail(`${key} 必须为正数`);
        }
        return value;
    }
    return fallback;
}

function scalePositiveNumber(value, multiplier, minimum) {
    if (!Number.isFinite(value) || value <= 0) {
        return value;
    }
    if (!Number.isFinite(multiplier) || multiplier <= 0 || multiplier === 1) {
        return Math.max(value, minimum || 1);
    }
    const scaled = Math.ceil(value * multiplier);
    const floor = Number.isFinite(minimum) && minimum > 0 ? minimum : 1;
    return Math.max(scaled, floor);
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

function ensureSetup(cfg) {
    if (!cfg || !cfg.baseUrl || !cfg.token || !cfg.dataset) {
        fail('setup 数据异常或缺失');
    }
    return cfg;
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

function parseDataset(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (err) {
        fail(`CREATE_ONLY_DATASET 解析失败: ${err.message}`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        fail('CREATE_ONLY_DATASET 必须是 JSON 对象');
    }
    enforceString(parsed, 'usernamePrefix');
    enforceString(parsed, 'basePassword');
    enforceString(parsed, 'emailDomain');

    parsed.usernamePrefix = sanitizeNameCandidate(parsed.usernamePrefix, 32);
    parsed.basePassword = parsed.basePassword.trim();
    parsed.emailDomain = parsed.emailDomain.trim().toLowerCase();

    parsed.seedNicknamePrefix = typeof parsed.seedNicknamePrefix === 'string'
        ? sanitizeNameCandidate(parsed.seedNicknamePrefix, 24)
        : 'create';

    parsed.sleepBetween = parseFloatSafe(parsed.sleepBetween, 0.05);
    parsed.sleepBaseline = parseFloatSafe(parsed.sleepBaseline, parsed.sleepBetween);
    parsed.sleepParallel = parseFloatSafe(parsed.sleepParallel, Math.max(parsed.sleepBetween, 0.02));
    parsed.sleepPending = parseFloatSafe(parsed.sleepPending, Math.max(parsed.sleepBetween, 0.2));

    parsed.userReadyTimeoutMs = parseIntSafe(parsed.userReadyTimeoutMs, 20000);
    parsed.loginFallbackTimeoutMs = parseIntSafe(parsed.loginFallbackTimeoutMs, Math.min(parsed.userReadyTimeoutMs, 5000));
    parsed.requestTimeout = typeof parsed.requestTimeout === 'string' && parsed.requestTimeout.trim() !== '' ? parsed.requestTimeout.trim() : '60s';

    const maxLength = parseIntSafe(parsed.maxUsernameLength, 45);
    parsed.maxUsernameLength = clamp(Math.max(maxLength, 8), 8, 45);

    parsed.pendingRetryInitialSleepMs = parseIntSafe(parsed.pendingRetryInitialSleepMs, 200);
    parsed.pendingRetryBackoffMultiplier = parseFloatSafe(parsed.pendingRetryBackoffMultiplier, 1.8);
    parsed.pendingRetryMaxSleepMs = parseIntSafe(parsed.pendingRetryMaxSleepMs, 1200);
    parsed.visibilityPollIntervalMs = parseIntSafe(parsed.visibilityPollIntervalMs, 200);
    parsed.pendingLogThresholdMs = parseIntSafe(parsed.pendingLogThresholdMs, 800);
    parsed.loginFallbackOnVisibility = parsed.loginFallbackOnVisibility !== false;
    parsed.waitForVisibilityBaseline = parsed.waitForVisibilityBaseline !== false;
    if (parsed.waitForVisibilityParallel === true || parsed.waitForVisibilityParallel === false) {
        parsed.waitForVisibilityParallel = parsed.waitForVisibilityParallel;
    } else {
        parsed.waitForVisibilityParallel = parsed.waitForVisibilityBaseline;
    }

    parsed.seedExtras = isPlainObject(parsed.seedExtras) ? parsed.seedExtras : null;
    parsed.seedLabels = isPlainObject(parsed.seedLabels) ? parsed.seedLabels : null;
    parsed.seedExtendTemplate = isPlainObject(parsed.seedExtendTemplate) ? parsed.seedExtendTemplate : null;
    parsed.extendTemplates = Array.isArray(parsed.extendTemplates) ? parsed.extendTemplates.filter(isPlainObject) : [];
    parsed.labels = isPlainObject(parsed.labels) ? parsed.labels : null;
    parsed.extras = isPlainObject(parsed.extras) ? parsed.extras : null;

    return parsed;
}

function enforceString(obj, field) {
    if (typeof obj[field] !== 'string' || obj[field].trim() === '') {
        fail(`CREATE_ONLY_DATASET.${field} 必须为非空字符串`);
    }
    obj[field] = obj[field].trim();
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

function isPlainObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value);
}

function cloneObject(value) {
    return JSON.parse(JSON.stringify(value));
}

function normalizeSegment(value) {
    if (value === undefined || value === null) {
        return '';
    }
    let normalized = String(value).toLowerCase();
    normalized = normalized.replace(/[^a-z0-9-]/g, '-');
    normalized = normalized.replace(/-+/g, '-');
    normalized = normalized.replace(/^-+/, '').replace(/-+$/, '');
    return normalized;
}

function sanitizeNameCandidate(name, maxLength) {
    let value = normalizeSegment(name);
    if (!value) {
        value = 'user';
    }
    if (value.length > maxLength) {
        value = value.slice(0, maxLength);
        value = value.replace(/-+$/, '');
    }
    if (!value) {
        value = 'user';
    }
    return value;
}

function uniqueSuffix() {
    const timestamp = Date.now().toString(36);
    const vu = typeof __VU === 'number' ? __VU.toString(36) : '0';
    const iter = typeof __ITER === 'number' ? __ITER.toString(36) : '0';
    const randomPart = Math.floor(Math.random() * 60466176).toString(36);
    return `${timestamp}${vu}${iter}${randomPart}`;
}

function randomPick(items) {
    if (!items || items.length === 0) {
        return null;
    }
    const idx = Math.floor(Math.random() * items.length);
    return items[idx];
}
