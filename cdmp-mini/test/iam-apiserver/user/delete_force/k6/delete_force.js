import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const CODE_SUCCESS = 100001;
const CODE_INVALID_PARAMETER = 110004;
const CODE_USER_NOT_FOUND = 110001;
const CODE_KAFKA_DEGRADED = 100401;

const HTTP_OK = 200;
const HTTP_NO_CONTENT = 204;
const HTTP_BAD_REQUEST = 400;
const HTTP_UNAUTHORIZED = 401;
const HTTP_NOT_FOUND = 404;

const singleDeleteLatency = new Trend('delete_force_single_latency', true);
const batchDeleteLatency = new Trend('delete_force_batch_latency', true);

const singleDeleteSuccessRate = new Rate('delete_force_single_success_rate');
const batchDeleteSuccessRate = new Rate('delete_force_batch_success_rate');
const invalidPayloadRejectedRate = new Rate('delete_force_invalid_payload_rejected_rate');
const degradedRate = new Rate('delete_force_degraded_rate');

const HIGH_CARDINALITY_TAG_KEYS = new Set(['username', 'request_id', 'x_request_id', 'x-request-id']);
const DEFAULT_TAG_VALUE = '_default';
const DEFAULT_ADMIN_USERNAME = 'admin';
const DEFAULT_ADMIN_PASSWORD = 'Admin@2021';

const scenarioRegistry = {};
const scenarioFilters = createScenarioFilters();

const scenarioDefinitions = [
    {
        name: 'delete_force_single_baseline',
        prefix: 'FORCE_SINGLE_BASELINE',
        config: {
            exec: 'scenarioSingleBaseline',
            rate: 25,
            duration: '1h',
            preAllocatedVUs: 24,
            maxVUs: 64,
        },
    },
    {
        name: 'delete_force_single_parallel',
        prefix: 'FORCE_SINGLE_PARALLEL',
        config: {
            exec: 'scenarioSingleParallel',
            rate: 160,
            duration: '1h',
            preAllocatedVUs: 96,
            maxVUs: 128,
        },
    },
    {
        name: 'delete_force_batch_parallel',
        prefix: 'FORCE_BATCH_PARALLEL',
        config: {
            exec: 'scenarioBatchParallel',
            rate: 40,
            duration: '1h',
            preAllocatedVUs: 48,
            maxVUs: 80,
        },
    },
    {
        name: 'delete_force_single_invalid_payload',
        prefix: 'FORCE_SINGLE_INVALID',
        config: {
            exec: 'scenarioSingleInvalidPayload',
            rate: 20,
            duration: '1h',
            preAllocatedVUs: 8,
            maxVUs: 24,
        },
    },
];

const GLOBAL_SCENARIO_DURATION = resolveGlobalScenarioDuration();
if (GLOBAL_SCENARIO_DURATION) {
    console.log(`[config] 使用全局场景时长覆盖: ${GLOBAL_SCENARIO_DURATION}`);
}

const RATE_MULTIPLIER = resolveMultiplier(['DELETE_FORCE_RATE_MULTIPLIER', 'K6_RATE_MULTIPLIER'], 1);
const VUS_MULTIPLIER = resolveMultiplier(['DELETE_FORCE_VUS_MULTIPLIER', 'K6_VUS_MULTIPLIER'], 1);
const MAX_VUS_MULTIPLIER = resolveMultiplier(['DELETE_FORCE_MAX_VUS_MULTIPLIER', 'K6_MAX_VUS_MULTIPLIER'], VUS_MULTIPLIER);
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
    setupTimeout: '300s',
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(99)<2000'],
        delete_force_single_latency: ['p(95)<500'],
        delete_force_batch_latency: ['p(95)<700'],
        delete_force_single_success_rate: ['rate>0.97'],
        delete_force_batch_success_rate: ['rate>0.95'],
        delete_force_invalid_payload_rejected_rate: ['rate>0.95'],
        delete_force_degraded_rate: ['rate<0.02'],
    },
    scenarios: buildScenarioOptions(scenarioDefinitions),
};

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

export function setup() {
    const baseUrl = sanitizeBaseUrl(requireEnv('BASE_URL'));
    const dataset = parseDataset(requireEnv('DELETE_FORCE_DATASET'));
    const token = obtainToken(baseUrl);

    return {
        baseUrl,
        dataset,
        token,
        scenarioMeta: buildScenarioMeta(),
    };
}

export function scenarioSingleBaseline(cfg) {
    const context = ensureSetup(cfg);
    runSingleDeleteScenario({
        context,
        scenarioName: 'scenarioSingleBaseline',
        tags: { scenario: 'delete_single_baseline' },
    });
}

export function scenarioSingleParallel(cfg) {
    const context = ensureSetup(cfg);
    runSingleDeleteScenario({
        context,
        scenarioName: 'scenarioSingleParallel',
        tags: { scenario: 'delete_single_parallel' },
    });
}

export function scenarioBatchParallel(cfg) {
    const context = ensureSetup(cfg);
    runBatchDeleteScenario({
        context,
        scenarioName: 'scenarioBatchParallel',
        tags: { scenario: 'delete_batch_parallel' },
    });
}

export function scenarioSingleInvalidPayload(cfg) {
    const context = ensureSetup(cfg);
    runInvalidPayloadScenario({
        context,
        scenarioName: 'scenarioSingleInvalidPayload',
        tags: { scenario: 'delete_single_invalid_payload' },
    });
}

function runSingleDeleteScenario({ context, scenarioName, tags }) {
    const state = ensureVuState(scenarioName);
    const target = ensureSingleTarget(context, state, scenarioName);
    const maxAttempts = resolveDeleteMaxAttempts(context.dataset);
    let attempt = 0;
    let pendingRetries = 0;
    let res = null;
    let parsed = { code: null, message: '', data: null };
    let success = false;
    let degraded = false;

    while (attempt < maxAttempts) {
        attempt += 1;
        res = sendForceDelete(context, target.name, tags);
        singleDeleteLatency.add(res.timings.duration);
        parsed = parseResponse(res);
        degraded = degraded || isDegraded(parsed);
        success = isSingleDeleteSuccess(res, parsed);
        if (success) {
            break;
        }
        if (shouldRetryPendingDeletion(res, parsed, context.dataset, pendingRetries)) {
            const delayMs = computePendingRetryDelayMs(context.dataset, pendingRetries);
            pendingRetries += 1;
            if (delayMs > 0) {
                sleep(delayMs / 1000);
            }
            attempt = Math.max(0, attempt - 1);
            continue;
        }
        break;
    }

    singleDeleteSuccessRate.add(success);
    degradedRate.add(degraded);

    const finalRes = res || { status: 0, timings: { duration: 0 } };
    const finalParsed = parsed || { code: null, message: '', data: null };

    recordChecks(finalRes, {
        delete_http_success: r => r.status === HTTP_OK || r.status === HTTP_NO_CONTENT || r.status === HTTP_NOT_FOUND,
        delete_code_success: () => success || finalParsed.code === CODE_USER_NOT_FOUND,
    });

    if (success && context.dataset.verifyDeletion) {
        verifyUserGone(context, target.name, context.dataset.deleteVerifyTimeoutMs, tags);
    }

    if (success || context.dataset.respawnOnFailure) {
        state.singleTarget = null;
    }

    sleep(context.dataset.sleepBetween);
}

function runBatchDeleteScenario({ context, scenarioName, tags }) {
    const state = ensureVuState(scenarioName);
    const targets = ensureBatchTargets(context, state, scenarioName);
    const maxAttempts = resolveDeleteMaxAttempts(context.dataset);
    let attempt = 0;
    let pendingRetries = 0;
    let res = null;
    let parsed = { code: null, message: '', data: null };
    let success = false;
    let degraded = false;

    while (attempt < maxAttempts) {
        attempt += 1;
        res = sendBatchForceDelete(context, targets, tags);
        batchDeleteLatency.add(res.timings.duration);
        parsed = parseResponse(res);
        degraded = degraded || isDegraded(parsed);
        success = isBatchDeleteSuccess(res, parsed);
        if (success) {
            break;
        }
        if (shouldRetryPendingDeletion(res, parsed, context.dataset, pendingRetries)) {
            const delayMs = computePendingRetryDelayMs(context.dataset, pendingRetries);
            pendingRetries += 1;
            if (delayMs > 0) {
                sleep(delayMs / 1000);
            }
            attempt = Math.max(0, attempt - 1);
            continue;
        }
        break;
    }

    batchDeleteSuccessRate.add(success);
    degradedRate.add(degraded);

    const finalRes = res || { status: 0, timings: { duration: 0 } };
    const finalParsed = parsed || { code: null, message: '', data: null };

    recordChecks(finalRes, {
        batch_http_success: r => r.status === HTTP_OK,
        batch_code_success: () => success,
    });

    if (success && context.dataset.verifyDeletion) {
        for (let i = 0; i < targets.length; i += 1) {
            verifyUserGone(context, targets[i].name, context.dataset.deleteVerifyTimeoutMs, tags);
        }
    }

    if (success || context.dataset.respawnOnFailure) {
        state.batchTargets = [];
    }

    sleep(context.dataset.sleepBetween);
}

function runInvalidPayloadScenario({ context, scenarioName, tags }) {
    const state = ensureVuState(scenarioName);
    state.invalidCounter = (state.invalidCounter || 0) + 1;
    const candidate = buildInvalidUsername(state.invalidCounter);
    const res = sendForceDelete(context, candidate, tags);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const rejected = isInvalidPayloadRejected(res, parsed);

    invalidPayloadRejectedRate.add(rejected);
    degradedRate.add(degraded);

    recordChecks(res, {
        invalid_http_status: r => r.status === HTTP_BAD_REQUEST,
        invalid_code: () => parsed.code === CODE_INVALID_PARAMETER,
    });

    sleep(context.dataset.sleepBetween);
}

function ensureSingleTarget(context, state, scenarioName) {
    if (state.singleTarget && !state.singleTarget.needsRecreate) {
        return state.singleTarget;
    }
    const target = createDeleteTarget(context, scenarioName, state);
    state.singleTarget = target;
    return target;
}

function ensureBatchTargets(context, state, scenarioName) {
    if (Array.isArray(state.batchTargets) && state.batchTargets.length === context.dataset.batchSize) {
        return state.batchTargets;
    }
    const targets = createBatchTargets(context, scenarioName, state, context.dataset.batchSize);
    state.batchTargets = targets;
    return targets;
}

function createDeleteTarget(context, scenarioName, state) {
    const dataset = context.dataset;
    const username = buildUsername(dataset, scenarioName, state);
    const payload = buildSeedPayload(dataset, username, state.seedCounter || 0);
    const res = sendCreateUser(context, payload, { scenario: scenarioName, phase: 'seed_single' });
    if (res.status !== HTTP_OK && res.status !== 201) {
        fail(`创建待删除用户失败: http=${res.status} body=${res.body}`);
    }
    waitForUserVisibility(context, username, context.dataset.userReadyTimeoutMs, { scenario: scenarioName, phase: 'seed_single_wait' });
    state.seedCounter = (state.seedCounter || 0) + 1;
    return { name: username };
}

function createBatchTargets(context, scenarioName, state, batchSize) {
    const targets = [];
    for (let i = 0; i < batchSize; i += 1) {
        const counterKey = `batchSeedCounter_${scenarioName}`;
        const counter = state[counterKey] || 0;
        const username = buildUsername(context.dataset, `${scenarioName}-batch`, state, counter);
        const payload = buildSeedPayload(context.dataset, username, counter);
        const res = sendCreateUser(context, payload, { scenario: scenarioName, phase: 'seed_batch' });
        if (res.status !== HTTP_OK && res.status !== 201) {
            fail(`批量创建用户失败: http=${res.status} body=${res.body}`);
        }
        waitForUserVisibility(context, username, context.dataset.userReadyTimeoutMs, { scenario: scenarioName, phase: 'seed_batch_wait' });
        targets.push({ name: username });
        state[counterKey] = counter + 1;
    }
    return targets;
}

function sendForceDelete(context, username, extraTags) {
    const tags = mergeTags({ endpoint: 'delete_force_single', username }, extraTags);
    return sendWithAuthRetry(context, tags, params =>
        http.del(`${context.baseUrl}/v1/users/${encodeURIComponent(username)}/force`, null, params)
    );
}

function sendBatchForceDelete(context, targets, extraTags) {
    if (!Array.isArray(targets) || targets.length === 0) {
        fail('批量删除目标为空');
    }
    const query = targets.map(target => `names=${encodeURIComponent(target.name)}`).join('&');
    const tags = mergeTags({ endpoint: 'delete_force_batch', batch: targets.length }, extraTags);
    return sendWithAuthRetry(context, tags, params =>
        http.del(`${context.baseUrl}/v1/users?${query}`, null, params)
    );
}

function sendCreateUser(context, payload, extraTags) {
    const tags = mergeTags({ endpoint: 'create_user_seed' }, extraTags);
    return sendWithAuthRetry(context, tags, params =>
        http.post(`${context.baseUrl}/v1/users`, JSON.stringify(payload), params)
    );
}

function waitForUserVisibility(context, username, timeoutMs, extraTags) {
    const deadline = Date.now() + timeoutMs;
    const tags = mergeTags({ endpoint: 'get_user_visibility', username }, extraTags);
    while (Date.now() < deadline) {
        const res = sendWithAuthRetry(context, tags, params => {
            const headers = Object.assign({}, params.headers, { 'X-Consistency-Mode': 'strong' });
            const reqParams = Object.assign({}, params, {
                headers,
                responseCallback: http.expectedStatuses(HTTP_OK, HTTP_NOT_FOUND),
            });
            return http.get(`${context.baseUrl}/v1/users/${encodeURIComponent(username)}?consistency=strong`, reqParams);
        });
        if (res.status === HTTP_OK) {
            return;
        }
        sleep(0.2);
    }
    if (waitForUserByLogin(context, username, timeoutMs, extraTags)) {
        return;
    }
    fail(`等待用户 ${username} 可见超时`);
}

function waitForUserByLogin(context, username, timeoutMs, extraTags) {
    const dataset = context && context.dataset ? context.dataset : {};
    if (dataset.loginFallbackOnVisibility === false) {
        return false;
    }
    const deadline = Date.now() + timeoutMs;
    const tags = mergeTags({ endpoint: 'login_user_visibility', username }, extraTags);
    while (Date.now() < deadline) {
        const payload = JSON.stringify({ username, password: dataset.basePassword });
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

function verifyUserGone(context, username, timeoutMs, extraTags) {
    const deadline = Date.now() + timeoutMs;
    const tags = mergeTags({ endpoint: 'verify_user_gone', username }, extraTags);
    while (Date.now() < deadline) {
        const res = sendWithAuthRetry(context, tags, params => {
            const headers = Object.assign({}, params.headers, { 'X-Consistency-Mode': 'strong' });
            const reqParams = Object.assign({}, params, {
                headers,
                responseCallback: http.expectedStatuses(HTTP_OK, HTTP_NOT_FOUND),
            });
            return http.get(`${context.baseUrl}/v1/users/${encodeURIComponent(username)}?consistency=strong`, reqParams);
        });
        if (res.status === HTTP_NOT_FOUND) {
            return;
        }
        sleep(0.3);
    }
    fail(`用户 ${username} 删除后仍然存在`);
}

function buildSeedPayload(dataset, username, index) {
    const payload = {
        metadata: { name: username },
        password: dataset.basePassword,
        email: `${username}@${dataset.emailDomain}`,
        nickname: `${dataset.seedNicknamePrefix}-${index}`,
        status: 1,
        isAdmin: 0,
    };
    if (dataset.seedExtras) {
        payload.extras = dataset.seedExtras;
    }
    if (dataset.seedLabels) {
        payload.labels = dataset.seedLabels;
    }
    if (dataset.seedExtendTemplate) {
        payload.metadata.extend = dataset.seedExtendTemplate;
    }
    return payload;
}

function buildUsername(dataset, scenarioName, state, counterOverride) {
    const baseCounter = Number.isFinite(counterOverride) ? counterOverride : (state.usernameCounter || 0);
    const counter = baseCounter + 1;
    state.usernameCounter = counter;
    const maxLength = Number.isFinite(dataset.maxUsernameLength) ? dataset.maxUsernameLength : 63;

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

function buildInvalidUsername(counter) {
    const suffix = typeof counter === 'number' && counter > 0 ? `-${counter}` : '';
    return `invalid!user${suffix}`;
}

function isSingleDeleteSuccess(res, parsed) {
    if (!res) {
        return false;
    }
    if (res.status === HTTP_OK || res.status === HTTP_NO_CONTENT) {
        return parsed.code === CODE_SUCCESS || parsed.code === null;
    }
    if (res.status === HTTP_NOT_FOUND) {
        return parsed.code === CODE_USER_NOT_FOUND || parsed.code === CODE_SUCCESS;
    }
    return false;
}

function isBatchDeleteSuccess(res, parsed) {
    if (!res) {
        return false;
    }
    if (res.status !== HTTP_OK) {
        return false;
    }
    return parsed.code === CODE_SUCCESS;
}

function isInvalidPayloadRejected(res, parsed) {
    if (!res) {
        return false;
    }
    if (res.status !== HTTP_BAD_REQUEST) {
        return false;
    }
    return parsed.code === CODE_INVALID_PARAMETER;
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

function normalizeData(value) {
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch (err) {
            return { value };
        }
    }
    return value;
}

function isDegraded(parsed) {
    return parsed && parsed.code === CODE_KAFKA_DEGRADED;
}

function resolveDeleteMaxAttempts(dataset) {
    const raw = dataset && Number.isFinite(Number(dataset.deleteMaxAttempts))
        ? parseInt(dataset.deleteMaxAttempts, 10)
        : NaN;
    let attempts = Number.isNaN(raw) ? 2 : raw;
    if (!Number.isFinite(attempts) || attempts < 1) {
        attempts = 1;
    }
    if (attempts > 5) {
        attempts = 5;
    }
    return attempts;
}

function shouldRetryPendingDeletion(res, parsed, dataset, pendingRetries) {
    if (!res || res.status !== HTTP_NOT_FOUND) {
        return false;
    }
    if (!parsed || parsed.code !== CODE_USER_NOT_FOUND) {
        return false;
    }
    if (!messageIndicatesPending(parsed.message)) {
        return false;
    }
    const maxPending = resolvePendingRetryMaxAttempts(dataset);
    return pendingRetries < maxPending;
}

function messageIndicatesPending(message) {
    if (typeof message !== 'string' || message.trim() === '') {
        return false;
    }
    const normalized = message.trim().toLowerCase();
    if (normalized.includes('pending')) {
        return true;
    }
    if (normalized.includes('creating')) {
        return true;
    }
    if (normalized.includes('queue')) {
        return true;
    }
    return message.indexOf('正在创建') !== -1 || message.indexOf('排队') !== -1;
}

function resolvePendingRetryMaxAttempts(dataset) {
    if (!dataset || !Number.isFinite(Number(dataset.pendingRetryMaxAttempts))) {
        return 0;
    }
    const value = Math.max(0, parseInt(dataset.pendingRetryMaxAttempts, 10));
    return Number.isFinite(value) ? value : 0;
}

function computePendingRetryDelayMs(dataset, retryIndex) {
    if (!dataset) {
        return 0;
    }
    const base = Number(dataset.pendingRetryInitialSleepMs) || 0;
    const max = Math.max(Number(dataset.pendingRetryMaxSleepMs) || 0, base);
    const multiplierRaw = Number(dataset.pendingRetryBackoffMultiplier);
    const multiplier = Number.isFinite(multiplierRaw) && multiplierRaw >= 1 ? multiplierRaw : 1;
    const index = Math.max(0, retryIndex);
    if (base <= 0) {
        return 0;
    }
    let delay = base;
    if (index > 0) {
        delay = base * Math.pow(multiplier, index);
    }
    if (!Number.isFinite(delay) || delay > max) {
        delay = max;
    }
    if (delay < 0) {
        return 0;
    }
    return delay;
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

function mergeTags(base, extra) {
    const merged = {};
    const apply = source => {
        if (!source) {
            return;
        }
        const keys = Object.keys(source);
        for (let i = 0; i < keys.length; i += 1) {
            merged[keys[i]] = source[keys[i]];
        }
    };
    apply(base);
    apply(extra);
    if (!hasStableNameTag(merged)) {
        merged.name = deriveStableName(merged);
    }
    return sanitizeMetricTags(merged);
}

function hasStableNameTag(tags) {
    if (!tags || !Object.prototype.hasOwnProperty.call(tags, 'name')) {
        return false;
    }
    const value = tags.name;
    if (value === null || value === undefined) {
        return false;
    }
    if (typeof value === 'string') {
        return value.trim() !== '';
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return true;
    }
    return false;
}

function deriveStableName(tags) {
    if (tags && typeof tags.endpoint === 'string') {
        const trimmed = tags.endpoint.trim();
        if (trimmed) {
            return trimmed;
        }
    }
    return 'delete_force';
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

function ensureVuState(scenarioName) {
    if (!scenarioRegistry[scenarioName]) {
        scenarioRegistry[scenarioName] = {};
    }
    const key = `${scenarioName}#${__VU}`;
    if (!scenarioRegistry[key]) {
        scenarioRegistry[key] = {
            usernameCounter: 0,
            seedCounter: 0,
            batchTargets: [],
        };
    }
    return scenarioRegistry[key];
}

function ensureSetup(cfg) {
    if (!cfg || !cfg.baseUrl || !cfg.token || !cfg.dataset) {
        fail('setup 数据异常或缺失');
    }
    return cfg;
}

function scenarioConfig(prefix, defaults) {
    const rate = parsePositiveIntEnv(`${prefix}_RATE`, defaults.rate);
    const perScenarioOverride = (__ENV[`${prefix}_DURATION`] || '').trim();
    const durationOverride = perScenarioOverride || GLOBAL_SCENARIO_DURATION;
    const duration = (durationOverride || defaults.duration).trim();
    const preAllocatedVUs = parsePositiveIntEnv(`${prefix}_VUS`, defaults.preAllocatedVUs);
    const maxVUs = parsePositiveIntEnv(`${prefix}_MAX_VUS`, defaults.maxVUs);
    const scaledRate = scalePositiveNumber(rate, RATE_MULTIPLIER, 1);
    const scaledPreAllocated = scalePositiveNumber(preAllocatedVUs, VUS_MULTIPLIER, 1);
    const scaledMaxVUs = Math.max(scaledPreAllocated, scalePositiveNumber(maxVUs, MAX_VUS_MULTIPLIER, scaledPreAllocated));
    const resolved = {
        executor: 'constant-arrival-rate',
        exec: defaults.exec,
        rate: scaledRate,
        timeUnit: '1s',
        duration,
        preAllocatedVUs: scaledPreAllocated,
        maxVUs: scaledMaxVUs,
    };
    scenarioRegistry[defaults.exec] = Object.assign({}, resolved);
    return resolved;
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

function buildScenarioMeta() {
    const meta = {};
    const keys = Object.keys(scenarioRegistry);
    for (let i = 0; i < keys.length; i += 1) {
        const value = scenarioRegistry[keys[i]];
        if (value && value.exec) {
            meta[value.exec] = value;
        }
    }
    return meta;
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
        fail(`DELETE_FORCE_DATASET 解析失败: ${err.message}`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        fail('DELETE_FORCE_DATASET 必须是 JSON 对象');
    }
    enforceString(parsed, 'usernamePrefix');
    enforceString(parsed, 'basePassword');
    enforceString(parsed, 'emailDomain');

    parsed.usernamePrefix = sanitizeNameCandidate(parsed.usernamePrefix, 32);
    parsed.basePassword = parsed.basePassword.trim();
    parsed.emailDomain = parsed.emailDomain.trim().toLowerCase();

    parsed.seedNicknamePrefix = parsed.seedNicknamePrefix ? sanitizeNameCandidate(parsed.seedNicknamePrefix, 24) : 'delete';
    parsed.requestTimeout = typeof parsed.requestTimeout === 'string' && parsed.requestTimeout.trim() !== '' ? parsed.requestTimeout.trim() : '60s';
    parsed.sleepBetween = parseFloatSafe(parsed.sleepBetween, 0.05);
    parsed.userReadyTimeoutMs = parseIntSafe(parsed.userReadyTimeoutMs, 20000);
    parsed.deleteVerifyTimeoutMs = parseIntSafe(parsed.deleteVerifyTimeoutMs, 20000);
    parsed.maxUsernameLength = parseIntSafe(parsed.maxUsernameLength, 63);
    if (parsed.maxUsernameLength < 16) {
        parsed.maxUsernameLength = 16;
    }
    if (parsed.maxUsernameLength > 45) {
        // 用户名在 MySQL 中最长 45 个字符，超出会被消费侧拒绝
        parsed.maxUsernameLength = 45;
    }
    parsed.batchSize = parseIntSafe(parsed.batchSize, 10);
    if (!Number.isFinite(parsed.batchSize) || parsed.batchSize <= 0) {
        parsed.batchSize = 10;
    }
    parsed.verifyDeletion = parsed.verifyDeletion !== false;
    parsed.respawnOnFailure = parsed.respawnOnFailure !== false;

    parsed.seedExtras = isPlainObject(parsed.seedExtras) ? parsed.seedExtras : null;
    parsed.seedLabels = isPlainObject(parsed.seedLabels) ? parsed.seedLabels : null;
    parsed.seedExtendTemplate = isPlainObject(parsed.seedExtendTemplate) ? parsed.seedExtendTemplate : null;

    return parsed;
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

function enforceString(obj, field) {
    if (typeof obj[field] !== 'string' || obj[field].trim() === '') {
        fail(`DELETE_FORCE_DATASET.${field} 必须为非空字符串`);
    }
    obj[field] = obj[field].trim();
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

function isPlainObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value);
}

function uniqueSuffix() {
    const timestamp = Date.now().toString(36);
    const vu = typeof __VU === 'number' ? __VU.toString(36) : '0';
    const iter = typeof __ITER === 'number' ? __ITER.toString(36) : '0';
    const randomPart = Math.floor(Math.random() * 60466176).toString(36);
    return `${timestamp}${vu}${iter}${randomPart}`;
}

function buildScenarioId(label) {
    const normalized = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, '_')
        .replace(/_+/g, '_')
        .slice(0, 48);
    return `k6-${normalized || DEFAULT_TAG_VALUE}`;
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
