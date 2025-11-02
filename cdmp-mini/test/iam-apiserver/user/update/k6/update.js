import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { randomSeed } from 'k6';

randomSeed(Date.now());

const CODE_SUCCESS = 100001;
const CODE_BIND_ERROR = 100003;
const CODE_VALIDATION = 100004;
const CODE_INVALID_PARAMETER = 110004;
const CODE_RESOURCE_CONFLICT = 110006;
const CODE_KAFKA_DEGRADED = 100401;

const HTTP_OK = 200;
const HTTP_ACCEPTED = 202;
const HTTP_CREATED = 201;
const HTTP_CONFLICT = 409;

const updatePutLatency = new Trend('update_put_latency', true);
const profilePatchLatency = new Trend('update_profile_latency', true);
const batchPatchLatency = new Trend('update_batch_latency', true);

const updatePutSuccessRate = new Rate('update_put_success_rate');
const profilePatchSuccessRate = new Rate('update_profile_success_rate');
const batchPatchSuccessRate = new Rate('update_batch_success_rate');
const degradedRate = new Rate('update_degraded_rate');

const HIGH_CARDINALITY_TAG_KEYS = new Set(['username', 'request_id', 'x_request_id', 'x-request-id']);
const DEFAULT_TAG_VALUE = '_default';

const scenarioSettings = {};

export const options = {
    setupTimeout: '300s',
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(99)<2000'],
        update_put_latency: ['p(95)<500'],
        update_profile_latency: ['p(95)<400'],
        update_batch_latency: ['p(95)<800'],
        update_put_success_rate: ['rate>0.95'],
        update_profile_success_rate: ['rate>0.97'],
        update_batch_success_rate: ['rate>0.90'],
        update_degraded_rate: ['rate<0.02'],
    },
    scenarios: {
        update_put_baseline: scenarioConfig('PUT_BASELINE', {
            exec: 'scenarioPutBaseline',
            rate: 30,
            duration: '1h',
            preAllocatedVUs: 4,
            maxVUs: 12,
        }),
        update_profile_baseline: scenarioConfig('PROFILE_BASELINE', {
            exec: 'scenarioProfileBaseline',
            rate: 40,
            duration: '1h',
            preAllocatedVUs: 4,
            maxVUs: 12,
        }),
        update_put_parallel: scenarioConfig('PUT_PARALLEL', {
            exec: 'scenarioPutParallel',
            rate: 200,
            duration: '1h',
            preAllocatedVUs: 32,
            maxVUs: 64,
        }),
        update_profile_parallel: scenarioConfig('PROFILE_PARALLEL', {
            exec: 'scenarioProfileParallel',
            rate: 220,
            duration: '1h',
            preAllocatedVUs: 32,
            maxVUs: 64,
        }),
        update_batch_condition: scenarioConfig('BATCH_CONDITION', {
            exec: 'scenarioBatchCondition',
            rate: 12,
            duration: '1h',
            preAllocatedVUs: 6,
            maxVUs: 24,
        }),
    },
};

export function setup() {
    const baseUrl = sanitizeBaseUrl(requireEnv('BASE_URL'));
    const dataset = parseDataset(requireEnv('UPDATE_DATASET'));
    const token = obtainToken(baseUrl);

    const context = {
        baseUrl,
        dataset,
        token,
        scenarioMeta: buildScenarioMeta(),
    };

    const creationTags = { scenario: 'seed_users' };
    context.serialUsers = seedUserGroup(context, 'serial', dataset.serialUserCount, creationTags);
    context.parallelPutUsers = seedUserGroup(context, 'put', dataset.parallelPutUserCount, creationTags);
    context.profileUsers = seedUserGroup(context, 'profile', dataset.profileUserCount, creationTags);
    context.batchUsers = seedUserGroup(context, 'batch', dataset.batchUserCount, creationTags);
    context.batchConditionSets = buildBatchConditionSets(context.batchUsers, dataset.batchChunkSize, dataset.batchTargetRatio);

    if (context.batchConditionSets.length === 0 && context.batchUsers.length > 0) {
        context.batchConditionSets.push(context.batchUsers.map(user => user.name));
    }

    return context;
}

export function scenarioPutBaseline(cfg) {
    const context = ensureSetup(cfg);
    runPutScenario({
        context,
        scenarioName: 'scenarioPutBaseline',
        userPool: context.serialUsers,
        tags: { scenario: 'put_baseline' },
    });
}

export function scenarioProfileBaseline(cfg) {
    const context = ensureSetup(cfg);
    runProfileScenario({
        context,
        scenarioName: 'scenarioProfileBaseline',
        userPool: context.serialUsers,
        tags: { scenario: 'profile_baseline' },
    });
}

export function scenarioPutParallel(cfg) {
    const context = ensureSetup(cfg);
    runPutScenario({
        context,
        scenarioName: 'scenarioPutParallel',
        userPool: context.parallelPutUsers,
        tags: { scenario: 'put_parallel' },
    });
}

export function scenarioProfileParallel(cfg) {
    const context = ensureSetup(cfg);
    runProfileScenario({
        context,
        scenarioName: 'scenarioProfileParallel',
        userPool: context.profileUsers,
        tags: { scenario: 'profile_parallel' },
    });
}

export function scenarioBatchCondition(cfg) {
    const context = ensureSetup(cfg);
    runBatchScenario({
        context,
        scenarioName: 'scenarioBatchCondition',
        tags: { scenario: 'batch_condition' },
    });
}

function runPutScenario({ context, scenarioName, userPool, tags }) {
    if (!Array.isArray(userPool) || userPool.length === 0) {
        fail(`[${scenarioName}] user pool empty`);
    }
    const state = ensureVuState(scenarioName);
    const user = pickUserForVu(state, scenarioName, userPool);
    const payload = buildPutPayload(context.dataset, state, user);
    const res = sendPutRequest(context, user, payload, tags);
    updatePutLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const success = res.status === HTTP_OK && parsed.code === CODE_SUCCESS;
    updatePutSuccessRate.add(success);
    degradedRate.add(degraded);

    if (success) {
        const newVersion = extractUpdateVersion(parsed.data);
        if (newVersion !== null) {
            state.versions[user.name] = newVersion;
        }
    } else if (needsVersionResync(parsed.code, res.status)) {
        const version = tryResyncVersion(context, user, tags);
        if (version !== null) {
            state.versions[user.name] = version;
        }
    }

    recordChecks(res, {
        put_http_200: r => r.status === HTTP_OK,
        put_code_success: () => parsed.code === CODE_SUCCESS,
    });

    sleep(context.dataset.sleepBetween);
}

function runProfileScenario({ context, scenarioName, userPool, tags }) {
    if (!Array.isArray(userPool) || userPool.length === 0) {
        fail(`[${scenarioName}] user pool empty`);
    }
    const state = ensureVuState(scenarioName);
    const user = pickUserForVu(state, scenarioName, userPool);
    const payload = buildProfilePayload(context.dataset, state, user);
    const res = sendProfilePatch(context, user, payload, tags);
    profilePatchLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const success = (res.status === HTTP_ACCEPTED || res.status === HTTP_OK) && parsed.code === CODE_SUCCESS;
    profilePatchSuccessRate.add(success);
    degradedRate.add(degraded);

    recordChecks(res, {
        profile_http_202: r => r.status === HTTP_ACCEPTED || r.status === HTTP_OK,
        profile_code_success: () => parsed.code === CODE_SUCCESS,
    });

    sleep(context.dataset.sleepBetween);
}

function runBatchScenario({ context, scenarioName, tags }) {
    if (!Array.isArray(context.batchConditionSets) || context.batchConditionSets.length === 0) {
        fail('未找到批量更新的目标集合');
    }
    const state = ensureVuState(scenarioName);
    if (state.batchIndex === undefined) {
        state.batchIndex = (__VU - 1) % context.batchConditionSets.length;
    }
    const targets = context.batchConditionSets[state.batchIndex];
    state.batchIndex = (state.batchIndex + 1) % context.batchConditionSets.length;

    const updates = buildBatchUpdates(context.dataset, state);
    const payload = {
        updates,
        conditions: {
            name: {
                in: targets,
            },
        },
    };
    const res = sendBatchPatch(context, payload, tags);
    batchPatchLatency.add(res.timings.duration);
    const parsed = parseResponse(res);
    const degraded = isDegraded(parsed);
    const success = (res.status === HTTP_ACCEPTED || res.status === HTTP_OK) && parsed.code === CODE_SUCCESS;
    batchPatchSuccessRate.add(success);
    degradedRate.add(degraded);

    recordChecks(res, {
        batch_http_202: r => r.status === HTTP_ACCEPTED || r.status === HTTP_OK,
        batch_code_success: () => parsed.code === CODE_SUCCESS,
    });

    sleep(context.dataset.sleepBetween);
}

function buildPutPayload(dataset, state, user) {
    if (!state.versions) {
        state.versions = {};
    }
    if (!state.counters) {
        state.counters = {};
    }
    if (state.versions[user.name] === undefined) {
        state.versions[user.name] = user.version;
    }
    const version = state.versions[user.name];
    const counter = (state.counters[user.name] || 0) + 1;
    state.counters[user.name] = counter;

    const nickname = `${dataset.putNicknamePrefix}-${counter}`;
    const emailLocal = `${user.baseEmail}-${counter}`;
    const payload = {
        metadata: { name: user.name },
        nickname,
        email: `${emailLocal}@${dataset.emailDomain}`,
        version,
    };

    if (dataset.putToggleAdmin) {
        payload.isAdmin = counter % 2 === 0 ? 1 : 0;
    }
    if (dataset.includePhoneUpdates && user.phone) {
        payload.phone = `${user.phone.slice(0, -dataset.phoneSuffixLength)}${padNumber(counter, dataset.phoneSuffixLength)}`;
    }
    return payload;
}

function buildProfilePayload(dataset, state, user) {
    if (!state.profileCounters) {
        state.profileCounters = {};
    }
    const counter = (state.profileCounters[user.name] || 0) + 1;
    state.profileCounters[user.name] = counter;
    const nickname = `${dataset.profileNicknamePrefix}-${counter}`;
    const payload = {
        nickname,
    };
    if (dataset.profileUpdateEmail) {
        payload.email = `${user.baseEmail}-p${counter}@${dataset.emailDomain}`;
    }
    if (dataset.profileUpdatePhone && user.phone) {
        payload.phone = `${user.phone.slice(0, -dataset.phoneSuffixLength)}${padNumber(counter + 1000, dataset.phoneSuffixLength)}`;
    }
    return payload;
}

function buildBatchUpdates(dataset, state) {
    if (!state.batchCounters) {
        state.batchCounters = 0;
    }
    state.batchCounters += 1;
    const counter = state.batchCounters;
    const updates = Object.assign({}, dataset.batchUpdates);
    if (dataset.batchNicknamePrefix) {
        updates.nickname = `${dataset.batchNicknamePrefix}-${counter}`;
    }
    if (dataset.batchToggleStatus) {
        updates.status = counter % 2;
    }
    return updates;
}

function sendPutRequest(context, user, payload, extraTags) {
    const tags = mergeTags({ endpoint: 'put_user', username: user.name }, extraTags);
    const params = buildRequestParams(context, tags);
    return http.put(`${context.baseUrl}/v1/users/${encodeURIComponent(user.name)}`, JSON.stringify(payload), params);
}

function sendProfilePatch(context, user, payload, extraTags) {
    const tags = mergeTags({ endpoint: 'profile_patch', username: user.name }, extraTags);
    const params = buildRequestParams(context, tags);
    return http.put(`${context.baseUrl}/api/users/${encodeURIComponent(user.name)}/profile`, JSON.stringify(payload), params);
}

function sendBatchPatch(context, payload, extraTags) {
    const tags = mergeTags({ endpoint: 'batch_patch' }, extraTags);
    const params = buildRequestParams(context, tags);
    return http.patch(`${context.baseUrl}/api/users`, JSON.stringify(payload), params);
}

function buildRequestParams(context, tags) {
    const headers = buildHeaders(context.token, tags);
    const params = { headers, tags };
    if (context.dataset.requestTimeout) {
        params.timeout = context.dataset.requestTimeout;
    }
    return params;
}

function seedUserGroup(context, label, count, tags) {
    if (!count || count <= 0) {
        return [];
    }
    const users = [];
    for (let i = 0; i < count; i += 1) {
        const username = buildSeedUsername(context.dataset, label, i);
        const payload = buildSeedPayload(context.dataset, username, i);
        const res = sendCreateUser(context, payload, tags);
        if (res.status !== HTTP_CREATED) {
            const parsed = parseResponse(res);
            fail(`创建种子用户失败: username=${username} http=${res.status} code=${parsed.code} message=${parsed.message}`);
        }
        waitForUserVisibility(context, username, tags);
        users.push({
            name: username,
            version: 1,
            baseEmail: username.slice(0, 48),
            phone: payload.phone || '',
        });
    }
    return users;
}

function buildSeedUsername(dataset, label, index) {
    const suffix = `${label}-${index}-${uniqueSuffix()}`;
    let candidate = `${dataset.usernamePrefix}-${suffix}`;
    if (candidate.length > dataset.maxUsernameLength) {
        candidate = candidate.slice(0, dataset.maxUsernameLength);
    }
    return candidate;
}

function buildSeedPayload(dataset, username, index) {
    const payload = {
        metadata: { name: username },
        password: dataset.basePassword,
        email: `${username}@${dataset.emailDomain}`,
        nickname: `${dataset.seedNicknamePrefix}-${index}`,
        status: 1,
        isAdmin: index % 5 === 0 ? 1 : 0,
    };
    if (dataset.phonePrefix) {
        payload.phone = dataset.phonePrefix + padNumber(index, dataset.phoneSuffixLength);
    }
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

function sendCreateUser(context, payload, tags) {
    const finalTags = mergeTags({ endpoint: 'create_user_seed' }, tags);
    const params = buildRequestParams(context, finalTags);
    return http.post(`${context.baseUrl}/v1/users`, JSON.stringify(payload), params);
}

function waitForUserVisibility(context, username, tags) {
    const deadline = Date.now() + context.dataset.userReadyTimeoutMs;
    const params = buildRequestParams(context, mergeTags({ endpoint: 'get_user_seed', username }, tags));
    while (Date.now() < deadline) {
        const res = http.get(`${context.baseUrl}/v1/users/${encodeURIComponent(username)}`, params);
        if (res.status === HTTP_OK) {
            return;
        }
        sleep(0.3);
    }
    fail(`等待用户 ${username} 可见超时`);
}

function buildBatchConditionSets(batchUsers, chunkSize, ratio) {
    if (!Array.isArray(batchUsers) || batchUsers.length === 0) {
        return [];
    }
    const names = batchUsers.map(user => user.name);
    const effectiveChunk = Math.max(1, Math.min(names.length, chunkSize || names.length));
    const sets = [];
    for (let i = 0; i < names.length; i += effectiveChunk) {
        sets.push(names.slice(i, i + effectiveChunk));
    }
    if (ratio && ratio > 0 && ratio < 1) {
        return sets.map(set => set.slice(0, Math.max(1, Math.floor(set.length * ratio))));
    }
    return sets;
}

function ensureSetup(cfg) {
    if (!cfg || !cfg.baseUrl || !cfg.token || !cfg.dataset) {
        fail('setup 数据异常或缺失');
    }
    return cfg;
}

function ensureVuState(scenarioName) {
    if (!scenarioSettings[scenarioName]) {
        scenarioSettings[scenarioName] = {};
    }
    const key = `${scenarioName}#${__VU}`;
    if (!scenarioSettings[key]) {
        scenarioSettings[key] = {
            stride: resolveScenarioStride(scenarioName),
            versions: {},
            counters: {},
        };
    }
    return scenarioSettings[key];
}

function pickUserForVu(state, scenarioName, userPool) {
    if (!Array.isArray(userPool) || userPool.length === 0) {
        fail(`[${scenarioName}] user pool empty`);
    }
    if (state.userIndex === undefined) {
        state.userIndex = (__VU - 1) % userPool.length;
    }
    const stride = state.stride || resolveScenarioStride(scenarioName);
    const user = userPool[state.userIndex % userPool.length];
    state.userIndex = (state.userIndex + stride) % userPool.length;
    if (state.versions[user.name] === undefined) {
        state.versions[user.name] = user.version;
    }
    if (!state.counters[user.name]) {
        state.counters[user.name] = 0;
    }
    return user;
}

function resolveScenarioStride(scenarioName) {
    const meta = buildScenarioMeta()[scenarioName] || {};
    const stride = meta.maxVUs || meta.preAllocatedVUs || 1;
    return Math.max(1, stride);
}

function buildScenarioMeta() {
    const meta = {};
    const keys = Object.keys(scenarioSettings);
    for (let i = 0; i < keys.length; i += 1) {
        const value = scenarioSettings[keys[i]];
        if (value && value.exec) {
            meta[value.exec] = value;
        }
    }
    return meta;
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

function extractUpdateVersion(data) {
    if (!data || typeof data !== 'object') {
        return null;
    }
    const candidate = data.update_user || data.UpdateUser || data.updateUser;
    if (!candidate || typeof candidate !== 'object') {
        return null;
    }
    const metadata = candidate.metadata || candidate.Metadata || candidate.objectMeta || candidate.ObjectMeta;
    if (!metadata) {
        return null;
    }
    if (typeof metadata.version === 'number') {
        return metadata.version;
    }
    if (typeof metadata.Version === 'number') {
        return metadata.Version;
    }
    const parsedVersion = Number(metadata.version || metadata.Version);
    return Number.isFinite(parsedVersion) ? parsedVersion : null;
}

function needsVersionResync(code, status) {
    if (status === HTTP_CONFLICT) {
        return true;
    }
    return code === CODE_RESOURCE_CONFLICT || code === CODE_INVALID_PARAMETER;
}

function tryResyncVersion(context, user, tags) {
    const params = buildRequestParams(context, mergeTags({ endpoint: 'get_user_sync', username: user.name }, tags));
    const res = http.get(`${context.baseUrl}/v1/users/${encodeURIComponent(user.name)}`, params);
    if (res.status !== HTTP_OK) {
        return null;
    }
    const parsed = parseResponse(res);
    const data = parsed.data;
    if (!data || typeof data !== 'object') {
        return null;
    }
    const metaCandidate = data.update_user || data.detail || data.user || data.User;
    if (metaCandidate && typeof metaCandidate === 'object') {
        const metadata = metaCandidate.metadata || metaCandidate.Metadata || metaCandidate.objectMeta || metaCandidate.ObjectMeta;
        if (metadata) {
            const version = metadata.version || metadata.Version;
            if (Number.isFinite(Number(version))) {
                return Number(version);
            }
        }
    }
    return null;
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

function isDegraded(parsed) {
    return parsed && parsed.code === CODE_KAFKA_DEGRADED;
}

function scenarioConfig(prefix, defaults) {
    const rate = parsePositiveIntEnv(`${prefix}_RATE`, defaults.rate);
    const duration = (__ENV[`${prefix}_DURATION`] || defaults.duration).trim();
    const preAllocatedVUs = parsePositiveIntEnv(`${prefix}_VUS`, defaults.preAllocatedVUs);
    const maxVUs = parsePositiveIntEnv(`${prefix}_MAX_VUS`, defaults.maxVUs);
    const resolved = {
        executor: 'constant-arrival-rate',
        exec: defaults.exec,
        rate,
        timeUnit: '1s',
        duration,
        preAllocatedVUs,
        maxVUs,
    };
    scenarioSettings[defaults.exec] = Object.assign({}, resolved);
    return resolved;
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
    if (res.status !== HTTP_OK) {
        fail(`管理员登录失败，状态码: ${res.status}`);
    }
    let body;
    try {
        body = res.json();
    } catch (err) {
        fail('解析登录响应失败: ' + err.message);
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

function parseDataset(raw) {
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (err) {
        fail(`UPDATE_DATASET 解析失败: ${err.message}`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        fail('UPDATE_DATASET 必须是 JSON 对象');
    }
    enforceString(parsed, 'usernamePrefix');
    enforceString(parsed, 'basePassword');
    enforceString(parsed, 'emailDomain');

    parsed.usernamePrefix = sanitizeNameCandidate(parsed.usernamePrefix, 32);
    parsed.basePassword = parsed.basePassword.trim();
    parsed.emailDomain = parsed.emailDomain.trim().toLowerCase();

    parsed.serialUserCount = parseIntSafe(parsed.serialUserCount, 4);
    parsed.parallelPutUserCount = parseIntSafe(parsed.parallelPutUserCount, 64);
    parsed.profileUserCount = parseIntSafe(parsed.profileUserCount, 64);
    parsed.batchUserCount = parseIntSafe(parsed.batchUserCount, 40);
    parsed.batchChunkSize = parseIntSafe(parsed.batchChunkSize, 10);
    parsed.batchTargetRatio = clamp(parsed.batchTargetRatio, 0, 1);

    parsed.requestTimeout = typeof parsed.requestTimeout === 'string' && parsed.requestTimeout.trim() !== '' ? parsed.requestTimeout.trim() : '60s';
    parsed.sleepBetween = parseFloatSafe(parsed.sleepBetween, 0.05);
    parsed.seedNicknamePrefix = parsed.seedNicknamePrefix ? sanitizeNameCandidate(parsed.seedNicknamePrefix, 24) : 'seed';
    parsed.putNicknamePrefix = parsed.putNicknamePrefix ? sanitizeNameCandidate(parsed.putNicknamePrefix, 24) : 'put';
    parsed.profileNicknamePrefix = parsed.profileNicknamePrefix ? sanitizeNameCandidate(parsed.profileNicknamePrefix, 24) : 'profile';
    parsed.batchNicknamePrefix = parsed.batchNicknamePrefix ? sanitizeNameCandidate(parsed.batchNicknamePrefix, 24) : 'batch';

    parsed.putToggleAdmin = Boolean(parsed.putToggleAdmin);
    parsed.includePhoneUpdates = Boolean(parsed.includePhoneUpdates);
    parsed.profileUpdateEmail = Boolean(parsed.profileUpdateEmail);
    parsed.profileUpdatePhone = Boolean(parsed.profileUpdatePhone);
    parsed.batchToggleStatus = Boolean(parsed.batchToggleStatus);

    parsed.maxUsernameLength = parseIntSafe(parsed.maxUsernameLength, 63);
    if (parsed.maxUsernameLength < 16) {
        parsed.maxUsernameLength = 16;
    }

    parsed.phonePrefix = typeof parsed.phonePrefix === 'string' ? parsed.phonePrefix.trim() : '';
    parsed.phoneSuffixLength = parseIntSafe(parsed.phoneSuffixLength, 6);
    parsed.userReadyTimeoutMs = parseIntSafe(parsed.userReadyTimeoutMs, 15000);

    parsed.batchUpdates = isPlainObject(parsed.batchUpdates) ? parsed.batchUpdates : { isAdmin: 1 };
    parsed.seedExtras = isPlainObject(parsed.seedExtras) ? parsed.seedExtras : null;
    parsed.seedLabels = isPlainObject(parsed.seedLabels) ? parsed.seedLabels : null;
    parsed.seedExtendTemplate = isPlainObject(parsed.seedExtendTemplate) ? parsed.seedExtendTemplate : null;

    return parsed;
}

function enforceString(obj, field) {
    if (typeof obj[field] !== 'string' || obj[field].trim() === '') {
        fail(`UPDATE_DATASET.${field} 必须为非空字符串`);
    }
    obj[field] = obj[field].trim();
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

function padNumber(num, length) {
    const raw = String(num);
    if (raw.length >= length) {
        return raw.slice(-length);
    }
    return raw.padStart(length, '0');
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
