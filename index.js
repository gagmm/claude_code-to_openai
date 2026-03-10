// ================================================================
// Claude API 代理 v4.4
// 功能：自定义Token鉴权 / Telegram Bot管理 / 多Key负载均衡 / 自动刷新
//       分布式锁 / 自动重试 / 流式thinking+tool_calls / 安全加固
//       Telegram 直接对话 / 会话管理 / 连接保活 / 动态长上下文支持
//       D1 数据库支持（优先） / KV 后备
// 修复：403/401 区分 / 空消息过滤 / 刷新风暴防护 / Tool调用链
//       TG鉴权 / HTML转义 / 锁竞态缓解 / tool_choice转发
// ================================================================

const MODEL_MAP = {
  "claude-opus-4-6": "claude-opus-4-6",
  "claude-opus-4-6-latest": "claude-opus-4-6",
  "claude-sonnet-4-6": "claude-sonnet-4-6",
  "claude-sonnet-4-6-latest": "claude-sonnet-4-6",

  "claude-sonnet-4-5": "claude-sonnet-4-5-20250929",
  "claude-sonnet-4-5-20250929": "claude-sonnet-4-5-20250929",
  "claude-haiku-4-5": "claude-haiku-4-5-20251001",
  "claude-haiku-4-5-20251001": "claude-haiku-4-5-20251001",
  "claude-opus-4-5": "claude-opus-4-5-20251101",
  "claude-opus-4-5-20251101": "claude-opus-4-5-20251101",

  "claude-opus-4-1": "claude-opus-4-1-20250805",
  "claude-opus-4-1-20250805": "claude-opus-4-1-20250805",
  "claude-opus-4-0": "claude-opus-4-20250514",
  "claude-opus-4-20250514": "claude-opus-4-20250514",
  "claude-sonnet-4-0": "claude-sonnet-4-20250514",
  "claude-sonnet-4-20250514": "claude-sonnet-4-20250514",

  "claude-3-7-sonnet": "claude-3-7-sonnet-20250219",
  "claude-3-7-sonnet-latest": "claude-3-7-sonnet-20250219",
  "claude-3-7-sonnet-20250219": "claude-3-7-sonnet-20250219",

  "claude-3-5-sonnet": "claude-3-5-sonnet-20241022",
  "claude-3-5-sonnet-20241022": "claude-3-5-sonnet-20241022",
  "claude-3-5-sonnet-20240620": "claude-3-5-sonnet-20240620",
  "claude-3-5-haiku": "claude-3-5-haiku-20241022",
  "claude-3-5-haiku-20241022": "claude-3-5-haiku-20241022",

  "claude-3-opus": "claude-3-opus-20240229",
  "claude-3-opus-20240229": "claude-3-opus-20240229",
  "claude-3-sonnet": "claude-3-sonnet-20240229",
  "claude-3-sonnet-20240229": "claude-3-sonnet-20240229",
  "claude-3-haiku": "claude-3-haiku-20240307",
  "claude-3-haiku-20240307": "claude-3-haiku-20240307",

  "claude-2.1": "claude-2.1",
  "claude-2.0": "claude-2.0",
  "claude-instant-1.2": "claude-instant-1.2",
  "claude-instant-1.1": "claude-instant-1.1"
};

var MODEL_MAX_OUTPUT = {
    "claude-opus-4-6":             128000,
    "claude-opus-4-5-20251101":    32768,
    "claude-opus-4-1-20250805":    32768,
    "claude-opus-4-20250514":      32768,
    "claude-sonnet-4-6":           65536,
    "claude-sonnet-4-5-20250929":  65536,
    "claude-sonnet-4-20250514":    16384,
    "claude-haiku-4-5-20251001":   16384,
    "claude-3-7-sonnet-20250219":  8192,
    "claude-3-5-sonnet-20241022":  8192,
    "claude-3-5-sonnet-20240620":  8192,
    "claude-3-5-haiku-20241022":   8192,
    "claude-3-opus-20240229":      4096,
    "claude-3-sonnet-20240229":    4096,
    "claude-3-haiku-20240307":     4096,
    "claude-2.1":                  4096,
    "claude-2.0":                  4096,
    "claude-instant-1.2":          4096,
    "claude-instant-1.1":          4096
};

var MODEL_MAX_OUTPUT_THINKING = {
    "claude-opus-4-6":             128000,
    "claude-opus-4-5-20251101":    64000,
    "claude-opus-4-1-20250805":    64000,
    "claude-opus-4-20250514":      64000,
    "claude-sonnet-4-6":           65536,
    "claude-sonnet-4-5-20250929":  65536,
    "claude-sonnet-4-20250514":    65536,
    "claude-3-7-sonnet-20250219":  128000
};

function getModelMaxTokens(model, hasThinking) {
    if (hasThinking && MODEL_MAX_OUTPUT_THINKING[model]) {
        return MODEL_MAX_OUTPUT_THINKING[model];
    }
    return MODEL_MAX_OUTPUT[model] ?? 32768;
}

var SUPPORTED_MODELS = Object.keys(MODEL_MAP).map(function(id) {
    return { id: id, object: "model", created: 0, owned_by: "anthropic" };
});

// ================================================================
// 工具函数
// ================================================================

function corsResponse(body, status) {
    if (status === undefined) status = 200;
    return new Response(body, {
        status: status,
        headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "*"
        }
    });
}

function errorResponse(message, status) {
    return corsResponse(JSON.stringify({
        error: { message: message, type: "api_error", code: status }
    }), status);
}

function escHtml(text) {
    if (!text) return "";
    return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function sleep(ms) {
    return new Promise(function(resolve) { setTimeout(resolve, ms); });
}

function mapStopReason(r) {
    var map = { "end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length", "tool_use": "tool_calls" };
    return map[r] || "stop";
}

function maskToken(token) {
    if (!token) return "[empty]";
    if (token.length <= 16) return token.substring(0, 4) + "...[HIDDEN]";
    return token.substring(0, 10) + "...[HIDDEN]";
}

function sanitizeContent(content) {
    if (typeof content === "string") {
        return content.trim() ? content : "(empty)";
    }
    if (Array.isArray(content)) {
        var cleaned = [];
        for (var i = 0; i < content.length; i++) {
            var block = content[i];
            if (block.type === "text") {
                if (block.text && String(block.text).trim()) {
                    cleaned.push(block);
                }
            } else if (block.type === "tool_result") {
                if (block.content && typeof block.content === "string" && !block.content.trim()) {
                    block.content = "(empty result)";
                }
                cleaned.push(block);
            } else {
                cleaned.push(block);
            }
        }
        if (cleaned.length === 0) return "(empty)";
        return cleaned;
    }
    if (!content && content !== 0) return "(empty)";
    return content;
}

// ================================================================
// D1 数据库初始化
// ================================================================

const D1_SCHEMA = `
CREATE TABLE IF NOT EXISTS api_keys (
    label TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS global_stats (
    id TEXT PRIMARY KEY DEFAULT 'global',
    data TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tg_sessions (
    chat_id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    expires_at INTEGER,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS locks (
    lock_name TEXT PRIMARY KEY,
    lock_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cooldowns (
    label TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
`;

async function initD1(env) {
    if (!env.DB) return { ok: false, error: "No D1 binding (env.DB)" };
    try {
        var statements = D1_SCHEMA.split(";").map(function(s) { return s.trim(); }).filter(Boolean);
        for (var i = 0; i < statements.length; i++) {
            await env.DB.prepare(statements[i]).run();
        }
        return { ok: true, tables: statements.length };
    } catch (e) {
        console.error("[D1 Init]", e.message);
        return { ok: false, error: e.message };
    }
}

function hasD1(env) {
    return !!env.DB;
}

// ================================================================
// 存储层：D1 优先，KV 后备
// ================================================================

async function saveKey(env, label, data) {
    if (hasD1(env)) {
        try {
            await env.DB.prepare(
                "INSERT OR REPLACE INTO api_keys (label, data, updated_at) VALUES (?, ?, datetime('now'))"
            ).bind(label, JSON.stringify(data)).run();
            return true;
        } catch (e) {
            console.error("[D1 saveKey]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            await env.TOKEN_STORE.put("key:" + label, JSON.stringify(data));
            return true;
        } catch (e) {
            console.error("[KV saveKey]", e.message);
        }
    }
    return false;
}

async function getKey(env, label) {
    if (hasD1(env)) {
        try {
            var row = await env.DB.prepare("SELECT data FROM api_keys WHERE label = ?").bind(label).first();
            if (row) return JSON.parse(row.data);
        } catch (e) {
            console.error("[D1 getKey]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            return await env.TOKEN_STORE.get("key:" + label, { type: "json" });
        } catch (e) {
            console.error("[KV getKey]", e.message);
        }
    }
    return null;
}

async function deleteKey(env, label) {
    if (hasD1(env)) {
        try {
            await env.DB.prepare("DELETE FROM api_keys WHERE label = ?").bind(label).run();
        } catch (e) {
            console.error("[D1 deleteKey]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            await env.TOKEN_STORE.delete("key:" + label);
        } catch (e) {
            console.error("[KV deleteKey]", e.message);
        }
    }
}

async function listAllKeys(env) {
    if (hasD1(env)) {
        try {
            var result = await env.DB.prepare("SELECT data FROM api_keys").all();
            return (result.results || []).map(function(r) {
                try { return JSON.parse(r.data); } catch(e) { return null; }
            }).filter(Boolean);
        } catch (e) {
            console.error("[D1 listAllKeys]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            var list = await env.TOKEN_STORE.list({ prefix: "key:" });
            var results = await Promise.all(
                list.keys.map(function(k) {
                    return env.TOKEN_STORE.get(k.name, { type: "json" });
                })
            );
            return results.filter(Boolean);
        } catch (e) {
            console.error("[KV listAllKeys]", e.message);
        }
    }
    return [];
}

async function getGlobalStats(env) {
    if (hasD1(env)) {
        try {
            var row = await env.DB.prepare("SELECT data FROM global_stats WHERE id = 'global'").first();
            if (row) return JSON.parse(row.data);
        } catch (e) {
            console.error("[D1 getGlobalStats]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            return await env.TOKEN_STORE.get("stats:global", { type: "json" }) || {};
        } catch (e) {}
    }
    return {};
}

async function incrementGlobalStats(env) {
    try {
        var stats = await getGlobalStats(env);
        var today = new Date().toISOString().split("T")[0];
        stats.totalRequests = (stats.totalRequests || 0) + 1;
        if (stats.today === today) {
            stats.todayRequests = (stats.todayRequests || 0) + 1;
        } else {
            stats.today = today;
            stats.todayRequests = 1;
        }
        if (hasD1(env)) {
            await env.DB.prepare(
                "INSERT OR REPLACE INTO global_stats (id, data, updated_at) VALUES ('global', ?, datetime('now'))"
            ).bind(JSON.stringify(stats)).run();
            return;
        }
        if (env.TOKEN_STORE) {
            await env.TOKEN_STORE.put("stats:global", JSON.stringify(stats));
        }
    } catch (e) {
        console.error("[Stats]", e.message);
    }
}

// ================================================================
// Telegram 会话管理（D1 + KV）
// ================================================================

async function getTGSession(env, chatId) {
    if (hasD1(env)) {
        try {
            var now = Math.floor(Date.now() / 1000);
            var row = await env.DB.prepare(
                "SELECT data FROM tg_sessions WHERE chat_id = ? AND (expires_at IS NULL OR expires_at > ?)"
            ).bind(chatId, now).first();
            if (row) return JSON.parse(row.data);
        } catch (e) {
            console.error("[D1 getTGSession]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            return await env.TOKEN_STORE.get("tg:session:" + chatId, { type: "json" });
        } catch (e) {}
    }
    return null;
}

async function saveTGSession(env, chatId, session) {
    var expiresAt = Math.floor(Date.now() / 1000) + 86400;
    if (hasD1(env)) {
        try {
            await env.DB.prepare(
                "INSERT OR REPLACE INTO tg_sessions (chat_id, data, expires_at, updated_at) VALUES (?, ?, ?, datetime('now'))"
            ).bind(chatId, JSON.stringify(session), expiresAt).run();
            return true;
        } catch (e) {
            console.error("[D1 saveTGSession]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try {
            await env.TOKEN_STORE.put("tg:session:" + chatId, JSON.stringify(session), { expirationTtl: 86400 });
            return true;
        } catch (e) {
            console.error("[KV saveTGSession]", e.message);
        }
    }
    return false;
}

async function deleteTGSession(env, chatId) {
    if (hasD1(env)) {
        try {
            await env.DB.prepare("DELETE FROM tg_sessions WHERE chat_id = ?").bind(chatId).run();
        } catch (e) {
            console.error("[D1 deleteTGSession]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try { await env.TOKEN_STORE.delete("tg:session:" + chatId); } catch (e) {}
    }
}

// ================================================================
// 分布式锁（D1 原子操作优先，KV 后备 + 竞态缓解）
// ================================================================

async function acquireLock(env, lockName, ttlSeconds) {
    var ttl = ttlSeconds || 30;
    var expiresAt = Math.floor(Date.now() / 1000) + ttl;
    var lockId = crypto.randomUUID();

    if (hasD1(env)) {
        try {
            // 先清理过期锁
            var now = Math.floor(Date.now() / 1000);
            await env.DB.prepare("DELETE FROM locks WHERE lock_name = ? AND expires_at <= ?").bind(lockName, now).run();
            // INSERT OR IGNORE：如果已存在则不覆盖，返回 changes=0
            var result = await env.DB.prepare(
                "INSERT OR IGNORE INTO locks (lock_name, lock_id, expires_at) VALUES (?, ?, ?)"
            ).bind(lockName, lockId, expiresAt).run();
            return result.meta.changes > 0;
        } catch (e) {
            console.error("[D1 Lock]", e.message);
            return true; // 出错时不阻塞
        }
    }

    if (env.TOKEN_STORE) {
        var lockKey = "lock:" + lockName;
        try {
            var existing = await env.TOKEN_STORE.get(lockKey);
            if (existing) return false;
            await env.TOKEN_STORE.put(lockKey, lockId, { expirationTtl: ttl });
            // 二次验证缓解竞态
            await sleep(50);
            var verify = await env.TOKEN_STORE.get(lockKey);
            return verify === lockId;
        } catch (e) {
            console.error("[KV Lock]", e.message);
            return true;
        }
    }

    return true;
}

async function releaseLock(env, lockName) {
    if (hasD1(env)) {
        try {
            await env.DB.prepare("DELETE FROM locks WHERE lock_name = ?").bind(lockName).run();
        } catch (e) {
            console.error("[D1 releaseLock]", e.message);
        }
    }
    if (env.TOKEN_STORE) {
        try { await env.TOKEN_STORE.delete("lock:" + lockName); } catch (e) {}
    }
}

// ================================================================
// 刷新冷却（D1 + KV）
// ================================================================

async function isRefreshOnCooldown(env, label) {
    var now = Math.floor(Date.now() / 1000);

    if (hasD1(env)) {
        try {
            var row = await env.DB.prepare(
                "SELECT expires_at FROM cooldowns WHERE label = ? AND expires_at > ?"
            ).bind(label, now).first();
            return !!row;
        } catch (e) {
            console.error("[D1 cooldown check]", e.message);
        }
    }

    if (env.TOKEN_STORE) {
        try {
            var cd = await env.TOKEN_STORE.get("cooldown:refresh:" + label, { type: "json" });
            if (!cd) return false;
            return cd.until > Date.now();
        } catch (e) {}
    }

    return false;
}

async function setRefreshCooldown(env, label, cooldownSeconds) {
    var expiresAt = Math.floor(Date.now() / 1000) + cooldownSeconds;
    var data = JSON.stringify({ until: Date.now() + cooldownSeconds * 1000, setAt: new Date().toISOString() });

    if (hasD1(env)) {
        try {
            await env.DB.prepare(
                "INSERT OR REPLACE INTO cooldowns (label, data, expires_at) VALUES (?, ?, ?)"
            ).bind(label, data, expiresAt).run();
            return;
        } catch (e) {
            console.error("[D1 setCooldown]", e.message);
        }
    }

    if (env.TOKEN_STORE) {
        try {
            await env.TOKEN_STORE.put("cooldown:refresh:" + label, data, { expirationTtl: cooldownSeconds });
        } catch (e) {}
    }
}

// ================================================================
// D1 定期清理（过期 sessions / locks / cooldowns）
// ================================================================

async function cleanupD1(env) {
    if (!hasD1(env)) return;
    var now = Math.floor(Date.now() / 1000);
    try {
        await env.DB.batch([
            env.DB.prepare("DELETE FROM tg_sessions WHERE expires_at IS NOT NULL AND expires_at <= ?").bind(now),
            env.DB.prepare("DELETE FROM locks WHERE expires_at <= ?").bind(now),
            env.DB.prepare("DELETE FROM cooldowns WHERE expires_at <= ?").bind(now)
        ]);
        console.log("[D1 Cleanup] Done");
    } catch (e) {
        console.error("[D1 Cleanup]", e.message);
    }
}

// ================================================================
// Telegram 发送函数
// ================================================================

async function sendTG(env, message) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    var chatId = env.TELEGRAM_CHAT_ID;
    if (!botToken || !chatId) return false;
    try {
        var resp = await fetch("https://api.telegram.org/bot" + botToken + "/sendMessage", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                chat_id: chatId,
                text: message,
                parse_mode: "HTML",
                disable_web_page_preview: true
            })
        });
        if (!resp.ok) {
            console.error("[TG] Send failed:", resp.status, await resp.text());
            return false;
        }
        return true;
    } catch (err) {
        console.error("[TG] Error:", err.message);
        return false;
    }
}

async function sendTGLong(env, message) {
    var MAX_LEN = 4000;
    if (message.length <= MAX_LEN) return await sendTG(env, message);
    var parts = [];
    var remaining = message;
    while (remaining.length > 0) {
        parts.push(remaining.substring(0, MAX_LEN));
        remaining = remaining.substring(MAX_LEN);
    }
    for (var i = 0; i < parts.length; i++) {
        var header = parts.length > 1 ? ("📄 (" + (i + 1) + "/" + parts.length + ")\n") : "";
        await sendTG(env, header + parts[i]);
        if (i < parts.length - 1) await sleep(500);
    }
    return true;
}

async function sendTGReply(env, chatId, message, replyToMessageId, parseMode) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return { ok: false, error: "No bot token" };
    try {
        var body = {
            chat_id: chatId,
            text: message,
            disable_web_page_preview: true
        };
        // [Fix #4] 支持不带 parse_mode 发送纯文本
        if (parseMode !== undefined) {
            body.parse_mode = parseMode;
        } else {
            body.parse_mode = "HTML";
        }
        if (replyToMessageId) body.reply_to_message_id = replyToMessageId;
        var resp = await fetch("https://api.telegram.org/bot" + botToken + "/sendMessage", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        });
        if (!resp.ok) return { ok: false, error: await resp.text() };
        var result = await resp.json();
        return { ok: true, messageId: result.result.message_id };
    } catch (err) {
        return { ok: false, error: err.message };
    }
}

async function deleteTGMessage(env, chatId, messageId) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return;
    try {
        await fetch("https://api.telegram.org/bot" + botToken + "/deleteMessage", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ chat_id: chatId, message_id: messageId })
        });
    } catch (e) {
        console.error("[TG] Delete message failed:", e.message);
    }
}

async function editTGMessage(env, chatId, messageId, newText, parseMode) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return false;
    try {
        var body = {
            chat_id: chatId,
            message_id: messageId,
            text: newText,
            disable_web_page_preview: true
        };
        // [Fix #4] 支持纯文本模式
        if (parseMode !== undefined) {
            if (parseMode) body.parse_mode = parseMode;
            // parseMode === null / "" → 不设置，发纯文本
        } else {
            body.parse_mode = "HTML";
        }
        var resp = await fetch("https://api.telegram.org/bot" + botToken + "/editMessageText", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        });
        return resp.ok;
    } catch (e) {
        console.error("[TG] Edit message failed:", e.message);
        return false;
    }
}

// ================================================================
// Token 刷新逻辑
// ================================================================

async function refreshTokenWithLock(env, keyData) {
    var onCooldown = await isRefreshOnCooldown(env, keyData.label);
    if (onCooldown) {
        console.log("[Refresh] On cooldown for:", keyData.label);
        return { success: false, error: "刷新冷却中，请稍后再试" };
    }

    var lockName = "refresh:" + keyData.label;
    var acquired = await acquireLock(env, lockName, 30);

    if (!acquired) {
        console.log("[Refresh] Lock held by another worker for:", keyData.label);
        await sleep(3000);
        var updated = await getKey(env, keyData.label);
        if (updated && updated.expiresAt > Date.now() + 60000) {
            return {
                success: true,
                newToken: updated.accessToken,
                expireStr: new Date(updated.expiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })
            };
        }
        return { success: false, error: "Refresh in progress by another worker" };
    }

    try {
        var result = await refreshSingleKey(env, keyData);
        await setRefreshCooldown(env, keyData.label, result.success ? 60 : 300);
        return result;
    } finally {
        await releaseLock(env, lockName);
    }
}

async function performTokenRefresh(refreshToken) {
    try {
        var resp = await fetch("https://console.anthropic.com/v1/oauth/token", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "User-Agent": "claude-code/2.0.62",
                "Accept": "application/json"
            },
            body: JSON.stringify({
                grant_type: "refresh_token",
                refresh_token: refreshToken,
                client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
            })
        });
        var respText = await resp.text();
        if (!resp.ok) {
            console.error("[Refresh] HTTP Error:", resp.status, respText);
            return { error_detail: "HTTP " + resp.status + ": " + respText };
        }
        try { return JSON.parse(respText); }
        catch (e) { return { error_detail: "Invalid JSON: " + respText.substring(0, 100) }; }
    } catch (err) {
        return { error_detail: "Network error: " + err.message };
    }
}

async function refreshSingleKey(env, keyData) {
    var now = Date.now();
    var refreshed = await performTokenRefresh(keyData.refreshToken);

    if (!refreshed) return { success: false, error: "Refresh returned null" };

    if (refreshed.error_detail) {
        if (refreshed.error_detail.includes("HTTP 401") ||
            refreshed.error_detail.includes("HTTP 400") ||
            refreshed.error_detail.includes("invalid_grant")) {
            keyData.enabled = false;
            await saveKey(env, keyData.label, keyData);
            return { success: false, error: refreshed.error_detail + "\n⚠️ Refresh Token 已失效，已自动禁用" };
        }
        return { success: false, error: refreshed.error_detail };
    }

    if (!refreshed.access_token) {
        return { success: false, error: "No access_token. Response: " + JSON.stringify(refreshed).substring(0, 300) };
    }

    var newExpiresAt = now + ((refreshed.expires_in || 3600) * 1000);
    var expireStr = new Date(newExpiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });

    keyData.accessToken = refreshed.access_token;
    keyData.refreshToken = refreshed.refresh_token || keyData.refreshToken;
    keyData.expiresAt = newExpiresAt;
    keyData.lastRefreshed = new Date().toISOString();

    var saved = await saveKey(env, keyData.label, keyData);
    if (!saved) return { success: false, error: "Storage save failed after refresh" };

    return { success: true, newToken: refreshed.access_token, expireStr: expireStr };
}

async function checkAndRefreshAllKeys(env, forceAll) {
    var keys = await listAllKeys(env);
    var now = Date.now();
    var bufferTime = 10 * 60 * 1000;
    var refreshed = 0, failed = 0, skipped = 0;

    for (var i = 0; i < keys.length; i++) {
        var keyData = keys[i];
        if (!keyData.enabled) { skipped++; continue; }
        var needsRefresh = forceAll || !keyData.expiresAt || keyData.expiresAt < now + bufferTime;
        if (!needsRefresh) { skipped++; continue; }

        var result = await refreshTokenWithLock(env, keyData);
        if (result.success) {
            refreshed++;
            var latestData = await getKey(env, keyData.label);
            await sendTGLong(env,
                "🔄 <b>Token 自动刷新成功</b>\n\n" +
                "📛 Label: <b>" + escHtml(keyData.label) + "</b>\n" +
                "⏰ 新到期: " + result.expireStr + "\n" +
                "🔑 AccessToken: <code>" + maskToken(latestData ? latestData.accessToken : "unknown") + "</code>\n" +
                "🔄 RefreshToken: <code>" + maskToken(latestData ? latestData.refreshToken : "unknown") + "</code>"
            );
        } else {
            failed++;
            await sendTG(env,
                "❌ <b>Token 刷新失败</b>\n\n📛 Label: <b>" + escHtml(keyData.label) + "</b>\n原因: " + escHtml(result.error)
            );
        }
        await sleep(1000);
    }
    return { checked: keys.length, refreshed: refreshed, failed: failed, skipped: skipped };
}

// ================================================================
// 负载均衡
// ================================================================

async function selectKey(env) {
    var keys = await listAllKeys(env);
    var now = Date.now();
    var bufferTime = 2 * 60 * 1000;

    var available = keys.filter(function(k) {
        if (!k.enabled || !k.accessToken || k.expiresAt <= now + bufferTime) return false;
        var recentError = k.lastErrorAt && (now - new Date(k.lastErrorAt).getTime() < 300000);
        if ((k.consecutiveErrors || 0) >= 5 && recentError) return false;
        return true;
    });

    if (available.length === 0) return null;

    var scored = available.map(function(k) {
        var useScore = k.useCount || 0;
        var errorPenalty = (k.errorCount || 0) * 10;
        var recentErr = (k.lastErrorAt && (now - new Date(k.lastErrorAt).getTime() < 300000)) ? 50 : 0;
        var freshBonus = k.lastUsed ? 0 : -5;
        return { key: k, score: useScore + errorPenalty + recentErr + freshBonus };
    });

    scored.sort(function(a, b) { return a.score - b.score; });
    var topN = Math.min(3, scored.length);
    return scored[Math.floor(Math.random() * topN)].key;
}

async function selectKeyWithRefresh(env) {
    var key = await selectKey(env);
    if (key) return key;

    var allKeys = await listAllKeys(env);
    var refreshable = allKeys.filter(function(k) { return k.enabled && k.refreshToken; });

    for (var i = 0; i < refreshable.length; i++) {
        var result = await refreshTokenWithLock(env, refreshable[i]);
        if (result.success) {
            var updated = await getKey(env, refreshable[i].label);
            if (updated && updated.accessToken && updated.expiresAt > Date.now() + 60000) return updated;
        }
    }
    return null;
}

async function recordKeyUsage(env, keyData, success) {
    keyData.useCount = (keyData.useCount || 0) + 1;
    keyData.lastUsed = new Date().toISOString();
    if (!success) {
        keyData.errorCount = (keyData.errorCount || 0) + 1;
        keyData.consecutiveErrors = (keyData.consecutiveErrors || 0) + 1;
        keyData.lastErrorAt = new Date().toISOString();
    } else {
        keyData.consecutiveErrors = 0;
    }
    await saveKey(env, keyData.label, keyData);
    await incrementGlobalStats(env);
}

// ================================================================
// 鉴权 & 格式转换
// ================================================================

function validateCustomToken(authHeader, env) {
    var token = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!token) return false;
    var allowed = (env.CUSTOM_TOKENS || "").split(",").map(function(t) { return t.trim(); }).filter(Boolean);
    if (allowed.length === 0) return false;
    return allowed.indexOf(token) !== -1;
}

function convertContent(content) {
    if (typeof content === "string") return content;
    if (Array.isArray(content)) {
        var parts = [];
        for (var i = 0; i < content.length; i++) {
            var part = content[i];
            if (part.type === "text") {
                parts.push({ type: "text", text: part.text || "" });
            } else if (part.type === "image_url" && part.image_url) {
                var url = part.image_url.url || "";
                var match = url.match(/^data:(image\/[a-zA-Z+]+);base64,(.+)$/);
                if (match) {
                    parts.push({ type: "image", source: { type: "base64", media_type: match[1], data: match[2] } });
                } else {
                    parts.push({ type: "text", text: "[Image: " + url + "]" });
                }
            }
        }
        return parts.length > 0 ? parts : "(empty)";
    }
    return typeof content === "object" ? JSON.stringify(content) : String(content || "");
}

function convertTool(openaiTool) {
    if (openaiTool.type === "function" && openaiTool.function) {
        return {
            name: openaiTool.function.name,
            description: openaiTool.function.description || "",
            input_schema: openaiTool.function.parameters || { type: "object", properties: {} }
        };
    }
    return openaiTool;
}

// [Fix #8] tool_choice 转换
function convertToolChoice(openaiToolChoice) {
    if (!openaiToolChoice) return undefined;
    if (openaiToolChoice === "none") return { type: "none" };  // 注意：Claude 不直接支持 none，但保留语义
    if (openaiToolChoice === "auto") return { type: "auto" };
    if (openaiToolChoice === "required") return { type: "any" };
    if (typeof openaiToolChoice === "object" && openaiToolChoice.type === "function" && openaiToolChoice.function) {
        return { type: "tool", name: openaiToolChoice.function.name };
    }
    return { type: "auto" };
}

function mergeConsecutiveRoles(messages) {
    if (messages.length === 0) return [];
    var merged = [];
    for (var i = 0; i < messages.length; i++) {
        var msg = messages[i];
        if (merged.length > 0 && merged[merged.length - 1].role === msg.role) {
            var last = merged[merged.length - 1];
            var toArr = function(c) {
                if (Array.isArray(c)) return c;
                if (typeof c === "string") return [{ type: "text", text: c }];
                return [c];
            };
            last.content = toArr(last.content).concat(toArr(msg.content));
        } else {
            merged.push({ role: msg.role, content: msg.content });
        }
    }
    return merged;
}

function anthropicToOpenaiResp(data, model) {
    var message = { role: "assistant", content: null };
    var textParts = [];
    var toolCalls = [];
    var reasoningContent = "";

    if (data.content && Array.isArray(data.content)) {
        for (var i = 0; i < data.content.length; i++) {
            var block = data.content[i];
            if (block.type === "text") {
                textParts.push(block.text);
            } else if (block.type === "tool_use") {
                toolCalls.push({
                    id: block.id,
                    type: "function",
                    function: { name: block.name, arguments: JSON.stringify(block.input) }
                });
            } else if (block.type === "thinking") {
                reasoningContent += block.thinking;
            }
        }
    }

    message.content = textParts.join("") || null;
    if (toolCalls.length > 0) message.tool_calls = toolCalls;
    if (reasoningContent) message.reasoning_content = reasoningContent;

    return {
        id: data.id || ("chatcmpl-" + crypto.randomUUID()),
        object: "chat.completion",
        created: Math.floor(Date.now() / 1000),
        model: model,
        choices: [{ index: 0, message: message, finish_reason: mapStopReason(data.stop_reason) }],
        usage: {
            prompt_tokens: (data.usage && data.usage.input_tokens) || 0,
            completion_tokens: (data.usage && data.usage.output_tokens) || 0,
            total_tokens: ((data.usage && data.usage.input_tokens) || 0) + ((data.usage && data.usage.output_tokens) || 0)
        }
    };
}

// ================================================================
// 构建 Anthropic 请求参数（含 Fix #1, #2, #8）
// ================================================================

function buildAnthropicRequest(openaiReq) {
    var systemPrompt = "";
    var rawMessages = [];
    var msgs = openaiReq.messages || [];

    for (var i = 0; i < msgs.length; i++) {
        var m = msgs[i];
        if (m.role === "system") {
            systemPrompt += (typeof m.content === "string" ? m.content : JSON.stringify(m.content)) + "\n";

        } else if (m.role === "user") {
            rawMessages.push({ role: "user", content: convertContent(m.content) });

        } else if (m.role === "assistant") {
            // [Fix #2] 将 assistant 消息中的 tool_calls 转换为 Anthropic tool_use blocks
            var assistantContent = convertContent(m.content);
            if (m.tool_calls && Array.isArray(m.tool_calls) && m.tool_calls.length > 0) {
                // 确保 content 是数组形式
                if (typeof assistantContent === "string") {
                    assistantContent = assistantContent.trim()
                        ? [{ type: "text", text: assistantContent }]
                        : [];
                }
                if (!Array.isArray(assistantContent)) {
                    assistantContent = assistantContent === "(empty)" ? [] : [{ type: "text", text: String(assistantContent) }];
                }
                for (var tc = 0; tc < m.tool_calls.length; tc++) {
                    var call = m.tool_calls[tc];
                    if (call.type === "function" && call.function) {
                        var parsedArgs;
                        try { parsedArgs = JSON.parse(call.function.arguments); }
                        catch (e) { parsedArgs = {}; }
                        assistantContent.push({
                            type: "tool_use",
                            id: call.id,
                            name: call.function.name,
                            input: parsedArgs
                        });
                    }
                }
            }
            rawMessages.push({ role: "assistant", content: assistantContent });

        } else if (m.role === "tool") {
            // [Fix #1] 将 OpenAI tool result 转换为 Anthropic tool_result（属于 user 消息）
            rawMessages.push({
                role: "user",
                content: [{
                    type: "tool_result",
                    tool_use_id: m.tool_call_id,
                    content: (typeof m.content === "string") ? m.content : JSON.stringify(m.content || "")
                }]
            });
        }
    }

    var anthropicMessages = mergeConsecutiveRoles(rawMessages);

    for (var j = 0; j < anthropicMessages.length; j++) {
        anthropicMessages[j].content = sanitizeContent(anthropicMessages[j].content);
    }

    if (anthropicMessages.length === 0) {
        anthropicMessages.push({ role: "user", content: "(empty conversation)" });
    }
    if (anthropicMessages[0].role !== "user") {
        anthropicMessages.unshift({ role: "user", content: "(continued)" });
    }

    var requestedModel = openaiReq.model || "claude-sonnet-4-5";
    var model = MODEL_MAP[requestedModel] || MODEL_MAP["claude-sonnet-4-5"];

    var hasThinking = false;
    var thinkingConfig = null;

    if (openaiReq.thinking) {
        hasThinking = true;
        thinkingConfig = (typeof openaiReq.thinking === "object")
            ? openaiReq.thinking
            : { type: "enabled", budget_tokens: 10000 };
    }

    var modelMax = getModelMaxTokens(model, hasThinking);
    var clientMax = openaiReq.max_tokens || openaiReq.max_completion_tokens || 0;
    var finalMaxTokens = (clientMax > 0) ? Math.min(clientMax, modelMax) : modelMax;

    var anthropicReq = {
        model: model,
        max_tokens: finalMaxTokens,
        messages: anthropicMessages
    };

    if (systemPrompt.trim()) anthropicReq.system = systemPrompt.trim();
    if (openaiReq.stream) anthropicReq.stream = true;

    if (hasThinking && thinkingConfig) {
        if (thinkingConfig.budget_tokens && thinkingConfig.budget_tokens >= finalMaxTokens) {
            thinkingConfig.budget_tokens = Math.floor(finalMaxTokens * 0.75);
        }
        anthropicReq.thinking = thinkingConfig;
    }

    if (openaiReq.tools && Array.isArray(openaiReq.tools)) {
        anthropicReq.tools = openaiReq.tools.map(convertTool);

        // [Fix #8] 转发 tool_choice
        if (openaiReq.tool_choice) {
            var converted = convertToolChoice(openaiReq.tool_choice);
            if (converted) anthropicReq.tool_choice = converted;
        }
    }

    if (hasThinking) {
        anthropicReq.temperature = 1;
    } else if (openaiReq.temperature !== undefined) {
        anthropicReq.temperature = openaiReq.temperature;
    }

    if (openaiReq.top_p !== undefined && !hasThinking) {
        anthropicReq.top_p = openaiReq.top_p;
    }

    console.log("[Build] Model:", model, "MaxTokens:", finalMaxTokens, "Thinking:", hasThinking, "Messages:", anthropicMessages.length);

    return { anthropicReq: anthropicReq, requestedModel: requestedModel, hasThinking: hasThinking };
}

function buildAnthropicHeaders(accessToken, hasThinking) {
    var headers = {
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
        "x-app": "cli",
        "User-Agent": "claude-code/2.0.62"
    };

    var betaFeatures = ["oauth-2025-04-20"];
    if (hasThinking) betaFeatures.push("interleaved-thinking-2025-05-14");
    headers["anthropic-beta"] = betaFeatures.join(",");

    if (accessToken.startsWith("sk-ant-oat")) {
        headers["Authorization"] = "Bearer " + accessToken;
    } else {
        headers["x-api-key"] = accessToken;
    }
    return headers;
}

// ================================================================
// 调用 Anthropic API
// ================================================================

async function callAnthropic(accessToken, anthropicReq, timeoutMs, hasThinking) {
    var headers = buildAnthropicHeaders(accessToken, hasThinking);
    var controller = new AbortController();
    var timeoutId = setTimeout(function() { controller.abort(); }, timeoutMs || 120000);

    try {
        var response = await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: headers,
            body: JSON.stringify(anthropicReq),
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        return { response: response, error: null };
    } catch (err) {
        clearTimeout(timeoutId);
        var isTimeout = err.name === "AbortError";
        return { response: null, error: isTimeout ? "Request timed out" : err.message, isTimeout: isTimeout };
    }
}

// ================================================================
// 流式处理
// ================================================================

function handleStream(anthropicResponse, model) {
    var transformStream = new TransformStream({ highWaterMark: 1024 * 64 });
    var writer = transformStream.writable.getWriter();
    var encoder = new TextEncoder();

    (async function() {
        var reader = anthropicResponse.body.getReader();
        var decoder = new TextDecoder();
        var buffer = "";
        var chatId = "chatcmpl-" + crypto.randomUUID();
        var blockTypes = {};
        var toolCallIndex = -1;
        var lastSendTime = Date.now();
        var keepAliveInterval = 10000;
        var chunkCount = 0;
        var totalTokensProcessed = 0;

        function writeChunk(data) {
            return writer.write(encoder.encode("data: " + JSON.stringify(data) + "\n\n"));
        }

        var heartbeatTimer = setInterval(async function() {
            try {
                if (Date.now() - lastSendTime > keepAliveInterval) {
                    await writeChunk({
                        id: chatId, object: "chat.completion.chunk",
                        created: Math.floor(Date.now() / 1000), model: model,
                        choices: [{ index: 0, delta: {}, finish_reason: null }]
                    });
                    lastSendTime = Date.now();
                }
            } catch (e) { clearInterval(heartbeatTimer); }
        }, keepAliveInterval);

        try {
            while (true) {
                var result = await reader.read();
                if (result.done) break;

                buffer += decoder.decode(result.value, { stream: true });
                var lines = buffer.split("\n");
                buffer = lines.pop();

                for (var li = 0; li < lines.length; li++) {
                    var line = lines[li];
                    if (!line.startsWith("data: ")) continue;
                    var dataStr = line.slice(6).trim();
                    if (!dataStr) continue;

                    try {
                        var event = JSON.parse(dataStr);

                        if (event.type === "message_start") {
                            await writeChunk({
                                id: chatId, object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000), model: model,
                                choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }]
                            });
                            lastSendTime = Date.now();
                            chunkCount++;

                        } else if (event.type === "content_block_start") {
                            var contentBlock = event.content_block;
                            if (contentBlock.type === "tool_use") {
                                blockTypes[event.index] = "tool_use";
                                toolCallIndex++;
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{
                                        index: 0,
                                        delta: {
                                            tool_calls: [{
                                                index: toolCallIndex, id: contentBlock.id,
                                                type: "function",
                                                function: { name: contentBlock.name, arguments: "" }
                                            }]
                                        },
                                        finish_reason: null
                                    }]
                                });
                                lastSendTime = Date.now(); chunkCount++;
                            } else if (contentBlock.type === "thinking") {
                                blockTypes[event.index] = "thinking";
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { reasoning_content: "[思考开始...]\n" }, finish_reason: null }]
                                });
                                lastSendTime = Date.now(); chunkCount++;
                            } else {
                                blockTypes[event.index] = "text";
                            }

                        } else if (event.type === "content_block_delta" && event.delta) {
                            if (event.delta.type === "text_delta") {
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { content: event.delta.text }, finish_reason: null }]
                                });
                            } else if (event.delta.type === "thinking_delta") {
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { reasoning_content: event.delta.thinking }, finish_reason: null }]
                                });
                                totalTokensProcessed += (event.delta.thinking || "").length;
                            } else if (event.delta.type === "input_json_delta") {
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{
                                        index: 0,
                                        delta: { tool_calls: [{ index: toolCallIndex, function: { arguments: event.delta.partial_json } }] },
                                        finish_reason: null
                                    }]
                                });
                            }
                            lastSendTime = Date.now(); chunkCount++;

                        } else if (event.type === "content_block_stop") {
                            if (blockTypes[event.index] === "thinking") {
                                await writeChunk({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { reasoning_content: "\n[思考结束]\n" }, finish_reason: null }]
                                });
                                lastSendTime = Date.now(); chunkCount++;
                            }

                        } else if (event.type === "message_delta") {
                            var finishReason = mapStopReason(event.delta && event.delta.stop_reason);
                            await writeChunk({
                                id: chatId, object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000), model: model,
                                choices: [{ index: 0, delta: {}, finish_reason: finishReason }]
                            });
                            lastSendTime = Date.now(); chunkCount++;
                            if (event.usage) {
                                totalTokensProcessed = event.usage.input_tokens + event.usage.output_tokens;
                            }

                        } else if (event.type === "message_stop") {
                            await writer.write(encoder.encode("data: [DONE]\n\n"));
                            lastSendTime = Date.now();
                        }
                    } catch (e) {
                        console.error("[Stream Parse]", e.message, "line:", line.substring(0, 100));
                    }
                }
            }
        } catch (err) {
            console.error("[Stream] Fatal:", err.message);
            try {
                await writeChunk({
                    id: chatId, object: "chat.completion.chunk",
                    created: Math.floor(Date.now() / 1000), model: model,
                    choices: [{ index: 0, delta: { content: "\n\n❌ 流式传输中断: " + err.message }, finish_reason: "error" }]
                });
            } catch (e) {}
        } finally {
            clearInterval(heartbeatTimer);
            console.log("[Stream] Final - chunks:", chunkCount, "tokens:", totalTokensProcessed);
            try { await writer.close(); } catch (e) {}
        }
    })();

    return new Response(transformStream.readable, {
        headers: {
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "X-Accel-Buffering": "no",
            "Transfer-Encoding": "chunked"
        }
    });
}

// ================================================================
// Telegram 直接对话（含 Fix #3, #4, #6）
// ================================================================

async function handleTGChat(env, chatId, userId, userMessage, replyToMessageId) {
    var session = await getTGSession(env, chatId);
    if (!session) {
        session = { messages: [], model: "claude-sonnet-4-5", createdAt: new Date().toISOString() };
    }

    session.messages.push({ role: "user", content: userMessage });
    if (session.messages.length > 20) {
        session.messages = session.messages.slice(-20);
    }

    // [Fix #6] 检查 sendTGReply 返回值
    var thinkingMsg = await sendTGReply(env, chatId, "🤔 正在思考...", replyToMessageId, "HTML");
    if (!thinkingMsg.ok) {
        console.error("[TG Chat] Failed to send thinking message:", thinkingMsg.error);
        return;
    }

    try {
        var selectedKey = await selectKeyWithRefresh(env);
        if (!selectedKey) {
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ 没有可用的 API Key");
            return;
        }

        var openaiReq = { messages: session.messages, model: session.model, max_tokens: 4096, stream: false };
        var built = buildAnthropicRequest(openaiReq);
        var result = await callAnthropic(selectedKey.accessToken, built.anthropicReq, 180000, built.hasThinking);

        if (result.error) {
            await recordKeyUsage(env, selectedKey, false);
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ 请求失败: " + escHtml(result.error));
            return;
        }

        var response = result.response;
        if (!response.ok) {
            var errorText = await response.text();
            await recordKeyUsage(env, selectedKey, false);
            await editTGMessage(env, chatId, thinkingMsg.messageId,
                "❌ API 错误 (" + response.status + "): " + escHtml(errorText.substring(0, 200)));
            return;
        }

        // [Fix #7] 安全解析 JSON
        var data;
        try { data = await response.json(); }
        catch (e) {
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ API 响应解析失败");
            return;
        }

        await recordKeyUsage(env, selectedKey, true);

        var respObj = anthropicToOpenaiResp(data, session.model);
        var assistantMessage = (respObj.choices && respObj.choices[0] && respObj.choices[0].message && respObj.choices[0].message.content) || "(无回复)";

        session.messages.push({ role: "assistant", content: assistantMessage });
        await saveTGSession(env, chatId, session);

        // [Fix #4] Claude 回复以纯文本发送（不使用 HTML parse_mode），避免代码中的 < > 破坏解析
        var MAX_TG_MSG = 4000;
        if (assistantMessage.length <= MAX_TG_MSG) {
            await editTGMessage(env, chatId, thinkingMsg.messageId, assistantMessage, "");
        } else {
            await editTGMessage(env, chatId, thinkingMsg.messageId, assistantMessage.substring(0, MAX_TG_MSG), "");
            var remaining = assistantMessage.substring(MAX_TG_MSG);
            var partNum = 2;
            while (remaining.length > 0) {
                var chunk = remaining.substring(0, MAX_TG_MSG);
                remaining = remaining.substring(MAX_TG_MSG);
                await sendTGReply(env, chatId, "📄 续 (" + partNum + ")\n\n" + chunk, replyToMessageId, "");
                await sleep(300);
                partNum++;
            }
        }
    } catch (err) {
        console.error("[TG Chat]", err.message);
        await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ 处理出错: " + escHtml(err.message));
    }
}

// ================================================================
// Telegram Webhook 处理（含 Fix #3 鉴权）
// ================================================================

async function setupTelegramWebhook(url, env) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return errorResponse("TELEGRAM_BOT_TOKEN not set", 500);
    var webhookUrl = url.origin + "/telegram/webhook";
    var setBody = { url: webhookUrl };
    if (env.TELEGRAM_WEBHOOK_SECRET) setBody.secret_token = env.TELEGRAM_WEBHOOK_SECRET;
    var resp = await fetch("https://api.telegram.org/bot" + botToken + "/setWebhook", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(setBody)
    });
    var result = await resp.json();
    return corsResponse(JSON.stringify({ webhook_url: webhookUrl, telegram_response: result }));
}

async function handleTelegramWebhook(request, env) {
    if (env.TELEGRAM_WEBHOOK_SECRET) {
        var secretHeader = request.headers.get("X-Telegram-Bot-Api-Secret-Token") || "";
        if (secretHeader !== env.TELEGRAM_WEBHOOK_SECRET) {
            return new Response("Forbidden", { status: 403 });
        }
    }

    var update = await request.json().catch(function() { return null; });
    if (!update || !update.message) return new Response("OK");

    var msg = update.message;
    var chatId = String(msg.chat.id);
    var userId = String(msg.from.id);
    var allowedChatId = String(env.TELEGRAM_CHAT_ID || "");
    var text = (msg.text || "").trim();

    // [Fix #3] 所有消息都需要鉴权，不仅仅是命令
    if (chatId !== allowedChatId) {
        return new Response("OK");
    }

    if (!text) return new Response("OK");

    if (!text.startsWith("/")) {
        await handleTGChat(env, chatId, userId, text, msg.message_id);
        return new Response("OK");
    }

    var parts = text.split(/\s+/);
    var cmd = parts[0].toLowerCase().split("@")[0];
    var args = parts.slice(1);

    try {
        switch (cmd) {
            case "/help":
                await sendTG(env,
                    "🤖 <b>Claude 代理管理 Bot v4.4</b>\n\n" +
                    "<b>直接对话：</b>\n直接发送文字即可与 Claude 对话\n" +
                    "/clear — 清空对话历史\n" +
                    "/model &lt;model&gt; — 切换模型\n\n" +
                    "<b>Key 管理：</b>\n" +
                    "/addkey &lt;label&gt; &lt;JSON&gt; — 添加 Key\n" +
                    "/removekey &lt;label&gt; — 删除 Key\n" +
                    "/listkeys — 列出所有 Key\n" +
                    "/status — 详细状态\n" +
                    "/enable &lt;label&gt; — 启用 Key\n" +
                    "/disable &lt;label&gt; — 禁用 Key\n" +
                    "/refresh &lt;label&gt; — 刷新指定 Key\n" +
                    "/refreshall — 刷新所有\n\n" +
                    "<b>数据库：</b>\n" +
                    "/dbinit — 初始化 D1 数据库\n" +
                    "/dbstatus — D1 状态\n" +
                    "/migrate — KV → D1 数据迁移\n"
                );
                break;

            case "/clear":
                await deleteTGSession(env, chatId);
                await sendTG(env, "✅ 已清空对话历史");
                break;

            case "/model":
                if (args.length < 1) {
                    var session = await getTGSession(env, chatId);
                    var currentModel = (session && session.model) || "claude-sonnet-4-5";
                    await sendTG(env, "当前模型: <b>" + escHtml(currentModel) + "</b>\n\n用法: /model &lt;model_name&gt;\n\n可用: " +
                        Object.keys(MODEL_MAP).slice(0, 20).join(", ") + "...");
                    break;
                }
                var newModel = args[0];
                if (!MODEL_MAP[newModel]) {
                    await sendTG(env, "❌ 未知模型: " + escHtml(newModel));
                    break;
                }
                var mSession = await getTGSession(env, chatId);
                if (!mSession) mSession = { messages: [], model: newModel, createdAt: new Date().toISOString() };
                mSession.model = newModel;
                await saveTGSession(env, chatId, mSession);
                await sendTG(env, "✅ 模型已切换为: <b>" + escHtml(newModel) + "</b>");
                break;

            case "/addkey":
                if (args.length < 2) {
                    await sendTG(env, "⚠️ 格式：/addkey &lt;label&gt; &lt;JSON配置&gt;");
                    break;
                }
                var addLabel = args[0];
                var addParsed;
                try { addParsed = JSON.parse(args.slice(1).join(" ")); }
                catch (e) { await sendTG(env, "❌ JSON解析失败"); break; }

                var addOauth = addParsed.claudeAiOauth;
                if (!addOauth || !addOauth.accessToken || !addOauth.refreshToken) {
                    await sendTG(env, "❌ 缺少 Token 数据"); break;
                }

                var addKeyData = {
                    label: addLabel,
                    accessToken: addOauth.accessToken,
                    refreshToken: addOauth.refreshToken,
                    expiresAt: addOauth.expiresAt || 0,
                    scopes: addOauth.scopes || [],
                    subscriptionType: addOauth.subscriptionType || "unknown",
                    rateLimitTier: addOauth.rateLimitTier || "default",
                    enabled: true, useCount: 0, errorCount: 0, consecutiveErrors: 0
                };

                await saveKey(env, addLabel, addKeyData);
                await deleteTGMessage(env, chatId, msg.message_id);
                await sendTG(env,
                    "✅ <b>Key 保存成功</b>\n📛 " + escHtml(addLabel) + "\n" +
                    "🔑 Token: <code>" + maskToken(addOauth.accessToken) + "</code>\n" +
                    "⚠️ 原消息已删除\n\n自动验证中..."
                );

                var addRefreshResult = await refreshTokenWithLock(env, addKeyData);
                if (addRefreshResult.success) {
                    await sendTG(env, "✅ <b>Token验证并刷新成功</b>");
                } else {
                    await sendTG(env, "❌ <b>Token验证失败：</b>\n" + escHtml(addRefreshResult.error));
                }
                break;

            case "/removekey":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/removekey &lt;label&gt;"); break; }
                await deleteKey(env, args[0]);
                await sendTG(env, "🗑️ 已删除: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/enable":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/enable &lt;label&gt;"); break; }
                var enKey = await getKey(env, args[0]);
                if (!enKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                enKey.enabled = true;
                enKey.consecutiveErrors = 0;
                await saveKey(env, enKey.label, enKey);
                await sendTG(env, "✅ 已启用: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/disable":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/disable &lt;label&gt;"); break; }
                var disKey = await getKey(env, args[0]);
                if (!disKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                disKey.enabled = false;
                await saveKey(env, disKey.label, disKey);
                await sendTG(env, "⏸️ 已禁用: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/listkeys":
                var allKeys = await listAllKeys(env);
                if (allKeys.length === 0) { await sendTG(env, "📭 没有 Key"); break; }
                var nowLk = Date.now();
                var listText = "📋 <b>Key 列表 (" + allKeys.length + ")</b>\n\n";
                for (var ki = 0; ki < allKeys.length; ki++) {
                    var k = allKeys[ki];
                    var remainMin = k.expiresAt ? Math.round((k.expiresAt - nowLk) / 60000) : "?";
                    var icon = !k.enabled ? "⏸️" : (remainMin > 0 ? "✅" : "❌");
                    listText += icon + " <b>" + escHtml(k.label) + "</b> (" + remainMin + "分) | 用" + (k.useCount || 0) + " 错" + (k.errorCount || 0) + " 连错" + (k.consecutiveErrors || 0) + "\n";
                }
                await sendTGLong(env, listText);
                break;

            case "/status":
                var statusKeys = await listAllKeys(env);
                var stats = await getGlobalStats(env);
                var nowSt = Date.now();
                var enabledCount = 0, availableCount = 0, totalUse = 0, totalErr = 0;
                for (var si = 0; si < statusKeys.length; si++) {
                    var sk = statusKeys[si];
                    if (sk.enabled) enabledCount++;
                    if (sk.enabled && sk.accessToken && sk.expiresAt > nowSt + 120000) availableCount++;
                    totalUse += (sk.useCount || 0);
                    totalErr += (sk.errorCount || 0);
                }

                var storageBackend = hasD1(env) ? "D1 (primary) + KV (fallback)" : (env.TOKEN_STORE ? "KV only" : "None ⚠️");

                var statusText = "📊 <b>系统状态 v4.4</b>\n\n" +
                    "💾 存储后端: " + storageBackend + "\n" +
                    "🔑 总 Key: " + statusKeys.length + " | ✅ 启用: " + enabledCount + " | 🟢 可用: " + availableCount + "\n\n" +
                    "📈 总请求: " + (stats.totalRequests || 0) + "\n" +
                    "📅 今日: " + (stats.todayRequests || 0) + " (" + (stats.today || "N/A") + ")\n" +
                    "📊 总调用: " + totalUse + " | 总错误: " + totalErr + "\n\n";

                for (var sj = 0; sj < statusKeys.length; sj++) {
                    var sd = statusKeys[sj];
                    var sRemain = sd.expiresAt ? Math.round((sd.expiresAt - nowSt) / 60000) : "?";
                    var sIcon = !sd.enabled ? "⏸️" : (sRemain > 0 ? "✅" : "❌");
                    statusText += sIcon + " <b>" + escHtml(sd.label) + "</b>\n";
                    statusText += "   剩余: " + sRemain + "分 | 用: " + (sd.useCount || 0) + " | 错: " + (sd.errorCount || 0) + " | 连错: " + (sd.consecutiveErrors || 0) + "\n";
                    if (sd.lastRefreshed) statusText += "   刷新: " + sd.lastRefreshed + "\n";
                    if (sd.lastUsed) statusText += "   使用: " + sd.lastUsed + "\n";
                    statusText += "\n";
                }
                await sendTGLong(env, statusText);
                break;

            case "/refresh":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/refresh &lt;label&gt;"); break; }
                var rKey = await getKey(env, args[0]);
                if (!rKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                var rResult = await refreshTokenWithLock(env, rKey);
                if (rResult.success) {
                    await sendTG(env, "✅ <b>刷新成功</b>\n📛 " + escHtml(args[0]) + "\n⏰ 到期: " + rResult.expireStr);
                } else {
                    await sendTG(env, "❌ <b>刷新失败</b>\n" + escHtml(rResult.error));
                }
                break;

            case "/refreshall":
                await sendTG(env, "🔄 正在刷新...");
                var raResult = await checkAndRefreshAllKeys(env, true);
                await sendTG(env, "✅ <b>批量刷新完成</b>\n成功: " + raResult.refreshed + " | 失败: " + raResult.failed + " | 跳过: " + raResult.skipped);
                break;

            // ============ D1 管理命令 ============

            case "/dbinit":
                var initResult = await initD1(env);
                if (initResult.ok) {
                    await sendTG(env, "✅ <b>D1 数据库初始化成功</b>\n创建了 " + initResult.tables + " 张表");
                } else {
                    await sendTG(env, "❌ <b>D1 初始化失败</b>\n" + escHtml(initResult.error));
                }
                break;

            case "/dbstatus":
                if (!hasD1(env)) {
                    await sendTG(env, "⚠️ 未配置 D1 数据库绑定 (env.DB)\n当前使用: " + (env.TOKEN_STORE ? "KV" : "无存储"));
                    break;
                }
                try {
                    var dbKeys = await env.DB.prepare("SELECT COUNT(*) as cnt FROM api_keys").first();
                    var dbSessions = await env.DB.prepare("SELECT COUNT(*) as cnt FROM tg_sessions").first();
                    var dbLocks = await env.DB.prepare("SELECT COUNT(*) as cnt FROM locks").first();
                    var dbCooldowns = await env.DB.prepare("SELECT COUNT(*) as cnt FROM cooldowns").first();
                    await sendTG(env,
                        "💾 <b>D1 数据库状态</b>\n\n" +
                        "🔑 api_keys: " + (dbKeys ? dbKeys.cnt : 0) + " 条\n" +
                        "💬 tg_sessions: " + (dbSessions ? dbSessions.cnt : 0) + " 条\n" +
                        "🔒 locks: " + (dbLocks ? dbLocks.cnt : 0) + " 条\n" +
                        "⏳ cooldowns: " + (dbCooldowns ? dbCooldowns.cnt : 0) + " 条\n"
                    );
                } catch (e) {
                    await sendTG(env, "❌ D1 查询失败: " + escHtml(e.message) + "\n提示：先执行 /dbinit");
                }
                break;

            case "/migrate":
                if (!hasD1(env)) {
                    await sendTG(env, "⚠️ 未配置 D1，无法迁移"); break;
                }
                if (!env.TOKEN_STORE) {
                    await sendTG(env, "⚠️ 未配置 KV，无数据可迁移"); break;
                }
                await sendTG(env, "🔄 正在从 KV 迁移到 D1...");
                var migrateResult = await migrateKVtoD1(env);
                await sendTG(env,
                    "✅ <b>迁移完成</b>\n" +
                    "🔑 Keys: " + migrateResult.keys + "\n" +
                    "📊 Stats: " + (migrateResult.stats ? "✅" : "⏭️") + "\n" +
                    "❌ Errors: " + migrateResult.errors
                );
                break;

            default:
                await sendTG(env, "❓ 未知命令，输入 /help 查看帮助");
        }
    } catch (err) {
        console.error("[TG Command]", err.message);
        await sendTG(env, "❌ 执行出错: " + escHtml(err.message));
    }
    return new Response("OK");
}

// ================================================================
// KV → D1 数据迁移
// ================================================================

async function migrateKVtoD1(env) {
    var result = { keys: 0, stats: false, errors: 0 };

    if (!env.TOKEN_STORE || !env.DB) return result;

    // 先确保表存在
    await initD1(env);

    // 迁移 Keys
    try {
        var kvList = await env.TOKEN_STORE.list({ prefix: "key:" });
        for (var i = 0; i < kvList.keys.length; i++) {
            try {
                var data = await env.TOKEN_STORE.get(kvList.keys[i].name, { type: "json" });
                if (data && data.label) {
                    await env.DB.prepare(
                        "INSERT OR REPLACE INTO api_keys (label, data, updated_at) VALUES (?, ?, datetime('now'))"
                    ).bind(data.label, JSON.stringify(data)).run();
                    result.keys++;
                }
            } catch (e) {
                result.errors++;
                console.error("[Migrate Key]", e.message);
            }
        }
    } catch (e) {
        console.error("[Migrate Keys List]", e.message);
        result.errors++;
    }

    // 迁移 Stats
    try {
        var stats = await env.TOKEN_STORE.get("stats:global", { type: "json" });
        if (stats) {
            await env.DB.prepare(
                "INSERT OR REPLACE INTO global_stats (id, data, updated_at) VALUES ('global', ?, datetime('now'))"
            ).bind(JSON.stringify(stats)).run();
            result.stats = true;
        }
    } catch (e) {
        console.error("[Migrate Stats]", e.message);
        result.errors++;
    }

    return result;
}

// ================================================================
// 主请求处理（含重试 + 403/401 区分 + Fix #7）
// ================================================================

async function handleChatCompletions(request, env) {
    var authHeader = request.headers.get("Authorization") || "";
    if (!validateCustomToken(authHeader, env)) {
        return errorResponse("Invalid API key", 401);
    }

    var openaiReq;
    try { openaiReq = await request.json(); }
    catch (e) { return errorResponse("Invalid JSON body", 400); }

    var built = buildAnthropicRequest(openaiReq);
    var anthropicReq = built.anthropicReq;
    var requestedModel = built.requestedModel;
    var hasThinking = built.hasThinking;

    var estimatedOutputTokens = anthropicReq.max_tokens || 8192;
    var timeoutMs = 120000;
    if (hasThinking) timeoutMs = 600000;
    else if (estimatedOutputTokens > 32000) timeoutMs = 300000;
    else if (estimatedOutputTokens > 16000) timeoutMs = 240000;

    console.log("[API] MaxTokens:", estimatedOutputTokens, "Timeout:", timeoutMs, "Thinking:", hasThinking);

    var MAX_RETRIES = 3;
    var lastErrorBody = "";
    var lastErrorStatus = 502;
    var triedKeys = {};

    for (var attempt = 0; attempt < MAX_RETRIES; attempt++) {
        var selectedKey = await selectKeyWithRefresh(env);
        if (!selectedKey) {
            return errorResponse("No available API keys. All keys may be expired or disabled.", 503);
        }

        // 避免反复选同一个刚失败的 Key
        if (triedKeys[selectedKey.label] && attempt < MAX_RETRIES - 1) {
            var allAvail = await listAllKeys(env);
            var altKey = null;
            for (var ak = 0; ak < allAvail.length; ak++) {
                if (allAvail[ak].enabled && allAvail[ak].accessToken &&
                    allAvail[ak].expiresAt > Date.now() + 120000 &&
                    !triedKeys[allAvail[ak].label]) {
                    altKey = allAvail[ak];
                    break;
                }
            }
            if (altKey) selectedKey = altKey;
        }

        var keyLabel = selectedKey.label;
        triedKeys[keyLabel] = true;
        console.log("[API] Attempt", attempt + 1, "key:", keyLabel);

        var result = await callAnthropic(selectedKey.accessToken, anthropicReq, timeoutMs, hasThinking);

        if (result.error) {
            await recordKeyUsage(env, selectedKey, false);
            lastErrorBody = result.error;
            lastErrorStatus = result.isTimeout ? 504 : 502;
            continue;
        }

        var response = result.response;

        if (response.ok) {
            await recordKeyUsage(env, selectedKey, true);
            if (openaiReq.stream) {
                return handleStream(response, requestedModel);
            }
            // [Fix #7] 安全解析
            var data;
            try { data = await response.json(); }
            catch (e) { return errorResponse("Failed to parse Anthropic response", 502); }
            return corsResponse(JSON.stringify(anthropicToOpenaiResp(data, requestedModel)));
        }

        var errorBody = await response.text().catch(function() { return "Unknown error"; });
        await recordKeyUsage(env, selectedKey, false);
        console.log("[API] Error", response.status, "key:", keyLabel, errorBody.substring(0, 200));

        // 401 → Token 过期，尝试刷新
        if (response.status === 401) {
            await sendTG(env, "⚠️ <b>Key Token 过期 (401)</b>\n📛 " + escHtml(keyLabel) + "\n尝试自动刷新...");
            var refreshResult = await refreshTokenWithLock(env, selectedKey);
            if (refreshResult.success) {
                await sendTG(env, "✅ 刷新成功，重试中...");
                delete triedKeys[keyLabel];
                continue;
            } else {
                selectedKey.expiresAt = 0;
                await saveKey(env, selectedKey.label, selectedKey);
                await sendTG(env, "❌ 刷新失败: " + escHtml(refreshResult.error));
                continue;
            }
        }

        // 403 → 封禁，直接禁用
        if (response.status === 403) {
            selectedKey.enabled = false;
            selectedKey.consecutiveErrors = (selectedKey.consecutiveErrors || 0) + 1;
            selectedKey.lastErrorAt = new Date().toISOString();
            selectedKey.disableReason = "403 Forbidden at " + new Date().toISOString();
            await saveKey(env, selectedKey.label, selectedKey);
            await sendTG(env,
                "🚫 <b>Key 被封禁 (403)</b>\n📛 " + escHtml(keyLabel) +
                "\n❌ 已自动禁用\n📝 " + escHtml(errorBody.substring(0, 200))
            );
            continue;
        }

        // 429 / 5xx → 换 Key 重试
        if (response.status === 429 || response.status >= 500) {
            lastErrorBody = errorBody;
            lastErrorStatus = response.status;
            if (response.status === 429) await sleep(2000);
            continue;
        }

        // 400 等客户端错误 → 不重试
        return new Response(errorBody, {
            status: response.status,
            headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
        });
    }

    return new Response(lastErrorBody || JSON.stringify({ error: { message: "All retries failed", type: "api_error" } }), {
        status: lastErrorStatus,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
}

// ================================================================
// 入口
// ================================================================

export default {
    async fetch(request, env, ctx) {
        if (request.method === "OPTIONS") {
            return new Response(null, {
                status: 204,
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            });
        }

        var url = new URL(request.url);

        try {
            if (url.pathname === "/telegram/webhook" && request.method === "POST") {
                return await handleTelegramWebhook(request, env);
            }
            if (url.pathname === "/telegram/setup" && request.method === "GET") {
                return await setupTelegramWebhook(url, env);
            }
            if (url.pathname === "/v1/models" && request.method === "GET") {
                return corsResponse(JSON.stringify({ object: "list", data: SUPPORTED_MODELS }));
            }
            if (url.pathname === "/v1/chat/completions" && request.method === "POST") {
                return await handleChatCompletions(request, env);
            }

            // D1 初始化 HTTP 端点（需鉴权）
            if (url.pathname === "/d1/init" && request.method === "POST") {
                var authH = request.headers.get("Authorization") || "";
                if (!validateCustomToken(authH, env)) {
                    return errorResponse("Unauthorized", 401);
                }
                var initRes = await initD1(env);
                return corsResponse(JSON.stringify(initRes));
            }

            // D1 状态 HTTP 端点
            if (url.pathname === "/d1/status" && request.method === "GET") {
                var authH2 = request.headers.get("Authorization") || "";
                if (!validateCustomToken(authH2, env)) {
                    return errorResponse("Unauthorized", 401);
                }
                if (!hasD1(env)) {
                    return corsResponse(JSON.stringify({ d1: false, backend: env.TOKEN_STORE ? "kv" : "none" }));
                }
                try {
                    var counts = {};
                    var tables = ["api_keys", "global_stats", "tg_sessions", "locks", "cooldowns"];
                    for (var t = 0; t < tables.length; t++) {
                        var row = await env.DB.prepare("SELECT COUNT(*) as cnt FROM " + tables[t]).first();
                        counts[tables[t]] = row ? row.cnt : 0;
                    }
                    return corsResponse(JSON.stringify({ d1: true, tables: counts }));
                } catch (e) {
                    return corsResponse(JSON.stringify({ d1: true, error: e.message, hint: "Run POST /d1/init first" }));
                }
            }

            return errorResponse("Not Found", 404);
        } catch (err) {
            console.error("[Fatal]", err.message, err.stack);
            return errorResponse("Internal Error: " + err.message, 500);
        }
    },

    async scheduled(event, env, ctx) {
        ctx.waitUntil((async function() {
            try {
                // 先清理 D1 过期数据
                await cleanupD1(env);
                // 再刷新 tokens
                await checkAndRefreshAllKeys(env);
            } catch (err) {
                console.error("[Scheduled]", err.message);
                await sendTG(env, "🚨 <b>定时任务异常</b>\n" + escHtml(err.message)).catch(function() {});
            }
        })());
    }
};
