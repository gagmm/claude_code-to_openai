// ================================================================
// Claude API 代理 v4.1 (完整版)
// 功能：自定义Token鉴权 / Telegram Bot管理 / 多Key负载均衡 / 自动刷新
//       分布式锁 / 自动重试 / 流式thinking+tool_calls / 安全加固
//       Telegram 直接对话 / 会话管理 / 连接保活
// ================================================================

const MODEL_MAP = {
  "claude-opus-4-6": "claude-opus-4-6",
  "claude-opus-4-6-latest": "claude-opus-4-6",
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
  "claude-3-5-haiku": "claude-3-5-haiku-20241022",
  "claude-3-5-haiku-20241022": "claude-3-5-haiku-20241022",
  "claude-3-opus": "claude-3-opus-20240229",
  "claude-3-opus-20240229": "claude-3-opus-20240229",
  "claude-3-haiku": "claude-3-haiku-20240307",
  "claude-3-haiku-20240307": "claude-3-haiku-20240307",
  "claude-2.1": "claude-2-1",
  "claude-2-1": "claude-2-1",
  "claude-2.0": "claude-2-0",
  "claude-2-0": "claude-2-0",
  "claude-instant-1.2": "claude-instant-1-2",
  "claude-instant-1-2": "claude-instant-1-2"
};

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
        error: {
            message: message,
            type: "api_error",
            code: status
        }
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
    var map = {
        "end_turn": "stop",
        "stop_sequence": "stop",
        "max_tokens": "length",
        "tool_use": "tool_calls"
    };
    return map[r] || "stop";
}

function maskToken(token) {
    if (!token) return "[empty]";
    if (token.length <= 16) return token.substring(0, 4) + "...[HIDDEN]";
    return token.substring(0, 10) + "...[HIDDEN]";
}

// ================================================================
// Telegram 发送函数
// ================================================================

async function sendTG(env, message) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    var chatId = env.TELEGRAM_CHAT_ID;
    if (!botToken || !chatId) {
        console.warn("[TG] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID");
        return false;
    }
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
            var errText = await resp.text();
            console.error("[TG] Send failed:", resp.status, errText);
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
    if (message.length <= MAX_LEN) {
        return await sendTG(env, message);
    }
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

async function sendTGReply(env, chatId, message, replyToMessageId) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return { ok: false, error: "No bot token" };
    try {
        var body = {
            chat_id: chatId,
            text: message,
            parse_mode: "HTML",
            disable_web_page_preview: true
        };
        if (replyToMessageId) {
            body.reply_to_message_id = replyToMessageId;
        }
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

async function editTGMessage(env, chatId, messageId, newText) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) return false;
    try {
        var resp = await fetch("https://api.telegram.org/bot" + botToken + "/editMessageText", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId,
                text: newText,
                parse_mode: "HTML",
                disable_web_page_preview: true
            })
        });
        return resp.ok;
    } catch (e) {
        console.error("[TG] Edit message failed:", e.message);
        return false;
    }
}

// ================================================================
// KV 存储操作
// ================================================================

async function saveKey(env, label, data) {
    if (!env.TOKEN_STORE) return false;
    try {
        await env.TOKEN_STORE.put("key:" + label, JSON.stringify(data));
        return true;
    } catch (e) {
        console.error("[KV Save]", e.message);
        return false;
    }
}

async function getKey(env, label) {
    if (!env.TOKEN_STORE) return null;
    try {
        return await env.TOKEN_STORE.get("key:" + label, { type: "json" });
    } catch (e) {
        console.error("[KV Get]", e.message);
        return null;
    }
}

async function deleteKey(env, label) {
    if (!env.TOKEN_STORE) return;
    try {
        await env.TOKEN_STORE.delete("key:" + label);
    } catch (e) {
        console.error("[KV Delete]", e.message);
    }
}

async function listAllKeys(env) {
    if (!env.TOKEN_STORE) return [];
    try {
        var list = await env.TOKEN_STORE.list({ prefix: "key:" });
        var results = await Promise.all(
            list.keys.map(function(k) {
                return env.TOKEN_STORE.get(k.name, { type: "json" });
            })
        );
        return results.filter(Boolean);
    } catch (e) {
        console.error("[KV List]", e.message);
        return [];
    }
}

async function getGlobalStats(env) {
    if (!env.TOKEN_STORE) return {};
    try {
        return await env.TOKEN_STORE.get("stats:global", { type: "json" }) || {};
    } catch (e) {
        return {};
    }
}

async function incrementGlobalStats(env) {
    if (!env.TOKEN_STORE) return;
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
        await env.TOKEN_STORE.put("stats:global", JSON.stringify(stats));
    } catch (e) {
        console.error("[Stats]", e.message);
    }
}

// ================================================================
// Telegram 会话管理
// ================================================================

async function getTGSession(env, chatId) {
    if (!env.TOKEN_STORE) return null;
    try {
        return await env.TOKEN_STORE.get("tg:session:" + chatId, { type: "json" });
    } catch (e) {
        return null;
    }
}

async function saveTGSession(env, chatId, session) {
    if (!env.TOKEN_STORE) return false;
    try {
        await env.TOKEN_STORE.put("tg:session:" + chatId, JSON.stringify(session), { expirationTtl: 86400 }); // 24小时过期
        return true;
    } catch (e) {
        console.error("[TG Session Save]", e.message);
        return false;
    }
}

async function deleteTGSession(env, chatId) {
    if (!env.TOKEN_STORE) return;
    try {
        await env.TOKEN_STORE.delete("tg:session:" + chatId);
    } catch (e) {
        console.error("[TG Session Delete]", e.message);
    }
}

// ================================================================
// 分布式锁 (KV-based)
// ================================================================

async function acquireLock(env, lockName, ttlSeconds) {
    if (!env.TOKEN_STORE) return true;
    var lockKey = "lock:" + lockName;
    try {
        var existing = await env.TOKEN_STORE.get(lockKey);
        if (existing) {
            return false;
        }
        await env.TOKEN_STORE.put(lockKey, String(Date.now()), { expirationTtl: ttlSeconds || 30 });
        return true;
    } catch (e) {
        console.error("[Lock] Acquire error:", e.message);
        return true;
    }
}

async function releaseLock(env, lockName) {
    if (!env.TOKEN_STORE) return;
    try {
        await env.TOKEN_STORE.delete("lock:" + lockName);
    } catch (e) {
        console.error("[Lock] Release error:", e.message);
    }
}

// ================================================================
// Token 刷新逻辑 (v4.1)
// ================================================================

async function refreshTokenWithLock(env, keyData) {
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
        return { success: false, error: "Refresh in progress by another worker, result not yet available" };
    }

    try {
        return await refreshSingleKey(env, keyData);
    } finally {
        await releaseLock(env, lockName);
    }
}

async function performTokenRefresh(refreshToken) {
    try {
        var body = JSON.stringify({
            grant_type: "refresh_token",
            refresh_token: refreshToken,
            client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
        });

        console.log("[Refresh] Sending JSON request to Anthropic. Prefix:", refreshToken.substring(0, 10));

        var resp = await fetch("https://console.anthropic.com/v1/oauth/token", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "User-Agent": "claude-code/2.0.62",
                "Accept": "application/json"
            },
            body: body
        });

        var respText = await resp.text();

        if (!resp.ok) {
            console.error("[Refresh] HTTP Error:", resp.status, respText);
            return { error_detail: "HTTP " + resp.status + ": " + respText };
        }

        try {
            return JSON.parse(respText);
        } catch (e) {
            return { error_detail: "Invalid JSON: " + respText.substring(0, 100) };
        }
    } catch (err) {
        return { error_detail: "Network error: " + err.message };
    }
}

async function refreshSingleKey(env, keyData) {
    var now = Date.now();
    var refreshed = await performTokenRefresh(keyData.refreshToken);

    if (!refreshed) {
        return { success: false, error: "Refresh returned null (Network issue?)" };
    }

    if (refreshed.error_detail) {
        if (refreshed.error_detail.includes("HTTP 401") ||
            refreshed.error_detail.includes("HTTP 400") ||
            refreshed.error_detail.includes("invalid_grant")) {
            keyData.enabled = false;
            await saveKey(env, keyData.label, keyData);
            return { success: false, error: refreshed.error_detail + "\n⚠️ (Refresh Token 已彻底失效，系统已自动禁用该 Key)" };
        }
        return { success: false, error: refreshed.error_detail };
    }

    if (!refreshed.access_token) {
        var debugInfo = JSON.stringify(refreshed).substring(0, 300);
        return { success: false, error: "No access_token. Response: " + debugInfo };
    }

    var newExpiresAt = now + ((refreshed.expires_in || 3600) * 1000);
    var expireStr = new Date(newExpiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });

    keyData.accessToken = refreshed.access_token;
    keyData.refreshToken = refreshed.refresh_token || keyData.refreshToken;
    keyData.expiresAt = newExpiresAt;
    keyData.lastRefreshed = new Date().toISOString();

    var saved = await saveKey(env, keyData.label, keyData);
    if (!saved) {
        return { success: false, error: "KV Save failed after refresh" };
    }

    return { success: true, newToken: refreshed.access_token, expireStr: expireStr };
}

async function checkAndRefreshAllKeys(env, forceAll) {
    var keys = await listAllKeys(env);
    var now = Date.now();
    var bufferTime = 10 * 60 * 1000;
    var refreshed = 0;
    var failed = 0;
    var skipped = 0;

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
                "❌ <b>Token 刷新失败</b>\n\n" +
                "📛 Label: <b>" + escHtml(keyData.label) + "</b>\n" +
                "原因: " + escHtml(result.error)
            );
        }
        await sleep(1000);
    }

    return { checked: keys.length, refreshed: refreshed, failed: failed, skipped: skipped };
}

// ================================================================
// 负载均衡（含兜底刷新）
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

    if (available.length === 0) {
        return null;
    }

    var scored = available.map(function(k) {
        var useScore = k.useCount || 0;
        var errorPenalty = (k.errorCount || 0) * 10;
        var recentErr = (k.lastErrorAt && (now - new Date(k.lastErrorAt).getTime() < 300000)) ? 50 : 0;
        var freshBonus = k.lastUsed ? 0 : -5;
        return { key: k, score: useScore + errorPenalty + recentErr + freshBonus };
    });

    scored.sort(function(a, b) { return a.score - b.score; });

    var topN = Math.min(3, scored.length);
    var selected = scored[Math.floor(Math.random() * topN)];
    return selected.key;
}

async function selectKeyWithRefresh(env) {
    var key = await selectKey(env);
    if (key) return key;

    var allKeys = await listAllKeys(env);
    var refreshable = allKeys.filter(function(k) {
        return k.enabled && k.refreshToken;
    });

    for (var i = 0; i < refreshable.length; i++) {
        var result = await refreshTokenWithLock(env, refreshable[i]);
        if (result.success) {
            var updated = await getKey(env, refreshable[i].label);
            if (updated && updated.accessToken && updated.expiresAt > Date.now() + 60000) {
                return updated;
            }
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
                parts.push({ type: "text", text: part.text });
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
    return typeof content === "object" ? JSON.stringify(content) : String(content);
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

function mergeConsecutiveRoles(messages) {
    if (messages.length === 0) return [];
    var merged = [];
    for (var i = 0; i < messages.length; i++) {
        var msg = messages[i];
        if (merged.length > 0 && merged[merged.length - 1].role === msg.role) {
            var last = merged[merged.length - 1];
            if (typeof last.content === "string" && typeof msg.content === "string") {
                last.content += "\n\n" + msg.content;
            } else {
                var toArr = function(c) {
                    if (Array.isArray(c)) return c;
                    if (typeof c === "string") return [{ type: "text", text: c }];
                    return [c];
                };
                last.content = toArr(last.content).concat(toArr(msg.content));
            }
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

    var fullText = textParts.join("");
    message.content = fullText || null;
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
// 构建 Anthropic 请求参数
// ================================================================

function buildAnthropicRequest(openaiReq) {
    var systemPrompt = "";
    var rawMessages = [];
    var msgs = openaiReq.messages || [];

    for (var i = 0; i < msgs.length; i++) {
        var m = msgs[i];
        if (m.role === "system") {
            systemPrompt += (typeof m.content === "string" ? m.content : JSON.stringify(m.content)) + "\n";
        } else if (m.role === "user" || m.role === "assistant") {
            rawMessages.push({ role: m.role, content: convertContent(m.content) });
        }
    }

    var anthropicMessages = mergeConsecutiveRoles(rawMessages);
    if (anthropicMessages.length > 0 && anthropicMessages[0].role !== "user") {
        anthropicMessages.unshift({ role: "user", content: "(continued)" });
    }

    var requestedModel = openaiReq.model || "claude-sonnet-4-5";
    var model = MODEL_MAP[requestedModel] || MODEL_MAP["claude-sonnet-4-5"];

    var anthropicReq = {
        model: model,
        max_tokens: openaiReq.max_tokens || 8192,
        messages: anthropicMessages
    };

    if (systemPrompt.trim()) anthropicReq.system = systemPrompt.trim();
    if (openaiReq.stream) anthropicReq.stream = true;

    if (openaiReq.thinking) {
        anthropicReq.thinking = openaiReq.thinking;
    }

    if (openaiReq.tools && Array.isArray(openaiReq.tools)) {
        anthropicReq.tools = openaiReq.tools.map(convertTool);
    }

    return { anthropicReq: anthropicReq, requestedModel: requestedModel };
}

function buildAnthropicHeaders(accessToken) {
    var headers = {
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
        "anthropic-beta": "oauth-2025-04-20",
        "x-app": "cli",
        "User-Agent": "claude-code/2.0.62"
    };

    if (accessToken.startsWith("sk-ant-oat")) {
        headers["Authorization"] = "Bearer " + accessToken;
    } else {
        headers["x-api-key"] = accessToken;
    }

    return headers;
}

// ================================================================
// 调用 Anthropic API（单次）
// ================================================================

async function callAnthropic(accessToken, anthropicReq, timeoutMs) {
    var headers = buildAnthropicHeaders(accessToken);
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
// 流式处理（支持连接保活，防止断断续续）
// ================================================================

function handleStream(anthropicResponse, model) {
    var transformStream = new TransformStream();
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
        var keepAliveInterval = 20000; // 20秒心跳

        function writeChunk(data) {
            return writer.write(encoder.encode("data: " + JSON.stringify(data) + "\n\n"));
        }

        // 发送心跳包，防止连接断开
        var heartbeatTimer = setInterval(async function() {
            try {
                if (Date.now() - lastSendTime > keepAliveInterval) {
                    await writeChunk({
                        id: chatId,
                        object: "chat.completion.chunk",
                        created: Math.floor(Date.now() / 1000),
                        model: model,
                        choices: [{ index: 0, delta: {}, finish_reason: null }]
                    });
                    lastSendTime = Date.now();
                }
            } catch (e) {
                clearInterval(heartbeatTimer);
            }
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
                    if (!dataStr || dataStr === "[DONE]") continue;

                    try {
                        var event = JSON.parse(dataStr);

                        if (event.type === "message_start") {
                            await writeChunk({
                                id: chatId,
                                object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000),
                                model: model,
                                choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }]
                            });
                            lastSendTime = Date.now();

                        } else if (event.type === "content_block_start") {
                            var blockIndex = event.index;
                            var contentBlock = event.content_block;

                            if (contentBlock.type === "tool_use") {
                                blockTypes[blockIndex] = "tool_use";
                                toolCallIndex++;
                                await writeChunk({
                                    id: chatId,
                                    object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000),
                                    model: model,
                                    choices: [{
                                        index: 0,
                                        delta: {
                                            tool_calls: [{
                                                index: toolCallIndex,
                                                id: contentBlock.id,
                                                type: "function",
                                                function: {
                                                    name: contentBlock.name,
                                                    arguments: ""
                                                }
                                            }]
                                        },
                                        finish_reason: null
                                    }]
                                });
                                lastSendTime = Date.now();
                            } else if (contentBlock.type === "thinking") {
                                blockTypes[blockIndex] = "thinking";
                            } else {
                                blockTypes[blockIndex] = "text";
                            }

                        } else if (event.type === "content_block_delta" && event.delta) {
                            if (event.delta.type === "text_delta") {
                                await writeChunk({
                                    id: chatId,
                                    object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000),
                                    model: model,
                                    choices: [{ index: 0, delta: { content: event.delta.text }, finish_reason: null }]
                                });
                                lastSendTime = Date.now();

                            } else if (event.delta.type === "thinking_delta") {
                                await writeChunk({
                                    id: chatId,
                                    object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000),
                                    model: model,
                                    choices: [{ index: 0, delta: { reasoning_content: event.delta.thinking }, finish_reason: null }]
                                });
                                lastSendTime = Date.now();

                            } else if (event.delta.type === "input_json_delta") {
                                await writeChunk({
                                    id: chatId,
                                    object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000),
                                    model: model,
                                    choices: [{
                                        index: 0,
                                        delta: {
                                            tool_calls: [{
                                                index: toolCallIndex,
                                                function: {
                                                    arguments: event.delta.partial_json
                                                }
                                            }]
                                        },
                                        finish_reason: null
                                    }]
                                });
                                lastSendTime = Date.now();
                            }

                        } else if (event.type === "message_delta") {
                            var finishReason = mapStopReason(event.delta && event.delta.stop_reason);
                            await writeChunk({
                                id: chatId,
                                object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000),
                                model: model,
                                choices: [{ index: 0, delta: {}, finish_reason: finishReason }]
                            });
                            lastSendTime = Date.now();

                        } else if (event.type === "message_stop") {
                            await writer.write(encoder.encode("data: [DONE]\n\n"));
                            lastSendTime = Date.now();
                        }

                    } catch (e) {
                        console.error("[Stream Parse]", e.message);
                    }
                }
            }
        } catch (err) {
            console.error("[Stream] Error:", err.message);
        } finally {
            clearInterval(heartbeatTimer);
            try { await writer.close(); } catch (e) {}
        }
    })();

    return new Response(transformStream.readable, {
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "X-Accel-Buffering": "no"  // 禁用代理缓冲
        }
    });
}

// ================================================================
// Telegram 直接对话功能
// ================================================================

async function handleTGChat(env, chatId, userId, userMessage, replyToMessageId) {
    // 获取或创建会话
    var session = await getTGSession(env, chatId);
    if (!session) {
        session = {
            messages: [],
            model: "claude-sonnet-4-5",
            createdAt: new Date().toISOString()
        };
    }

    // 添加用户消息到会话历史
    session.messages.push({
        role: "user",
        content: userMessage
    });

    // 限制会话长度（防止太长）
    if (session.messages.length > 20) {
        session.messages = session.messages.slice(-20);
    }

    // 发送"正在思考..."消息
    var thinkingMsg = await sendTGReply(env, chatId, "🤔 正在思考...", replyToMessageId);

    try {
        // 选择 Key
        var selectedKey = await selectKeyWithRefresh(env);
        if (!selectedKey) {
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ 没有可用的 API Key");
            return;
        }

        // 构建请求
        var openaiReq = {
            messages: session.messages,
            model: session.model,
            max_tokens: 2048,
            stream: false
        };

        var built = buildAnthropicRequest(openaiReq);
        var result = await callAnthropic(selectedKey.accessToken, built.anthropicReq, 120000);

        if (result.error) {
            await recordKeyUsage(env, selectedKey, false);
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ 请求失败: " + escHtml(result.error));
            return;
        }

        var response = result.response;
        if (!response.ok) {
            var errorText = await response.text();
            await recordKeyUsage(env, selectedKey, false);
            await editTGMessage(env, chatId, thinkingMsg.messageId, "❌ API 错误: " + escHtml(errorText.substring(0, 200)));
            return;
        }

        var data = await response.json();
        await recordKeyUsage(env, selectedKey, true);

        // 提取回复
        var respObj = anthropicToOpenaiResp(data, session.model);
        var assistantMessage = (respObj.choices && respObj.choices[0] && respObj.choices[0].message && respObj.choices[0].message.content) || "(无回复)";

        // 保存到会话历史
        session.messages.push({
            role: "assistant",
            content: assistantMessage
        });

        await saveTGSession(env, chatId, session);

        // 分割长消息（Telegram 最多 4096 字）
        var MAX_TG_MSG = 4000;
        if (assistantMessage.length <= MAX_TG_MSG) {
            await editTGMessage(env, chatId, thinkingMsg.messageId, assistantMessage);
        } else {
            // 第一条编辑原消息
            await editTGMessage(env, chatId, thinkingMsg.messageId, assistantMessage.substring(0, MAX_TG_MSG));
            
            // 后续消息作为新回复
            var remaining = assistantMessage.substring(MAX_TG_MSG);
            var partNum = 2;
            while (remaining.length > 0) {
                var chunk = remaining.substring(0, MAX_TG_MSG);
                remaining = remaining.substring(MAX_TG_MSG);
                
                var header = "📄 续 (" + partNum + ")\n\n";
                await sendTGReply(env, chatId, header + chunk, replyToMessageId);
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
// Telegram Webhook 处理
// ================================================================

async function setupTelegramWebhook(url, env) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) {
        return errorResponse("TELEGRAM_BOT_TOKEN not set", 500);
    }
    var webhookUrl = url.origin + "/telegram/webhook";
    var setBody = { url: webhookUrl };
    if (env.TELEGRAM_WEBHOOK_SECRET) {
        setBody.secret_token = env.TELEGRAM_WEBHOOK_SECRET;
    }
    var resp = await fetch("https://api.telegram.org/bot" + botToken + "/setWebhook", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(setBody)
    });
    var result = await resp.json();
    return corsResponse(JSON.stringify({ webhook_url: webhookUrl, telegram_response: result }));
}

async function handleTelegramWebhook(request, env) {
    // 验证 Telegram Webhook Secret
    if (env.TELEGRAM_WEBHOOK_SECRET) {
        var secretHeader = request.headers.get("X-Telegram-Bot-Api-Secret-Token") || "";
        if (secretHeader !== env.TELEGRAM_WEBHOOK_SECRET) {
            console.warn("[TG Webhook] Invalid secret token");
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

    // 管理命令只允许在指定群/用户
    if (text.startsWith("/") && chatId !== allowedChatId) {
        return new Response("OK");
    }

    // 非命令消息作为 AI 对话
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
                    "🤖 <b>Claude 代理管理 Bot v4.1</b>\n\n" +
                    "<b>直接对话：</b>\n" +
                    "直接发送文字消息即可与 Claude 对话\n" +
                    "/clear — 清空对话历史\n\n" +
                    "<b>Key 管理：</b>\n" +
                    "/addkey &lt;label&gt; &lt;JSON&gt; — 添加 Key\n" +
                    "/removekey &lt;label&gt; — 删除 Key\n" +
                    "/listkeys — 列出所有 Key\n" +
                    "/status — 详细状态\n" +
                    "/enable &lt;label&gt; — 启用 Key\n" +
                    "/disable &lt;label&gt; — 禁用 Key\n" +
                    "/refresh &lt;label&gt; — 刷新指定 Key\n" +
                    "/refreshall — 刷新所有\n"
                );
                break;

            case "/clear":
                await deleteTGSession(env, chatId);
                await sendTG(env, "✅ 已清空对话历史");
                break;

            case "/addkey":
                if (args.length < 2) {
                    await sendTG(env, "⚠️ 格式：/addkey &lt;label&gt; &lt;JSON配置&gt;");
                    break;
                }
                var addLabel = args[0];
                var addJsonStr = args.slice(1).join(" ");
                var addParsed;
                try {
                    addParsed = JSON.parse(addJsonStr);
                } catch (e) {
                    await sendTG(env, "❌ JSON解析失败");
                    break;
                }

                var addOauth = addParsed.claudeAiOauth;
                if (!addOauth || !addOauth.accessToken || !addOauth.refreshToken) {
                    await sendTG(env, "❌ 缺少 Token 数据");
                    break;
                }

                var addKeyData = {
                    label: addLabel,
                    accessToken: addOauth.accessToken,
                    refreshToken: addOauth.refreshToken,
                    expiresAt: addOauth.expiresAt || 0,
                    scopes: addOauth.scopes || [],
                    subscriptionType: addOauth.subscriptionType || "unknown",
                    rateLimitTier: addOauth.rateLimitTier || "default",
                    enabled: true,
                    useCount: 0,
                    errorCount: 0,
                    consecutiveErrors: 0
                };

                await saveKey(env, addLabel, addKeyData);
                await deleteTGMessage(env, chatId, msg.message_id);

                await sendTG(env,
                    "✅ <b>Key 保存成功</b>\n" +
                    "📛 " + escHtml(addLabel) + "\n" +
                    "🔑 Token: <code>" + maskToken(addOauth.accessToken) + "</code>\n" +
                    "⚠️ 原消息已删除（含敏感信息）\n\n自动验证中..."
                );

                var addRefreshResult = await refreshTokenWithLock(env, addKeyData);
                if (addRefreshResult.success) {
                    await sendTG(env, "✅ <b>Token验证并刷新成功，已就绪</b>");
                } else {
                    await sendTG(env, "❌ <b>Token验证失败：</b>\n" + escHtml(addRefreshResult.error));
                }
                break;

            case "/removekey":
                if (args.length < 1) {
                    await sendTG(env, "⚠️ 格式：/removekey &lt;label&gt;");
                    break;
                }
                await deleteKey(env, args[0]);
                await sendTG(env, "🗑️ 已删除: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/enable":
                if (args.length < 1) {
                    await sendTG(env, "⚠️ 格式：/enable &lt;label&gt;");
                    break;
                }
                var enKey = await getKey(env, args[0]);
                if (!enKey) {
                    await sendTG(env, "❌ 未找到: " + escHtml(args[0]));
                    break;
                }
                enKey.enabled = true;
                enKey.consecutiveErrors = 0;
                await saveKey(env, enKey.label, enKey);
                await sendTG(env, "✅ 已启用: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/disable":
                if (args.length < 1) {
                    await sendTG(env, "⚠️ 格式：/disable &lt;label&gt;");
                    break;
                }
                var disKey = await getKey(env, args[0]);
                if (!disKey) {
                    await sendTG(env, "❌ 未找到: " + escHtml(args[0]));
                    break;
                }
                disKey.enabled = false;
                await saveKey(env, disKey.label, disKey);
                await sendTG(env, "⏸️ 已禁用: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/listkeys":
                var allKeys = await listAllKeys(env);
                if (allKeys.length === 0) {
                    await sendTG(env, "📭 没有 Key");
                    break;
                }
                var now = Date.now();
                var listText = "📋 <b>Key 列表 (" + allKeys.length + ")</b>\n\n";
                for (var ki = 0; ki < allKeys.length; ki++) {
                    var k = allKeys[ki];
                    var remainMin = k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : "?";
                    var icon = !k.enabled ? "⏸️" : (remainMin > 0 ? "✅" : "❌");
                    listText += icon + " <b>" + escHtml(k.label) + "</b> (" + remainMin + "分) | 用" + (k.useCount || 0) + " 错" + (k.errorCount || 0) + " 连错" + (k.consecutiveErrors || 0) + "\n";
                }
                await sendTGLong(env, listText);
                break;

            case "/status":
                var statusKeys = await listAllKeys(env);
                var stats = await getGlobalStats(env);
                var nowSt = Date.now();
                var enabledCount = 0;
                var availableCount = 0;
                var totalUse = 0;
                var totalErr = 0;

                for (var si = 0; si < statusKeys.length; si++) {
                    var sk = statusKeys[si];
                    if (sk.enabled) enabledCount++;
                    if (sk.enabled && sk.accessToken && sk.expiresAt > nowSt + 120000) availableCount++;
                    totalUse += (sk.useCount || 0);
                    totalErr += (sk.errorCount || 0);
                }

                var statusText = "📊 <b>系统状态</b>\n\n" +
                    "🔑 总 Key 数: " + statusKeys.length + "\n" +
                    "✅ 已启用: " + enabledCount + "\n" +
                    "🟢 当前可用: " + availableCount + "\n\n" +
                    "📈 总请求数: " + (stats.totalRequests || 0) + "\n" +
                    "📅 今日请求: " + (stats.todayRequests || 0) + " (" + (stats.today || "N/A") + ")\n" +
                    "📊 总调用: " + totalUse + " | 总错误: " + totalErr + "\n\n";

                for (var sj = 0; sj < statusKeys.length; sj++) {
                    var sd = statusKeys[sj];
                    var sRemain = sd.expiresAt ? Math.round((sd.expiresAt - nowSt) / 60000) : "?";
                    var sIcon = !sd.enabled ? "⏸️" : (sRemain > 0 ? "✅" : "❌");
                    statusText += sIcon + " <b>" + escHtml(sd.label) + "</b>\n";
                    statusText += "   过期剩余: " + sRemain + "分 | 用量: " + (sd.useCount || 0) + " | 错误: " + (sd.errorCount || 0) + " | 连错: " + (sd.consecutiveErrors || 0) + "\n";
                    if (sd.lastRefreshed) statusText += "   上次刷新: " + sd.lastRefreshed + "\n";
                    if (sd.lastUsed) statusText += "   上次使用: " + sd.lastUsed + "\n";
                    statusText += "\n";
                }

                await sendTGLong(env, statusText);
                break;

            case "/refresh":
                if (args.length < 1) {
                    await sendTG(env, "⚠️ 格式：/refresh &lt;label&gt;");
                    break;
                }
                var rKey = await getKey(env, args[0]);
                if (!rKey) {
                    await sendTG(env, "❌ 未找到: " + escHtml(args[0]));
                    break;
                }
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
// 主请求处理（含自动重试）
// ================================================================

async function handleChatCompletions(request, env) {
    var authHeader = request.headers.get("Authorization") || "";
    if (!validateCustomToken(authHeader, env)) {
        return errorResponse("Invalid API key", 401);
    }

    var openaiReq = await request.json().catch(function() { return {}; });
    var built = buildAnthropicRequest(openaiReq);
    var anthropicReq = built.anthropicReq;
    var requestedModel = built.requestedModel;

    var timeoutMs = (openaiReq.thinking || anthropicReq.thinking) ? 300000 : 120000;

    var MAX_RETRIES = 2;
    var lastErrorBody = "";
    var lastErrorStatus = 502;

    for (var attempt = 0; attempt < MAX_RETRIES; attempt++) {
        var selectedKey = await selectKeyWithRefresh(env);
        if (!selectedKey) {
            return errorResponse("No available API keys. All keys may be expired or disabled.", 503);
        }

        var keyLabel = selectedKey.label;
        var result = await callAnthropic(selectedKey.accessToken, anthropicReq, timeoutMs);

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
            } else {
                var data = await response.json();
                return corsResponse(JSON.stringify(anthropicToOpenaiResp(data, requestedModel)));
            }
        }

        var errorBody = await response.text().catch(function() { return "Unknown error"; });
        await recordKeyUsage(env, selectedKey, false);

        if (response.status === 401 || response.status === 403) {
            await sendTG(env, "⚠️ <b>Key 请求失败 (" + response.status + ")</b>\n📛 " + escHtml(keyLabel) + "\n尝试自动刷新...");

            var refreshResult = await refreshTokenWithLock(env, selectedKey);
            if (refreshResult.success) {
                await sendTG(env, "✅ 刷新成功，正在重试请求...");
                continue;
            } else {
                selectedKey.expiresAt = 0;
                await saveKey(env, selectedKey.label, selectedKey);
                await sendTG(env, "❌ 刷新失败: " + escHtml(refreshResult.error) + "\n已标记为过期，尝试其他 Key...");
                continue;
            }
        }

        if (response.status === 429 || response.status >= 500) {
            lastErrorBody = errorBody;
            lastErrorStatus = response.status;
            continue;
        }

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
// 入口 export
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
                return corsResponse(JSON.stringify({
                    object: "list",
                    data: SUPPORTED_MODELS
                }));
            }

            if (url.pathname === "/v1/chat/completions" && request.method === "POST") {
                return await handleChatCompletions(request, env);
            }

            return errorResponse("Not Found", 404);
        } catch (err) {
            console.error("[Fatal]", err.message, err.stack);
            return errorResponse("Internal Error: " + err.message, 500);
        }
    },

    async scheduled(event, env, ctx) {
        ctx.waitUntil(
            checkAndRefreshAllKeys(env).catch(async function(err) {
                console.error("[Scheduled] Error:", err.message);
                await sendTG(env, "🚨 <b>定时刷新异常</b>\n" + escHtml(err.message)).catch(function() {});
            })
        );
    }
};
