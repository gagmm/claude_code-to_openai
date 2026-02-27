// ================================================================
// Claude API 代理 v3.3 (JSON 请求回退版 + 防死循环机制)
// 功能：自定义Token鉴权 / Telegram Bot管理 / 多Key负载均衡 / 自动刷新 / 详细调试
// ================================================================

const pendingRefreshes = new Map();

const MODEL_MAP = {
    "claude-opus-4-6": "claude-opus-4-20250601",
    "claude-sonnet-4-5": "claude-sonnet-4-20250514",
    "claude-haiku-4-5": "claude-haiku-4-20250506",
    "claude-opus-4-20250601": "claude-opus-4-20250601",
    "claude-sonnet-4-20250514": "claude-sonnet-4-20250514",
    "claude-haiku-4-20250506": "claude-haiku-4-20250506",
    "claude-3-7-sonnet-20250219": "claude-3-7-sonnet-20250219",
    "claude-3-5-sonnet-20241022": "claude-3-5-sonnet-20241022",
    "claude-3-5-haiku-20241022": "claude-3-5-haiku-20241022",
    "claude-3-opus-20240229": "claude-3-opus-20240229",
};

const SUPPORTED_MODELS = Object.keys(MODEL_MAP).map(function(id) {
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
            "Access-Control-Allow-Headers": "*",
        }
    });
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
        var keys = [];
        for (var i = 0; i < list.keys.length; i++) {
            var data = await env.TOKEN_STORE.get(list.keys[i].name, { type: "json" });
            if (data) keys.push(data);
        }
        return keys;
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
// Token 刷新逻辑 (v3.3 恢复 JSON 格式)
// ================================================================

async function refreshTokenWithLock(refreshToken) {
    if (pendingRefreshes.has(refreshToken)) {
        return pendingRefreshes.get(refreshToken);
    }
    var promise = performTokenRefresh(refreshToken);
    pendingRefreshes.set(refreshToken, promise);
    try {
        return await promise;
    } finally {
        pendingRefreshes.delete(refreshToken);
    }
}

async function performTokenRefresh(refreshToken) {
    try {
        // v3.3: 恢复使用 application/json，防止官方强校验
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
    var refreshed = await refreshTokenWithLock(keyData.refreshToken);

    if (!refreshed) {
        return { success: false, error: "Refresh returned null (Network issue?)" };
    }

    // 检测到 400 或 401 错误说明 Refresh Token 彻底失效，直接禁用该 Key
    if (refreshed.error_detail) {
        if (refreshed.error_detail.includes("HTTP 401") || refreshed.error_detail.includes("HTTP 400") || refreshed.error_detail.includes("invalid_grant")) {
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
    // 必须保存官方下发的【全新】refreshToken！
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

        var result = await refreshSingleKey(env, keyData);

        if (result.success) {
            refreshed++;
            var fullConfig = {
                claudeAiOauth: {
                    accessToken: keyData.accessToken,
                    refreshToken: keyData.refreshToken,
                    expiresAt: keyData.expiresAt,
                    scopes: keyData.scopes || [],
                    subscriptionType: keyData.subscriptionType || "unknown",
                    rateLimitTier: keyData.rateLimitTier || "default",
                }
            };
            await sendTGLong(env,
                "🔄 <b>Token 自动刷新成功</b>\n\n" +
                "📛 Label: <b>" + escHtml(keyData.label) + "</b>\n" +
                "⏰ 新到期: " + result.expireStr + "\n\n" +
                "<b>完整配置（备份用）：</b>\n" +
                "<pre>" + escHtml(JSON.stringify(fullConfig, null, 2)) + "</pre>"
            );
        } else {
            failed++;
            await sendTG(env,
                "❌ <b>Token 刷新失败</b>\n\n" +
                "📛 Label: <b>" + escHtml(keyData.label) + "</b>\n" +
                "原因: " + escHtml(result.error)
            );
        }
        await sleep(1000); // 间隔1秒，防止频繁请求被拦截
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
        return k.enabled && k.accessToken && k.expiresAt > now + bufferTime;
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

async function recordKeyUsage(env, label, success) {
    var keyData = await getKey(env, label);
    if (!keyData) return;
    keyData.useCount = (keyData.useCount || 0) + 1;
    keyData.lastUsed = new Date().toISOString();
    if (!success) {
        keyData.errorCount = (keyData.errorCount || 0) + 1;
        keyData.lastErrorAt = new Date().toISOString();
    }
    await saveKey(env, label, keyData);
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

function anthropicToOpenaiResp(data, model, injectionText) {
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
    message.content = injectionText ? (injectionText + fullText) : (fullText || null);
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
// Telegram Webhook 处理
// ================================================================

async function setupTelegramWebhook(url, env) {
    var botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) {
        return corsResponse(JSON.stringify({ error: "TELEGRAM_BOT_TOKEN not set" }), 500);
    }
    var webhookUrl = url.origin + "/telegram/webhook";
    var resp = await fetch("https://api.telegram.org/bot" + botToken + "/setWebhook", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: webhookUrl })
    });
    var result = await resp.json();
    return corsResponse(JSON.stringify({ webhook_url: webhookUrl, telegram_response: result }));
}

async function handleTelegramWebhook(request, env) {
    var update = await request.json().catch(function() { return null; });
    if (!update || !update.message) return new Response("OK");

    var msg = update.message;
    var chatId = String(msg.chat.id);
    var allowedChatId = String(env.TELEGRAM_CHAT_ID || "");
    var text = (msg.text || "").trim();

    if (chatId !== allowedChatId) return new Response("OK");
    if (!text.startsWith("/")) return new Response("OK");

    var parts = text.split(/\s+/);
    var cmd = parts[0].toLowerCase().split("@")[0];
    var args = parts.slice(1);

    try {
        switch (cmd) {
            case "/help":
                await sendTG(env,
                    "🤖 <b>Claude 代理管理 Bot</b>\n\n" +
                    "/addkey &lt;label&gt; &lt;JSON&gt; — 添加 Key\n" +
                    "/removekey &lt;label&gt; — 删除 Key\n" +
                    "/listkeys — 列出所有 Key\n" +
                    "/status — 详细状态\n" +
                    "/refresh &lt;label&gt; — 刷新指定 Key\n" +
                    "/refreshall — 刷新所有\n"
                );
                break;

            case "/addkey":
                if (args.length < 2) { await sendTG(env, "⚠️ 格式：/addkey &lt;label&gt; &lt;JSON配置&gt;"); break; }
                var addLabel = args[0];
                var addJsonStr = args.slice(1).join(" ");
                var addParsed;
                try { addParsed = JSON.parse(addJsonStr); } catch (e) { await sendTG(env, "❌ JSON解析失败"); break; }
                
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
                    enabled: true,
                    useCount: 0,
                    errorCount: 0,
                };

                await saveKey(env, addLabel, addKeyData);
                await sendTG(env, "✅ <b>Key 保存成功</b>\n📛 " + escHtml(addLabel) + "\n自动验证中...");

                var addRefreshResult = await refreshSingleKey(env, addKeyData);
                if (addRefreshResult.success) {
                    await sendTG(env, "✅ <b>Token验证并刷新成功，已就绪</b>");
                } else {
                    await sendTG(env, "❌ <b>Token验证失败：</b>\n" + escHtml(addRefreshResult.error));
                }
                break;

            case "/removekey":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/removekey &lt;label&gt;"); break; }
                await deleteKey(env, args[0]);
                await sendTG(env, "🗑️ 已删除: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/listkeys":
                var allKeys = await listAllKeys(env);
                if (allKeys.length === 0) { await sendTG(env, "📭 没有 Key"); break; }
                var now = Date.now();
                var listText = "📋 <b>Key 列表 (" + allKeys.length + ")</b>\n\n";
                for (var ki = 0; ki < allKeys.length; ki++) {
                    var k = allKeys[ki];
                    var remainMin = k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : "?";
                    var icon = !k.enabled ? "⏸️" : (remainMin > 0 ? "✅" : "❌");
                    listText += icon + " <b>" + escHtml(k.label) + "</b> (" + remainMin + "分) | 用" + (k.useCount || 0) + " 错" + (k.errorCount || 0) + "\n";
                }
                await sendTGLong(env, listText);
                break;

            case "/refresh":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/refresh &lt;label&gt;"); break; }
                var rKey = await getKey(env, args[0]);
                if (!rKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                var rResult = await refreshSingleKey(env, rKey);
                if (rResult.success) {
                    await sendTG(env, "✅ <b>刷新成功</b>\n📛 " + escHtml(args[0]));
                } else {
                    await sendTG(env, "❌ <b>刷新失败</b>\n" + escHtml(rResult.error));
                }
                break;

            case "/refreshall":
                await sendTG(env, "🔄 正在刷新...");
                var raResult = await checkAndRefreshAllKeys(env, true);
                await sendTG(env, "✅ <b>批量刷新完成</b>\n成功: " + raResult.refreshed + " | 失败: " + raResult.failed);
                break;

            default:
                await sendTG(env, "❓ 未知命令 /help");
        }
    } catch (err) {
        await sendTG(env, "❌ 执行出错: " + escHtml(err.message));
    }
    return new Response("OK");
}

// ================================================================
// 主请求处理
// ================================================================

async function handleChatCompletions(request, env) {
    var authHeader = request.headers.get("Authorization") || "";
    if (!validateCustomToken(authHeader, env)) {
        return corsResponse(JSON.stringify({ error: "Invalid API key" }), 401);
    }

    var selectedKey = await selectKey(env);
    if (!selectedKey) {
        return corsResponse(JSON.stringify({ error: "No available API keys" }), 503);
    }

    var activeAccessToken = selectedKey.accessToken;
    var keyLabel = selectedKey.label;

    var openaiReq = await request.json().catch(function() { return {}; });

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
        messages: anthropicMessages,
    };
    if (systemPrompt.trim()) anthropicReq.system = systemPrompt.trim();
    if (openaiReq.stream) anthropicReq.stream = true;

    var anthropicHeaders = {
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
        "anthropic-beta": "oauth-2025-04-20",
        "x-app": "cli",
        "User-Agent": "claude-code/2.0.62"
    };

    if (activeAccessToken.startsWith("sk-ant-oat")) {
        anthropicHeaders["Authorization"] = "Bearer " + activeAccessToken;
    } else {
        anthropicHeaders["x-api-key"] = activeAccessToken;
    }

    var controller = new AbortController();
    var timeoutId = setTimeout(function() { controller.abort(); }, 120000);

    var response;
    try {
        response = await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: anthropicHeaders,
            body: JSON.stringify(anthropicReq),
            signal: controller.signal
        });
    } catch (err) {
        clearTimeout(timeoutId);
        await recordKeyUsage(env, keyLabel, false);
        var isTimeout = err.name === "AbortError";
        return corsResponse(JSON.stringify({ error: isTimeout ? "Request timed out" : err.message }), isTimeout ? 504 : 502);
    }
    clearTimeout(timeoutId);

    if (!response.ok) {
        var errorBody = await response.text().catch(function() { return "Unknown"; });
        await recordKeyUsage(env, keyLabel, false);

        if (response.status === 401 || response.status === 403) {
            await sendTG(env, "⚠️ <b>Key 失效</b>\n📛 " + escHtml(keyLabel) + "\n状态码: " + response.status + "\n系统已强制使其过期。");
            try {
                var failedKey = await getKey(env, keyLabel);
                if (failedKey) {
                    failedKey.expiresAt = 0; 
                    await saveKey(env, keyLabel, failedKey);
                }
            } catch(e) {}
        }
        return new Response(errorBody, { status: response.status, headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } });
    }

    await recordKeyUsage(env, keyLabel, true);

    if (openaiReq.stream) {
        return handleStream(response, requestedModel);
    } else {
        var data = await response.json();
        return corsResponse(JSON.stringify(anthropicToOpenaiResp(data, requestedModel, "")));
    }
}

// ================================================================
// 流式处理
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
                            await writer.write(encoder.encode("data: " + JSON.stringify({ id: chatId, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] }) + "\n\n"));
                        } else if (event.type === "content_block_delta" && event.delta && event.delta.type === "text_delta") {
                            await writer.write(encoder.encode("data: " + JSON.stringify({ id: chatId, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model, choices: [{ index: 0, delta: { content: event.delta.text }, finish_reason: null }] }) + "\n\n"));
                        } else if (event.type === "message_stop") {
                            await writer.write(encoder.encode("data: [DONE]\n\n"));
                        }
                    } catch (e) {}
                }
            }
        } catch (err) {} finally {
            try { await writer.close(); } catch (e) {}
        }
    })();

    return new Response(transformStream.readable, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive", "Access-Control-Allow-Origin": "*" }
    });
}

// ================================================================
// 入口 export
// ================================================================

export default {
    async fetch(request, env, ctx) {
        if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "*", "Access-Control-Allow-Headers": "*" } });
        var url = new URL(request.url);
        try {
            if (url.pathname === "/telegram/webhook" && request.method === "POST") return await handleTelegramWebhook(request, env);
            if (url.pathname === "/v1/chat/completions" && request.method === "POST") return await handleChatCompletions(request, env);
            return corsResponse(JSON.stringify({ error: "Not Found" }), 404);
        } catch (err) {
            return new Response(JSON.stringify({ error: "Internal Error" }), { status: 500 });
        }
    },
    async scheduled(event, env, ctx) {
        ctx.waitUntil(checkAndRefreshAllKeys(env));
    }
};
