// ================================================================
// Claude API 代理 v3.1 (修复版)
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
// 工具函数（必须在最前面定义）
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
    if (!env.TOKEN_STORE) {
        console.error("[KV] TOKEN_STORE not bound! Check wrangler.toml or Dashboard bindings.");
        return false;
    }
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
// Token 刷新 (增强版调试)
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
        // 构造请求体
        var body = JSON.stringify({
            grant_type: "refresh_token",
            refresh_token: refreshToken,
            client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e" // 这是目前已知的 Claude Code Client ID
        });

        console.log("[Refresh] Sending request to Anthropic...");

        var resp = await fetch("https://console.anthropic.com/v1/oauth/token", {
            method: "POST",
            headers: { 
                "Content-Type": "application/json",
                "User-Agent": "claude-code/2.0.62", // 模拟官方 CLI
                "Accept": "application/json"
            },
            body: body
        });

        var respText = await resp.text();
        console.log("[Refresh] Status:", resp.status);
        console.log("[Refresh] Response Body Preview:", respText.substring(0, 500)); // 打印返回内容前500字符

        if (!resp.ok) {
            console.error("[Refresh] HTTP Error:", resp.status, respText);
            return { error_detail: "HTTP " + resp.status + ": " + respText };
        }

        try {
            return JSON.parse(respText);
        } catch (e) {
            console.error("[Refresh] JSON Parse Error:", e.message, respText);
            return { error_detail: "Invalid JSON: " + respText.substring(0, 100) };
        }
    } catch (err) {
        console.error("[Refresh] Network Error:", err.message);
        return { error_detail: "Network error: " + err.message };
    }
}

async function refreshSingleKey(env, keyData) {
    var now = Date.now();
    var refreshed = await refreshTokenWithLock(keyData.refreshToken);

    // 检查是否有详细错误信息
    if (!refreshed) {
        return { success: false, error: "Refresh returned null (Network issue?)" };
    }

    if (refreshed.error_detail) {
        return { success: false, error: refreshed.error_detail };
    }

    // 关键修改：如果缺失 access_token，把整个返回对象打印出来作为错误信息
    if (!refreshed.access_token) {
        var debugInfo = JSON.stringify(refreshed).substring(0, 300);
        console.error("[Refresh] Missing access_token. Full response:", debugInfo);
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

        console.log("[Cron] Refreshing: " + keyData.label);
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
        await sleep(1000);
    }

    console.log("[Cron] Done: " + refreshed + " refreshed, " + failed + " failed, " + skipped + " skipped");
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
        console.error("[LB] No available keys");
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
    console.log("[LB] Selected: " + selected.key.label + " (score: " + selected.score + ", pool: " + available.length + ")");
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
// 鉴权
// ================================================================

function validateCustomToken(authHeader, env) {
    var token = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!token) return false;
    var allowed = (env.CUSTOM_TOKENS || "").split(",").map(function(t) { return t.trim(); }).filter(Boolean);
    if (allowed.length === 0) {
        console.warn("[Auth] No CUSTOM_TOKENS configured");
        return false;
    }
    return allowed.indexOf(token) !== -1;
}

// ================================================================
// 格式转换
// ================================================================

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

    if (chatId !== allowedChatId) {
        console.log("[TG] Ignored chat " + chatId);
        return new Response("OK");
    }

    if (!text.startsWith("/")) return new Response("OK");

    var parts = text.split(/\s+/);
    var cmd = parts[0].toLowerCase().split("@")[0];
    var args = parts.slice(1);

    try {
        switch (cmd) {
            case "/help":
                await sendTG(env,
                    "🤖 <b>Claude 代理管理 Bot</b>\n\n" +
                    "<b>Key 管理：</b>\n" +
                    "/addkey &lt;label&gt; &lt;JSON&gt; — 添加 Key\n" +
                    "/removekey &lt;label&gt; — 删除 Key\n" +
                    "/listkeys — 列出所有 Key\n" +
                    "/status — 详细状态\n" +
                    "/setlabel &lt;旧&gt; &lt;新&gt; — 重命名\n\n" +
                    "<b>启用/禁用：</b>\n" +
                    "/enable &lt;label&gt; — 启用\n" +
                    "/disable &lt;label&gt; — 禁用\n\n" +
                    "<b>刷新：</b>\n" +
                    "/refresh &lt;label&gt; — 刷新指定 Key\n" +
                    "/refreshall — 刷新所有\n\n" +
                    "<b>统计：</b>\n" +
                    "/stats — 使用统计"
                );
                break;

            case "/addkey":
                if (args.length < 2) {
                    await sendTG(env, "⚠️ 格式：/addkey &lt;label&gt; &lt;JSON配置&gt;");
                    break;
                }
                var addLabel = args[0];
                var addJsonStr = args.slice(1).join(" ");
                var addParsed;
                try { addParsed = JSON.parse(addJsonStr); } catch (e) {
                    await sendTG(env, "❌ JSON 解析失败：" + escHtml(e.message));
                    break;
                }
                var addOauth = addParsed.claudeAiOauth;
                if (!addOauth || !addOauth.accessToken || !addOauth.refreshToken) {
                    await sendTG(env, "❌ 缺少 accessToken 或 refreshToken");
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
                    addedAt: new Date().toISOString(),
                    addedBy: msg.from ? (msg.from.first_name || "") + " (" + msg.from.id + ")" : "unknown",
                    lastRefreshed: null,
                    lastUsed: null,
                    useCount: 0,
                    errorCount: 0,
                };

                // 保存到 KV
                var addSaved = await saveKey(env, addLabel, addKeyData);
                if (!addSaved) {
                    await sendTG(env, "❌ 保存失败！请检查 KV (TOKEN_STORE) 是否已绑定");
                    break;
                }

                // 验证是否真的存入了
                var addVerify = await getKey(env, addLabel);
                if (!addVerify) {
                    await sendTG(env, "❌ 保存后验证失败！KV 可能未正确绑定");
                    break;
                }

                var addNow = Date.now();
                var addExpStr = addKeyData.expiresAt
                    ? new Date(addKeyData.expiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })
                    : "未知";
                var addRemainMin = addKeyData.expiresAt ? Math.round((addKeyData.expiresAt - addNow) / 60000) : 0;

                await sendTG(env,
                    "✅ <b>Key 已保存</b>\n\n" +
                    "📛 Label: <code>" + escHtml(addLabel) + "</code>\n" +
                    "📋 订阅: " + escHtml(addKeyData.subscriptionType) + "\n" +
                    "⏰ 到期: " + addExpStr + " (" + addRemainMin + "分钟)\n" +
                    "🔑 Token: <code>" + addOauth.accessToken.substring(0, 25) + "...</code>"
                );

                // 如果已过期或即将过期（10分钟内），立即刷新
                if (!addKeyData.expiresAt || addKeyData.expiresAt < addNow + 10 * 60 * 1000) {
                    await sendTG(env, "🔄 Token 已过期或即将过期，正在自动刷新...");
                    var addRefreshResult = await refreshSingleKey(env, addKeyData);
                    if (addRefreshResult.success) {
                        // 刷新后重新读取最新数据构建完整配置
                        var addRefreshedKey = await getKey(env, addLabel);
                        var addFullConfig = {
                            claudeAiOauth: {
                                accessToken: addRefreshedKey.accessToken,
                                refreshToken: addRefreshedKey.refreshToken,
                                expiresAt: addRefreshedKey.expiresAt,
                                scopes: addRefreshedKey.scopes || [],
                                subscriptionType: addRefreshedKey.subscriptionType || "unknown",
                                rateLimitTier: addRefreshedKey.rateLimitTier || "default",
                            }
                        };
                        await sendTGLong(env,
                            "✅ <b>刷新成功！Key 已可用</b>\n\n" +
                            "📛 Label: <b>" + escHtml(addLabel) + "</b>\n" +
                            "⏰ 新到期: " + addRefreshResult.expireStr + "\n\n" +
                            "<b>最新配置（备份）：</b>\n" +
                            "<pre>" + escHtml(JSON.stringify(addFullConfig, null, 2)) + "</pre>"
                        );
                    } else {
                        await sendTG(env,
                            "⚠️ <b>自动刷新失败</b>\n\n" +
                            "原因: " + escHtml(addRefreshResult.error) + "\n" +
                            "Key 已保存但可能无法使用，请检查 refreshToken 是否有效\n" +
                            "可以稍后用 /refresh " + escHtml(addLabel) + " 重试"
                        );
                    }
                } else {
                    await sendTG(env, "✅ Token 仍在有效期内，已加入负载均衡池");
                }
                break;

            case "/removekey":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/removekey &lt;label&gt;"); break; }
                var existing = await getKey(env, args[0]);
                if (!existing) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                await deleteKey(env, args[0]);
                await sendTG(env, "🗑️ 已删除: <b>" + escHtml(args[0]) + "</b>");
                break;

            case "/listkeys":
                var allKeys = await listAllKeys(env);
                if (allKeys.length === 0) { await sendTG(env, "📭 没有 Key，用 /addkey 添加"); break; }
                var now = Date.now();
                var listText = "📋 <b>Key 列表 (" + allKeys.length + " 个)</b>\n\n";
                for (var ki = 0; ki < allKeys.length; ki++) {
                    var k = allKeys[ki];
                    var remainMin = k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : "?";
                    var icon = !k.enabled ? "⏸️" : (remainMin > 10 ? "✅" : (remainMin > 0 ? "⚠️" : "❌"));
                    listText += icon + " <b>" + escHtml(k.label) + "</b> | " +
                        (k.enabled ? "启用" : "禁用") + " | " + remainMin + "分 | " +
                        (k.useCount || 0) + "次 | 错误" + (k.errorCount || 0) + "\n";
                }
                await sendTGLong(env, listText);
                break;

            case "/status":
                var sKeys = await listAllKeys(env);
                var sNow = Date.now();
                var sText = "📊 <b>系统状态</b>\n\n" +
                    "总 Key: " + sKeys.length + "\n" +
                    "活跃: " + sKeys.filter(function(k) { return k.enabled && k.expiresAt > sNow; }).length + "\n\n";
                for (var si = 0; si < sKeys.length; si++) {
                    var sk = sKeys[si];
                    var sRemain = sk.expiresAt ? Math.round((sk.expiresAt - sNow) / 60000) : "?";
                    var sExp = sk.expiresAt ? new Date(sk.expiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" }) : "未知";
                    sText += "━━━━━━━━━━━━━━━\n" +
                        "📛 <b>" + escHtml(sk.label) + "</b>\n" +
                        "   启用: " + (sk.enabled ? "✅" : "⏸️") + " | 到期: " + sExp + " (" + sRemain + "分)\n" +
                        "   使用: " + (sk.useCount || 0) + "次 | 错误: " + (sk.errorCount || 0) + "\n" +
                        "   上次使用: " + (sk.lastUsed || "从未") + "\n\n";
                }
                await sendTGLong(env, sText);
                break;

            case "/refresh":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/refresh &lt;label&gt;"); break; }
                var rKey = await getKey(env, args[0]);
                if (!rKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                await sendTG(env,
                    "🔄 正在刷新 <b>" + escHtml(args[0]) + "</b>...\n" +
                    "RefreshToken: <code>" + rKey.refreshToken.substring(0, 25) + "...</code>"
                );
                var rResult = await refreshSingleKey(env, rKey);
                if (rResult.success) {
                    var rRefreshedKey = await getKey(env, args[0]);
                    var rFullConfig = {
                        claudeAiOauth: {
                            accessToken: rRefreshedKey.accessToken,
                            refreshToken: rRefreshedKey.refreshToken,
                            expiresAt: rRefreshedKey.expiresAt,
                            scopes: rRefreshedKey.scopes || [],
                            subscriptionType: rRefreshedKey.subscriptionType || "unknown",
                            rateLimitTier: rRefreshedKey.rateLimitTier || "default",
                        }
                    };
                    await sendTGLong(env,
                        "✅ <b>刷新成功</b>\n\n" +
                        "📛 " + escHtml(args[0]) + "\n" +
                        "⏰ 新到期: " + rResult.expireStr + "\n\n" +
                        "<b>最新配置：</b>\n" +
                        "<pre>" + escHtml(JSON.stringify(rFullConfig, null, 2)) + "</pre>"
                    );
                } else {
                    await sendTG(env,
                        "❌ <b>刷新失败</b>\n\n" +
                        "📛 " + escHtml(args[0]) + "\n" +
                        "<b>错误信息：</b>\n" +
                        "<pre>" + escHtml(rResult.error) + "</pre>\n\n" +
                        "<b>可能原因：</b>\n" +
                        "1. refreshToken 已失效\n" +
                        "2. Anthropic 服务限制\n" +
                        "3. 网络问题"
                    );
                }
                break;

            case "/refreshall":
                await sendTG(env, "🔄 正在刷新所有 Key...");
                var raResult = await checkAndRefreshAllKeys(env, true);
                await sendTG(env,
                    "✅ <b>批量刷新完成</b>\n" +
                    "检查: " + raResult.checked + " | 刷新: " + raResult.refreshed +
                    " | 失败: " + raResult.failed + " | 跳过: " + raResult.skipped
                );
                break;

            case "/setlabel":
                if (args.length < 2) { await sendTG(env, "⚠️ 格式：/setlabel &lt;旧&gt; &lt;新&gt;"); break; }
                var oldKey = await getKey(env, args[0]);
                if (!oldKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                var newExists = await getKey(env, args[1]);
                if (newExists) { await sendTG(env, "❌ " + escHtml(args[1]) + " 已存在"); break; }
                oldKey.label = args[1];
                await saveKey(env, args[1], oldKey);
                await deleteKey(env, args[0]);
                await sendTG(env, "✅ 重命名: " + escHtml(args[0]) + " → " + escHtml(args[1]));
                break;

            case "/enable":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/enable &lt;label&gt;"); break; }
                var enKey = await getKey(env, args[0]);
                if (!enKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                enKey.enabled = true;
                await saveKey(env, args[0], enKey);
                await sendTG(env, "✅ 已启用: " + escHtml(args[0]));
                break;

            case "/disable":
                if (args.length < 1) { await sendTG(env, "⚠️ 格式：/disable &lt;label&gt;"); break; }
                var disKey = await getKey(env, args[0]);
                if (!disKey) { await sendTG(env, "❌ 未找到: " + escHtml(args[0])); break; }
                disKey.enabled = false;
                await saveKey(env, args[0], disKey);
                await sendTG(env, "⏸️ 已禁用: " + escHtml(args[0]));
                break;

            case "/stats":
                var stKeys = await listAllKeys(env);
                var gStats = await getGlobalStats(env);
                var stText = "📈 <b>使用统计</b>\n\n" +
                    "总请求: " + (gStats.totalRequests || 0) + "\n" +
                    "今日请求: " + (gStats.todayRequests || 0) + "\n\n" +
                    "<b>各 Key 排名：</b>\n";
                var sorted = stKeys.slice().sort(function(a, b) { return (b.useCount || 0) - (a.useCount || 0); });
                for (var sti = 0; sti < sorted.length; sti++) {
                    stText += (sti + 1) + ". " + escHtml(sorted[sti].label) +
                        " — " + (sorted[sti].useCount || 0) + "次 (错误" + (sorted[sti].errorCount || 0) + ")\n";
                }
                await sendTG(env, stText);
                break;

            default:
                await sendTG(env, "❓ 未知命令，发送 /help 查看帮助");
        }
    } catch (err) {
        console.error("[TG Cmd Error]", err.message);
        await sendTG(env, "❌ 执行出错: " + escHtml(err.message));
    }

    return new Response("OK");
}

// ================================================================
// 管理路由
// ================================================================

async function handleAdmin(url, request, env) {
    var authHeader = request.headers.get("Authorization") || "";
    var adminKey = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!env.ADMIN_KEY || adminKey !== env.ADMIN_KEY) {
        return corsResponse(JSON.stringify({ error: "Unauthorized" }), 401);
    }
    if (url.pathname === "/admin/status") {
        var keys = await listAllKeys(env);
        var now = Date.now();
        var result = keys.map(function(k) {
            return {
                label: k.label, enabled: k.enabled,
                remainingMin: k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : null,
                useCount: k.useCount || 0, errorCount: k.errorCount || 0
            };
        });
        return corsResponse(JSON.stringify(result, null, 2));
    }
    if (url.pathname === "/admin/refresh-all" && request.method === "POST") {
        var r = await checkAndRefreshAllKeys(env, true);
        return corsResponse(JSON.stringify(r));
    }
    return corsResponse(JSON.stringify({ error: "Not Found" }), 404);
}

// ================================================================
// 主请求处理
// ================================================================

async function handleChatCompletions(request, env) {
    // 验证自定义 token
    var authHeader = request.headers.get("Authorization") || "";
    if (!validateCustomToken(authHeader, env)) {
        return corsResponse(JSON.stringify({ error: "Invalid API key" }), 401);
    }

    // 负载均衡选择 key
    var selectedKey = await selectKey(env);
    if (!selectedKey) {
        return corsResponse(JSON.stringify({
            error: "No available API keys. Add keys via Telegram bot using /addkey command."
        }), 503);
    }

    var activeAccessToken = selectedKey.accessToken;
    var keyLabel = selectedKey.label;

    // 解析请求
    var openaiReq = await request.json().catch(function(e) {
        console.error("[Body Parse]", e.message);
        return {};
    });

    // 构建消息
    var systemPrompt = "";
    var rawMessages = [];
    var msgs = openaiReq.messages || [];

    for (var i = 0; i < msgs.length; i++) {
        var m = msgs[i];
        if (m.role === "system") {
            systemPrompt += (typeof m.content === "string" ? m.content : JSON.stringify(m.content)) + "\n";
        } else if (m.role === "user" || m.role === "assistant") {
            rawMessages.push({ role: m.role, content: convertContent(m.content) });
        } else if (m.role === "tool") {
            rawMessages.push({
                role: "user",
                content: [{
                    type: "tool_result",
                    tool_use_id: m.tool_call_id || "unknown",
                    content: typeof m.content === "string" ? m.content : JSON.stringify(m.content)
                }]
            });
        } else {
            rawMessages.push({ role: "user", content: typeof m.content === "string" ? m.content : JSON.stringify(m.content) });
        }
    }

    var anthropicMessages = mergeConsecutiveRoles(rawMessages);
    if (anthropicMessages.length > 0 && anthropicMessages[0].role !== "user") {
        anthropicMessages.unshift({ role: "user", content: "(continued)" });
    }

    // 模型映射
    var requestedModel = openaiReq.model || "claude-sonnet-4-5";
    var model = MODEL_MAP[requestedModel] || MODEL_MAP["claude-sonnet-4-5"];

    var anthropicReq = {
        model: model,
        max_tokens: openaiReq.max_tokens || 8192,
        messages: anthropicMessages,
    };

    if (systemPrompt.trim()) anthropicReq.system = systemPrompt.trim();
    if (openaiReq.stream) anthropicReq.stream = true;
    if (openaiReq.temperature !== undefined) anthropicReq.temperature = openaiReq.temperature;
    if (openaiReq.top_p !== undefined) anthropicReq.top_p = openaiReq.top_p;
    if (openaiReq.tools && openaiReq.tools.length > 0) {
        anthropicReq.tools = openaiReq.tools.map(convertTool);
    }

    // 请求头
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

    // 发送请求
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
        return corsResponse(JSON.stringify({
            error: isTimeout ? "Request timed out" : "Fetch error: " + err.message
        }), isTimeout ? 504 : 502);
    }
    clearTimeout(timeoutId);

    if (!response.ok) {
        var errorBody = await response.text().catch(function() { return "Unknown"; });
        console.error("[Anthropic] " + response.status + ": " + errorBody);
        await recordKeyUsage(env, keyLabel, false);

        // 如果是认证错误，通知 Telegram
        if (response.status === 401 || response.status === 403) {
            await sendTG(env,
                "⚠️ <b>Key 认证失败</b>\n\n" +
                "📛 Label: " + escHtml(keyLabel) + "\n" +
                "状态码: " + response.status + "\n" +
                "可能需要刷新，尝试 /refresh " + escHtml(keyLabel)
            );
        }

        return new Response(errorBody, {
            status: response.status,
            headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
        });
    }

    // 成功，记录使用
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

    var streamPromise = (async function() {
        var reader = anthropicResponse.body.getReader();
        var decoder = new TextDecoder();
        var buffer = "";
        var chatId = "chatcmpl-" + crypto.randomUUID();
        var currentToolCallIndex = -1;

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
                            chatId = (event.message && event.message.id) || chatId;
                            await writer.write(encoder.encode("data: " + JSON.stringify({
                                id: chatId, object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000), model: model,
                                choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }]
                            }) + "\n\n"));

                        } else if (event.type === "content_block_start") {
                            var block = event.content_block;
                            if (block && block.type === "tool_use") {
                                currentToolCallIndex++;
                                await writer.write(encoder.encode("data: " + JSON.stringify({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{
                                        index: 0,
                                        delta: {
                                            tool_calls: [{
                                                index: currentToolCallIndex,
                                                id: block.id, type: "function",
                                                function: { name: block.name, arguments: "" }
                                            }]
                                        },
                                        finish_reason: null
                                    }]
                                }) + "\n\n"));
                            }

                        } else if (event.type === "content_block_delta") {
                            if (event.delta && event.delta.type === "text_delta" && event.delta.text) {
                                await writer.write(encoder.encode("data: " + JSON.stringify({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { content: event.delta.text }, finish_reason: null }]
                                }) + "\n\n"));
                            } else if (event.delta && event.delta.type === "thinking_delta" && event.delta.thinking) {
                                await writer.write(encoder.encode("data: " + JSON.stringify({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{ index: 0, delta: { reasoning_content: event.delta.thinking }, finish_reason: null }]
                                }) + "\n\n"));
                            } else if (event.delta && event.delta.type === "input_json_delta" && event.delta.partial_json !== undefined) {
                                await writer.write(encoder.encode("data: " + JSON.stringify({
                                    id: chatId, object: "chat.completion.chunk",
                                    created: Math.floor(Date.now() / 1000), model: model,
                                    choices: [{
                                        index: 0,
                                        delta: {
                                            tool_calls: [{
                                                index: currentToolCallIndex,
                                                function: { arguments: event.delta.partial_json }
                                            }]
                                        },
                                        finish_reason: null
                                    }]
                                }) + "\n\n"));
                            }

                        } else if (event.type === "message_delta") {
                            var chunk = {
                                id: chatId, object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000), model: model,
                                choices: [{ index: 0, delta: {}, finish_reason: mapStopReason(event.delta && event.delta.stop_reason) }]
                            };
                            if (event.usage) {
                                chunk.usage = {
                                    prompt_tokens: event.usage.input_tokens || 0,
                                    completion_tokens: event.usage.output_tokens || 0,
                                    total_tokens: (event.usage.input_tokens || 0) + (event.usage.output_tokens || 0)
                                };
                            }
                            await writer.write(encoder.encode("data: " + JSON.stringify(chunk) + "\n\n"));

                        } else if (event.type === "message_stop") {
                            await writer.write(encoder.encode("data: [DONE]\n\n"));

                        } else if (event.type === "error") {
                            console.error("[Stream Error]", JSON.stringify(event.error));
                            await writer.write(encoder.encode("data: " + JSON.stringify({
                                id: chatId, object: "chat.completion.chunk",
                                created: Math.floor(Date.now() / 1000), model: model,
                                choices: [{ index: 0, delta: { content: "\n[Error: " + ((event.error && event.error.message) || "Unknown") + "]" }, finish_reason: "stop" }]
                            }) + "\n\n"));
                            await writer.write(encoder.encode("data: [DONE]\n\n"));
                        }
                    } catch (parseErr) {
                        console.error("[Stream Parse]", parseErr.message);
                    }
                }
            }
        } catch (err) {
            console.error("[Stream]", err.message);
        } finally {
            try { await writer.close(); } catch (e) {}
        }
    })();

    return new Response(transformStream.readable, {
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
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
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "*",
                }
            });
        }

        var url = new URL(request.url);

        try {
            if (url.pathname === "/telegram/webhook" && request.method === "POST") {
                return await handleTelegramWebhook(request, env);
            }
            if (url.pathname === "/v1/chat/completions" && request.method === "POST") {
                return await handleChatCompletions(request, env);
            }
            if (url.pathname === "/v1/models" && request.method === "GET") {
                return corsResponse(JSON.stringify({ object: "list", data: SUPPORTED_MODELS }));
            }
            if (url.pathname.startsWith("/admin/")) {
                return await handleAdmin(url, request, env);
            }
            if (url.pathname === "/setup-webhook" && request.method === "GET") {
                return await setupTelegramWebhook(url, env);
            }
            if (url.pathname === "/debug/version") {
                return corsResponse(JSON.stringify({
                    version: "3.1-loadbalance-debug",
                    features: ["custom-token-auth", "telegram-bot", "multi-key-lb", "auto-refresh", "detail-errors"],
                    models: Object.keys(MODEL_MAP)
                }));
            }

            // 根路径返回简单说明
            if (url.pathname === "/" || url.pathname === "") {
                return corsResponse(JSON.stringify({
                    service: "Claude API Proxy",
                    status: "running",
                    endpoints: {
                        chat: "/v1/chat/completions",
                        models: "/v1/models",
                        version: "/debug/version"
                    }
                }));
            }

            return corsResponse(JSON.stringify({ error: "Not Found" }), 404);
        } catch (err) {
            console.error("[Global Error]", err.message, err.stack);
            return new Response(JSON.stringify({ error: "Internal Server Error", detail: err.message }), {
                status: 500,
                headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
            });
        }
    },

    async scheduled(event, env, ctx) {
        console.log("[Cron] Triggered at", new Date().toISOString());
        ctx.waitUntil(checkAndRefreshAllKeys(env));
    }
};
