// ================================================================
// Claude API 代理 v3.0
// 功能：自定义Token鉴权 / Telegram Bot管理 / 多Key负载均衡 / 自动刷新
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

const SUPPORTED_MODELS = Object.keys(MODEL_MAP).map(id => ({
    id, object: "model", created: 0, owned_by: "anthropic"
}));

// ================================================================
// 入口
// ================================================================
export default {
    async fetch(request, env, ctx) {
        if (request.method === "OPTIONS") {
            return corsResponse(null, 204);
        }

        const url = new URL(request.url);

        try {
            // Telegram Webhook
            if (url.pathname === "/telegram/webhook" && request.method === "POST") {
                return await handleTelegramWebhook(request, env);
            }

            // API 路由
            if (url.pathname === "/v1/chat/completions" && request.method === "POST") {
                return await handleChatCompletions(request, env);
            }

            if (url.pathname === "/v1/models" && request.method === "GET") {
                return corsResponse(JSON.stringify({ object: "list", data: SUPPORTED_MODELS }));
            }

            // 管理路由
            if (url.pathname.startsWith("/admin/")) {
                return await handleAdmin(url, request, env);
            }

            // 设置 Telegram Webhook 的便捷端点
            if (url.pathname === "/setup-webhook" && request.method === "GET") {
                return await setupTelegramWebhook(url, env);
            }

            if (url.pathname === "/debug/version") {
                return corsResponse(JSON.stringify({
                    version: "3.0-loadbalance",
                    features: [
                        "custom-token-auth",
                        "telegram-bot-management",
                        "multi-key-load-balance",
                        "auto-refresh",
                        "kv-persistent-storage"
                    ],
                    models: Object.keys(MODEL_MAP)
                }));
            }

            return corsResponse(JSON.stringify({ error: "Not Found" }), 404);
        } catch (err) {
            console.error("[Global Error]", err.message, err.stack);
            return corsResponse(JSON.stringify({ error: "Internal Server Error" }), 500);
        }
    },

    // 定时任务
    async scheduled(event, env, ctx) {
        console.log("[Cron] Token check at", new Date().toISOString());
        ctx.waitUntil(checkAndRefreshAllKeys(env));
    }
};

// ================================================================
// 鉴权：验证自定义 Token
// ================================================================
function validateCustomToken(authHeader, env) {
    const token = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!token) return false;

    const allowedTokens = (env.CUSTOM_TOKENS || "").split(",").map(t => t.trim()).filter(Boolean);

    // 如果没配置自定义 token，拒绝所有请求
    if (allowedTokens.length === 0) {
        console.warn("[Auth] No CUSTOM_TOKENS configured, rejecting all requests");
        return false;
    }

    return allowedTokens.includes(token);
}

// ================================================================
// Telegram Bot 命令处理
// ================================================================
async function setupTelegramWebhook(url, env) {
    const botToken = env.TELEGRAM_BOT_TOKEN;
    if (!botToken) {
        return corsResponse(JSON.stringify({ error: "TELEGRAM_BOT_TOKEN not set" }), 500);
    }

    const webhookUrl = `${url.origin}/telegram/webhook`;
    const resp = await fetch(`https://api.telegram.org/bot${botToken}/setWebhook`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: webhookUrl })
    });
    const result = await resp.json();
    return corsResponse(JSON.stringify({ webhook_url: webhookUrl, telegram_response: result }));
}

async function handleTelegramWebhook(request, env) {
    const update = await request.json().catch(() => null);
    if (!update || !update.message) {
        return new Response("OK");
    }

    const msg = update.message;
    const chatId = String(msg.chat.id);
    const allowedChatId = String(env.TELEGRAM_CHAT_ID || "");
    const text = (msg.text || "").trim();

    // 只处理指定群组/用户的消息
    if (chatId !== allowedChatId) {
        console.log(`[TG] Ignored message from chat ${chatId}, expected ${allowedChatId}`);
        return new Response("OK");
    }

    // 命令路由
    if (text.startsWith("/")) {
        const parts = text.split(/\s+/);
        const cmd = parts[0].toLowerCase().split("@")[0]; // 去掉 @botname
        const args = parts.slice(1);

        try {
            switch (cmd) {
                case "/help":
                    await handleHelp(env);
                    break;
                case "/addkey":
                    await handleAddKey(args, msg, env);
                    break;
                case "/removekey":
                    await handleRemoveKey(args, env);
                    break;
                case "/listkeys":
                    await handleListKeys(env);
                    break;
                case "/status":
                    await handleStatus(env);
                    break;
                case "/refresh":
                    await handleForceRefresh(args, env);
                    break;
                case "/refreshall":
                    await handleRefreshAll(env);
                    break;
                case "/setlabel":
                    await handleSetLabel(args, env);
                    break;
                case "/enable":
                    await handleToggleKey(args, true, env);
                    break;
                case "/disable":
                    await handleToggleKey(args, false, env);
                    break;
                case "/stats":
                    await handleStats(env);
                    break;
                default:
                    await sendTG(env, "❓ 未知命令，发送 /help 查看帮助");
            }
        } catch (err) {
            console.error("[TG Command Error]", err.message);
            await sendTG(env, `❌ 命令执行出错：${escHtml(err.message)}`);
        }
    }

    return new Response("OK");
}

async function handleHelp(env) {
    await sendTG(env,
        `🤖 <b>Claude 代理管理 Bot</b>\n\n` +
        `<b>Key 管理：</b>\n` +
        `/addkey &lt;label&gt; &lt;JSON配置&gt; — 添加 OAuth Key\n` +
        `/removekey &lt;label&gt; — 删除 Key\n` +
        `/listkeys — 列出所有 Key\n` +
        `/status — 查看详细状态\n` +
        `/setlabel &lt;旧label&gt; &lt;新label&gt; — 重命名\n\n` +
        `<b>启用/禁用：</b>\n` +
        `/enable &lt;label&gt; — 启用 Key\n` +
        `/disable &lt;label&gt; — 禁用 Key（不参与负载均衡）\n\n` +
        `<b>刷新：</b>\n` +
        `/refresh &lt;label&gt; — 强制刷新指定 Key\n` +
        `/refreshall — 强制刷新所有 Key\n\n` +
        `<b>统计：</b>\n` +
        `/stats — 查看使用统计\n\n` +
        `<b>添加示例：</b>\n` +
        `<code>/addkey mykey1 {"claudeAiOauth":{"accessToken":"sk-ant-oat01-xxx","refreshToken":"sk-ant-ort01-xxx","expiresAt":1772108485349}}</code>`
    );
}

async function handleAddKey(args, msg, env) {
    if (args.length < 2) {
        await sendTG(env, "⚠️ 格式：/addkey &lt;label&gt; &lt;JSON配置&gt;\n\n例如：\n<code>/addkey mykey1 {\"claudeAiOauth\":{...}}</code>");
        return;
    }

    const label = args[0];
    const jsonStr = args.slice(1).join(" ");

    let parsed;
    try {
        parsed = JSON.parse(jsonStr);
    } catch (e) {
        await sendTG(env, `❌ JSON 解析失败：${escHtml(e.message)}\n\n请确保 JSON 格式正确`);
        return;
    }

    const oauth = parsed.claudeAiOauth;
    if (!oauth || !oauth.accessToken || !oauth.refreshToken) {
        await sendTG(env, "❌ JSON 缺少必要字段：claudeAiOauth.accessToken 和 refreshToken");
        return;
    }

    // 检查 label 是否已存在
    const existing = await getKey(env, label);
    if (existing) {
        await sendTG(env, `⚠️ Label "<b>${escHtml(label)}</b>" 已存在，将覆盖旧数据`);
    }

    const keyData = {
        label: label,
        accessToken: oauth.accessToken,
        refreshToken: oauth.refreshToken,
        expiresAt: oauth.expiresAt || 0,
        scopes: oauth.scopes || [],
        subscriptionType: oauth.subscriptionType || "unknown",
        rateLimitTier: oauth.rateLimitTier || "default",
        enabled: true,
        addedAt: new Date().toISOString(),
        addedBy: msg.from ? `${msg.from.first_name || ""} (${msg.from.id})` : "unknown",
        lastRefreshed: null,
        lastUsed: null,
        useCount: 0,
        errorCount: 0,
    };

    await saveKey(env, label, keyData);

    const expStr = keyData.expiresAt
        ? new Date(keyData.expiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })
        : "未知";

    await sendTG(env,
        `✅ <b>Key 添加成功</b>\n\n` +
        `📛 Label: <code>${escHtml(label)}</code>\n` +
        `📋 订阅: ${escHtml(keyData.subscriptionType)}\n` +
        `⏰ 到期: ${expStr}\n` +
        `🔑 Token: <code>${oauth.accessToken.substring(0, 25)}...</code>\n\n` +
        `此 Key 已加入负载均衡池，将自动刷新。`
    );
}

async function handleRemoveKey(args, env) {
    if (args.length < 1) {
        await sendTG(env, "⚠️ 格式：/removekey &lt;label&gt;");
        return;
    }

    const label = args[0];
    const existing = await getKey(env, label);
    if (!existing) {
        await sendTG(env, `❌ 未找到 Label "<b>${escHtml(label)}</b>"`);
        return;
    }

    await deleteKey(env, label);
    await sendTG(env, `🗑️ Key "<b>${escHtml(label)}</b>" 已删除`);
}

async function handleListKeys(env) {
    const keys = await listAllKeys(env);
    if (keys.length === 0) {
        await sendTG(env, "📭 当前没有存储任何 Key\n\n使用 /addkey 添加");
        return;
    }

    const now = Date.now();
    let text = `📋 <b>Key 列表 (${keys.length} 个)</b>\n\n`;

    for (const k of keys) {
        const remainMin = k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : "?";
        const statusIcon = !k.enabled ? "⏸️" : (remainMin > 10 ? "✅" : (remainMin > 0 ? "⚠️" : "❌"));
        const enableStr = k.enabled ? "启用" : "禁用";

        text += `${statusIcon} <b>${escHtml(k.label)}</b>\n`;
        text += `   状态: ${enableStr} | 剩余: ${remainMin}分钟\n`;
        text += `   使用: ${k.useCount || 0}次 | 错误: ${k.errorCount || 0}次\n`;
        text += `   订阅: ${k.subscriptionType || "?"}\n\n`;
    }

    await sendTGLong(env, text);
}

async function handleStatus(env) {
    const keys = await listAllKeys(env);
    const now = Date.now();
    const activeKeys = keys.filter(k => k.enabled && k.expiresAt > now);

    let text = `📊 <b>系统状态</b>\n\n`;
    text += `总 Key 数: ${keys.length}\n`;
    text += `活跃 Key: ${activeKeys.length}\n`;
    text += `禁用 Key: ${keys.filter(k => !k.enabled).length}\n`;
    text += `过期 Key: ${keys.filter(k => k.enabled && k.expiresAt <= now).length}\n\n`;

    for (const k of keys) {
        const remainMin = k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : "?";
        const expStr = k.expiresAt
            ? new Date(k.expiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })
            : "未知";

        text += `━━━━━━━━━━━━━━━\n`;
        text += `📛 <b>${escHtml(k.label)}</b>\n`;
        text += `   启用: ${k.enabled ? "✅ 是" : "⏸️ 否"}\n`;
        text += `   到期: ${expStr} (${remainMin}分)\n`;
        text += `   订阅: ${k.subscriptionType || "?"}\n`;
        text += `   使用: ${k.useCount || 0}次\n`;
        text += `   错误: ${k.errorCount || 0}次\n`;
        text += `   上次使用: ${k.lastUsed || "从未"}\n`;
        text += `   上次刷新: ${k.lastRefreshed || "从未"}\n`;
        text += `   Token: <code>${(k.accessToken || "").substring(0, 20)}...</code>\n\n`;
    }

    await sendTGLong(env, text);
}

async function handleForceRefresh(args, env) {
    if (args.length < 1) {
        await sendTG(env, "⚠️ 格式：/refresh &lt;label&gt;");
        return;
    }

    const label = args[0];
    const keyData = await getKey(env, label);
    if (!keyData) {
        await sendTG(env, `❌ 未找到 Label "<b>${escHtml(label)}</b>"`);
        return;
    }

    await sendTG(env, `🔄 正在刷新 "<b>${escHtml(label)}</b>"...`);
    const result = await refreshSingleKey(env, keyData);

    if (result.success) {
        await sendTG(env,
            `✅ <b>刷新成功</b>\n\n` +
            `📛 ${escHtml(label)}\n` +
            `⏰ 新到期: ${result.expireStr}\n` +
            `🔑 新Token: <code>${result.newToken.substring(0, 25)}...</code>`
        );
    } else {
        await sendTG(env, `❌ 刷新失败：${escHtml(result.error)}`);
    }
}

async function handleRefreshAll(env) {
    await sendTG(env, "🔄 正在刷新所有 Key...");
    const result = await checkAndRefreshAllKeys(env, true);
    await sendTG(env,
        `✅ <b>批量刷新完成</b>\n\n` +
        `检查: ${result.checked} 个\n` +
        `刷新: ${result.refreshed} 个\n` +
        `失败: ${result.failed} 个\n` +
        `跳过: ${result.skipped} 个`
    );
}

async function handleSetLabel(args, env) {
    if (args.length < 2) {
        await sendTG(env, "⚠️ 格式：/setlabel &lt;旧label&gt; &lt;新label&gt;");
        return;
    }

    const [oldLabel, newLabel] = args;
    const keyData = await getKey(env, oldLabel);
    if (!keyData) {
        await sendTG(env, `❌ 未找到 Label "<b>${escHtml(oldLabel)}</b>"`);
        return;
    }

    const existingNew = await getKey(env, newLabel);
    if (existingNew) {
        await sendTG(env, `❌ Label "<b>${escHtml(newLabel)}</b>" 已被占用`);
        return;
    }

    keyData.label = newLabel;
    await saveKey(env, newLabel, keyData);
    await deleteKey(env, oldLabel);
    await sendTG(env, `✅ 已重命名：<b>${escHtml(oldLabel)}</b> → <b>${escHtml(newLabel)}</b>`);
}

async function handleToggleKey(args, enabled, env) {
    if (args.length < 1) {
        await sendTG(env, `⚠️ 格式：/${enabled ? "enable" : "disable"} &lt;label&gt;`);
        return;
    }

    const label = args[0];
    const keyData = await getKey(env, label);
    if (!keyData) {
        await sendTG(env, `❌ 未找到 Label "<b>${escHtml(label)}</b>"`);
        return;
    }

    keyData.enabled = enabled;
    await saveKey(env, label, keyData);
    await sendTG(env, `${enabled ? "✅ 已启用" : "⏸️ 已禁用"} Key "<b>${escHtml(label)}</b>"`);
}

async function handleStats(env) {
    const keys = await listAllKeys(env);
    const totalUse = keys.reduce((s, k) => s + (k.useCount || 0), 0);
    const totalErr = keys.reduce((s, k) => s + (k.errorCount || 0), 0);

    // 读取全局统计
    const globalStats = await getGlobalStats(env);

    let text = `📈 <b>使用统计</b>\n\n`;
    text += `总请求数: ${globalStats.totalRequests || 0}\n`;
    text += `总 Key 调用: ${totalUse}\n`;
    text += `总错误数: ${totalErr}\n`;
    text += `今日请求: ${globalStats.todayRequests || 0}\n\n`;

    text += `<b>各 Key 使用排名：</b>\n`;
    const sorted = [...keys].sort((a, b) => (b.useCount || 0) - (a.useCount || 0));
    for (let i = 0; i < sorted.length; i++) {
        const k = sorted[i];
        text += `${i + 1}. ${escHtml(k.label)} — ${k.useCount || 0}次 (错误${k.errorCount || 0})\n`;
    }

    await sendTG(env, text);
}

// ================================================================
// 负载均衡：选择最优 Key
// ================================================================
async function selectKey(env) {
    const keys = await listAllKeys(env);
    const now = Date.now();
    const bufferTime = 2 * 60 * 1000; // 2分钟缓冲

    // 过滤出可用的 key
    const available = keys.filter(k =>
        k.enabled &&
        k.accessToken &&
        k.expiresAt > now + bufferTime
    );

    if (available.length === 0) {
        console.error("[LB] No available keys!");
        return null;
    }

    // 负载均衡策略：加权最少使用 + 错误惩罚
    // 分数越低越优先
    const scored = available.map(k => {
        const useScore = (k.useCount || 0);
        const errorPenalty = (k.errorCount || 0) * 10;
        const recentErrorPenalty = k.lastErrorAt && (now - new Date(k.lastErrorAt).getTime() < 300000) ? 50 : 0;
        const freshBonus = k.lastUsed ? 0 : -5; // 从未使用过的优先

        return {
            key: k,
            score: useScore + errorPenalty + recentErrorPenalty + freshBonus
        };
    });

    scored.sort((a, b) => a.score - b.score);

    // 从得分最低的前几个中随机选一个（避免总是打同一个）
    const topN = Math.min(3, scored.length);
    const selected = scored[Math.floor(Math.random() * topN)];

    console.log(`[LB] Selected key "${selected.key.label}" (score: ${selected.score}, from ${available.length} available)`);
    return selected.key;
}

// 更新 Key 使用统计
async function recordKeyUsage(env, label, success) {
    const keyData = await getKey(env, label);
    if (!keyData) return;

    keyData.useCount = (keyData.useCount || 0) + 1;
    keyData.lastUsed = new Date().toISOString();

    if (!success) {
        keyData.errorCount = (keyData.errorCount || 0) + 1;
        keyData.lastErrorAt = new Date().toISOString();
    }

    await saveKey(env, label, keyData);

    // 更新全局统计
    await incrementGlobalStats(env);
}

// ================================================================
// KV 存储操作
// ================================================================
async function saveKey(env, label, data) {
    if (!env.TOKEN_STORE) return;
    try {
        await env.TOKEN_STORE.put(`key:${label}`, JSON.stringify(data));
    } catch (e) {
        console.error("[KV Save Error]", e.message);
    }
}

async function getKey(env, label) {
    if (!env.TOKEN_STORE) return null;
    try {
        return await env.TOKEN_STORE.get(`key:${label}`, { type: "json" });
    } catch (e) {
        console.error("[KV Get Error]", e.message);
        return null;
    }
}

async function deleteKey(env, label) {
    if (!env.TOKEN_STORE) return;
    try {
        await env.TOKEN_STORE.delete(`key:${label}`);
    } catch (e) {
        console.error("[KV Delete Error]", e.message);
    }
}

async function listAllKeys(env) {
    if (!env.TOKEN_STORE) return [];
    try {
        const list = await env.TOKEN_STORE.list({ prefix: "key:" });
        const keys = [];
        for (const item of list.keys) {
            const data = await env.TOKEN_STORE.get(item.name, { type: "json" });
            if (data) keys.push(data);
        }
        return keys;
    } catch (e) {
        console.error("[KV List Error]", e.message);
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
        const stats = await getGlobalStats(env);
        const today = new Date().toISOString().split("T")[0];
        stats.totalRequests = (stats.totalRequests || 0) + 1;
        if (stats.today === today) {
            stats.todayRequests = (stats.todayRequests || 0) + 1;
        } else {
            stats.today = today;
            stats.todayRequests = 1;
        }
        await env.TOKEN_STORE.put("stats:global", JSON.stringify(stats));
    } catch (e) {
        console.error("[Stats Error]", e.message);
    }
}

// ================================================================
// Token 刷新
// ================================================================
async function refreshTokenWithLock(refreshToken) {
    if (pendingRefreshes.has(refreshToken)) {
        return pendingRefreshes.get(refreshToken);
    }
    const promise = performTokenRefresh(refreshToken);
    pendingRefreshes.set(refreshToken, promise);
    try {
        return await promise;
    } finally {
        pendingRefreshes.delete(refreshToken);
    }
}

async function performTokenRefresh(refreshToken) {
    try {
        const resp = await fetch("https://console.anthropic.com/v1/oauth/token", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                grant_type: "refresh_token",
                refresh_token: refreshToken,
                client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
            })
        });
        if (!resp.ok) {
            const errText = await resp.text().catch(() => "");
            console.error(`[Refresh] HTTP ${resp.status}: ${errText}`);
            return null;
        }
        return await resp.json();
    } catch (err) {
        console.error("[Refresh] Network error:", err.message);
        return null;
    }
}

async function refreshSingleKey(env, keyData) {
    const now = Date.now();
    const refreshed = await refreshTokenWithLock(keyData.refreshToken);

    if (!refreshed || !refreshed.access_token) {
        return { success: false, error: "Refresh API returned no token" };
    }

    const newExpiresAt = now + ((refreshed.expires_in || 3600) * 1000);
    const expireStr = new Date(newExpiresAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });

    keyData.accessToken = refreshed.access_token;
    keyData.refreshToken = refreshed.refresh_token || keyData.refreshToken;
    keyData.expiresAt = newExpiresAt;
    keyData.lastRefreshed = new Date().toISOString();

    await saveKey(env, keyData.label, keyData);

    return { success: true, newToken: refreshed.access_token, expireStr };
}

async function checkAndRefreshAllKeys(env, forceAll = false) {
    const keys = await listAllKeys(env);
    const now = Date.now();
    const bufferTime = 10 * 60 * 1000;
    let refreshed = 0, failed = 0, skipped = 0;

    for (const keyData of keys) {
        if (!keyData.enabled) {
            skipped++;
            continue;
        }

        const needsRefresh = forceAll || !keyData.expiresAt || keyData.expiresAt < now + bufferTime;
        if (!needsRefresh) {
            skipped++;
            continue;
        }

        console.log(`[Cron] Refreshing "${keyData.label}"`);
        const result = await refreshSingleKey(env, keyData);

        if (result.success) {
            refreshed++;

            // 构建完整配置用于 Telegram 通知
            const fullConfig = {
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
                `🔄 <b>Token 自动刷新成功</b>\n\n` +
                `📛 Label: <b>${escHtml(keyData.label)}</b>\n` +
                `⏰ 新到期: ${result.expireStr}\n\n` +
                `<b>完整配置（备份用）：</b>\n` +
                `<pre>${escHtml(JSON.stringify(fullConfig, null, 2))}</pre>`
            );
        } else {
            failed++;
            await sendTG(env,
                `❌ <b>Token 刷新失败</b>\n\n` +
                `📛 Label: <b>${escHtml(keyData.label)}</b>\n` +
                `原因: ${escHtml(result.error)}\n\n` +
                `请检查 refreshToken 是否仍然有效`
            );
        }

        // 避免频率限制
        await sleep(1000);
    }

    console.log(`[Cron] Done: ${refreshed} refreshed, ${failed} failed, ${skipped} skipped`);
    return { checked: keys.length, refreshed, failed, skipped };
}

// ================================================================
// 管理路由
// ================================================================
async function handleAdmin(url, request, env) {
    const authHeader = request.headers.get("Authorization") || "";
    const adminKey = authHeader.replace(/^Bearer\s+/i, "").trim();
    if (!env.ADMIN_KEY || adminKey !== env.ADMIN_KEY) {
        return corsResponse(JSON.stringify({ error: "Unauthorized" }), 401);
    }

    if (url.pathname === "/admin/status") {
        const keys = await listAllKeys(env);
        const now = Date.now();
        return corsResponse(JSON.stringify(keys.map(k => ({
            label: k.label,
            enabled: k.enabled,
            expiresAt: k.expiresAt ? new Date(k.expiresAt).toISOString() : null,
            remainingMin: k.expiresAt ? Math.round((k.expiresAt - now) / 60000) : null,
            useCount: k.useCount || 0,
            errorCount: k.errorCount || 0,
            lastUsed: k.lastUsed,
        })), null, 2));
    }

    if (url.pathname === "/admin/refresh-all" && request.method === "POST") {
        const result = await checkAndRefreshAllKeys(env, true);
        return corsResponse(JSON.stringify(result));
    }

    return corsResponse(JSON.stringify({ error: "Not Found" }), 404);
}

// ================================================================
