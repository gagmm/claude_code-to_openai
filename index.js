const pendingRefreshes = new Map();
const tokenCache = new Map();
export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }
    const url = new URL(request.url);
    if (url.pathname === "/v1/chat/completions" && request.method === "POST") {
      return await handleChatCompletions(request, env);
    }
    if (url.pathname === "/v1/models" && request.method === "GET") {
      return new Response(
        JSON.stringify({
          object: "list",
          data: [
            {
              id: "claude-3-7-sonnet-20250219",
              object: "model",
              created: 0,
              owned_by: "system",
            },
          ],
        }),
        {
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        },
      );
    }
    if (url.pathname === "/debug/version") {
      return new Response(
        JSON.stringify({ version: "2.0-fixed", timestamp: "2025-06-27" }),
        {
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        },
      );
    }
    return new Response(JSON.stringify({ error: "Not Found" }), {
      status: 404,
    });
  },
};
async function handleChatCompletions(a, b) {
  const authHeader = a.headers.get("Authorization") || "";
  let rawTokenString = authHeader.replace(/^Bearer\s+/i, "").trim();
  console.log(
    "[DEBUG] rawTokenString starts with:",
    rawTokenString.substring(0, 30),
  );
  console.log("[DEBUG] rawTokenString length:", rawTokenString.length);
  console.log("[DEBUG] startsWith '{':", rawTokenString.startsWith("{"));
  let activeAccessToken = rawTokenString;
  let injectionText = "";
  if (rawTokenString.startsWith("{")) {
    try {
      const parsed = JSON.parse(rawTokenString);
      console.log("[DEBUG] JSON parsed successfully");
      const oauthInfo = parsed.claudeAiOauth;
      console.log("[DEBUG] oauthInfo exists:", !!oauthInfo);
      console.log(
        "[DEBUG] oauthInfo.accessToken exists:",
        !!(oauthInfo && oauthInfo.accessToken),
      );
      if (oauthInfo && oauthInfo.accessToken) {
        const { accessToken, refreshToken, expiresAt } = oauthInfo;
        const currentTime = Date.now();
        const bufferTime = 5 * 60 * 1000;
        console.log(
          "[DEBUG] accessToken starts with:",
          accessToken.substring(0, 20),
        );
        console.log("[DEBUG] expiresAt:", expiresAt);
        console.log("[DEBUG] currentTime:", currentTime);
        console.log(
          "[DEBUG] token expired?:",
          expiresAt < currentTime + bufferTime,
        );
        let cached = null;
        if (b.TOKEN_CACHE) {
          try {
            cached = await b.TOKEN_CACHE.get(refreshToken, { type: "json" });
          } catch (e) {
            console.error("[KV Read Error]", e.message);
          }
        }
        if (!cached) {
          cached = tokenCache.get(refreshToken) || null;
        }
        if (cached && cached.expiresAt > currentTime + bufferTime) {
          console.log("[DEBUG] Using cached token");
          activeAccessToken = cached.accessToken;
        } else if (expiresAt < currentTime + bufferTime) {
          console.log("[DEBUG] Token needs refresh");
          const refreshedData = await refreshTokenWithLock(refreshToken);
          if (refreshedData && refreshedData.access_token) {
            activeAccessToken = refreshedData.access_token;
            const newRefreshToken = refreshedData.refresh_token || refreshToken;
            const newExpiresAt =
              currentTime + (refreshedData.expires_in || 3600) * 1000;
            const cacheEntry = {
              accessToken: activeAccessToken,
              expiresAt: newExpiresAt,
            };
            if (b.TOKEN_CACHE) {
              try {
                await b.TOKEN_CACHE.put(
                  refreshToken,
                  JSON.stringify(cacheEntry),
                  {
                    expirationTtl: Math.max(
                      Math.floor(refreshedData.expires_in || 3600),
                      60,
                    ),
                  },
                );
              } catch (e) {
                console.error("[KV Write Error]", e.message);
              }
            }
            tokenCache.set(refreshToken, cacheEntry);
            parsed.claudeAiOauth.accessToken = activeAccessToken;
            parsed.claudeAiOauth.refreshToken = newRefreshToken;
            parsed.claudeAiOauth.expiresAt = newExpiresAt;
            const expireDateStr = new Date(newExpiresAt).toLocaleString(
              "zh-CN",
              { timeZone: "Asia/Shanghai" },
            );
            injectionText = `>⚠️**[系统提示：Token已在后台自动刷新]**\n>⏰**下次到期时间：**\`${expireDateStr}\`\n>📋**请复制下方全新配置并更新到您的客户端API Key设置中：**\n\n\`\`\`json\n${JSON.stringify(parsed, null, 2)}\n\`\`\`\n\n---\n\n`;
          } else {
            console.warn(
              "[Token Refresh Failed] Falling back to original access token",
            );
            activeAccessToken = accessToken;
          }
        } else {
          console.log("[DEBUG] Token still valid, using original accessToken");
          activeAccessToken = accessToken;
        }
      }
    } catch (e) {
      console.error("[Token Parse Error]", e.message, e.stack);
    }
  }
  console.log(
    "[DEBUG] Final activeAccessToken starts with:",
    activeAccessToken.substring(0, 30),
  );
  console.log(
    "[DEBUG] Is OAuth token (sk-ant-oat)?:",
    activeAccessToken.startsWith("sk-ant-oat"),
  );
  if (!activeAccessToken) {
    return new Response(
      JSON.stringify({ error: "Missing or invalid Authorization header" }),
      {
        status: 401,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      },
    );
  }
  const openaiReq = await a.json().catch((e) => {
    console.error("[Request Body Parse Error]", e.message);
    return {};
  });
  let systemPrompt = "";
  const rawMessages = [];
  for (const msg of openaiReq.messages || []) {
    if (msg.role === "system") {
      systemPrompt +=
        (typeof msg.content === "string"
          ? msg.content
          : JSON.stringify(msg.content)) + "\n";
    } else if (msg.role === "user" || msg.role === "assistant") {
      const content = convertContent(msg.content);
      rawMessages.push({ role: msg.role, content });
    } else {
      console.warn(
        `[Role Mapping]Unsupported role"${msg.role}",mapping to"user"`,
      );
      const content =
        typeof msg.content === "string"
          ? msg.content
          : JSON.stringify(msg.content);
      rawMessages.push({ role: "user", content });
    }
  }
  const anthropicMessages = mergeConsecutiveRoles(rawMessages);
  if (anthropicMessages.length > 0 && anthropicMessages[0].role !== "user") {
    anthropicMessages.unshift({ role: "user", content: "(continued)" });
  }
  let model = openaiReq.model || "claude-3-7-sonnet-20250219";
  if (!model.startsWith("claude-")) model = "claude-3-7-sonnet-20250219";
  const anthropicReq = {
    model: model,
    max_tokens: openaiReq.max_tokens || 8192,
    messages: anthropicMessages,
  };
  if (systemPrompt.trim()) anthropicReq.system = systemPrompt.trim();
  if (openaiReq.stream) anthropicReq.stream = true;
  if (openaiReq.temperature !== undefined)
    anthropicReq.temperature = openaiReq.temperature;
  if (openaiReq.top_p !== undefined) anthropicReq.top_p = openaiReq.top_p;
  const anthropicHeaders = {
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json",
    "anthropic-beta": "oauth-2025-04-20",
    "x-app": "cli",
    "User-Agent": "claude-code/2.0.62",
  };
  if (activeAccessToken.startsWith("sk-ant-oat")) {
    anthropicHeaders["Authorization"] = `Bearer ${activeAccessToken}`;
    console.log("[DEBUG] Using Authorization: Bearer header");
  } else {
    anthropicHeaders["x-api-key"] = activeAccessToken;
    console.log("[DEBUG] Using x-api-key header");
    console.log("[DEBUG] WARNING: Token does NOT start with sk-ant-oat!");
    console.log(
      "[DEBUG] Token first 50 chars:",
      activeAccessToken.substring(0, 50),
    );
  }
  console.log(
    "[DEBUG] Final Anthropic request:",
    JSON.stringify({
      model: anthropicReq.model,
      max_tokens: anthropicReq.max_tokens,
      message_count: anthropicReq.messages.length,
      has_system: !!anthropicReq.system,
      stream: !!anthropicReq.stream,
      auth_type: activeAccessToken.startsWith("sk-ant-oat")
        ? "oauth"
        : "api-key",
    }),
  );
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 120000);
  let response;
  try {
    response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: anthropicHeaders,
      body: JSON.stringify(anthropicReq),
      signal: controller.signal,
    });
  } catch (err) {
    clearTimeout(timeoutId);
    const isTimeout = err.name === "AbortError";
    console.error(
      "[Anthropic Fetch Error]",
      isTimeout ? "Request timed out (120s)" : err.message,
    );
    return new Response(
      JSON.stringify({
        error: isTimeout
          ? "Request to Anthropic API timed out"
          : `Fetch error:${err.message}`,
      }),
      {
        status: isTimeout ? 504 : 502,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      },
    );
  }
  clearTimeout(timeoutId);
  console.log("[DEBUG] Anthropic response status:", response.status);
  if (!response.ok) {
    const errorBody = await response.text().catch(() => "Unknown error");
    console.error(
      `[Anthropic API Error]Status:${response.status},Body:${errorBody}`,
    );
    return new Response(errorBody, {
      status: response.status,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
  if (openaiReq.stream) {
    return handleStream(response, model, injectionText);
  } else {
    const data = await response.json();
    return new Response(
      JSON.stringify(anthropicToOpenaiResp(data, model, injectionText)),
      {
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      },
    );
  }
}
function convertContent(a) {
  if (typeof a === "string") {
    return a;
  }
  if (Array.isArray(a)) {
    const anthropicParts = [];
    for (const part of a) {
      if (part.type === "text") {
        anthropicParts.push({ type: "text", text: part.text });
      } else if (part.type === "image_url" && part.image_url) {
        const url = part.image_url.url || "";
        const dataUriMatch = url.match(
          /^data:(image\/[a-zA-Z+]+);base64,(.+)$/,
        );
        if (dataUriMatch) {
          anthropicParts.push({
            type: "image",
            source: {
              type: "base64",
              media_type: dataUriMatch[1],
              data: dataUriMatch[2],
            },
          });
        } else {
          anthropicParts.push({ type: "text", text: `[Image URL:${url}]` });
        }
      }
    }
    return anthropicParts.length > 0 ? anthropicParts : "(empty content)";
  }
  return typeof a === "object" ? JSON.stringify(a) : String(a);
}
function mergeConsecutiveRoles(a) {
  if (a.length === 0) return [];
  const merged = [];
  for (const msg of a) {
    if (merged.length > 0 && merged[merged.length - 1].role === msg.role) {
      const last = merged[merged.length - 1];
      if (typeof last.content === "string" && typeof msg.content === "string") {
        last.content += "\n\n" + msg.content;
      } else {
        const toArray = (c) => {
          if (Array.isArray(c)) return c;
          if (typeof c === "string") return [{ type: "text", text: c }];
          return [c];
        };
        last.content = [...toArray(last.content), ...toArray(msg.content)];
      }
    } else {
      merged.push({ role: msg.role, content: msg.content });
    }
  }
  return merged;
}
async function refreshTokenWithLock(a) {
  if (pendingRefreshes.has(a)) {
    return pendingRefreshes.get(a);
  }
  const promise = performTokenRefresh(a);
  pendingRefreshes.set(a, promise);
  try {
    return await promise;
  } finally {
    pendingRefreshes.delete(a);
  }
}
async function performTokenRefresh(a) {
  try {
    const response = await fetch(
      "https://console.anthropic.com/v1/oauth/token",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          grant_type: "refresh_token",
          refresh_token: a,
          client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
        }),
      },
    );
    if (!response.ok) {
      const errText = await response.text().catch(() => "");
      console.error(
        `[Token Refresh HTTP Error]Status:${response.status},Body:${errText}`,
      );
      return null;
    }
    return await response.json();
  } catch (err) {
    console.error("[Token Refresh Network Error]", err.message);
    return null;
  }
}
function anthropicToOpenaiResp(a, b, d) {
  let originalText = "";
  if (a.content && Array.isArray(a.content)) {
    originalText = a.content
      .filter((c) => c.type === "text")
      .map((c) => c.text)
      .join("");
  }
  let finalText = d ? d + originalText : originalText;
  return {
    id: a.id || `chatcmpl-${crypto.randomUUID()}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: b,
    choices: [
      {
        index: 0,
        message: { role: "assistant", content: finalText },
        finish_reason: mapStopReason(a.stop_reason),
      },
    ],
    usage: {
      prompt_tokens: a.usage?.input_tokens || 0,
      completion_tokens: a.usage?.output_tokens || 0,
      total_tokens:
        (a.usage?.input_tokens || 0) + (a.usage?.output_tokens || 0),
    },
  };
}
function mapStopReason(a) {
  const map = { end_turn: "stop", stop_sequence: "stop", max_tokens: "length" };
  return map[a] || "stop";
}
function handleStream(a, b, c) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  (async () => {
    const reader = a.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let chatId = `chatcmpl-${crypto.randomUUID()}`;
    try {
      if (c) {
        const injectionChunk = {
          id: chatId,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model: b,
          choices: [
            {
              index: 0,
              delta: { role: "assistant", content: c },
              finish_reason: null,
            },
          ],
        };
        await writer.write(
          encoder.encode(`data:${JSON.stringify(injectionChunk)}\n\n`),
        );
      }
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const dataStr = line.slice(6).trim();
          if (!dataStr || dataStr === "[DONE]") continue;
          try {
            const event = JSON.parse(dataStr);
            if (event.type === "message_start") {
              chatId = event.message?.id || chatId;
              if (!c) {
                const roleChunk = {
                  id: chatId,
                  object: "chat.completion.chunk",
                  created: Math.floor(Date.now() / 1000),
                  model: b,
                  choices: [
                    {
                      index: 0,
                      delta: { role: "assistant" },
                      finish_reason: null,
                    },
                  ],
                };
                await writer.write(
                  encoder.encode(`data:${JSON.stringify(roleChunk)}\n\n`),
                );
              }
            } else if (event.type === "content_block_delta") {
              if (event.delta?.type === "text_delta" && event.delta.text) {
                const chunk = {
                  id: chatId,
                  object: "chat.completion.chunk",
                  created: Math.floor(Date.now() / 1000),
                  model: b,
                  choices: [
                    {
                      index: 0,
                      delta: { content: event.delta.text },
                      finish_reason: null,
                    },
                  ],
                };
                await writer.write(
                  encoder.encode(`data:${JSON.stringify(chunk)}\n\n`),
                );
              }
            } else if (event.type === "message_delta") {
              const finishReason = mapStopReason(event.delta?.stop_reason);
              const chunk = {
                id: chatId,
                object: "chat.completion.chunk",
                created: Math.floor(Date.now() / 1000),
                model: b,
                choices: [{ index: 0, delta: {}, finish_reason: finishReason }],
                usage: event.usage
                  ? {
                      prompt_tokens: event.usage.input_tokens || 0,
                      completion_tokens: event.usage.output_tokens || 0,
                      total_tokens:
                        (event.usage.input_tokens || 0) +
                        (event.usage.output_tokens || 0),
                    }
                  : undefined,
              };
              await writer.write(
                encoder.encode(`data:${JSON.stringify(chunk)}\n\n`),
              );
            } else if (event.type === "message_stop") {
              await writer.write(encoder.encode("data: [DONE]\n\n"));
            } else if (event.type === "error") {
              console.error(
                "[Anthropic Stream Error]",
                JSON.stringify(event.error),
              );
              const errorChunk = {
                id: chatId,
                object: "chat.completion.chunk",
                created: Math.floor(Date.now() / 1000),
                model: b,
                choices: [
                  {
                    index: 0,
                    delta: {
                      content: `\n\n[Error:${event.error?.message || "Unknown stream error"}]`,
                    },
                    finish_reason: "stop",
                  },
                ],
              };
              await writer.write(
                encoder.encode(`data:${JSON.stringify(errorChunk)}\n\n`),
              );
              await writer.write(encoder.encode("data: [DONE]\n\n"));
            }
          } catch (e) {
            console.error(
              "[Stream Event Parse Error]",
              e.message,
              "Raw:",
              dataStr.substring(0, 200),
            );
          }
        }
      }
      if (buffer.trim().startsWith("data: ")) {
        const dataStr = buffer.trim().slice(6).trim();
        if (dataStr && dataStr !== "[DONE]") {
          try {
            const event = JSON.parse(dataStr);
            if (event.type === "message_stop") {
              await writer.write(encoder.encode("data: [DONE]\n\n"));
            }
          } catch (e) {
            console.error("[Stream Buffer Residual Parse Error]", e.message);
          }
        }
      }
    } catch (err) {
      console.error("[Stream Processing Error]", err.message);
      try {
        const errorChunk = {
          id: chatId,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model: b,
          choices: [
            {
              index: 0,
              delta: { content: `\n\n[Stream Error:${err.message}]` },
              finish_reason: "stop",
            },
          ],
        };
        await writer.write(
          encoder.encode(`data:${JSON.stringify(errorChunk)}\n\n`),
        );
        await writer.write(encoder.encode("data: [DONE]\n\n"));
      } catch (_) {}
    } finally {
      try {
        await writer.close();
      } catch (_) {}
    }
  })();
  return new Response(readable, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
    },
  });
}
