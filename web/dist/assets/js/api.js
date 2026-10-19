/* ============================================================
 * api.js — 请求封装：Token 管理 + 401 重认证
 * ============================================================ */

const TOKEN_KEY = "filemover_api_token";

/** 认证弹窗注入点（由 dialogs.js 在启动时设置，避免循环依赖） */
let authPrompt = null;

/** 注册 401 时使用的取 Token 弹窗（返回 Promise<string|null>） */
export function setAuthPrompt(fn) { authPrompt = fn; }

export function getToken() {
  try { return sessionStorage.getItem(TOKEN_KEY) || ""; } catch (_) { return ""; }
}

export function setToken(value) {
  try {
    if (value && value.trim()) sessionStorage.setItem(TOKEN_KEY, value.trim());
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch (_) { /* 忽略 */ }
}

export function hasToken() { return !!getToken(); }

/**
 * 请求 JSON API。
 * - 自动附带 Bearer Token
 * - 401 时弹认证框重试一次（可被 opts.onAuthChallenge 观测）
 * - 非 2xx 抛出 Error(detail)
 */
export async function api(path, opts = {}) {
  const { onAuthChallenge, ...requestOpts } = opts;
  async function send() {
    const headers = new Headers(requestOpts.headers || {});
    const token = getToken();
    if (token) headers.set("Authorization", "Bearer " + token);
    return fetch(path, { ...requestOpts, headers, credentials: "same-origin" });
  }

  let r = await send();
  if (r.status === 401 && authPrompt) {
    onAuthChallenge?.("start");
    setToken("");
    const value = await authPrompt();
    if (value) { onAuthChallenge?.("resume"); r = await send(); }
  }
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(typeof body.detail === "string" ? body.detail : `请求失败 (${r.status})`);
  }
  return r.json();
}

/** POST JSON 快捷方式 */
export function apiPost(path, data, opts = {}) {
  return api(path, {
    ...opts,
    method: "POST",
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    body: JSON.stringify(data),
  });
}
