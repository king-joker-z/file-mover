/* ============================================================
 * utils.js — 通用工具：转义 / 格式化 / Toast / 防抖 / 倒计时
 * ============================================================ */

/** HTML 转义 */
export function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

/** 字节数 → 可读大小 */
export function fmtSize(n) {
  if (!n) return "-";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < 4) { n /= 1024; i++; }
  return n.toFixed(1) + u[i];
}

export const $ = s => document.querySelector(s);
export const $$ = s => document.querySelectorAll(s);

/** 轻提示（替代阻塞式 alert） */
export function toast(msg, type) {
  let host = document.getElementById("toast-host");
  if (!host) {
    host = document.createElement("div");
    host.id = "toast-host";
    document.body.appendChild(host);
  }
  const el = document.createElement("div");
  el.className = "toast" + (type ? " " + type : "");
  el.textContent = msg;
  host.appendChild(el);
  requestAnimationFrame(() => el.classList.add("show"));
  setTimeout(() => { el.classList.remove("show"); setTimeout(() => el.remove(), 300); },
    type === "error" ? 6000 : 4000);
}

/** 输入防抖 */
export function debounce(fn, delay = 300) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), delay); };
}

/** 秒数 → 「x小时 mm分 ss秒」或「mm:ss」 */
export function formatCountdown(seconds) {
  const n = Math.ceil(Math.max(0, seconds));
  const h = Math.floor(n / 3600), m = Math.floor(n % 3600 / 60), s = n % 60;
  return h
    ? `${h}小时 ${String(m).padStart(2, "0")}分 ${String(s).padStart(2, "0")}秒`
    : `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

/** Unix 秒 → 本地时间字符串 */
export function fmtDateTime(unixSeconds) {
  return unixSeconds ? new Date(unixSeconds * 1000).toLocaleString("zh-CN") : "—";
}

/** Unix 秒 → 本地时间（仅时分秒） */
export function fmtTime(unixSeconds) {
  return unixSeconds ? new Date(unixSeconds * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : "";
}
