/* ============================================================
 * theme.js — 多主题管理：4 套主题 + 跟随系统 + 持久化
 * ============================================================ */

export const THEMES = [
  { id: "light",  name: "晨蓝", icon: "🌤" },
  { id: "dark",   name: "夜幕", icon: "🌙" },
  { id: "paper",  name: "暖沙", icon: "🏺" },
  { id: "forest", name: "青竹", icon: "🌿" },
];

const STORAGE_KEY = "fm-theme";

function savedTheme() {
  try { return localStorage.getItem(STORAGE_KEY); } catch (_) { return null; }
}

/** 应用主题到 <html data-theme> */
export function applyTheme(id) {
  if (!THEMES.some(t => t.id === id)) id = "light";
  document.documentElement.dataset.theme = id;
  try { localStorage.setItem(STORAGE_KEY, id); } catch (_) { /* 隐私模式忽略 */ }
}

/** 初始化：优先本地记忆，其次跟随系统深浅色 */
export function initTheme() {
  const saved = savedTheme();
  const prefersDark = window.matchMedia
    && matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(saved || (prefersDark ? "dark" : "light"));
}

/** 在顶栏渲染主题选择器并绑定切换 */
export function mountThemeSelect(container) {
  const label = document.createElement("label");
  label.className = "theme-select";
  label.title = "切换界面主题";
  const current = document.documentElement.dataset.theme || "light";
  label.innerHTML =
    `<span class="icon" aria-hidden="true">${THEMES.find(t => t.id === current)?.icon ?? "🎨"}</span>` +
    `<select aria-label="选择主题">` +
    THEMES.map(t => `<option value="${t.id}"${t.id === current ? " selected" : ""}>${t.name}</option>`).join("") +
    `</select>`;
  const select = label.querySelector("select");
  select.addEventListener("change", () => {
    applyTheme(select.value);
    label.querySelector(".icon").textContent =
      THEMES.find(t => t.id === select.value)?.icon ?? "🎨";
  });
  container.appendChild(label);
}
