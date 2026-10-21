/* ============================================================
 * main.js — 应用入口：主题 / Hash 路由 / 轮询调度
 * ============================================================ */

import { initTheme, mountThemeSelect } from "./theme.js";
import { setToken, getToken } from "./api.js";
import { promptDialog } from "./components/dialogs.js";
import { watchSelects } from "./components/customSelect.js";
import { tickCountdowns } from "./features/sync.js";
import { bindRoundModalEsc } from "./features/rounds.js";
import { toast } from "./utils.js";

import * as dash from "./pages/dash.js";
import * as tasks from "./pages/tasks.js";
import * as rounds from "./pages/rounds.js";
import * as eio from "./pages/eio.js";
import * as queue from "./pages/queue.js";
import * as exclusions from "./pages/exclusions.js";
import * as logs from "./pages/logs.js";
import * as settings from "./pages/settings.js";
import * as browse from "./pages/browse.js";
import { store } from "./store.js";

const TABS = [
  { id: "dash", label: "仪表盘", page: dash },
  { id: "tasks", label: "任务管理", page: tasks },
  { id: "rounds", label: "上传轮次", page: rounds },
  { id: "eio", label: "EIO 故障", page: eio },
  { id: "queue", label: "队列", page: queue },
  { id: "exclusions", label: "永久排除", page: exclusions },
  { id: "logs", label: "日志", page: logs },
  { id: "settings", label: "设置", page: settings },
  { id: "browse", label: "文件浏览", page: browse },
];

const mountedPages = [];  // 已挂载页面（启动时全部预挂载，保证轮询可安全更新任意页）

/* ---------- 顶栏 ---------- */

function buildHeader() {
  initTheme();
  const actions = document.querySelector(".header-actions");

  const tokenBtn = document.createElement("button");
  tokenBtn.className = "btn small gray";
  tokenBtn.type = "button";
  tokenBtn.textContent = "API Token";
  tokenBtn.addEventListener("click", async () => {
    const value = await promptDialog(
      "输入 API Token（仅保存在此标签页；留空并确认清除）",
      { title: "API Token", value: getToken(), okText: "保存" },
    );
    if (value === null) return;  // 用户取消
    setToken(value);
    toast(value.trim() ? "API Token 已保存" : "API Token 已清除");
  });
  actions.prepend(tokenBtn);

  mountThemeSelect(actions);
}

/* ---------- 标签导航 ---------- */

function buildTabs() {
  const nav = document.getElementById("tabs");
  nav.innerHTML = TABS.map(t =>
    `<button type="button" data-tab="${t.id}">${t.label}</button>`).join("");
  nav.querySelectorAll("button").forEach(b =>
    b.addEventListener("click", () => { location.hash = "#/" + b.dataset.tab; }));
}

function currentTabFromHash() {
  const id = (location.hash || "").replace(/^#\//, "");
  return TABS.some(t => t.id === id) ? id : "dash";
}

function activate(tabId) {
  const tab = TABS.find(t => t.id === tabId) || TABS[0];
  store.activeTab = tab.id;

  document.querySelectorAll("#tabs button").forEach(b =>
    b.classList.toggle("active", b.dataset.tab === tab.id));

  TABS.forEach(t => {
    const el = document.getElementById("tab-" + t.id);
    if (el) el.hidden = t.id !== tab.id;
  });

  tab.page.show?.();

  // 与 hash 保持同步（初始加载时 hash 可能为空）
  const expected = "#/" + tab.id;
  if (location.hash !== expected) {
    history.replaceState(null, "", expected);
  }
  window.scrollTo({ top: 0 });
}

/** 预挂载全部页面骨架：轮询与数据联动始终可安全写入任意页面的 DOM */
function mountAllPages() {
  TABS.forEach(t => {
    t.page.mount(document.getElementById("tab-" + t.id));
    mountedPages.push(t.id);
  });
}

/* ---------- 轮询调度 ---------- */

function startPolling() {
  // 每 3 秒：仪表盘数据始终刷新（供倒计时/徽标使用）；
  // 轮次 / EIO 页仅在可见时刷新 —— 与旧版行为一致。
  setInterval(() => {
    if (document.hidden) return;
    dash.loadDash();
    if (store.activeTab === "rounds") rounds.load();
    if (store.activeTab === "eio") eio.load();
  }, 3000);

  // 每秒：扫描倒计时与轮次耗时
  setInterval(() => {
    if (document.hidden) return;
    tickCountdowns();
  }, 1000);

  // 每 30 秒：迁移趋势统计低频刷新（仅仪表盘可见时）
  setInterval(() => {
    if (document.hidden || store.activeTab !== "dash") return;
    dash.loadTrend();
  }, 30000);

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    dash.loadDash();
    const tab = TABS.find(t => t.id === store.activeTab);
    tab?.page.show?.();
  });

  // 任意页面数据变化（新建/删除任务、排除项等）→ 联动刷新
  document.addEventListener("fm:data-changed", () => {
    dash.loadDash();
    if (store.activeTab === "tasks") tasks.loadTasks();
    if (store.activeTab === "eio") eio.load();
    if (store.activeTab === "queue") queue.load();
    if (store.activeTab === "exclusions") exclusions.load();
  });
}

/* ---------- 启动 ---------- */

buildHeader();
buildTabs();
mountAllPages();
watchSelects();       // 所有 select 接管为主题化自绘下拉
bindRoundModalEsc();   // 轮次弹窗 Escape 关闭
activate(currentTabFromHash());
window.addEventListener("hashchange", () => activate(currentTabFromHash()));
startPolling();
