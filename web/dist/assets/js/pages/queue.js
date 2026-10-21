/* ============================================================
 * pages/queue.js — 迁移队列
 * ============================================================ */

import { api } from "../api.js";
import { confirmDialog } from "../components/dialogs.js";
import { renderPager } from "../components/pager.js";
import { PAGE_SIZE } from "../features/rounds.js";
import { esc, toast, fmtSize, debounce } from "../utils.js";

let rootEl = null;
let page = 0, requestId = 0, curFilter = "";

const statusLabels = {
  pending: "待迁移", transferring: "迁移中", accounting_pending: "待记账",
  done: "已完成", failed: "失败", conflict: "冲突",
};
const filterTabs = [
  ["", "全部"], ["pending", "待迁移"], ["transferring", "迁移中"], ["done", "已完成"],
  ["failed", "失败"], ["conflict", "冲突"], ["accounting_pending", "待记账"],
];

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel">
      <div class="panel-heading"><div><span class="eyebrow">TRANSFER QUEUE</span>
        <h2>迁移队列</h2><p>按状态与路径查找全部队列记录</p></div></div>
      <div class="toolbar"><label class="ctl-label" for="queue-search">搜索文件</label>
        <input id="queue-search" type="search" placeholder="文件名 / 相对路径 / 源或目标路径" autocomplete="off" maxlength="256"></div>
      <div class="filters" id="queue-filters">
        ${filterTabs.map(([v, l]) => `<button type="button" data-s="${v}" class="${v === curFilter ? "active" : ""}">${l}</button>`).join("")}
        <button type="button" class="btn small gray" style="margin-left:auto" data-clear="done">清除已完成</button>
        <button type="button" class="btn small gray" data-clear="failed">清除失败</button>
        <button type="button" class="btn small gray" data-clear="conflict">清除冲突</button>
        <button type="button" class="btn small danger-ghost" data-clear="all">清空全部</button>
        <button type="button" class="btn small ghost" data-retry-failed>重试全部失败项</button>
        <button type="button" class="btn small ghost" data-pause-all>⏸ 暂停全部迁移</button>
        <button type="button" class="btn small ghost" data-resume-all>▶ 恢复全部迁移</button>
        <span class="hint" id="queue-pause-hint"></span>
      </div>
      <div id="queue-list"><div class="empty">加载队列中…</div></div>
      <div class="pager" id="queue-pager"></div>
    </div>`;

  const search = root.querySelector("#queue-search");
  search.addEventListener("input", debounce(() => { page = 0; load(); }, 300));
  root.querySelectorAll("#queue-filters button[data-s]").forEach(b =>
    b.addEventListener("click", () => {
      root.querySelectorAll("#queue-filters button[data-s]").forEach(x => x.classList.remove("active"));
      b.classList.add("active");
      curFilter = b.dataset.s;
      page = 0; load();
    }));
  root.querySelectorAll("button[data-clear]").forEach(b =>
    b.addEventListener("click", () => clearQueue(b.dataset.clear)));
  root.querySelector("[data-retry-failed]").addEventListener("click", retryFailed);
  root.querySelector("[data-pause-all]").addEventListener("click", () => setAllPaused(true));
  root.querySelector("[data-resume-all]").addEventListener("click", () => setAllPaused(false));
}

export function show() { load(); }

export async function load() {
  if (!rootEl) return;
  const id = ++requestId;
  const searchValue = rootEl.querySelector("#queue-search").value.trim();
  const params = new URLSearchParams({
    status: curFilter, search: searchValue,
    limit: PAGE_SIZE, offset: page * PAGE_SIZE, with_total: "true",
  });
  try {
    const { items, total } = await api("/api/queue?" + params);
    if (id !== requestId) return;
    try {
      const tasks = await api("/api/tasks");
      const pausedCount = tasks.filter(t => t.paused).length;
      const hint = rootEl.querySelector("#queue-pause-hint");
      if (hint) hint.textContent = pausedCount ? `已暂停迁移的任务：${pausedCount} 个` : "";
    } catch (_) { /* 暂停提示失败不影响队列展示 */ }
    if (id !== requestId) return;  // 第二个 await 期间切换了筛选/搜索时，丢弃本次旧响应
    if (!items.length && total && page > 0) { page = Math.ceil(total / PAGE_SIZE) - 1; return load(); }
    const list = rootEl.querySelector("#queue-list");
    list.innerHTML = !items.length
      ? `<div class="empty">${searchValue || curFilter ? "未找到匹配的队列项，请调整搜索或状态" : "队列为空"}</div>`
      : `<div class="table-wrap"><table><tr><th>ID</th><th>文件</th><th>大小</th><th>状态</th><th>重试</th><th>时间</th><th></th></tr>` +
        items.map(q => `<tr><td>${q.id}</td><td class="path-cell" title="${esc(q.src_path)}">${esc(q.rel_path)}</td>
          <td>${fmtSize(q.size)}</td>
          <td><span class="tag ${esc(q.status)}">${statusLabels[q.status] || esc(q.status)}</span></td>
          <td>${q.retries}</td><td>${esc(q.updated_at)}</td>
          <td>${q.status === "accounting_pending"
            ? `<button class="btn small" data-retry="${q.id}">重试记账</button>`
            : ["failed", "conflict"].includes(q.status)
              ? `<button class="btn small" data-retry="${q.id}">重试</button>` : ""}</td></tr>`).join("")
        + "</table></div>";
    list.querySelectorAll("button[data-retry]").forEach(b =>
      b.addEventListener("click", () => retryOne(Number(b.dataset.retry))));
    renderPager("#queue-pager", page, total, PAGE_SIZE, p => {
      page = p; load();
      rootEl.scrollIntoView({ behavior: "smooth" });
    });
  } catch (e) {
    if (id !== requestId) return;
    rootEl.querySelector("#queue-list").innerHTML = '<div class="empty">队列加载失败：' + esc(e.message) + "</div>";
    rootEl.querySelector("#queue-pager").innerHTML = "";
  }
}

async function retryOne(id) {
  try {
    await api(`/api/queue/${id}/retry`, { method: "POST" });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("重试失败: " + e.message, "error"); }
}

async function clearQueue(scope) {
  const names = { done: "已完成", failed: "失败", conflict: "冲突", all: "全部" };
  const ok = await confirmDialog(`确认清除${names[scope]}队列条目？`, { title: "清除队列", danger: scope === "all", okText: "清除" });
  if (!ok) return;
  const q = scope === "all" ? "?status=all" : scope === "done" ? "" : `?status=${scope}`;
  try {
    await api(`/api/queue${q}`, { method: "DELETE" });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("清除队列失败: " + e.message, "error"); }
}

async function setAllPaused(paused) {
  const ok = await confirmDialog(
    paused
      ? "确认暂停全部任务的迁移？\n正在传输的文件会完成后停止，新文件不再开始；扫描与队列记录继续。"
      : "确认恢复全部任务的迁移？",
    { title: paused ? "暂停全部迁移" : "恢复全部迁移", okText: paused ? "暂停" : "恢复" });
  if (!ok) return;
  try {
    const r = await api("/api/tasks/pause-all", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ paused }),
    });
    toast(paused ? `已暂停 ${r.changed} 个任务的迁移` : `已恢复 ${r.changed} 个任务的迁移`, "success");
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("操作失败: " + e.message, "error"); }
}

async function retryFailed() {
  const ok = await confirmDialog("确认重试全部失败队列条目？", { title: "批量重试", okText: "重试" });
  if (!ok) return;
  try {
    const result = await api("/api/queue/retry-failed", { method: "POST" });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
    if (!result.retried) toast("当前没有失败项");
  } catch (e) { load(); toast("批量重试失败: " + e.message, "error"); }
}
