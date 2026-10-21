/* ============================================================
 * pages/logs.js — 迁移日志
 * ============================================================ */

import { api } from "../api.js";
import { confirmDialog } from "../components/dialogs.js";
import { renderPager } from "../components/pager.js";
import { PAGE_SIZE } from "../features/rounds.js";
import { esc, toast, debounce } from "../utils.js";

let rootEl = null;
let page = 0, requestId = 0;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel"><div class="panel-heading"><div><span class="eyebrow">ACTIVITY</span>
      <h2>迁移日志</h2><p>按结果、任务及关键字查找历史记录</p></div>
      <button class="btn small danger-ghost" type="button" id="btn-clear-logs">清空全部日志</button></div>
      <div class="toolbar"><label class="ctl-label" for="log-search">搜索日志</label>
        <input id="log-search" type="search" placeholder="源/目标路径、详情" autocomplete="off" maxlength="256">
        <select id="log-status" aria-label="日志结果">
          <option value="">全部结果</option><option value="success">成功</option>
          <option value="failed">失败</option><option value="conflict">冲突</option>
          <option value="accounting_pending">待记账</option></select>
        <select id="log-task" aria-label="所属任务"><option value="">全部任务</option></select></div>
      <div id="log-list"><div class="empty">加载日志中…</div></div>
      <div class="pager" id="log-pager"></div>
    </div>`;

  root.querySelector("#log-search").addEventListener("input", debounce(() => { page = 0; load(); }, 300));
  ["#log-status", "#log-task"].forEach(sel =>
    root.querySelector(sel).addEventListener("change", () => { page = 0; load(); }));
  root.querySelector("#btn-clear-logs").addEventListener("click", clearLogs);
}

export function show() { loadTaskOptions(); load(); }

async function loadTaskOptions() {
  try {
    const tasks = await api("/api/tasks");
    const select = rootEl.querySelector("#log-task");
    const chosen = select.value;
    select.innerHTML = '<option value="">全部任务</option>'
      + tasks.map(t => `<option value="${t.id}">${esc(t.name)}</option>`).join("");
    if ([...select.options].some(o => o.value === chosen)) select.value = chosen;
  } catch (_) { /* 任务暂不可用时保持现有选择，不影响日志搜索。 */ }
}

export async function load() {
  if (!rootEl) return;
  const id = ++requestId;
  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: page * PAGE_SIZE, with_total: "true" });
  const searchValue = rootEl.querySelector("#log-search").value.trim();
  if (searchValue) params.set("search", searchValue);
  if (rootEl.querySelector("#log-status").value) params.set("status", rootEl.querySelector("#log-status").value);
  if (rootEl.querySelector("#log-task").value) params.set("task_id", rootEl.querySelector("#log-task").value);
  try {
    const { items, total } = await api("/api/logs?" + params);
    if (id !== requestId) return;
    if (!items.length && total && page > 0) { page = Math.ceil(total / PAGE_SIZE) - 1; return load(); }
    const list = rootEl.querySelector("#log-list");
    list.innerHTML = !items.length
      ? `<div class="empty">${params.has("search") || params.has("status") || params.has("task_id") ? "未找到匹配的日志" : "暂无日志"}</div>`
      : `<div class="table-wrap"><table><tr><th>时间</th><th>文件</th><th>结果</th><th>耗时</th><th>详情</th></tr>` +
        items.map(l => `<tr><td>${esc(l.created_at)}</td><td class="path-cell" title="${esc(l.dst_path)}">${esc(l.src_path)}</td>
          <td><span class="tag ${esc(l.result)}">
            ${{ success: "成功", failed: "失败", conflict: "冲突", accounting_pending: "待记账" }[l.result] || esc(l.result)}</span></td>
          <td>${l.duration_ms}ms</td><td class="log-detail path-cell">${esc(l.detail)}</td></tr>`).join("")
        + "</table></div>";
    renderPager("#log-pager", page, total, PAGE_SIZE, p => {
      page = p; load();
      rootEl.scrollIntoView({ behavior: "smooth" });
    });
  } catch (e) {
    if (id !== requestId) return;
    rootEl.querySelector("#log-list").innerHTML = '<div class="empty">日志加载失败：' + esc(e.message) + "</div>";
    rootEl.querySelector("#log-pager").innerHTML = "";
  }
}

async function clearLogs() {
  const ok = await confirmDialog("确认清空全部日志？", { title: "清空日志", danger: true, okText: "清空" });
  if (!ok) return;
  try { await api("/api/logs", { method: "DELETE" }); load(); }
  catch (e) { toast("清空日志失败: " + e.message, "error"); }
}
