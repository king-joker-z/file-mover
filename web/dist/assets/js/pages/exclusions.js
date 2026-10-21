/* ============================================================
 * pages/exclusions.js — 永久排除的文件（人工移除且不再入队）
 * ============================================================ */

import { api } from "../api.js";
import { confirmDialog } from "../components/dialogs.js";
import { renderPager } from "../components/pager.js";
import { PAGE_SIZE } from "../features/rounds.js";
import { esc, toast, fmtDateTime } from "../utils.js";

let rootEl = null;
let page = 0, requestId = 0;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel"><div class="panel-heading"><div><span class="eyebrow">PERMANENT EXCLUSIONS</span>
      <h2>永久排除</h2><p>从队列「永久移除」的文件：扫描时不再入队。点「恢复」移除排除并立即重新扫描，文件会重新回到队列。</p></div></div>
      <div class="toolbar"><label class="ctl-label" for="excl-task">任务筛选</label>
        <select id="excl-task" aria-label="筛选排除任务"><option value="">全部任务</option></select>
        <span class="hint">不删除任何源文件；仅阻止这些路径再次入队</span></div>
      <div id="excl-list"><div class="empty">加载排除记录中…</div></div>
      <div class="pager" id="excl-pager"></div>
    </div>`;

  root.querySelector("#excl-task").addEventListener("change", () => { page = 0; load(); });
}

export function show() { loadTaskOptions(); load(); }

async function loadTaskOptions() {
  try {
    const tasks = await api("/api/tasks");
    const select = rootEl.querySelector("#excl-task");
    const chosen = select.value;
    select.innerHTML = '<option value="">全部任务</option>'
      + tasks.map(t => `<option value="${t.id}">${esc(t.name)}</option>`).join("");
    if ([...select.options].some(o => o.value === chosen)) select.value = chosen;
  } catch (_) { /* 排除记录仍可查看。 */ }
}

export async function load() {
  if (!rootEl) return;
  const id = ++requestId, filter = rootEl.querySelector("#excl-task").value;
  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: page * PAGE_SIZE });
  if (filter) params.set("task_id", filter);
  try {
    const { items, total } = await api("/api/exclusions?" + params);
    if (id !== requestId) return;
    if (!items.length && total && page > 0) { page = Math.ceil(total / PAGE_SIZE) - 1; return load(); }
    const list = rootEl.querySelector("#excl-list");
    list.innerHTML = !items.length
      ? '<div class="empty">暂无永久排除的文件</div>'
      : `<div class="table-wrap"><table><tr><th>任务</th><th>文件</th><th>源路径快照</th><th>移除时间</th><th>操作</th></tr>` +
        items.map(x => `<tr><td><b>${esc(x.task_name || "已删除任务 #" + x.task_id)}</b></td>
          <td class="path-cell" title="${esc(x.src_path || x.rel_path)}">${esc(x.rel_path)}</td>
          <td class="path-cell log-detail">${esc(x.src_path || "—")}</td>
          <td>${fmtDateTime(x.created_at)}</td>
          <td><button class="btn small" data-restore="${x.id}">恢复</button></td></tr>`).join("")
        + "</table></div>";
    list.querySelectorAll("button[data-restore]").forEach(b =>
      b.addEventListener("click", () => restoreOne(Number(b.dataset.restore))));
    renderPager("#excl-pager", page, total, PAGE_SIZE, p => { page = p; load(); });
  } catch (e) {
    if (id !== requestId) return;
    rootEl.querySelector("#excl-list").innerHTML = '<div class="empty">排除记录加载失败：' + esc(e.message) + "</div>";
    rootEl.querySelector("#excl-pager").innerHTML = "";
  }
}

async function restoreOne(id) {
  const ok = await confirmDialog(
    "确认恢复该文件？\n移除永久排除并立即重新扫描源目录；源文件仍存在时会重新回到迁移队列。",
    { title: "恢复排除项", okText: "恢复" });
  if (!ok) return;
  try {
    const r = await api(`/api/exclusions/${id}`, { method: "DELETE" });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
    toast(r.message || "已恢复并重新入队", "success");
  } catch (e) { toast("恢复失败: " + e.message, "error"); }
}
