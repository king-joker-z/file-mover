/* ============================================================
 * pages/eio.js — EIO 故障文件
 * ============================================================ */

import { api } from "../api.js";
import { confirmDialog } from "../components/dialogs.js";
import { renderPager } from "../components/pager.js";
import { PAGE_SIZE } from "../features/rounds.js";
import { esc, toast, fmtDateTime } from "../utils.js";

let rootEl = null;
let page = 0, requestId = 0;
let busy = false;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel"><div class="panel-heading"><div><span class="eyebrow">I/O FAILURES</span>
      <h2>EIO 故障文件</h2><p>首次 EIO 即记录并自动排除该文件或故障目录；不会删除源文件。目标端 EIO 也会记录，若多文件连续失败请检查挂载。</p></div></div>
      <div class="toolbar"><label class="ctl-label" for="eio-task">任务筛选</label>
        <select id="eio-task" aria-label="筛选故障任务"><option value="">全部任务</option></select>
        <span class="hint">点「重试」自动移除对应排除项并立即重新扫描（已成功迁移的也可重试）；「清除」仅删除故障记录，不影响排除规则与队列</span>
        <button type="button" class="btn small gray" id="eio-clear-done" style="margin-left:auto">清除已成功记录</button></div>
      <div id="eio-list"><div class="empty">加载故障记录中…</div></div>
      <div class="pager" id="eio-pager"></div>
    </div>`;

  root.querySelector("#eio-task").addEventListener("change", () => { page = 0; load(); });
  root.querySelector("#eio-clear-done").addEventListener("click", clearMigrated);
}

export function show() { loadTaskOptions(); load(); }

async function loadTaskOptions() {
  try {
    const tasks = await api("/api/tasks");
    const select = rootEl.querySelector("#eio-task");
    const chosen = select.value;
    select.innerHTML = '<option value="">全部任务</option>'
      + tasks.map(t => `<option value="${t.id}">${esc(t.name)}</option>`).join("");
    if ([...select.options].some(o => o.value === chosen)) select.value = chosen;
  } catch (_) { /* 故障记录仍可查看。 */ }
}

export async function load() {
  if (!rootEl) return;
  const id = ++requestId, filter = rootEl.querySelector("#eio-task").value;
  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: page * PAGE_SIZE });
  if (filter) params.set("task_id", filter);
  try {
    const { items, total } = await api("/api/eio-failures?" + params);
    if (id !== requestId) return;
    if (!items.length && total && page > 0) { page = Math.ceil(total / PAGE_SIZE) - 1; return load(); }
    const list = rootEl.querySelector("#eio-list");
    list.innerHTML = !items.length
      ? '<div class="empty">暂无 EIO 故障记录</div>'
      : `<div class="table-wrap"><table><tr><th>任务 / 文件</th><th>发生位置</th><th>次数 / 最近发生</th><th>状态</th><th>详情</th><th>操作</th></tr>` +
        items.map(f => {
          // 顺序与操作列一致：已成功迁移的记录同样提供重试/清除按钮。
          const state = f.busy ? ["队列处理中", "transferring"]
            : f.migrated ? ["已成功迁移", "done"]
            : f.active ? ["已排除", "cancelled"]
            : f.auto_excluded ? ["规则已移除", "pending"] : ["待重试", "pending"];
          const actions = [];
          if (f.task_name) {
            actions.push(`<button class="btn small" data-task="${f.task_id}" data-path="${esc(f.rel_path)}" ${busy ? "disabled" : ""} title="移除对应排除项并立即重新扫描">重试</button>`);
          }
          actions.push(`<button class="btn small gray" data-clear-task="${f.task_id}" data-clear-path="${esc(f.rel_path)}" ${busy ? "disabled" : ""} title="仅删除这条故障记录，不影响排除规则与队列">清除</button>`);
          return `<tr><td><b>${esc(f.task_name || "已删除任务 #" + f.task_id)}</b><br><span class="log-detail">${esc(f.rel_path)}</span></td>
          <td>${f.origin === "source" ? "源文件" : f.origin === "directory" ? "源目录" : "目标挂载"}</td>
          <td>${f.occurrences} 次<br><span class="log-detail">${fmtDateTime(f.last_at)}</span></td>
          <td><span class="tag ${state[1]}">${state[0]}</span></td>
          <td class="path-cell log-detail">${esc(f.detail)}</td><td>${actions.join("")}</td></tr>`;
        }).join("") + "</table></div>";
    list.querySelectorAll("button[data-path]").forEach(b =>
      b.addEventListener("click", () => retryEio(Number(b.dataset.task), b.dataset.path, b)));
    list.querySelectorAll("button[data-clear-path]").forEach(b =>
      b.addEventListener("click", () => clearEio(Number(b.dataset.clearTask), b.dataset.clearPath)));
    renderPager("#eio-pager", page, total, PAGE_SIZE, p => { page = p; load(); });
  } catch (e) {
    if (id !== requestId) return;
    rootEl.querySelector("#eio-list").innerHTML = '<div class="empty">故障记录加载失败：' + esc(e.message) + "</div>";
    rootEl.querySelector("#eio-pager").innerHTML = "";
  }
}

async function clearEio(taskId, relPath) {
  const ok = await confirmDialog(
    `确认清除该条故障记录？\n${relPath}\n仅删除记录；若文件仍在排除规则中，可在任务编辑里移除。`,
    { title: "清除故障记录", okText: "清除" });
  if (!ok) return;
  try {
    await api(`/api/eio-failures?task_id=${taskId}&rel_path=${encodeURIComponent(relPath)}`,
      { method: "DELETE" });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("清除失败: " + e.message, "error"); }
}

async function clearMigrated() {
  const filter = rootEl.querySelector("#eio-task").value;
  const scope = filter ? "当前任务" : "全部任务";
  const ok = await confirmDialog(
    `确认清除${scope}中「已成功迁移」且队列无未完成项的故障记录？不影响队列与排除规则。`,
    { title: "清除已成功记录", okText: "清除" });
  if (!ok) return;
  try {
    const params = new URLSearchParams({ migrated: "true" });
    if (filter) params.set("task_id", filter);
    const r = await api("/api/eio-failures?" + params, { method: "DELETE" });
    toast(`已清除 ${r.removed} 条故障记录`, "success");
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("清除失败: " + e.message, "error"); }
}

async function retryEio(taskId, relPath, btn) {
  if (busy) return;
  busy = true;
  if (btn) btn.disabled = true;
  try {
    const r = await api("/api/eio-failures/retry", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task_id: taskId, rel_path: relPath }),
    });
    load();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
    toast((r.removed_rule ? "已移除该条排除项，" : "") + "已触发重新扫描，稍后可在队列中查看进展。\n" +
      "若故障仍在，会重新记录并再次自动排除。" +
      (r.removed_rule ? "\n提示：若 I/O 问题是永久性的，重试后仍会失败。" : ""), "success");
  } catch (e) { toast("重试失败: " + e.message, "error"); }
  finally { busy = false; if (btn) btn.disabled = false; }
}
