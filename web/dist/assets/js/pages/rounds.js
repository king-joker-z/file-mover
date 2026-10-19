/* ============================================================
 * pages/rounds.js — 上传轮次历史
 * ============================================================ */

import { api } from "../api.js";
import { store } from "../store.js";
import { renderPager } from "../components/pager.js";
import { openRoundDetail, PAGE_SIZE, roundState } from "../features/rounds.js";
import { roundElapsed } from "../features/sync.js";
import { esc, fmtDateTime } from "../utils.js";

let rootEl = null;
let page = 0, requestId = 0;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel"><div class="panel-heading"><div><span class="eyebrow">UPLOAD ROUNDS / 历史记录</span>
      <h2>上传轮次</h2><p>一次连续待上传批次为一轮；总数是入队数，不等于成功数。耗时从首个文件开始上传计至本轮结束，未开始显示 00:00。</p></div></div>
      <div class="toolbar"><label class="ctl-label" for="round-task">任务筛选</label>
        <select id="round-task" aria-label="筛选任务"><option value="">全部任务</option></select>
        <span class="hint">取消表示队列项因修改规则或清空队列而移除，不代表上传成功；失败包含重试中</span></div>
      <div id="round-list"><div class="empty">加载轮次中…</div></div>
      <div class="pager" id="round-pager"></div>
    </div>`;

  root.querySelector("#round-task").addEventListener("change", () => { page = 0; load(); });
}

export function show() { loadTaskOptions(); load(); }

async function loadTaskOptions() {
  try {
    const tasks = await api("/api/tasks");
    const select = rootEl.querySelector("#round-task");
    const chosen = select.value;
    select.innerHTML = '<option value="">全部任务</option>'
      + tasks.map(t => `<option value="${t.id}">${esc(t.name)}</option>`).join("");
    if ([...select.options].some(o => o.value === chosen)) select.value = chosen;
  } catch (_) { /* 保留已有筛选项；历史轮次仍可查看。 */ }
}

export async function load() {
  if (!rootEl) return;
  const id = ++requestId, filter = rootEl.querySelector("#round-task").value;
  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: page * PAGE_SIZE });
  if (filter) params.set("task_id", filter);
  try {
    const { items, total } = await api("/api/rounds?" + params);
    if (id !== requestId) return;
    if (!items.length && total && page > 0) { page = Math.ceil(total / PAGE_SIZE) - 1; return load(); }
    const list = rootEl.querySelector("#round-list");
    list.innerHTML = !items.length
      ? '<div class="empty">暂无上传轮次</div>'
      : `<div class="table-wrap"><table><tr><th>轮次 / 任务</th><th>入队 / 上传开始 / 结束</th><th>文件（总 / 视频 / NFO / 图片 / 其他）</th><th>结果（成功 / 失败 / 冲突 / 取消）</th><th>上传耗时</th><th>状态</th><th>操作</th></tr>` +
        items.map(r => {
          const st = roundState(r);
          return `<tr><td><b>#${r.id}</b><br><span class="log-detail">${esc(r.task_name)}</span></td>
          <td>${fmtDateTime(r.created_at)}<br><span class="log-detail">${r.started_at ? fmtDateTime(r.started_at) : "未开始"} / ${r.finished_at ? fmtDateTime(r.finished_at) : "—"}</span></td>
          <td>${r.total} / ${r.video_count} / ${r.nfo_count} / ${r.image_count} / ${r.other_count}</td>
          <td><b style="color:var(--ok-ink)">${r.success_count}</b> / <b style="color:var(--err-ink)">${r.failed_count}</b> / ${r.conflict_count} / <b style="color:var(--warn-ink)" title="已从队列移除，不代表上传成功">${r.cancelled_count}</b> · 待处理 ${r.remaining_count}</td>
          <td data-history-elapsed="${r.id}">${roundElapsed(r)}</td>
          <td><span class="tag ${st.tag}">${st.label}</span></td>
          <td><button class="btn small gray" data-detail="${r.id}">详情</button></td></tr>`;
        }).join("") + "</table></div>";
    list.querySelectorAll("button[data-detail]").forEach(b =>
      b.addEventListener("click", () => openRoundDetail(Number(b.dataset.detail))));
    store.historyRounds = items;
    renderPager("#round-pager", page, total, PAGE_SIZE, p => {
      page = p; load();
      rootEl.scrollIntoView({ behavior: "smooth" });
    });
  } catch (e) {
    if (id !== requestId) return;
    rootEl.querySelector("#round-list").innerHTML = '<div class="empty">轮次加载失败：' + esc(e.message) + "</div>";
    rootEl.querySelector("#round-pager").innerHTML = "";
  }
}
