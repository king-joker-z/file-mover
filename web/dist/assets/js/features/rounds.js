/* ============================================================
 * rounds.js — 轮次模型：状态判定 / 明细弹窗（仪表盘与轮次页共用）
 * ============================================================ */

import { api } from "../api.js";
import { openModal } from "../components/modal.js";
import { renderPager } from "../components/pager.js";
import { esc, fmtSize, fmtDateTime } from "../utils.js";
import { roundElapsed } from "./sync.js";

export const PAGE_SIZE = 50;

/** 轮次状态 → 展示文案 + 标签类 */
export function roundState(r) {
  if (r.scanning && !r.finished_at) return { label: "扫描中", tag: "transferring" };
  if (!r.finished_at) return { label: r.started_at ? "上传中" : "等待上传", tag: "pending" };
  if (r.cancelled_count) return { label: r.cancelled_count === r.total ? "全部取消" : "部分取消", tag: "cancelled" };
  if (r.failed_count || r.conflict_count) return { label: "存在失败或冲突", tag: "failed" };
  if (r.success_count === r.total) return { label: "全部成功", tag: "done" };
  return { label: "已结束", tag: "pending" };
}

/* ---------- 轮次文件明细弹窗 ---------- */

const roundModal = { id: null, page: 0, status: "", requestId: 0, modal: null };
const roundFileLabels = {
  done: "成功", failed: "失败", conflict: "冲突", cancelled: "已取消",
  pending: "待迁移", transferring: "迁移中", accounting_pending: "待记账",
};

export function openRoundDetail(id) {
  roundModal.id = id;
  roundModal.page = 0;
  roundModal.status = "";
  roundModal.requestId += 1;

  const body = document.createElement("div");
  body.style.cssText = "display:flex;flex-direction:column;gap:10px;min-height:0;flex:1";
  body.innerHTML =
    `<div data-head style="flex:none"></div>` +
    `<div class="filters" data-filters style="margin-bottom:0"></div>` +
    `<div data-list style="overflow:auto;flex:1;min-height:120px"></div>` +
    `<div class="pager" data-pager style="flex:none"></div>`;

  let modal;
  const entry = openModal({
    title: `轮次 #${id}`,
    body,
    width: "920px",
    onClose: () => { roundModal.id = null; roundModal.modal = null; },
  });
  modal = entry;
  roundModal.modal = entry;
  loadRoundDetail();
}

function closeRoundModal() {
  roundModal.modal?.close();
}

export function bindRoundModalEsc() {
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeRoundModal();
  });
}

async function loadRoundDetail() {
  const requestId = ++roundModal.requestId;
  const id = roundModal.id;
  if (!id || !roundModal.modal) return;
  const root = roundModal.modal.body;
  const params = new URLSearchParams({ limit: PAGE_SIZE, offset: roundModal.page * PAGE_SIZE });
  if (roundModal.status) params.set("status", roundModal.status);
  try {
    const data = await api(`/api/rounds/${id}/files?` + params);
    if (requestId !== roundModal.requestId || roundModal.id !== id || !roundModal.modal) return;
    const r = data.round;
    const scanning = r.scanning && !r.finished_at;
    const stateTag = scanning
      ? '<span class="tag transferring">扫描中</span>'
      : (!r.finished_at ? '<span class="tag pending">进行中</span>' : "");
    root.querySelector("[data-head]").innerHTML =
      `<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">` +
      `<h2 style="font-size:16px;margin:0">轮次 #${r.id} · ${esc(r.task_name)}</h2>${stateTag}</div>` +
      `<div class="log-detail" style="margin-top:4px">${fmtDateTime(r.created_at)} 创建 · ` +
      `${r.started_at ? "开始上传 " + fmtDateTime(r.started_at) : "未开始"}` +
      `${r.finished_at ? " · 结束 " + fmtDateTime(r.finished_at) : ""} · 耗时 ` +
      `<span ${scanning || !r.finished_at ? `data-round-elapsed="${r.id}"` : ""}>${roundElapsed(r)}</span></div>` +
      `<div style="display:flex;gap:14px;flex-wrap:wrap;margin-top:8px;font-size:12px">` +
      `<span>总数 <b>${r.total}</b></span><span>视频 <b>${r.video_count}</b></span>` +
      `<span>NFO <b>${r.nfo_count}</b></span><span>图片 <b>${r.image_count}</b></span>` +
      `<span style="color:var(--ok-ink)">成功 <b>${r.success_count}</b></span>` +
      `<span style="color:var(--err-ink)">失败 <b>${r.failed_count}</b></span>` +
      `<span style="color:var(--warn-ink)">冲突 <b>${r.conflict_count}</b></span>` +
      `<span>取消 <b>${r.cancelled_count}</b></span><span>待处理 <b>${r.remaining_count}</b></span></div>`;

    const filters = [["", "全部"], ["done", "成功"], ["failed", "失败"], ["conflict", "冲突"],
      ["cancelled", "已取消"], ["active", "待处理"]];
    root.querySelector("[data-filters]").innerHTML = filters.map(([v, l]) =>
      `<button type="button" data-rf="${v}" class="${roundModal.status === v ? "active" : ""}">${l}</button>`).join("");
    root.querySelectorAll("button[data-rf]").forEach(b =>
      b.onclick = () => { roundModal.status = b.dataset.rf; roundModal.page = 0; loadRoundDetail(); });

    const typeLabels = { video: "视频", nfo: "NFO", image: "图片", other: "其他" };
    root.querySelector("[data-list]").innerHTML = !data.items.length
      ? '<div class="empty">该筛选下暂无文件</div>'
      : `<div class="table-wrap"><table style="min-width:560px"><tr><th>文件</th><th>类型</th><th>大小</th><th>状态</th><th>更新时间</th></tr>` +
        data.items.map(f => {
          const tagCls = { done: "done", failed: "failed", conflict: "conflict", cancelled: "cancelled",
            transferring: "transferring" }[f.round_status] || "pending";
          return `<tr><td class="path-cell" title="${esc(f.src_path || "")}">` +
            `${f.rel_path ? esc(f.rel_path) : `<span class="log-detail">已从队列移除 · #${f.queue_id}</span>`}</td>` +
            `<td>${typeLabels[f.file_type] || esc(f.file_type)}</td><td>${f.size ? fmtSize(f.size) : "-"}</td>` +
            `<td><span class="tag ${tagCls}">${roundFileLabels[f.round_status] || esc(f.round_status)}</span></td>` +
            `<td class="log-detail">${esc(f.updated_at || "—")}</td></tr>`;
        }).join("") + "</table></div>";
    renderPager(root.querySelector("[data-pager]"), roundModal.page, data.total, PAGE_SIZE,
      p => { roundModal.page = p; loadRoundDetail(); });
  } catch (e) {
    if (requestId !== roundModal.requestId) return;
    root.querySelector("[data-list]").innerHTML =
      '<div class="empty">轮次详情加载失败：' + esc(e.message) + "</div>";
    root.querySelector("[data-pager]").innerHTML = "";
  }
}
