/* ============================================================
 * pages/dash.js — 仪表盘：统计卡 / 队列分布图 / 扫描计划 / 运行状态
 * ============================================================ */

import { api } from "../api.js";
import { store } from "../store.js";
import { confirmDialog } from "../components/dialogs.js";
import { adoptRunningSchedules, syncNow, updateSyncBtn, roundElapsed } from "../features/sync.js";
import { openRoundDetail, roundState } from "../features/rounds.js";
import { esc, toast, $ } from "../utils.js";

let rootEl = null;
let dashBusy = false;

const chartSeries = [
  { key: "pending", label: "待迁移", color: "#3b82f6" },
  { key: "transferring", label: "迁移中", color: "#805ee8" },
  { key: "accounting_pending", label: "待记账", color: "#13a6a0" },
  { key: "failed", label: "失败", color: "#d64752" },
  { key: "conflict", label: "冲突", color: "#dd9730" },
  { key: "done", label: "已完成", color: "#0e9f6e" },
];

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="page-intro">
      <div><span class="eyebrow">OVERVIEW / 实时概览</span><h2>迁移运行台</h2>
        <p>掌握扫描节奏、队列状态和每个任务的下一步动作。</p></div>
      <span class="live-badge"><i></i> 每 3 秒更新</span>
    </div>
    <div class="cards">
      <div class="stat green"><div class="num" id="s-moved">0</div><div class="lbl">今日已迁移</div></div>
      <div class="stat"><div class="num" id="s-pending">0</div><div class="lbl">待迁移</div></div>
      <div class="stat"><div class="num" id="s-transferring">0</div><div class="lbl">正在迁移</div></div>
      <div class="stat red"><div class="num" id="s-failed">0</div><div class="lbl">失败</div></div>
      <div class="stat orange"><div class="num" id="s-conflict">0</div><div class="lbl">冲突</div></div>
      <div class="stat"><div class="num" id="s-done">0</div><div class="lbl">队列已完成</div></div>
    </div>
    <div class="insight-grid">
      <section class="panel"><div class="panel-heading"><div><span class="eyebrow">QUEUE MIX</span>
        <h2>队列状态分布</h2><p>当前全部队列条目，不含今日迁移计数</p></div></div>
        <div class="donut-layout">
          <div class="donut" id="queue-donut" role="img" aria-label="队列状态分布">
            <div class="donut-center"><strong id="queue-total">—</strong><span>队列总数</span></div>
          </div>
          <div class="chart-legend" id="queue-legend"></div>
        </div>
      </section>
      <section class="panel"><div class="panel-heading"><div><span class="eyebrow">WORKLOAD</span>
        <h2>当前待处理</h2><p>待迁移、进行中及异常项的相对数量</p></div></div>
        <div class="stage-bars" id="stage-bars"></div>
      </section>
    </div>
    <div class="panel scan-panel"><div class="panel-heading"><div><span class="eyebrow">SCAN SCHEDULE</span>
      <h2>扫描计划</h2><p>按配置展示下一次扫描；倒计时每秒更新，可单独立即扫描源目录；历史目标 NFO 巡检需在任务管理中手动确认。</p></div>
      <span class="panel-hint">手动扫描不会中断正在进行的迁移</span></div>
      <div id="dash-scan-list" class="scan-grid"><div class="empty">加载扫描计划中…</div></div>
    </div>
    <div class="panel"><div class="panel-heading"><div><span class="eyebrow">SYSTEM STATUS</span>
      <h2>运行状态</h2></div></div>
      <div class="current" id="current-file">空闲</div>
      <div class="log-detail" id="uptime"></div>
      <div class="status-warnings" id="status-warnings" role="status"></div>
      <button id="exclude-error-dir" class="btn small gray" type="button" style="display:none;margin-top:8px">将故障目录加入排除项…</button>
    </div>`;
}

export function show() { loadDash(); }

/* ================= 扫描计划卡 ================= */

function renderScanSchedules() {
  const box = $("#dash-scan-list");
  if (!box) return;
  if (!store.scanSchedules.length) {
    box.innerHTML = '<div class="empty">暂无任务 · 前往任务管理创建第一个迁移任务</div>';
    return;
  }
  box.innerHTML = store.scanSchedules.map(s => {
    const label = s.status === "manual" && s.backfill_nfo ? "历史 NFO 巡检"
      : ({ disabled: "已停用", manual: "手动扫描", scanning: "定时扫描", cooldown: "故障冷却",
        manual_wait: "等待手动扫描", scheduled: "运行中" }[s.status] || "等待中");
    const active = s.status === "manual" || s.status === "scanning";
    const detail = active
      ? `${s.current || "扫描准备中"} · ${s.backfill_nfo && s.backfill_total ? `历史视频已检查 ${s.backfill_checked}/${s.backfill_total} · ` : ""}源目录已发现 ${s.scanned} · 新入队 ${s.enqueued}`
      : s.status === "cooldown" ? "挂载 I/O 故障后等待重试"
      : s.status === "manual_wait"
        ? `仅手动扫描：点「立即扫描」触发${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""}`
        : `扫描间隔 ${s.interval_seconds} 秒${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""} · ${s.last_scan_at ? "上次扫描 " + new Date(s.last_scan_at * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : "等待首次扫描"}`;
    const r = s.round;
    const roundBlock = r
      ? `<div class="round-mini" style="cursor:pointer" title="点击查看轮次文件明细" data-round-detail="${r.id}">
          <div class="round-mini-title">最近一轮 #${r.id} · ${roundState(r).label} ▸</div>
          <div class="round-mini-stats"><span>总数 <b>${r.total}</b></span><span>视频 <b>${r.video_count}</b></span>
          <span>NFO <b>${r.nfo_count}</b></span><span>图片 <b>${r.image_count}</b></span>
          ${r.other_count ? `<span>其他 <b>${r.other_count}</b></span>` : ""}</div>
          <div class="round-mini-time">上传耗时 <b data-round-elapsed="${s.id}">${roundElapsed(r)}</b> ·
          成功 ${r.success_count} / 失败 ${r.failed_count} / 冲突 ${r.conflict_count} / 取消 ${r.cancelled_count} / 待处理 ${r.remaining_count}</div></div>`
      : `<div class="round-mini"><div class="round-mini-title">当前暂无上传轮次</div>
          <div class="round-mini-time">发现新文件并入队后开始统计</div></div>`;
    return `<article class="scan-card">
      <div class="scan-card-top"><div class="scan-symbol" aria-hidden="true">↻</div>
        <span class="tag scan-state ${esc(s.status)}">${label}</span></div>
      <h3 title="${esc(s.name)}">${esc(s.name)}</h3>
      <div class="scan-path" title="${esc(s.src_dir)}">${esc(s.src_dir)} <span>→</span> ${esc(s.dst_dir)}</div>
      <div class="scan-divider"></div>
      <div class="scan-caption">${s.status === "cooldown" ? "下次尝试" : s.status === "disabled" || s.status === "manual_wait" ? "扫描方式" : "距下次扫描"}</div>
      <div class="countdown" data-countdown-id="${s.id}">—</div>
      <div class="scan-detail" title="${esc(detail)}">${esc(detail)}</div>
      ${roundBlock}
      <button class="btn scan-trigger" data-sync-id="${s.id}" data-sync-now="${s.id}"
        ${!s.enabled || active || s.status === "cooldown" ? "disabled" : ""}>
        ${s.status === "disabled" ? "任务已停用" : s.status === "cooldown" ? "冷却中" : active ? "扫描中…" : "立即扫描"}</button>
      <div class="sync-error" data-sync-error="${s.id}" role="alert"></div>
      <div class="sync-status" data-sync-status="${s.id}" role="status"></div>
    </article>`;
  }).join("");

  // 事件绑定
  box.querySelectorAll("button[data-sync-now]").forEach(b =>
    b.addEventListener("click", () => syncNow(Number(b.dataset.syncNow))));
  box.querySelectorAll("[data-round-detail]").forEach(el =>
    el.addEventListener("click", () => openRoundDetail(Number(el.dataset.roundDetail))));

  store.scanSchedules.forEach(s => updateSyncBtn(s.id));
}

/* ================= 队列图表 ================= */

function renderQueueCharts(d) {
  if (d.pending == null || d.transferring == null || d.failed == null
    || d.conflict == null || d.done == null) {
    $("#queue-donut").style.background = "var(--donut-empty)";
    $("#queue-donut").setAttribute("aria-label", "队列统计暂不可用");
    $("#queue-legend").textContent = "状态暂不可用";
    $("#stage-bars").textContent = "状态暂不可用";
    return;
  }
  const series = chartSeries.map(item => ({ ...item, value: Math.max(0, Number(d[item.key]) || 0) }));
  const total = series.reduce((sum, item) => sum + item.value, 0);
  const donut = $("#queue-donut");
  let from = 0;
  donut.style.background = total
    ? `conic-gradient(${series.filter(i => i.value).map(item => {
      const start = from; from += item.value / total * 100;
      return `${item.color} ${start}% ${from}%`;
    }).join(",")})`
    : "var(--donut-empty)";
  donut.setAttribute("aria-label", total
    ? series.filter(i => i.value).map(i => `${i.label} ${i.value}`).join("，")
    : "队列为空");
  $("#queue-total").textContent = total.toLocaleString("zh-CN");
  $("#queue-legend").innerHTML = series.map(item =>
    `<div class="legend-row"><span class="legend-dot" style="background:${item.color}"></span>${item.label}<b>${item.value.toLocaleString("zh-CN")}</b></div>`).join("");
  const active = series.filter(i => i.key !== "done");
  const max = Math.max(1, ...active.map(i => i.value));
  $("#stage-bars").innerHTML = active.map(item =>
    `<div class="stage-row"><span>${item.label}</span>` +
    `<div class="stage-track"><span class="stage-fill" style="width:${item.value / max * 100}%;background:${item.color}"></span></div>` +
    `<b>${item.value.toLocaleString("zh-CN")}</b></div>`).join("");
}

/* ================= 主加载 ================= */

export async function loadDash() {
  if (dashBusy) return;
  dashBusy = true;
  try {
    const d = await api("/api/dashboard");
    store.serverClockOffset = Date.now() / 1000 - d.server_time;
    const schedulesAvailable = Array.isArray(d.scan_schedules);
    store.scanSchedules = schedulesAvailable ? d.scan_schedules : [];
    if (schedulesAvailable) {
      adoptRunningSchedules(store.scanSchedules);
      renderScanSchedules();
    } else {
      $("#dash-scan-list").innerHTML = '<div class="empty">任务列表暂不可用，请稍后重试；已有任务未被清空</div>';
    }
    $("#s-moved").textContent = d.moved_today ?? "—";
    $("#s-pending").textContent = d.pending ?? "—";
    $("#s-transferring").textContent = d.transferring ?? "—";
    $("#s-failed").textContent = d.failed ?? "—";
    $("#s-conflict").textContent = d.conflict ?? "—";
    $("#s-done").textContent = d.done ?? "—";
    renderQueueCharts(d);
    const cur = d.current_files && d.current_files.length
      ? "迁移中: " + d.current_files.map(t => `${t.rel_path}（${t.stage || "迁移中"}）`).join("；")
      : (d.current_file ? "迁移中: " + d.current_file : "空闲");
    $("#current-file").textContent = cur;
    const h = Math.floor(d.uptime_seconds / 3600), m = Math.floor(d.uptime_seconds % 3600 / 60);
    $("#uptime").textContent = `已运行 ${h}小时${m}分钟 · 全局扫描间隔 ${d.settings?.scan_interval ?? "—"}s`;
    const warnings = [];
    if (d.stats_error) warnings.push("状态数据读取失败: " + d.stats_error);
    if (d.scanner_alive === false) warnings.push("扫描线程已停止，请重启服务");
    if (d.worker_alive === false) warnings.push("迁移线程已停止，请重启服务");
    if (d.last_scan_error) warnings.push("最近扫描异常: " + d.last_scan_error);
    if (d.accounting_pending > 0) warnings.push(`待补偿记账: ${d.accounting_pending} 项`);
    if (d.last_worker_error) warnings.push("最近迁移异常: " + d.last_worker_error);
    if (d.last_accounting_error) warnings.push(d.last_accounting_error);
    $("#status-warnings").textContent = warnings.join("\n");
    const excludeBtn = $("#exclude-error-dir");
    excludeBtn.style.display = d.last_scan_error_path && d.last_scan_error_task_id != null
      && d.last_scan_error?.includes("枚举目录失败") ? "" : "none";
    excludeBtn.onclick = () => prepareExcludeDir(d.last_scan_error_task_id, d.last_scan_error_path);
  } catch (e) {
    $("#current-file").textContent = "状态加载失败: " + e.message;
    ["#s-moved", "#s-pending", "#s-transferring", "#s-failed", "#s-conflict", "#s-done"]
      .forEach(id => { const el = $(id); if (el) el.textContent = "—"; });
    $("#uptime").textContent = "";
    $("#status-warnings").textContent = "";
    const excludeBtn = $("#exclude-error-dir");
    if (excludeBtn) excludeBtn.style.display = "none";
    const list = $("#dash-scan-list");
    if (list) { list.innerHTML = '<div class="empty">扫描计划加载失败：' + esc(e.message) + "</div>"; }
    store.scanSchedules = [];
    if ($("#queue-donut")) renderQueueCharts({});
  } finally { dashBusy = false; }
}

/* ================= 故障目录加入排除 ================= */

async function prepareExcludeDir(taskId, path) {
  try {
    const task = (await api("/api/tasks")).find(t => t.id === taskId);
    if (!task) throw new Error("任务不存在");
    const src = task.src_dir.replace(/\/+$/, "");
    if (!path.startsWith(src + "/")) throw new Error("故障路径不在任务源目录内");
    const rel = path.slice(src.length + 1);
    const ok = await confirmDialog(
      `将故障目录加入排除项？\n${rel}\n排除后重新扫描将跳过该目录下所有文件，不会修复底层 I/O 故障；` +
      "可在任务管理的排除项中随时删除。",
      { title: "加入排除项", okText: "加入排除" });
    if (!ok) return;
    await api(`/api/tasks/${taskId}/exclude-patterns`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pattern: rel }),
    });
    loadDash();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
    toast(`已加入排除项：${rel}\n下次扫描开始生效。`, "success");
  } catch (e) { toast("无法加入排除项: " + e.message, "error"); }
}
