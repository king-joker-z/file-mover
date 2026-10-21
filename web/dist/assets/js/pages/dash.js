/* ============================================================
 * pages/dash.js — 仪表盘：统计卡 / 队列分布图 / 扫描计划 / 运行状态
 * ============================================================ */

import { api } from "../api.js";
import { store } from "../store.js";
import { confirmDialog } from "../components/dialogs.js";
import { adoptRunningSchedules, syncNow, updateSyncBtn, roundElapsed } from "../features/sync.js";
import { openRoundDetail, roundState } from "../features/rounds.js";
import { esc, toast, formatCountdown, fmtSize, $ } from "../utils.js";

let rootEl = null;
let dashBusy = false;
let scanRendered = false;   // 扫描卡是否已渲染过（用于增量更新）
let trendDays = 14;         // 趋势柱状图时间范围
let trendBusy = false;
const HEATMAP_DAYS = 365;   // 热力图最大跨度（一年）；周/月/年视图都在此范围内本地切片
const HEATMAP_SPAN = { week: 7, month: 30, year: 365 };
let heatmapView = "month";  // 热力图视图：week / month / year
let trendAll = [];          // 最近 HEATMAP_DAYS 天的按日数据
let trendTotal = {};        // 累计迁移统计
const transferSamples = new Map();  // queue_id -> 上次进度采样（计算实时速度）

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
    <div class="panel"><div class="panel-heading"><div><span class="eyebrow">SYSTEM STATUS</span>
      <h2>运行状态</h2></div></div>
      <div class="current" id="current-file">空闲</div>
      <div class="transfer-list" id="transfer-list"></div>
      <div class="log-detail" id="uptime"></div>
      <div class="status-warnings" id="status-warnings" role="status"></div>
      <button id="exclude-error-dir" class="btn small gray" type="button" style="display:none;margin-top:8px">将故障目录加入排除项…</button>
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
    <div class="panel trend-panel"><div class="panel-heading"><div><span class="eyebrow">TREND</span>
      <h2>迁移趋势</h2><p>按日统计成功迁移的文件数与流量，鼠标悬停查看当日明细</p></div>
      <div class="trend-ranges" id="trend-ranges">
        <button type="button" data-days="7">7 天</button>
        <button type="button" data-days="14">14 天</button>
        <button type="button" data-days="30">30 天</button>
      </div></div>
      <div class="trend-summary" id="trend-summary">加载趋势数据中…</div>
      <div class="trend-chart" id="trend-chart" role="img" aria-label="每日迁移量柱状图">
        <div class="empty">加载中…</div>
      </div>
      <div class="heatmap-wrap">
        <div class="heatmap-head">
          <span class="hm-caption">迁移活跃度 · 颜色越深当天迁移量越大，悬停查看明细</span>
          <div class="hm-views" id="hm-views">
            <button type="button" data-view="week">周</button>
            <button type="button" data-view="month">月</button>
            <button type="button" data-view="year">年</button>
          </div>
        </div>
        <div id="trend-heatmap" class="heatmap-body hm-view-month"><div class="empty">加载中…</div></div>
        <div class="hm-legend-row">少<span class="hm-cell q0"></span><span class="hm-cell q1"></span><span class="hm-cell q2"></span><span class="hm-cell q3"></span><span class="hm-cell q4"></span>多</div>
      </div>
    </div>
    <div class="panel scan-panel"><div class="panel-heading"><div><span class="eyebrow">SCAN SCHEDULE</span>
      <h2>扫描计划</h2><p>按配置展示下一次扫描；倒计时每秒更新，可单独立即扫描源目录；历史目标 NFO 巡检需在任务管理中手动确认。</p></div>
      <span class="panel-hint">手动扫描不会中断正在进行的迁移</span></div>
      <div id="dash-scan-list" class="scan-grid"><div class="empty">加载扫描计划中…</div></div>
    </div>`;

  const ranges = $("#trend-ranges");
  ranges.addEventListener("click", e => {
    const btn = e.target.closest("button[data-days]");
    if (!btn) return;
    trendDays = Number(btn.dataset.days);
    ranges.querySelectorAll("button").forEach(b =>
      b.classList.toggle("active", b === btn));
    if (trendAll.length) renderTrendBars();   // 数据已在本地，直接重渲染柱状图
    else loadTrend();
  });
  ranges.querySelectorAll("button").forEach(b =>
    b.classList.toggle("active", Number(b.dataset.days) === trendDays));

  const views = $("#hm-views");
  views.addEventListener("click", e => {
    const btn = e.target.closest("button[data-view]");
    if (!btn) return;
    heatmapView = btn.dataset.view;
    views.querySelectorAll("button").forEach(b =>
      b.classList.toggle("active", b === btn));
    if (trendAll.length) renderHeatmap();   // 数据已在本地，直接重渲染
  });
  views.querySelectorAll("button").forEach(b =>
    b.classList.toggle("active", b.dataset.view === heatmapView));
}

export function show() { loadDash(); loadTrend(); }

/* ================= 扫描计划卡 ================= */

/** 倒计时当前文本（渲染/增量更新时直接写初值，避免「—」闪现） */
function countdownText(s) {
  if (s.status === "disabled") return "已停用";
  if (s.status === "manual") return "手动扫描中";
  if (s.status === "scanning") return "定时扫描中";
  if (s.status === "manual_wait") return "等待手动触发";
  if (s.status === "paused") return "迁移已暂停";
  if (s.next_scan_at == null) return "等待调度";
  const remaining = s.next_scan_at - (Date.now() / 1000 - store.serverClockOffset);
  return remaining <= 0 ? "等待调度" : formatCountdown(remaining);
}

function roundMiniHtml(s) {
  const r = s.round;
  if (!r) {
    return `<div class="round-mini"><div class="round-mini-title">当前暂无上传轮次</div>
      <div class="round-mini-time">发现新文件并入队后开始统计</div></div>`;
  }
  return `<div class="round-mini" style="cursor:pointer" title="点击查看轮次文件明细" data-round-detail="${r.id}">
    <div class="round-mini-title">最近一轮 #${r.id} · ${roundState(r).label} ▸</div>
    <div class="round-mini-stats"><span>总数 <b>${r.total}</b></span><span>视频 <b>${r.video_count}</b></span>
    <span>NFO <b>${r.nfo_count}</b></span><span>图片 <b>${r.image_count}</b></span>
    ${r.other_count ? `<span>其他 <b>${r.other_count}</b></span>` : ""}</div>
    <div class="round-mini-time">上传耗时 <b data-round-elapsed="${s.id}">${roundElapsed(r)}</b> ·
    成功 ${r.success_count} / 失败 ${r.failed_count} / 冲突 ${r.conflict_count} / 取消 ${r.cancelled_count} / 待处理 ${r.remaining_count}</div></div>`;
}

/** 卡片集合不变时原地更新文本；倒计时节点不销毁，由每秒 tick 平滑刷新 */
function updateScanCardInPlace(card, s) {
  const active = s.status === "manual" || s.status === "scanning";
  const label = s.status === "manual" && s.backfill_nfo ? "历史 NFO 巡检"
    : ({ disabled: "已停用", manual: "手动扫描", scanning: "定时扫描", cooldown: "故障冷却",
      manual_wait: "等待手动扫描", paused: "迁移已暂停", scheduled: "运行中" }[s.status] || "等待中");
  const tag = card.querySelector(".tag.scan-state");
  if (tag) { tag.textContent = label; tag.className = `tag scan-state ${esc(s.status)}`; }

  const cap = card.querySelector(".scan-caption");
  if (cap) cap.textContent = s.status === "cooldown" ? "下次尝试"
    : s.status === "disabled" || s.status === "manual_wait" ? "扫描方式" : "距下次扫描";

  const detail = s.status === "cooldown" ? "挂载 I/O 故障后等待重试"
    : s.status === "manual_wait"
      ? `仅手动扫描：点「立即扫描」触发${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""}`
      : `扫描间隔 ${s.interval_seconds} 秒${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""} · ${s.last_scan_at ? "上次扫描 " + new Date(s.last_scan_at * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : "等待首次扫描"}`;
  const dEl = card.querySelector(".scan-detail");
  if (dEl) { dEl.textContent = detail; dEl.title = detail; }

  const btn = card.querySelector(".scan-trigger");
  if (btn) {
    btn.textContent = s.status === "disabled" ? "任务已停用"
      : s.status === "cooldown" ? "冷却中" : active ? "扫描中…" : "立即扫描";
    btn.disabled = !s.enabled || active || s.status === "cooldown";
  }

  const oldRound = card.querySelector(".round-mini");
  if (oldRound) {
    oldRound.outerHTML = roundMiniHtml(s);
    const rd = card.querySelector("[data-round-detail]");
    if (rd) rd.addEventListener("click", () => openRoundDetail(Number(rd.dataset.roundDetail)));
  }

  const cd = card.querySelector(".countdown");
  if (cd) cd.textContent = countdownText(s);   // 状态切换时立即纠正；平时由 tick 每秒刷新
}

function renderScanSchedules() {
  const box = $("#dash-scan-list");
  if (!box) return;
  if (!store.scanSchedules.length) {
    box.innerHTML = '<div class="empty">暂无任务 · 前往任务管理创建第一个迁移任务</div>';
    scanRendered = false;
    return;
  }

  /* 卡片集合未变 → 原地增量更新（顺序无关），避免每 3 秒整块重建导致倒计时闪烁 */
  if (scanRendered) {
    const cards = box.querySelectorAll(".scan-card[data-scan-id]");
    const byId = new Map([...cards].map(c => [Number(c.dataset.scanId), c]));
    if (cards.length === store.scanSchedules.length
      && store.scanSchedules.every(s => byId.has(s.id))) {
      store.scanSchedules.forEach(s => updateScanCardInPlace(byId.get(s.id), s));
      store.scanSchedules.forEach(s => updateSyncBtn(s.id));
      return;
    }
  }

  box.innerHTML = store.scanSchedules.map(s => {
    const label = s.status === "manual" && s.backfill_nfo ? "历史 NFO 巡检"
      : ({ disabled: "已停用", manual: "手动扫描", scanning: "定时扫描", cooldown: "故障冷却",
        manual_wait: "等待手动扫描", paused: "迁移已暂停", scheduled: "运行中" }[s.status] || "等待中");
    const active = s.status === "manual" || s.status === "scanning";
    const detail = s.status === "cooldown" ? "挂载 I/O 故障后等待重试"
      : s.status === "manual_wait"
        ? `仅手动扫描：点「立即扫描」触发${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""}`
        : `扫描间隔 ${s.interval_seconds} 秒${s.async_transfer ? " · 异步迁移" : ""}${s.transfer_workers > 1 ? " · 并发×" + s.transfer_workers : ""} · ${s.last_scan_at ? "上次扫描 " + new Date(s.last_scan_at * 1000).toLocaleTimeString("zh-CN", { hour12: false }) : "等待首次扫描"}`;
    return `<article class="scan-card" data-scan-id="${s.id}">
      <div class="scan-card-top"><div class="scan-symbol" aria-hidden="true">↻</div>
        <span class="tag scan-state ${esc(s.status)}">${label}</span></div>
      <h3 title="${esc(s.name)}">${esc(s.name)}</h3>
      <div class="scan-path" title="${esc(s.src_dir)}">${esc(s.src_dir)} <span>→</span> ${esc(s.dst_dir)}</div>
      <div class="scan-divider"></div>
      <div class="scan-caption">${s.status === "cooldown" ? "下次尝试" : s.status === "disabled" || s.status === "manual_wait" ? "扫描方式" : "距下次扫描"}</div>
      <div class="countdown" data-countdown-id="${s.id}">${countdownText(s)}</div>
      <div class="scan-detail" title="${esc(detail)}">${esc(detail)}</div>
      ${roundMiniHtml(s)}
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

  scanRendered = true;
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
    if (!schedulesAvailable) {
      /* 数据瞬时不可读（如 DB 写锁占用）：保留现有卡片与数值，跳过本次刷新，避免整块闪烁 */
      return;
    }
    store.scanSchedules = d.scan_schedules;
    adoptRunningSchedules(store.scanSchedules);
    renderScanSchedules();
    $("#s-moved").textContent = d.moved_today ?? "—";
    $("#s-pending").textContent = d.pending ?? "—";
    $("#s-transferring").textContent = d.transferring ?? "—";
    $("#s-failed").textContent = d.failed ?? "—";
    $("#s-conflict").textContent = d.conflict ?? "—";
    $("#s-done").textContent = d.done ?? "—";
    renderQueueCharts(d);
    const files = Array.isArray(d.current_files) ? d.current_files : [];
    $("#current-file").textContent = files.length
      ? `迁移中 · ${files.length} 个文件`
      : (d.current_file ? "迁移中: " + d.current_file : "空闲");
    renderTransfers(files);
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
    if (d.last_retention_error) warnings.push("数据保留清理失败: " + d.last_retention_error);
    if (Array.isArray(d.mount_breakers) && d.mount_breakers.length) {
      const mounts = d.mount_breakers.map(b => `${b.mount}（冷却剩 ${b.backoff_seconds}s，串行保护剩 ${b.degraded_seconds}s）`);
      warnings.push(`挂载瞬时 I/O 保护中: ${mounts.join("、")}——网盘抖动已被自动重试吸收（本次运行累计 ${d.mount_transient_absorbed ?? 0} 次），不是故障，无需处理`);
    }
    $("#status-warnings").textContent = warnings.join("\n");
    const excludeBtn = $("#exclude-error-dir");
    excludeBtn.style.display = d.last_scan_error_path && d.last_scan_error_task_id != null
      && d.last_scan_error?.includes("枚举目录失败") ? "" : "none";
    excludeBtn.onclick = () => prepareExcludeDir(d.last_scan_error_task_id, d.last_scan_error_path);
  } catch (e) {
    /* 网络闪断：保留现有卡片与数值，仅在运行状态区提示，避免整页闪烁 */
    const cf = $("#current-file");
    if (cf) cf.textContent = "状态刷新失败（保留上次数据）: " + e.message;
  } finally { dashBusy = false; }
}

/* ================= 迁移进度（速度 / ETA） ================= */

/** 采样进度并计算平滑速度（字节/秒）；进度回退（重试）时重置采样 */
function transferSpeed(t) {
  const now = Date.now() / 1000 - store.serverClockOffset;
  const copied = Math.max(0, Number(t.copied_bytes) || 0);
  const prev = transferSamples.get(t.id);
  let speed = null;
  if (prev && copied >= prev.copied && now > prev.t) {
    const inst = (copied - prev.copied) / (now - prev.t);
    speed = prev.ema == null ? inst : prev.ema * 0.5 + inst * 0.5;
  }
  transferSamples.set(t.id, { copied, t: now, ema: speed });
  return speed;
}

function renderTransfers(files) {
  const box = $("#transfer-list");
  if (!box) return;
  const active = new Set(files.map(f => f.id));
  for (const id of transferSamples.keys()) {
    if (!active.has(id)) transferSamples.delete(id);
  }
  if (!files.length) { box.innerHTML = ""; return; }
  const now = Date.now() / 1000 - store.serverClockOffset;
  const keepScroll = box.scrollTop;   // 固定高度容器：3 秒重建后恢复滚动位置，避免阅读中断
  box.innerHTML = files.map(t => {
    const total = Number(t.total_bytes) || 0;
    const copied = Math.max(0, Number(t.copied_bytes) || 0);
    const pct = total > 0 ? Math.min(100, Math.round(copied / total * 100)) : 0;
    const verifying = total > 0 && copied >= total;   // 复制完成，处于读回校验/删源阶段
    const speed = verifying ? null : transferSpeed(t);
    const eta = speed > 0 && total > copied ? formatCountdown((total - copied) / speed) : "";
    const elapsed = t.started_at ? formatCountdown(Math.max(0, now - t.started_at)) : "";
    const sizeText = total > 0
      ? `${fmtSize(copied)} / ${fmtSize(total)} · ${pct}%${verifying ? " · 校验中" : ""}`
      : fmtSize(copied);
    const meta = [
      `<span>${esc(sizeText)}</span>`,
      speed > 0 ? `<span>速度 <b>${fmtSize(speed)}/s</b></span>` : "",
      eta ? `<span>剩余 <b>${esc(eta)}</b></span>` : "",
      elapsed ? `<span>已用 ${esc(elapsed)}</span>` : "",
    ].filter(Boolean).join("");
    return `<div class="transfer-item">
      <div class="transfer-head">
        <span class="transfer-name" title="${esc(t.rel_path)}">${esc(t.rel_path)}</span>
        <span class="tag transferring">${esc(t.stage || "迁移中")}</span>
      </div>
      <div class="transfer-track"><span class="transfer-fill${verifying ? " done" : ""}" style="width:${pct}%"></span></div>
      <div class="transfer-meta">${meta}</div>
    </div>`;
  }).join("");
  box.scrollTop = keepScroll;
}

/* ================= 迁移趋势统计 ================= */

function trendBytes(n) { n = Number(n) || 0; return n ? fmtSize(n) : "0 B"; }

function renderTrendBars() {
  const items = trendAll.slice(-trendDays);
  const files = items.reduce((s, x) => s + (Number(x.files) || 0), 0);
  const bytes = items.reduce((s, x) => s + (Number(x.bytes) || 0), 0);
  $("#trend-summary").innerHTML =
    `近 ${items.length} 天迁移 <b>${files.toLocaleString("zh-CN")}</b> 个文件 · <b>${trendBytes(bytes)}</b>` +
    `<span class="sep">|</span>累计 <b>${(Number(trendTotal.files) || 0).toLocaleString("zh-CN")}</b> 个文件 · <b>${trendBytes(trendTotal.bytes)}</b>`;
  const chart = $("#trend-chart");
  if (!chart) return;
  if (!items.length) {
    chart.innerHTML = '<div class="empty">暂无趋势数据</div>';
    return;
  }
  const max = Math.max(1, ...items.map(x => Number(x.bytes) || 0));
  const showAllLabels = items.length <= 14;
  chart.innerHTML = items.map((x, i) => {
    const n = Number(x.bytes) || 0;
    const pct = n ? Math.max(2, Math.round(n / max * 100)) : 0;
    const showLabel = showAllLabels || i % 5 === 0 || i === items.length - 1;
    return `<div class="trend-col" title="${esc(x.day)} · ${x.files} 个文件 · ${trendBytes(x.bytes)}">
      <div class="trend-barbox">
        <span class="trend-bubble">${esc(String(x.day).slice(5))} · ${x.files} 个 · ${trendBytes(x.bytes)}</span>
        <span class="trend-bar${n ? "" : " zero"}" style="height:${pct}%"></span>
      </div>
      <span class="trend-day"${showLabel ? "" : " hidden"}>${esc(String(x.day).slice(5))}</span>
    </div>`;
  }).join("");
}

/** 迁移活跃度热力图：周 = 横排 7 大格；月/年 = GitHub 风格日历（列 = 周，行 = 周一至周日），5 档颜色 */
function renderHeatmap() {
  const box = $("#trend-heatmap");
  if (!box) return;
  box.className = `heatmap-body hm-view-${heatmapView}`;
  const days = trendAll.slice(-HEATMAP_SPAN[heatmapView] || []);
  if (!days.length) {
    box.innerHTML = '<div class="empty">暂无热力图数据</div>';
    return;
  }
  const max = Math.max(1, ...days.map(x => Number(x.bytes) || 0));
  const lvl = n => (n ? Math.min(4, 1 + Math.floor(n / max * 4)) : 0);

  if (heatmapView === "week") {
    // 周视图：7 个大格子横排，格内显示日号，下方标注星期
    const labels = "日一二三四五六";
    box.innerHTML = `<div class="hm-week-row">${days.map(x => {
      const n = Number(x.bytes) || 0;
      const d = new Date(x.day + "T00:00:00");
      return `<div class="hm-day">
        <span class="hm-cell q${lvl(n)}" data-tip="${esc(x.day)} · ${x.files} 个文件 · ${trendBytes(x.bytes)}"><b>${d.getDate()}</b></span>
        <span class="hm-day-label">周${labels[d.getDay()]}</span>
      </div>`;
    }).join("")}</div>`;
    return;
  }

  // 月/年视图：首列对齐星期（getDay() 周日=0 → 偏移 6）
  const lead = (new Date(days[0].day + "T00:00:00").getDay() + 6) % 7;
  const cells = [];
  for (let i = 0; i < lead; i++) cells.push('<span class="hm-cell blank"></span>');
  days.forEach(x => {
    const n = Number(x.bytes) || 0;
    cells.push(`<span class="hm-cell q${lvl(n)}" data-tip="${esc(x.day)} · ${x.files} 个文件 · ${trendBytes(x.bytes)}"></span>`);
  });
  // 月份标签：按视图列距标注月份变化处
  const pitch = heatmapView === "month" ? 25 : 13;  // 列宽 + 间距
  const cols = Math.ceil((lead + days.length) / 7);
  let months = "";
  let prevMonth = "";
  for (let c = 0; c < cols; c++) {
    // 第 c 列首格是 days[c*7 - lead]（前 lead 格是首列空白）；负数说明首列只含部分天，取 days[0]。
    const idx = Math.max(0, c * 7 - lead);
    if (idx >= days.length) break;
    const month = days[idx].day.slice(5, 7);
    if (month !== prevMonth) {
      months += `<span style="left:${c * pitch}px">${Number(month)}月</span>`;
      prevMonth = month;
    }
  }
  const label = heatmapView === "month" ? "近一月" : "近一年";
  box.innerHTML =
    `<div class="hm-canvas">
      <div class="hm-weekdays" aria-hidden="true"><span></span><span>一</span><span></span><span>三</span><span></span><span>五</span><span></span></div>
      <div class="hm-scroll"><div class="hm-months">${months}</div>
        <div class="hm-grid" role="img" aria-label="${label}每日迁移热力图">${cells.join("")}</div>
      </div>
    </div>`;
}

/* ================= 热力图悬浮提示 ================= */

/** 即时悬浮气泡：全局事件委托（模块加载即绑定一次），替代原生 title 的延迟提示 */
const hmTip = document.createElement("div");
hmTip.className = "hm-tip";
const hideHmTip = () => hmTip.classList.remove("show");
document.addEventListener("mouseover", e => {
  const cell = e.target instanceof Element ? e.target.closest(".hm-cell[data-tip]") : null;
  if (!cell || !cell.isConnected) { hideHmTip(); return; }
  if (!hmTip.isConnected) document.body.appendChild(hmTip);
  hmTip.textContent = cell.dataset.tip;
  hmTip.classList.add("show");
  const r = cell.getBoundingClientRect();
  const t = hmTip.getBoundingClientRect();
  let x = r.left + r.width / 2 - t.width / 2;
  let y = r.top - t.height - 8;
  x = Math.max(8, Math.min(x, window.innerWidth - t.width - 8));
  if (y < 8) y = r.bottom + 8;   // 顶部空间不足 → 改为显示在下方
  hmTip.style.left = `${x}px`;
  hmTip.style.top = `${y}px`;
});
document.addEventListener("scroll", hideHmTip, true);   // 滚动后定位失效，直接隐藏

export async function loadTrend() {
  if (trendBusy) return;
  trendBusy = true;
  try {
    // 一次拉取热力图全跨度；柱状图范围切换在本地切片，不再重复请求
    const d = await api(`/api/stats/trend?days=${HEATMAP_DAYS}`);
    trendAll = Array.isArray(d?.days) ? d.days : [];
    trendTotal = d?.total || {};
    renderTrendBars();
    renderHeatmap();
  } catch (e) {
    const chart = $("#trend-chart");
    if (chart) chart.innerHTML = `<div class="empty">趋势数据加载失败: ${esc(e.message)}</div>`;
  } finally { trendBusy = false; }
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
