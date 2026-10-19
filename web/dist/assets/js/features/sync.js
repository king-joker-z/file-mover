/* ============================================================
 * sync.js — 「立即同步」状态机：触发 / 按钮渲染 / 进度轮询 / 倒计时刷新
 * 仪表盘扫描卡与任务管理页共用。
 * ============================================================ */

import { api } from "../api.js";
import { store } from "../store.js";
import { formatCountdown } from "../utils.js";

function newSyncRequestId() {
  // 局域网 HTTP 地址不是安全上下文，randomUUID 可能不可用；getRandomValues 仍可使用。
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * 触发立即同步（或历史 NFO 巡检）。
 * @param {number} id 任务 ID
 * @param {boolean} backfillNfo 是否附带历史 NFO 巡检
 */
export async function syncNow(id, backfillNfo = false) {
  const syncUI = store.syncUI;
  if (syncUI[id]?.running) return;  // 调用方负责 NFO 巡检前的风险确认
  const previous = syncUI[id]?.resetTimer;
  if (previous) clearTimeout(previous);
  const requestId = newSyncRequestId();
  const state = syncUI[id] = {
    running: true, starting: true, scanned: 0, enqueued: 0,
    error: null, finished: false, requestId, backfillNfo,
  };
  updateSyncBtn(id);
  const controller = new AbortController();
  let timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const accepted = await api(
      `/api/tasks/${id}/sync-now?request_id=${requestId}${backfillNfo ? "&backfill_nfo=true" : ""}`,
      {
        method: "POST", signal: controller.signal,
        onAuthChallenge: phase => {
          clearTimeout(timeout);
          if (phase === "resume") timeout = setTimeout(() => controller.abort(), 15000);
        },
      },
    );
    if (syncUI[id] !== state) return;
    state.requestId = accepted.request_id || requestId;
    state.starting = false;
    pollSyncProgress();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) {
    if (syncUI[id] !== state) return;
    state.running = false;
    state.starting = false;
    state.error = e.name === "AbortError" ? "提交超时，正在查询扫描状态" : "触发失败: " + e.message;
    updateSyncBtn(id);
    if (e.name === "AbortError") {
      try {
        const p = await api(`/api/tasks/${id}/sync-progress`, { signal: AbortSignal.timeout(10000) });
        if (syncUI[id] === state && p.request_id === requestId && (p.running || p.done)) {
          state.running = !!p.running;
          state.scanned = p.scanned || 0;
          state.enqueued = p.enqueued || 0;
          state.current = p.current || "";
          state.backfillNfo = !!p.backfill_nfo;
          state.error = p.error || null;
          state.finished = !!p.done && !state.error;
          if (state.running) pollSyncProgress();
        } else if (syncUI[id] === state) {
          state.error = "未确认本次请求已受理，请检查扫描计划后重试";
        }
      } catch (_) {
        if (syncUI[id] === state) state.error = "无法查询提交结果，请检查扫描计划后重试";
      }
      updateSyncBtn(id);
    }
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } finally { clearTimeout(timeout); }
}

/** 刷新某任务全部同步按钮与状态文本（作用于当前 DOM 中存在的元素） */
export function updateSyncBtn(id) {
  const st = store.syncUI[id];
  const schedule = store.scanSchedules.find(s => s.id === Number(id));
  const unavailable = schedule && (!schedule.enabled || schedule.status === "cooldown");
  document.querySelectorAll(`button[data-sync-id="${id}"]`).forEach(btn => {
    const dashboard = btn.classList.contains("scan-trigger");
    btn.title = st?.error || st?.current || "";
    if (st?.running) {
      btn.textContent = st.starting ? "提交中…"
        : st.backfillNfo ? "历史 NFO 巡检中…"
        : `已发现 ${st.scanned} · 新入队 ${st.enqueued}`;
      btn.disabled = true;
    } else if (unavailable) {
      btn.textContent = dashboard ? (schedule.enabled ? "冷却中" : "任务已停用") : "立即同步";
      btn.disabled = true;
    } else if (schedule?.status === "scanning") {
      btn.textContent = "定时扫描中…";
      btn.disabled = true;
    } else if (st?.error) {
      btn.textContent = dashboard ? "重试扫描" : "重试同步";
      btn.disabled = false;
    } else if (st?.finished) {
      btn.textContent = `✓ 新入队 ${st.enqueued}`;
      btn.disabled = false;
    } else {
      btn.textContent = dashboard ? "立即扫描" : "立即同步";
      btn.disabled = false;
    }
  });
  document.querySelectorAll(`button[data-backfill-id="${id}"]`).forEach(btn => {
    btn.disabled = !!st?.running || !!unavailable || schedule?.status === "scanning";
    btn.textContent = st?.running && st.backfillNfo ? "历史 NFO 巡检中…" : "补齐历史 NFO…";
  });
  document.querySelectorAll(`[data-sync-error="${id}"]`).forEach(el => el.textContent = st?.error || "");
  document.querySelectorAll(`[data-sync-status="${id}"]`).forEach(el =>
    el.textContent = st?.running && !st.starting ? st.current || "" : "");
  if (st?.finished && !st.resetTimer) {
    st.resetTimer = setTimeout(() => {
      if (store.syncUI[id] !== st || st.running || st.error) return;
      st.finished = false;
      updateSyncBtn(id);
    }, 4000);
  }
}

let pollTimer = null, pollBusy = false;

/** 轮询进行中的同步进度（自动启停） */
export function pollSyncProgress() {
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    if (pollBusy) return;
    pollBusy = true;
    try {
      for (const id of Object.keys(store.syncUI)) {
        const st = store.syncUI[id];
        if (!st.running || st.starting) continue;
        try {
          const p = await api(`/api/tasks/${id}/sync-progress`, { signal: AbortSignal.timeout(10000) });
          if (store.syncUI[id] !== st) continue;
          if (p.request_id !== st.requestId) {
            st.running = false;
            st.error = "无法确认本次扫描状态，请检查扫描计划";
            updateSyncBtn(id);
            continue;
          }
          st.scanned = p.scanned || 0;
          st.enqueued = p.enqueued || 0;
          st.current = p.current || "";
          st.backfillNfo = !!p.backfill_nfo;
          st.error = p.error || null;
          st.pollFailures = 0;
          st.running = !!p.running;
          st.finished = !st.running && !st.error;
          updateSyncBtn(id);
          if (!st.running) document.dispatchEvent(new CustomEvent("fm:data-changed"));
        } catch (e) {
          if (store.syncUI[id] !== st) continue;
          st.pollFailures = (st.pollFailures || 0) + 1;
          st.error = (st.pollFailures >= 10
            ? "获取同步进度失败，请刷新页面核对任务状态: "
            : "进度暂不可用，正在重试: ") + e.message;
          if (st.pollFailures >= 10) st.running = false;
          updateSyncBtn(id);
        }
      }
    } finally {
      pollBusy = false;
      if (!Object.values(store.syncUI).some(st => st.running)) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }
  }, 500);
}

/** 同步进行中的任务恢复（刷新页面后从 dashboard 计划恢复状态） */
export function adoptRunningSchedules(schedules) {
  schedules.forEach(s => {
    if (s.status === "manual" && s.request_id && store.syncUI[s.id]?.requestId !== s.request_id) {
      store.syncUI[s.id] = {
        running: true, starting: false,
        scanned: s.scanned, enqueued: s.enqueued, current: s.current || "",
        error: null, finished: false, requestId: s.request_id, backfillNfo: !!s.backfill_nfo,
      };
      pollSyncProgress();
    }
  });
}

/* ---------- 每秒倒计时刷新（扫描倒计时 / 轮次耗时） ---------- */

export function tickCountdowns() {
  document.querySelectorAll("[data-round-elapsed]").forEach(el => {
    const entry = store.scanSchedules.find(s => s.id === Number(el.dataset.roundElapsed));
    if (entry?.round) el.textContent = roundElapsed(entry.round);
  });
  if (store.activeTab === "rounds") {
    document.querySelectorAll("[data-history-elapsed]").forEach(el => {
      const entry = store.historyRounds.find(r => r.id === Number(el.dataset.historyElapsed));
      if (entry) el.textContent = roundElapsed(entry);
    });
  }
  document.querySelectorAll("[data-countdown-id]").forEach(el => {
    const entry = store.scanSchedules.find(s => s.id === Number(el.dataset.countdownId));
    if (!entry) return;
    if (entry.status === "disabled") el.textContent = "已停用";
    else if (entry.status === "manual") el.textContent = "手动扫描中";
    else if (entry.status === "scanning") el.textContent = "定时扫描中";
    else if (entry.status === "manual_wait") el.textContent = "等待手动触发";
    else if (entry.next_scan_at == null) el.textContent = "等待调度";
    else {
      const remaining = entry.next_scan_at - (Date.now() / 1000 - store.serverClockOffset);
      el.textContent = remaining <= 0 ? "等待调度" : formatCountdown(remaining);
    }
  });
}

/** 轮次已进行时长（含进行中轮次的实时推进） */
export function roundElapsed(r) {
  const now = Date.now() / 1000 - store.serverClockOffset;
  return formatCountdown(r.started_at ? Math.max(0, (r.finished_at || now) - r.started_at) : 0);
}
