/* ============================================================
 * store.js — 跨页面共享的运行时状态
 * ============================================================ */

export const store = {
  /** 当前激活的 Tab（由 main.js 路由维护） */
  activeTab: "dash",

  /** /api/dashboard 返回的扫描计划列表 */
  scanSchedules: [],

  /** 服务器时钟偏移（秒），用于本地校准倒计时 */
  serverClockOffset: 0,

  /** 每个任务的「立即同步」UI 状态：id -> {running, starting, scanned, enqueued, ...} */
  syncUI: {},

  /** 任务列表中展开的排除规则明细（task id 集合） */
  openExclude: new Set(),

  /** 轮次历史页当前可见的轮次（用于 1s 耗时刷新） */
  historyRounds: [],
};
