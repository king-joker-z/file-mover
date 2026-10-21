/* ============================================================
 * pages/tasks.js — 任务管理页
 * 卡片式任务列表 + 弹窗分组表单（新建 / 编辑共用）。
 * ============================================================ */

import { api } from "../api.js";
import { store } from "../store.js";
import { openModal } from "../components/modal.js";
import { confirmDialog } from "../components/dialogs.js";
import { pickDirectory } from "../components/dirPicker.js";
import { syncNow, updateSyncBtn } from "../features/sync.js";
import { esc, toast } from "../utils.js";

let rootEl = null;
let tasksCache = [];
let formExcludes = [];
let editingTaskId = null;   // null = 新建模式
let editingModal = null;

const VERIFY_HINTS = {
  strict: "完整多轮校验 + 删除前暂存，最稳但大文件耗时长；适合小文件或重要归档。",
  fast: "上传后单次读回校验，无暂存文件；大文件在速度与安全间的推荐平衡。",
  off: "不读回内容，仅核对文件大小，速度最快；若网盘静默损坏将无法检出，请仅用于可重新下载的资源。",
};

/* ================= 页面骨架 ================= */

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="page-intro">
      <div><span class="eyebrow">TASKS / 迁移任务</span><h2>任务管理</h2>
        <p>每个任务定义一条「源目录 → 网盘」的自动迁移流水线，可独立启停与触发。</p></div>
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
        <span class="task-count" id="task-count" role="status"></span>
        <button class="btn" id="btn-new-task" type="button">＋ 新建任务</button>
      </div>
    </div>
    <div id="task-list"><div class="empty">加载任务中…</div></div>`;

  root.querySelector("#btn-new-task").addEventListener("click", () => openTaskModal(null));
}

export function show() { loadTasks(); }

/* ================= 任务列表（卡片） ================= */

export async function loadTasks() {
  if (!rootEl) return;
  const list = rootEl.querySelector("#task-list");
  try {
    const tasks = await api("/api/tasks");
    tasksCache = tasks;
    const count = rootEl.querySelector("#task-count");
    if (count) {
      const enabled = tasks.filter(t => t.enabled).length;
      count.textContent = tasks.length
        ? `共 ${tasks.length} 个任务 · 启用 ${enabled} 个`
        : "";
    }
    if (!tasks.length) {
      list.innerHTML =
        `<div class="empty"><span class="empty-icon">🗂</span>还没有迁移任务<br>` +
        `<span class="log-detail">创建一个任务，指定源目录与网盘目标目录即可开始自动迁移</span>` +
        `<div class="empty-action"><button class="btn" id="btn-empty-new" type="button">＋ 创建第一个任务</button></div></div>`;
      list.querySelector("#btn-empty-new")?.addEventListener("click", () => openTaskModal(null));
      return;
    }
    list.innerHTML = `<div class="task-grid">` + tasks.map(taskCard).join("") + `</div>`;

    // 卡片内事件绑定
    tasks.forEach(t => {
      const card = list.querySelector(`.task-card[data-task="${t.id}"]`);
      if (!card) return;
      card.querySelector("input[data-toggle]")?.addEventListener("change", ev => toggleTask(t.id, ev.target));
      card.querySelector("[data-edit]")?.addEventListener("click", () => openTaskModal(t));
      card.querySelector("[data-del]")?.addEventListener("click", () => delTask(t.id));
      card.querySelector("[data-sync]")?.addEventListener("click", () => syncNow(t.id));
      card.querySelector("[data-pause]")?.addEventListener("click", () => togglePause(t));
      card.querySelector("[data-backfill]")?.addEventListener("click", () => confirmBackfill(t.id));
      card.querySelectorAll("button[data-del-pattern]").forEach(b =>
        b.addEventListener("click", () => removeTaskPattern(t.id, b.dataset.delPattern)));
      const details = card.querySelector("details[data-exc-task]");
      if (details) {
        details.open = store.openExclude.has(t.id);
        details.addEventListener("toggle", () => {
          if (details.open) store.openExclude.add(t.id); else store.openExclude.delete(t.id);
        });
      }
    });
    tasks.forEach(t => updateSyncBtn(t.id));
  } catch (e) {
    list.innerHTML = '<div class="empty">任务加载失败：' + esc(e.message) + "</div>";
  }
}

/** NFO 巡检前的风险确认 */
async function confirmBackfill(id) {
  const ok = await confirmDialog(
    "补齐历史 NFO 会逐个检查已迁移视频的目标目录；对缺少 NFO 的视频会完整读回并校验哈希，" +
    "可能产生大量远端 I/O，且会等待当前迁移完成。确认继续？",
    { title: "补齐历史 NFO", okText: "开始巡检" },
  );
  if (ok) syncNow(id, true);
}

function taskCard(t) {
  const patterns = (t.exclude_patterns || "").split(",").map(x => x.trim()).filter(Boolean);
  const badges = [
    `<span class="badge">⏱ 文件间隔 ${t.interval_seconds}s</span>`,
    t.scan_interval ? `<span class="badge">⟳ 扫描 ${t.scan_interval}s</span>` : `<span class="badge">⟳ 全局扫描</span>`,
    `<span class="badge">${t.run_windows ? "🕐 " + esc(t.run_windows) : "全天运行"}</span>`,
    `<span class="badge">${{ skip: "冲突跳过", overwrite: "冲突覆盖", rename: "冲突重命名" }[t.conflict_policy] || esc(t.conflict_policy)}</span>`,
    `<span class="badge">${{ keep_structure: "保持结构", flatten: "平铺目标", by_date: "按日期归档" }[t.path_rule] || esc(t.path_rule)}</span>`,
    `<span class="badge">${t.after_action === "delete" ? "迁移（删源）" : "复制（留源）"}</span>`,
    t.verify_mode === "fast" ? `<span class="badge accent" title="单次读回，无暂存文件，校验提速">快速校验</span>`
      : t.verify_mode === "off" ? `<span class="badge warn" title="不读回内容，仅核对大小，最快但信任网盘写入">不读回校验</span>`
      : `<span class="badge ok" title="发布后读回 + 删除前暂存核对">严格校验</span>`,
t.direct_publish ? `<span class="badge warn" title="跳过 rename 转正，目标名独占创建直写（挂载 rename 卡死大文件时启用）">直写发布</span>` : "",
    t.async_transfer ? `<span class="badge accent" title="与其它任务并行迁移">异步</span>` : "",
    t.transfer_workers > 1 ? `<span class="badge accent" title="同一任务最多 ${t.transfer_workers} 个文件同时迁移">并发×${t.transfer_workers}</span>` : "",
    t.manual_only ? `<span class="badge warn" title="不参与自动定时扫描，需手动触发">仅手动扫描</span>` : "",
    t.paused ? `<span class="badge warn" title="迁移已暂停：正在传输的文件会完成，新文件不再开始（扫描继续）">已暂停迁移</span>` : "",
    t.video_only ? `<span class="badge" title="仅迁移视频文件（.mp4/.mkv/.mov/.flv/.webm）">仅视频</span>` : "",
    (t.min_size_mb || t.max_size_mb)
      ? `<span class="badge" title="${t.min_size_mb ? "≥" + t.min_size_mb + "MB" : ""}${t.min_size_mb && t.max_size_mb ? " 且 " : ""}${t.max_size_mb ? "≤" + t.max_size_mb + "MB" : ""}">大小过滤</span>` : "",
    t.min_age_minutes ? `<span class="badge" title="仅迁移最后修改至少 ${t.min_age_minutes} 分钟前的文件">年龄≥${t.min_age_minutes}分</span>` : "",
    t.filename_nfo_enabled ? `<span class="badge">NFO 补全</span>` : "",
  ].filter(Boolean).join("");

  const excBlock = patterns.length
    ? `<details class="exc-drop" data-exc-task="${t.id}"><summary>排除 ${patterns.length} 条</summary><div class="exc-list">` +
      patterns.map(p =>
        `<div class="exc-row"><span title="${esc(p)}">${esc(p)}</span>` +
        `<button type="button" class="btn small danger-ghost" data-del-pattern="${esc(p)}">删除</button></div>`).join("") +
      `</div></details>`
    : `<span class="log-detail">无排除规则</span>`;

  return `<article class="task-card${t.enabled ? "" : " is-disabled"}" data-task="${t.id}">
    <header class="task-card-head">
      <span class="task-dot" aria-hidden="true"></span>
      <h3 title="${esc(t.name)}">${esc(t.name)}</h3>
      <label class="switch" title="${t.enabled ? "点击停用" : "点击启用"}">
        <input type="checkbox" data-toggle aria-label="启用任务 ${esc(t.name)}" ${t.enabled ? "checked" : ""}>
        <span class="slider"></span>
      </label>
    </header>
    <div class="task-path">
      <span class="path-pill" title="${esc(t.src_dir)}">${esc(t.src_dir)}</span>
      <span class="arrow">→</span>
      <span class="path-pill" title="${esc(t.dst_dir)}">${esc(t.dst_dir)}</span>
    </div>
    <div class="task-badges">${badges}</div>
    <div class="task-exc">${excBlock}</div>
    <div class="task-sync-line">
      <span class="sync-error" data-sync-error="${t.id}" role="alert"></span>
      <span class="sync-status" data-sync-status="${t.id}" role="status"></span>
    </div>
    <footer class="task-actions">
      <button class="btn small" type="button" data-sync-id="${t.id}" data-sync>立即同步</button>
      <button class="btn small ghost" type="button" data-pause>${t.paused ? "▶ 恢复迁移" : "⏸ 暂停迁移"}</button>
      <button class="btn small ghost" type="button" data-backfill-id="${t.id}" data-backfill
        title="逐个校验历史目标视频，缺 NFO 时读取整段视频，可能产生大量远端 I/O">补齐历史 NFO…</button>
      <span class="spacer"></span>
      <button class="btn small gray" type="button" data-edit>编辑</button>
      <button class="btn small danger-ghost" type="button" data-del>删除</button>
    </footer>
  </article>`;
}

/* ================= 任务操作 ================= */

async function toggleTask(id, checkbox) {
  checkbox.disabled = true;
  try {
    const result = await api(`/api/tasks/${id}/toggle`, { method: "POST" });
    checkbox.checked = !!result.enabled;
    const card = rootEl?.querySelector(`.task-card[data-task="${id}"]`);
    card?.classList.toggle("is-disabled", !result.enabled);
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) {
    checkbox.checked = !checkbox.checked;
    toast("切换任务状态失败: " + e.message, "error");
  } finally { checkbox.disabled = false; }
}

async function togglePause(t) {
  try {
    const result = await api(`/api/tasks/${t.id}/pause`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ paused: !t.paused }),
    });
    t.paused = result.paused;
    const card = rootEl?.querySelector(`.task-card[data-task="${t.id}"]`);
    const btn = card?.querySelector("[data-pause]");
    if (btn) btn.textContent = result.paused ? "▶ 恢复迁移" : "⏸ 暂停迁移";
    toast(result.paused
      ? "已暂停迁移：正在传输的文件会完成，新文件不再开始（扫描继续）"
      : "已恢复迁移", "success");
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("暂停状态切换失败: " + e.message, "error"); }
}

async function delTask(id) {
  const ok = await confirmDialog("确认删除该任务及其队列？", { title: "删除任务", danger: true, okText: "删除" });
  if (!ok) return;
  try {
    await api(`/api/tasks/${id}`, { method: "DELETE" });
    delete store.syncUI[id];
    loadTasks();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("删除任务失败: " + e.message, "error"); }
}

async function removeTaskPattern(taskId, pattern) {
  const ok = await confirmDialog(
    `确认删除排除项「${pattern}」？\n删除后重新扫描会重新处理该路径；源文件始终保留。`,
    { title: "删除排除项", danger: true, okText: "删除" });
  if (!ok) return;
  try {
    await api(`/api/tasks/${taskId}/exclude-patterns?pattern=${encodeURIComponent(pattern)}`, { method: "DELETE" });
    loadTasks();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (e) { toast("删除排除项失败: " + e.message, "error"); }
}

/* ================= 弹窗分组表单（新建 / 编辑共用） ================= */

export function openTaskModal(task) {
  editingTaskId = task ? task.id : null;
  formExcludes = task
    ? (task.exclude_patterns || "").split(",").map(x => x.trim()).filter(Boolean)
    : [];

  const body = document.createElement("div");
  body.innerHTML = modalFormHtml(task);
  const modal = openModal({
    title: task ? `编辑任务 · ${esc(task.name)}` : "新建迁移任务",
    body,
    width: "780px",
    onClose: () => { editingModal = null; editingTaskId = null; },
  });
  editingModal = modal;
  bindModalEvents(modal, modal.close);
  updateVerifyHint(modal.body);
}

function field(label, inner, cls = "", hint = "") {
  return `<div class="field ${cls}"><label>${label}</label>${inner}` +
    (hint ? `<div class="field-hint">${hint}</div>` : "") + `</div>`;
}

function modalFormHtml(t) {
  const v = (k, dflt) => esc(t?.[k] ?? dflt ?? "");
  const sel = (k, dflt) => t ? String(t[k] ?? dflt ?? "") : String(dflt ?? "");
  return `
  <form id="task-form">
    <section class="form-section">
      <div class="section-title"><b>基本信息</b><span>源目录中的新文件将被迁移到目标目录</span></div>
      <div class="row">
        ${field("任务名称", `<input id="f-name" required placeholder="我的迁移任务" autocomplete="off">`)}
      </div>
      <div class="row">
        <div class="field"><label>源目录</label>
          <div class="inline-controls"><input id="f-src" class="path-input" readonly placeholder="点击右侧选择" onclick="void 0">
          <button type="button" class="btn small" data-pick="src">选择</button></div></div>
        <div class="field"><label>目标目录</label>
          <div class="inline-controls"><input id="f-dst" class="path-input" readonly placeholder="点击右侧选择">
          <button type="button" class="btn small" data-pick="dst">选择</button></div></div>
      </div>
    </section>

    <section class="form-section">
      <div class="section-title"><b>扫描与调度</b><span>决定多久发现一次新文件、什么时段工作</span></div>
      <div class="row">
        ${field("文件间隔 · 秒", `<input id="f-interval" type="number" step="0.5" min="0" max="86400" value="${v("interval_seconds", 5)}">`, "small")}
        ${field("扫描间隔 · 秒", `<input id="f-scan" type="number" min="0" max="86400" value="${sel("scan_interval", 0)}">`, "small", "0 = 使用全局设置")}
        ${field("扫描方式", `<select id="f-manual">
            <option value="0">自动定时扫描（默认）</option>
            <option value="1">仅手动扫描</option></select>`)}
      </div>
      <div class="row">
        ${field("运行周期", `<input id="f-windows" placeholder="01:00-07:00">
          <div class="field-hint">多段用逗号分隔，如 01:00-07:00,23:00-24:00；留空 = 全天运行</div>`)}
      </div>
    </section>

    <section class="form-section">
      <div class="section-title"><b>迁移策略</b><span>同名文件、目录结构与源文件的处理方式</span></div>
      <div class="row">
        ${field("同名冲突策略", `<select id="f-conflict">
            <option value="skip">跳过（提醒人工处理）</option>
            <option value="overwrite">覆盖</option>
            <option value="rename">自动重命名</option></select>`)}
        ${field("目标路径规则", `<select id="f-path">
            <option value="keep_structure">保持相对结构</option>
            <option value="flatten">平铺到目标根</option>
            <option value="by_date">按日期归档</option></select>`)}
        ${field("迁移后源文件", `<select id="f-after">
            <option value="delete">删除（迁移）</option>
            <option value="keep">保留（复制）</option></select>`)}
      </div>
      <div class="row">
${field("校验强度", `<select id="f-verify">
<option value="strict">严格：发布后读回 + 删除前暂存（默认）</option>
<option value="fast">快速：单次读回，无暂存（大文件推荐）</option>
<option value="off">极速：仅核对大小（信任网盘写入）</option></select>`)}
${field("发布方式", `<select id="f-direct">
<option value="0">rename 转正（默认，原子性最好）</option>
<option value="1">直写：目标名独占创建（挂载 rename 卡死大文件时启用）</option></select>`)}
</div>
      <div class="row">
        ${field("文件类型范围", `<select id="f-video-only">
            <option value="0">全部文件（默认）</option>
            <option value="1">仅视频文件</option></select>`, "",
          "仅视频 = 只迁移 .mp4/.mkv/.mov/.flv/.webm；NFO/封面等不再迁移，NFO 自动生成也会跳过")}
      </div>
      <div class="field-hint" id="verify-hint"></div>
    </section>

    <section class="form-section">
      <div class="section-title"><b>性能与元数据</b><span>大文件并行与 NFO 自动生成</span></div>
      <div class="row">
        ${field("迁移方式", `<select id="f-async">
            <option value="0">同步：依次迁移（默认）</option>
            <option value="1">异步：与其它任务并行</option></select>`)}
        ${field("并发迁移数", `<select id="f-workers">
            <option value="1">1（串行，默认）</option><option value="2">2 个并行</option>
            <option value="3">3 个并行</option><option value="4">4 个并行</option></select>`, "small",
          "需开启异步；线程不足时自动补足")}
        ${field("缺失 NFO 补全", `<select id="f-filename-nfo">
            <option value="0">关闭（默认）</option>
            <option value="1">开启</option></select>`, "",
          "按 视频名-作者-YYYYMMDD 与 -poster 封面生成")}
      </div>
    </section>

    <section class="form-section">
      <div class="section-title"><b>包含与排除规则</b><span>通配匹配文件名；也可按大小/年龄限定迁移范围</span></div>
      <div class="row">
        ${field("包含（通配，逗号分隔）", `<input id="f-inc" placeholder="*.mp4, *.jpg">`, "", "留空 = 全部文件")}
      </div>
      <div class="row">
        ${field("最小大小 · MB", `<input id="f-minsize" type="number" min="0" step="0.1" value="0">`, "small", "0 = 不限")}
        ${field("最大大小 · MB", `<input id="f-maxsize" type="number" min="0" step="0.1" value="0">`, "small", "0 = 不限")}
        ${field("最小年龄 · 分钟", `<input id="f-age" type="number" min="0" step="1" value="0">`, "small", "仅迁移最后修改至少 X 分钟前的文件；0 = 不限")}
      </div>
      <div id="f-exc-chips" class="chip-list"></div>
      <div class="inline-controls" style="display:flex;gap:6px;margin-top:8px">
        <input id="f-exc-input" placeholder="输入路径或通配后回车添加，如 B站/投稿/xxx、*.tmp" autocomplete="off">
        <button type="button" class="btn small" data-add-exc>添加</button>
      </div>
      <div class="field-hint">支持相对源目录路径或完整路径；排除目录后不进入扫描。遇到 EIO 会自动加入精确路径并记录，删除后可在「EIO 故障」页重试。</div>
    </section>
  </form>`;
}

function bindModalEvents(modal, closeFn) {
  const body = modal.body;
  const t = editingTaskId != null ? tasksCache.find(x => x.id === editingTaskId) : null;

  // 回填表单值
  const set = (id, value) => { const el = body.querySelector("#" + id); if (el) el.value = value; };
  if (t) {
    set("f-name", t.name); set("f-src", t.src_dir); set("f-dst", t.dst_dir);
    set("f-interval", t.interval_seconds); set("f-scan", t.scan_interval);
    set("f-inc", t.include_patterns || "");
    set("f-conflict", t.conflict_policy); set("f-path", t.path_rule);
    set("f-after", t.after_action); set("f-windows", t.run_windows || "");
    set("f-filename-nfo", t.filename_nfo_enabled ? "1" : "0");
    set("f-async", t.async_transfer ? "1" : "0");
    set("f-workers", String(t.transfer_workers || 1));
    set("f-manual", t.manual_only ? "1" : "0");
    set("f-video-only", t.video_only ? "1" : "0");
    set("f-verify", t.verify_mode || "strict");
set("f-direct", t.direct_publish ? "1" : "0");
    set("f-minsize", t.min_size_mb || 0); set("f-maxsize", t.max_size_mb || 0);
    set("f-age", t.min_age_minutes || 0);
  } else {
    set("f-conflict", "skip"); set("f-path", "keep_structure"); set("f-after", "delete");
    set("f-verify", "strict"); set("f-manual", "0"); set("f-async", "0");
set("f-direct", "0"); set("f-video-only", "0");
    set("f-workers", "1"); set("f-filename-nfo", "0");
    set("f-minsize", "0"); set("f-maxsize", "0"); set("f-age", "0");
  }
  renderExcChips(body);

  // 底部操作：通过 form 属性关联提交，置于卡片底部操作条
  const foot = document.createElement("div");
  foot.className = "modal-foot";
  foot.style.borderTop = "none";
  foot.style.marginTop = "0";
  foot.innerHTML =
    `<button type="button" class="btn gray" data-cancel>取消</button>` +
    `<button type="submit" form="task-form" class="btn ${t ? "green" : ""}">${t ? "保存修改" : "创建任务"}</button>`;
  modal.el.appendChild(foot);
  foot.querySelector("[data-cancel]").addEventListener("click", closeFn);

  body.querySelector("#task-form").addEventListener("submit", e => {
    e.preventDefault();
    submitTask(modal, closeFn);
  });

  // 目录选择
  body.querySelectorAll("button[data-pick]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const target = body.querySelector("#f-" + btn.dataset.pick);
      const picked = await pickDirectory();
      if (picked) target.value = picked;
    });
  });

  // 排除规则
  body.querySelector("[data-add-exc]").addEventListener("click", () => addExcludeFromInput(body));
  body.querySelector("#f-exc-input").addEventListener("keydown", e => {
    if (e.key === "Enter") { e.preventDefault(); addExcludeFromInput(body); }
  });

  // 校验强度提示
  body.querySelector("#f-verify").addEventListener("change", () => updateVerifyHint(body));
}

function updateVerifyHint(body) {
  const sel = body.querySelector("#f-verify"), tip = body.querySelector("#verify-hint");
  if (sel && tip) tip.textContent = VERIFY_HINTS[sel.value] || "";
}

function renderExcChips(body) {
  body.querySelector("#f-exc-chips").innerHTML = formExcludes.map((p, i) =>
    `<span class="chip" title="${esc(p)}"><span>${esc(p)}</span>` +
    `<button type="button" aria-label="删除排除项 ${esc(p)}" data-del-chip="${i}">✕</button></span>`).join("");
  body.querySelectorAll("[data-del-chip]").forEach(b =>
    b.addEventListener("click", () => {
      formExcludes.splice(Number(b.dataset.delChip), 1);
      renderExcChips(body);
    }));
}

function addExcludeFromInput(body) {
  const input = body.querySelector("#f-exc-input");
  const parts = input.value.split(",").map(x => x.trim()).filter(Boolean);
  if (!parts.length) { input.focus(); return; }
  let added = 0, duplicate = false;
  parts.forEach(p => {
    if (formExcludes.includes(p)) duplicate = true;
    else { formExcludes.push(p); added++; }
  });
  input.value = "";
  renderExcChips(body);
  input.focus();
  if (!added && duplicate) toast("该排除项已存在");
}

async function submitTask(modal, closeFn) {
  const body = modal.body;
  const val = id => body.querySelector("#" + id).value;
  const src = val("f-src"), dst = val("f-dst");
  if (!src || !dst) { toast("请先点击「选择」挑选源目录和目标目录"); return; }
  if (dst === src || dst.startsWith(src + "/") || src.startsWith(dst + "/")) {
    toast("源目录与目标目录不能相同或互相包含"); return;
  }
  const payload = {
    name: val("f-name"), src_dir: src, dst_dir: dst,
    interval_seconds: +val("f-interval"), scan_interval: +val("f-scan"),
    include_patterns: val("f-inc"), exclude_patterns: formExcludes.join(", "),
    conflict_policy: val("f-conflict"), path_rule: val("f-path"), after_action: val("f-after"),
    run_windows: val("f-windows").trim(),
    filename_nfo_enabled: val("f-filename-nfo") === "1",
    async_transfer: val("f-async") === "1",
    transfer_workers: +val("f-workers") || 1,
    manual_only: val("f-manual") === "1",
    video_only: val("f-video-only") === "1",
    verify_mode: val("f-verify") || "strict",
    direct_publish: val("f-direct") === "1",
    min_size_mb: +val("f-minsize") || 0,
    max_size_mb: +val("f-maxsize") || 0,
    min_age_minutes: +val("f-age") || 0,
  };
  const submitBtn = modal.el.querySelector('button[form="task-form"]');
  submitBtn.disabled = true;
  try {
    if (editingTaskId != null) {
      await api("/api/tasks/" + editingTaskId, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      toast("任务已更新", "success");
    } else {
      await api("/api/tasks", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      toast("任务已创建", "success");
    }
    closeFn();
    loadTasks();
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
  } catch (err) {
    submitBtn.disabled = false;
    toast("保存失败: " + err.message, "error");
  }
}

/** 供仪表盘「将故障目录加入排除项」复用的任务查询 */
export function findTask(id) { return tasksCache.find(t => t.id === id); }
