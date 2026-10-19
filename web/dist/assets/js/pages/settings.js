/* ============================================================
 * pages/settings.js — 全局设置
 * ============================================================ */

import { api } from "../api.js";

let rootEl = null;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="settings-intro"><span class="eyebrow">PREFERENCES / 全局配置</span>
      <h2>设置与安全策略</h2><p>按用途调整扫描节奏与文件处理方式；保存后从下一周期开始生效。</p></div>
    <form id="settings-form">
      <div class="settings-grid">
        <section class="panel settings-card"><span class="eyebrow">01 / SCHEDULING</span><h2>扫描与重试</h2>
          <p class="log-detail">控制定时扫描和异常后的重试频率。</p>
          <div class="row">
            <div class="field small"><label for="g-scan">全局扫描间隔 · 秒</label><input id="g-scan" type="number" min="1" required></div>
            <div class="field small"><label for="g-retries">最大重试次数</label><input id="g-retries" type="number" min="0" required></div>
            <div class="field small"><label for="g-async-workers">异步迁移线程数</label><input id="g-async-workers" type="number" min="1" max="8" required></div>
          </div>
          <div class="settings-note">任务单独设置的扫描间隔优先于此处的全局值。异步迁移线程数：开启「异步迁移」的任务可与其它任务并行迁移；调大即时生效，调小重启后生效。任务并发迁移数需要更多线程时会自动补足（上限 8）。</div>
        </section>
        <section class="panel settings-card"><span class="eyebrow">02 / FILE SAFETY</span><h2>文件稳定性</h2>
          <p class="log-detail">避免未写完的文件过早进入迁移队列。</p>
          <div class="row">
            <div class="field small"><label for="g-stable-s">采样间隔 · 秒</label><input id="g-stable-s" type="number" min="0" max="3600" required></div>
            <div class="field small"><label for="g-quiet-s">文件静默期 · 秒</label><input id="g-quiet-s" type="number" min="0" max="86400" required></div>
          </div>
          <label class="settings-option"><input type="checkbox" id="g-stable">
            <span><b>双采样检测</b><small>额外检查文件大小是否稳定；静默期保护始终启用，包括立即同步。</small></span></label>
          <div class="settings-note">安全迁移及完整性校验始终启用：复制后 hash / 读回校验，再按任务策略处理源文件。</div>
        </section>
        <section class="panel settings-card"><span class="eyebrow">03 / HOUSEKEEPING</span><h2>目录整理</h2>
          <p class="log-detail">迁移完成后的本地目录处理。</p>
          <label class="settings-option"><input type="checkbox" id="g-empty">
            <span><b>清理空目录</b><small>仅清理迁移后留下的空目录。</small></span></label>
        </section>
        <section class="panel settings-card"><span class="eyebrow">04 / METADATA</span><h2>NFO 元数据</h2>
          <p class="log-detail">这些选项作用于迁移前的文件，不会自动巡检历史目标目录。</p>
          <label class="settings-option"><input type="checkbox" id="g-nfo">
            <span><b>NFO 预处理</b><small>修正 actor 数字 UID 的 name → role。</small></span></label>
          <label class="settings-option"><input type="checkbox" id="g-douyin">
            <span><b>抖音 NFO 自动生成</b><small>无 NFO 的视频按文件名生成简易信息。</small></span></label>
        </section>
        <section class="panel settings-card"><span class="eyebrow">05 / RETENTION</span><h2>数据保留</h2>
          <p class="log-detail">自动清理超过保留期的迁移日志与历史轮次，控制数据库体积；填 0 = 永久保留。</p>
          <div class="row">
            <div class="field small"><label for="g-log-retention">日志保留 · 天</label><input id="g-log-retention" type="number" min="0" max="3650" required></div>
            <div class="field small"><label for="g-round-retention">轮次保留 · 天</label><input id="g-round-retention" type="number" min="0" max="3650" required></div>
          </div>
          <div class="settings-note">每小时自动清理一次，保存后立即执行。过期日志删除前会把按日趋势聚合归档，趋势图不受影响；进行中的队列与未完结轮次永不会被清理。删除的空间由 SQLite 后续写入复用。</div>
        </section>
        <section class="panel settings-card"><span class="eyebrow">06 / GLOBAL FILTERS</span><h2>全局忽略后缀</h2>
          <p class="log-detail">不完整的下载/临时文件不参与任何任务的扫描。</p>
          <div class="field"><label for="g-ignore">忽略后缀（逗号分隔）</label>
            <input id="g-ignore" placeholder=".tmp, .part, .crdownload, .!ut">
            <div class="field-hint">清空 = 不忽略任何后缀。单个任务的过滤还可在任务表单中按大小/年龄设置。</div></div>
        </section>
      </div>
      <div class="settings-save"><button class="btn" type="submit">保存设置</button>
        <span class="log-detail">更改在下一周期生效</span>
        <span id="settings-msg" role="status" style="margin-left:auto;font-size:13px"></span></div>
    </form>`;

  root.querySelector("#settings-form").addEventListener("submit", onSubmit);
}

export function show() { load(); }

async function load() {
  if (!rootEl) return;
  try {
    const s = await api("/api/settings");
    const set = (id, v) => { const el = rootEl.querySelector("#" + id); if (el) el.value = v; };
    const setChk = (id, v) => { const el = rootEl.querySelector("#" + id); if (el) el.checked = !!v; };
    set("g-scan", s.scan_interval);
    set("g-stable-s", s.stable_check_seconds);
    set("g-quiet-s", s.stable_quiet_seconds);
    set("g-retries", s.max_retries);
    set("g-async-workers", s.max_async_transfers ?? 2);
    set("g-log-retention", s.log_retention_days ?? 0);
    set("g-round-retention", s.round_retention_days ?? 0);
    set("g-ignore", (s.ignore_suffixes || []).join(", "));
    setChk("g-stable", s.stable_check);
    setChk("g-empty", s.remove_empty_dirs);
    setChk("g-nfo", s.nfo_fix_enabled !== false);
    setChk("g-douyin", s.douyin_nfo_enabled);
  } catch (e) {
    rootEl.querySelector("#settings-msg").textContent = "设置加载失败: " + e.message;
  }
}

async function onSubmit(e) {
  e.preventDefault();
  const val = id => rootEl.querySelector("#" + id).value;
  const chk = id => rootEl.querySelector("#" + id).checked;
  const msg = rootEl.querySelector("#settings-msg");
  try {
    await api("/api/settings", {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        scan_interval: +val("g-scan"),
        stable_check_seconds: +val("g-stable-s"),
        stable_quiet_seconds: +val("g-quiet-s"),
        max_retries: +val("g-retries"),
        max_async_transfers: +val("g-async-workers"),
        stable_check: chk("g-stable"),
        remove_empty_dirs: chk("g-empty"),
        nfo_fix_enabled: chk("g-nfo"),
        douyin_nfo_enabled: chk("g-douyin"),
        log_retention_days: +val("g-log-retention") || 0,
        round_retention_days: +val("g-round-retention") || 0,
        ignore_suffixes: val("g-ignore").split(/[,，]/).map(x => x.trim()).filter(Boolean),
      }),
    });
    msg.style.color = "var(--ok-ink)";
    msg.textContent = "已保存 ✓";
    document.dispatchEvent(new CustomEvent("fm:data-changed"));
    setTimeout(() => { msg.textContent = ""; }, 2000);
  } catch (err) {
    msg.style.color = "var(--err-ink)";
    msg.textContent = "保存失败: " + err.message;
  }
}
