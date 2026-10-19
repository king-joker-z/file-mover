/* ============================================================
 * customSelect.js — 主题化自绘下拉
 *
 * 背景：原生 <select> 的展开列表由操作系统/浏览器绘制，
 * 页面 CSS 无法控制；在页面缩放或系统辅助功能设置下会
 * 字号异常、位置漂移。本组件接管全部渲染：
 *
 * - 保留原 <select>（视觉隐藏）→ 既有 `select.value` 取值、
 *   `change` 监听、动态填充 options 的代码零改动；
 * - 在其位置渲染触发按钮 + 弹层，样式完全跟随主题；
 * - fixed 定位弹层紧贴控件下方，空间不足自动向上翻转；
 * - 支持键盘导航（↑↓ 选择、Enter 确认、Esc 关闭）；
 * - watchSelects() 全局监听 DOM 变化，新增 select 自动增强。
 * ============================================================ */

const enhanced = new WeakSet();
const stateMap = new WeakMap();  // select -> { btn, pop, hl }

/** 增强单个 select */
export function enhanceSelect(select) {
  if (enhanced.has(select) || !select.parentNode) return;
  enhanced.add(select);

  const st = { btn: null, pop: null, hl: -1, onDocDown: null, onScroll: null, onKey: null };
  stateMap.set(select, st);

  /* ---------- 结构：wrap > [隐藏 select + 触发按钮] ---------- */
  const wrap = document.createElement("div");
  wrap.className = "cs-wrap";
  select.parentNode.insertBefore(wrap, select);
  wrap.appendChild(select);
  select.classList.add("cs-native");
  select.tabIndex = -1;

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "cs-btn";
  btn.setAttribute("aria-haspopup", "listbox");
  btn.setAttribute("aria-expanded", "false");
  const txt = document.createElement("span");
  txt.className = "cs-text";
  btn.appendChild(txt);
  wrap.appendChild(btn);
  st.btn = btn;

  const pop = document.createElement("div");
  pop.className = "cs-pop";
  pop.setAttribute("role", "listbox");
  st.pop = pop;

  /* ---------- 渲染与同步 ---------- */

  function renderOptions() {
    pop.innerHTML = "";
    [...select.options].forEach((opt, i) => {
      const item = document.createElement("div");
      item.className = "cs-opt" + (opt.selected ? " cs-on" : "") + (opt.disabled ? " cs-off" : "");
      item.textContent = opt.text;
      item.setAttribute("role", "option");
      item.setAttribute("aria-selected", opt.selected ? "true" : "false");
      item.addEventListener("click", () => choose(i));
      item.addEventListener("mousemove", () => setHL(i));
      pop.appendChild(item);
    });
    syncLabel();
  }

  function syncLabel() {
    const opt = select.options[select.selectedIndex];
    txt.textContent = opt ? opt.text : "";
    [...pop.children].forEach((el, i) => {
      el.classList.toggle("cs-on", i === select.selectedIndex);
      el.setAttribute("aria-selected", i === select.selectedIndex ? "true" : "false");
    });
  }

  function choose(i) {
    if (!select.options[i] || select.options[i].disabled) return;
    const changed = select.selectedIndex !== i;
    select.selectedIndex = i;
    syncLabel();
    close();
    btn.focus({ preventScroll: true });
    if (changed) select.dispatchEvent(new Event("change", { bubbles: true }));
  }

  /* 编程赋值 `select.value = x` 不会触发 change，拦截 setter 同步 UI */
  const valueDesc = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, "value");
  if (valueDesc?.set) {
    Object.defineProperty(select, "value", {
      configurable: true,
      get() { return valueDesc.get.call(this); },
      set(v) { valueDesc.set.call(this, v); syncLabel(); },
    });
  }

  /* options 动态填充 / disabled 变化时同步 */
  new MutationObserver(renderOptions).observe(select, { childList: true });
  new MutationObserver(() => {
    btn.classList.toggle("disabled", select.disabled);
    btn.setAttribute("aria-disabled", select.disabled ? "true" : "false");
    if (select.disabled) close();
  }).observe(select, { attributes: true, attributeFilter: ["disabled"] });

  /* ---------- 打开 / 关闭 / 定位 ---------- */

  function position() {
    const r = btn.getBoundingClientRect();
    pop.style.minWidth = Math.round(r.width) + "px";
    pop.style.left = Math.round(r.left) + "px";
    const below = window.innerHeight - r.bottom - 8;
    const above = r.top - 8;
    const want = Math.min(pop.scrollHeight, 280);
    if (below >= want || below >= above) {
      pop.style.top = Math.round(r.bottom + 4) + "px";
      pop.style.bottom = "auto";
      pop.style.maxHeight = Math.max(140, Math.min(280, below)) + "px";
    } else {
      pop.style.bottom = Math.round(window.innerHeight - r.top + 4) + "px";
      pop.style.top = "auto";
      pop.style.maxHeight = Math.max(140, Math.min(280, above)) + "px";
    }
    const on = pop.children[select.selectedIndex];
    if (on) on.scrollIntoView({ block: "nearest" });
  }

  function setHL(i) {
    st.hl = i;
    [...pop.children].forEach((el, j) => el.classList.toggle("hl", j === i));
  }

  function open() {
    if (select.disabled || pop.isConnected) return;
    renderOptions();
    document.body.appendChild(pop);
    position();
    btn.setAttribute("aria-expanded", "true");
    btn.classList.add("open");
    setHL(select.selectedIndex);
    st.openRect = btn.getBoundingClientRect();
    st.onDocDown = e => {
      if (!pop.contains(e.target) && !btn.contains(e.target)) close();
    };
    st.onScroll = e => {
      if (pop.contains(e.target)) return;  // 弹层内部滚动不关闭
      const r = btn.getBoundingClientRect();       // 控件位置未变（如焦点微滚）不关闭
      if (Math.abs(r.top - st.openRect.top) < 2 && Math.abs(r.left - st.openRect.left) < 2) return;
      close();
    };
    /* 捕获阶段拦截 Esc：只关弹层，不让外层弹窗响应 */
    st.onKey = e => {
      if (e.key !== "Escape") return;
      e.preventDefault();
      e.stopPropagation();
      close();
      btn.focus({ preventScroll: true });
    };
    document.addEventListener("mousedown", st.onDocDown, true);
    document.addEventListener("keydown", st.onKey, true);
    window.addEventListener("scroll", st.onScroll, true);
    window.addEventListener("resize", close);
    btn.focus({ preventScroll: true });
  }

  function close() {
    if (!pop.isConnected) return;
    pop.remove();
    btn.setAttribute("aria-expanded", "false");
    btn.classList.remove("open");
    if (st.onDocDown) document.removeEventListener("mousedown", st.onDocDown, true);
    if (st.onKey) document.removeEventListener("keydown", st.onKey, true);
    if (st.onScroll) window.removeEventListener("scroll", st.onScroll, true);
    window.removeEventListener("resize", close);
  }

  const toggle = () => (pop.isConnected ? close() : open());

  /* ---------- 交互 ---------- */

  btn.addEventListener("click", toggle);
  btn.addEventListener("keydown", e => {
    if (!pop.isConnected) {
      if (["Enter", " ", "ArrowDown", "ArrowUp"].includes(e.key)) {
        e.preventDefault();
        open();
        if (e.key === "ArrowUp") setHL(pop.children.length - 1);
      }
      return;
    }
    if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();          // 避免冒泡误关外层弹窗
      close();
      btn.focus();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setHL(Math.min(st.hl + 1, pop.children.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHL(Math.max(st.hl - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      choose(st.hl >= 0 ? st.hl : select.selectedIndex);
    } else if (e.key === "Tab") {
      close();
    }
  });

  /* 包裹式 label（如主题选择器）：阻止其转发点击到隐藏 select 弹出系统菜单 */
  const hostLabel = select.closest("label");
  if (hostLabel) {
    hostLabel.addEventListener("click", e => {
      e.preventDefault();
      if (!btn.contains(e.target)) toggle();
    });
  } else {
    /* 同 field 内无 for 的 label（表单字段标题）：点击也能打开下拉 */
    const fieldLabel = select.closest(".field")?.querySelector(":scope > label");
    if (fieldLabel) fieldLabel.addEventListener("click", e => { e.preventDefault(); toggle(); });
  }

  /* 显式 for 关联的 label：重定向点击到触发按钮 */
  if (select.id) {
    document.querySelectorAll(`label[for="${CSS.escape(select.id)}"]`).forEach(l => {
      l.removeAttribute("for");
      l.addEventListener("click", e => { e.preventDefault(); toggle(); });
    });
  }

  renderOptions();
  btn.classList.toggle("disabled", select.disabled);
}

/** 全局启动：增强现有 select，并监听后续动态插入的 select */
export function watchSelects() {
  const scan = root => {
    if (root.querySelectorAll) root.querySelectorAll("select").forEach(enhanceSelect);
  };
  scan(document.body);
  new MutationObserver(muts => {
    for (const m of muts) {
      for (const n of m.addedNodes) {
        if (n.nodeType !== 1) continue;
        if (n.tagName === "SELECT") enhanceSelect(n);
        else scan(n);
      }
    }
  }).observe(document.body, { childList: true, subtree: true });
}
