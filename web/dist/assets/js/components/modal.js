/* ============================================================
 * modal.js — 通用弹窗生命周期
 * ============================================================ */

let openModals = [];

/**
 * 打开弹窗。
 * @param {object} opts
 *   title    标题（HTML，需调用方转义）
 *   body     正文 DOM 元素或 HTML 字符串
 *   footer   底部 DOM 元素（可选）
 *   width    卡片宽度（CSS 值，默认 520px）
 *   dialogClass 附加类名
 *   onClose  关闭回调
 *   static   true 时点遮罩不关闭
 * @returns {{el: HTMLElement, close: Function}} 弹窗根元素与关闭函数
 */
export function openModal(opts) {
  const {
    title = "", body = "", footer = null,
    width = "520px", dialogClass = "", onClose = null, static: isStatic = false,
  } = opts;

  const mask = document.createElement("div");
  mask.className = "modal-mask";
  mask.innerHTML =
    `<div class="modal-card ${dialogClass}" style="width:${width}" role="dialog" aria-modal="true">` +
    `<div class="modal-head"><h2>${title}</h2>` +
    `<button type="button" class="modal-close" aria-label="关闭">✕</button></div>` +
    `<div class="modal-body"></div>` +
    (footer ? `<div class="modal-foot"></div>` : "") +
    `</div>`;

  const card = mask.querySelector(".modal-card");
  const bodyEl = mask.querySelector(".modal-body");
  if (typeof body === "string") bodyEl.innerHTML = body;
  else bodyEl.appendChild(body);
  if (footer) {
    const footEl = mask.querySelector(".modal-foot");
    if (typeof footer === "string") footEl.innerHTML = footer;
    else footEl.appendChild(footer);
  }

  function close() {
    if (!mask.isConnected) return;
    mask.remove();
    openModals = openModals.filter(m => m !== entry);
    if (onClose) onClose();
    if (!openModals.length) document.removeEventListener("keydown", onKey);
  }

  function onKey(e) {
    if (e.key === "Escape") {
      const top = openModals[openModals.length - 1];
      if (top === entry) close();
    }
  }

  if (!isStatic) {
    mask.addEventListener("mousedown", e => { if (e.target === mask) close(); });
  }
  mask.querySelector(".modal-close").addEventListener("click", close);
  document.addEventListener("keydown", onKey);

  document.body.appendChild(mask);
  const entry = { mask, close };
  openModals.push(entry);
  return { el: card, body: bodyEl, close };
}
