/* ============================================================
 * pager.js — 通用分页控件（支持选择器或元素）
 * ============================================================ */

/**
 * 渲染分页到指定容器。
 * @param {string|Element} target 容器选择器或元素
 * @param {number} page     当前页（0 起）
 * @param {number} total    总条数
 * @param {number} pageSize 每页条数
 * @param {(page: number) => void} onChange 翻页回调
 */
export function renderPager(target, page, total, pageSize, onChange) {
  const box = typeof target === "string" ? document.querySelector(target) : target;
  if (!box) return;
  const pages = Math.ceil(total / pageSize);
  if (!total) { box.innerHTML = ""; return; }
  box.innerHTML =
    `<span>共 ${total.toLocaleString("zh-CN")} 条 · 第 ${page + 1} / ${pages} 页</span>` +
    `<button class="btn small gray" data-page="prev" ${page === 0 ? "disabled" : ""}>上一页</button>` +
    `<button class="btn small gray" data-page="next" ${page >= pages - 1 ? "disabled" : ""}>下一页</button>`;
  box.querySelector('[data-page="prev"]')?.addEventListener("click", () => onChange(page - 1));
  box.querySelector('[data-page="next"]')?.addEventListener("click", () => onChange(page + 1));
}
