/* ============================================================
 * dirPicker.js — 目录树选择弹窗（懒展开，Promise 化）
 * ============================================================ */

import { api } from "../api.js";
import { openModal } from "./modal.js";
import { toast, esc } from "../utils.js";

/**
 * 打开目录选择弹窗。
 * @returns {Promise<string|null>} 确认返回绝对路径；取消返回 null
 */
export function pickDirectory() {
  return new Promise(resolve => {
    let selRel = "", selAbs = "";
    let requestId = 0;
    let settled = false;

    const foot = document.createElement("div");
    foot.style.cssText = "display:flex;gap:10px;justify-content:flex-end;width:100%;align-items:center";
    foot.innerHTML =
      `<span class="log-detail" data-sel style="flex:1;min-height:16px;word-break:break-all"></span>` +
      `<button type="button" class="btn gray" data-act="cancel">取消</button>` +
      `<button type="button" class="btn" data-act="ok">使用选中目录</button>`;

    const body = document.createElement("div");
    body.innerHTML =
      `<div class="log-detail" style="margin-bottom:8px">点 <b>▸</b> 展开子层级，点击目录名选中</div>` +
      `<div class="dir-tree" data-tree><div class="empty">加载中…</div></div>`;

    const modal = openModal({ title: "选择目录", body, footer: foot, width: "480px",
      onClose: () => done(null) });
    const tree = modal.body.querySelector("[data-tree]");
    const selLabel = foot.querySelector("[data-sel]");

    const done = value => {
      if (settled) return;
      settled = true;
      resolve(value);
      modal.close();
    };

    foot.querySelector('[data-act="ok"]').onclick = () => {
      if (!selRel) { toast("请先点击一个目录选中"); return; }
      if (!selAbs) { toast("目录路径尚未加载完成，请稍后重试"); return; }
      done(selAbs);
    };
    foot.querySelector('[data-act="cancel"]').onclick = () => done(null);

    function markSelected(row, rel) {
      const id = ++requestId;
      selRel = rel; selAbs = "";
      tree.querySelectorAll(".dir-row.sel").forEach(x => x.classList.remove("sel"));
      row.classList.add("sel");
      selLabel.textContent = "已选: /" + rel;
      // 预取绝对路径用于回填
      api("/api/dirs?rel=" + encodeURIComponent(rel)).then(d => {
        if (id !== requestId || selRel !== rel || settled) return;
        selAbs = d.path;
        selLabel.textContent = "已选: " + d.path;
      }).catch(e => {
        if (id === requestId) selLabel.textContent = "目录读取失败: " + e.message;
      });
    }

    function makeNode(rel, name) {
      const wrap = document.createElement("div");
      const row = document.createElement("div");
      row.className = "dir-row";
      const arrow = document.createElement("span");
      arrow.className = "arrow";
      arrow.textContent = "▸";
      const label = document.createElement("span");
      label.className = "label";
      label.innerHTML = "📁 " + esc(name);
      row.appendChild(arrow);
      row.appendChild(label);

      const childBox = document.createElement("div");
      childBox.className = "dir-children";
      childBox.style.display = "none";

      let loaded = false, expanded = false;
      async function toggle() {
        expanded = !expanded;
        arrow.textContent = expanded ? "▾" : "▸";
        childBox.style.display = expanded ? "" : "none";
        if (expanded && !loaded) {
          loaded = true;
          childBox.innerHTML = '<div class="log-detail" style="padding:4px 8px">加载中…</div>';
          try {
            const d = await api("/api/dirs?rel=" + encodeURIComponent(rel));
            childBox.innerHTML = "";
            if (!d.dirs.length) {
              childBox.innerHTML = '<div class="log-detail" style="padding:4px 8px">（无子目录）</div>';
            }
            d.dirs.forEach(n => childBox.appendChild(makeNode(d.rel ? d.rel + "/" + n : n, n)));
          } catch (_) {
            loaded = false;
            childBox.innerHTML = '<div class="log-detail" style="padding:4px 8px">加载失败，点箭头重试</div>';
          }
        }
      }
      arrow.onclick = ev => { ev.stopPropagation(); toggle(); };
      row.onclick = () => markSelected(row, rel);

      wrap.appendChild(row);
      wrap.appendChild(childBox);
      return wrap;
    }

    api("/api/dirs").then(d => {
      if (settled) return;
      tree.innerHTML = "";
      d.dirs.forEach(n => tree.appendChild(makeNode(d.rel ? d.rel + "/" + n : n, n)));
    }).catch(e => {
      if (!settled) tree.innerHTML = '<div class="empty">加载失败: ' + esc(e.message) + "</div>";
    });
  });
}
