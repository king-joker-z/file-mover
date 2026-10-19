/* ============================================================
 * pages/browse.js — 文件浏览（目标/源根目录视图）
 * ============================================================ */

import { api } from "../api.js";
import { esc, fmtSize } from "../utils.js";

let rootEl = null;
let rel = "", navRel = "", fsRoot = "", requestId = 0;

export function mount(root) {
  rootEl = root;
  root.innerHTML =
    `<div class="panel">
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap">
        <span id="browse-crumb" class="log-detail" style="flex:1;word-break:break-all"></span>
        <button class="btn small gray" type="button" id="browse-up">上级</button>
      </div>
      <div id="browse-error" class="status-warnings" role="alert"></div>
      <div id="browse-list"><div class="empty">加载目录中…</div></div>
    </div>`;
  root.querySelector("#browse-up").addEventListener("click", goUp);
}

export function show() { to(rel); }

export function to(targetRel) {
  const requested = targetRel || "", id = ++requestId;
  navRel = requested;
  rootEl.querySelector("#browse-error").textContent = "";
  api("/api/browse" + (requested ? "?rel=" + encodeURIComponent(requested) : "")).then(d => {
    if (id !== requestId) return;
    fsRoot = d.root;
    rel = d.rel || "";
    navRel = rel;
    renderCrumb();
    const box = rootEl.querySelector("#browse-list");
    if (!d.items.length) { box.innerHTML = '<div class="empty">空目录</div>'; return; }
    box.innerHTML = `<div class="table-wrap"><table style="min-width:420px"><tr><th>类型</th><th>名称</th><th>大小</th></tr>` +
      d.items.map(it => {
        const icon = it.type === "dir" ? "📁" : (it.type === "file" ? "📄" : "·");
        const childRel = (d.rel ? d.rel + "/" : "") + it.name;
        return `<tr ${it.type === "dir" ? `data-rel="${esc(encodeURIComponent(childRel))}" style="cursor:pointer"` : ""}>
          <td>${icon} ${it.type === "dir" ? "目录" : (it.type === "file" ? "文件" : "其他")}</td>
          <td style="word-break:break-all">${esc(it.name)}</td>
          <td>${it.type === "file" ? fmtSize(it.size) : "-"}</td></tr>`;
      }).join("") + "</table></div>";
    box.querySelectorAll("tr[data-rel]").forEach(tr =>
      tr.addEventListener("click", () => to(decodeURIComponent(tr.dataset.rel))));
  }).catch(e => {
    if (id !== requestId) return;
    const error = rootEl.querySelector("#browse-error");
    const attempted = joinPath(fsRoot || "/" + requested, requested);
    const current = joinPath(fsRoot, rel);
    error.innerHTML = "尝试加载：" + esc(attempted) + "<br>失败原因：" + esc(e.message) +
      "<br>当前仍在：" + esc(current) +
      ' <button type="button" class="btn small gray" data-action="retry">重试该目录</button>' +
      ' <button type="button" class="btn small gray" data-action="up">再上一级</button>';
    error.querySelector('[data-action="retry"]').addEventListener("click", () => to(requested));
    error.querySelector('[data-action="up"]').addEventListener("click", goUp);
  });
}

function joinPath(root, r) {
  return root.replace(/\/+$/, "") + (r ? "/" + r : (root === "/" ? "/" : ""));
}

function renderCrumb() {
  const parts = rel ? rel.split("/") : [];
  const links = [{ name: fsRoot, rel: "" }];
  parts.forEach((name, i) => links.push({ name, rel: parts.slice(0, i + 1).join("/") }));
  const crumb = rootEl.querySelector("#browse-crumb");
  crumb.innerHTML = "当前: " + links.map(x =>
    `<button type="button" class="btn small gray" data-crumb="${esc(encodeURIComponent(x.rel))}">${esc(x.name)}</button>`).join(" / ");
  crumb.querySelectorAll("button").forEach(b =>
    b.addEventListener("click", () => to(decodeURIComponent(b.dataset.crumb))));
}

function goUp() { to(navRel.split("/").slice(0, -1).join("/")); }
