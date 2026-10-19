/* ============================================================
 * dialogs.js — Promise 化的确认 / 输入对话框（替代原生 confirm/prompt）
 * 同时向 api.js 注册 401 认证弹窗。
 * ============================================================ */

import { openModal } from "./modal.js";
import { setAuthPrompt, setToken, getToken } from "../api.js";

/**
 * 确认对话框。
 * @returns {Promise<boolean>} 用户点击「确定」返回 true
 */
export function confirmDialog(message, { title = "请确认", okText = "确定", danger = false } = {}) {
  return new Promise(resolve => {
    const foot = document.createElement("div");
    foot.style.cssText = "display:flex;gap:10px;justify-content:flex-end;width:100%";
    foot.innerHTML =
      `<button type="button" class="btn gray" data-act="cancel">取消</button>` +
      `<button type="button" class="btn ${danger ? "red" : ""}" data-act="ok">${okText}</button>`;
    const modal = openModal({ title, body: `<div class="dialog-msg"></div>`, footer: foot, dialogClass: "dialog-card" });
    modal.body.querySelector(".dialog-msg").textContent = message;
    let settled = false;
    const done = value => { if (!settled) { settled = true; resolve(value); modal.close(); } };
    foot.querySelector('[data-act="ok"]').onclick = () => done(true);
    foot.querySelector('[data-act="cancel"]').onclick = () => done(false);
    const originClose = modal.close;
    modal.close = () => { done(false); originClose(); };
  });
}

/**
 * 输入对话框。
 * @returns {Promise<string|null>} 取消返回 null
 */
export function promptDialog(message, { title = "请输入", okText = "确定", value = "", placeholder = "" } = {}) {
  return new Promise(resolve => {
    const body = document.createElement("div");
    body.innerHTML =
      `<div class="dialog-msg"></div>` +
      `<input type="password" autocomplete="off" placeholder="${placeholder}" style="width:100%">`;
    body.querySelector(".dialog-msg").textContent = message;
    const input = body.querySelector("input");
    input.value = value;

    const foot = document.createElement("div");
    foot.style.cssText = "display:flex;gap:10px;justify-content:flex-end;width:100%";
    foot.innerHTML =
      `<button type="button" class="btn gray" data-act="cancel">取消</button>` +
      `<button type="button" class="btn" data-act="ok">${okText}</button>`;

    const modal = openModal({ title, body, footer: foot, dialogClass: "dialog-card" });
    let settled = false;
    const done = value => { if (!settled) { settled = true; resolve(value); modal.close(); } };
    foot.querySelector('[data-act="ok"]').onclick = () => done(input.value);
    foot.querySelector('[data-act="cancel"]').onclick = () => done(null);
    const originClose = modal.close;
    modal.close = () => { done(null); originClose(); };
    input.focus();
    input.select();
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { e.preventDefault(); done(input.value); }
    });
  });
}

// 401 时由 api.js 调用：录入 API Token 后写回 sessionStorage
setAuthPrompt(async () => {
  const value = await promptDialog(
    "该接口需要 API Token，请输入（仅保存在当前标签页；留空取消）",
    { title: "API Token", okText: "保存并重试", value: getToken() },
  );
  if (value === null || !value.trim()) return false;
  setToken(value);
  return true;
});
