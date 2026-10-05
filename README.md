# File Mover

Docker 部署的本地文件定期迁移工具：按可配置间隔逐个把本地文件迁移到远程网盘挂载目录，带 Web UI。

详细设计文档见仓库上层的 `file-mover-方案文档.md`。

## GitHub Actions 自动构建发布到 Docker Hub

仓库已含 `.github/workflows/docker.yml`：push 到 main/master（或打 `v*` tag）时自动构建 **amd64 + arm64** 双架构镜像并推送 Docker Hub。

### 一次性配置

1. GitHub 仓库 → Settings → Secrets and variables → Actions，添加两个 Secret：
   - `DOCKERHUB_USERNAME`：你的 Docker Hub 用户名
   - `DOCKERHUB_TOKEN`：Docker Hub → Account Settings → Security → New Access Token 生成
2. 如镜像名不是 `<用户名>/file-mover`，把 workflow 里 `env.IMAGE_NAME` 改成目标仓库（或增加 `DOCKERHUB_REPO` secret 引用）
3. push 代码后 Actions 自动构建；发版打 tag：`git tag v1.0.0 && git push --tags` 会额外产出 `1.0.0` 版本 tag

### 用 Docker Hub 镜像部署

```yaml
services:
  file-mover:
    image: <你的用户名>/file-mover:latest
    ...  # 其余同上
```

## 快速开始

```bash
cd file-mover
# 1. 修改 docker-compose.yml 中的两个挂载路径
#    /path/to/local  → 你的本地源目录
#    /path/to/cloud-mount → 网盘挂载目录
# 2. 启动
docker compose up -d --build
```

打开 `http://localhost:8787`：

1. 「任务管理」→ 新建任务：填源目录 `/data/local`、目标目录 `/data/cloud`、文件间隔（秒）
2. 保存后扫描器自动发现文件并逐个迁移

## 使用说明

- **文件间隔**：每迁移一个文件后的等待秒数，支持小数（如 0.5）
- **同名冲突**：目标已有同名文件时按策略处理（默认跳过并在队列中标 conflict）
- **目录行为**：以文件为单位迁移；目标已有同名目录时直接并入，不新建文件夹
- **安全模式**：设置中开启后走 copy→校验→删源，适合不稳定的网盘挂载
- **失败处理**：自动指数退避重试，超限标 failed，UI 可手动/批量重试
- **配置热更新**：所有修改下一周期生效，无需重启容器

## 本地开发（不依赖 Docker）

```bash
pip install -r requirements.txt
FILEMOVER_DB=/tmp/fm.db FILEMOVER_CONFIG=/tmp/fm.json \
FILEMOVER_WEB=./web/dist uvicorn app.main:app --port 8787
```

## 目录结构

```
app/
  main.py       FastAPI 入口 + REST API
  config.py     全局设置（settings.json，热更新）
  db.py         SQLite（tasks/queue/logs）
  scanner.py    目录扫描 + 稳定性检测 + 入队
  scheduler.py  扫描循环 + 迁移主循环（节流）
  transfer.py   单文件迁移（move 或 copy+校验+删源）+ 冲突策略
web/dist/       单文件前端（无构建步骤）
```
