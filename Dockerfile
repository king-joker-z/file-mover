FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY web/dist/ ./web/dist/

# 兜底：确保静态资源目录存在，避免任何代码路径因缺目录而崩溃
RUN mkdir -p /app/web/dist/assets

# 运行时以宿主机 PUID/PGID 指定用户可选；默认 root 保证跨挂载权限
ENV FILEMOVER_DB=/app/config/app.db \
    FILEMOVER_CONFIG=/app/config/settings.json

EXPOSE 8787

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8787"]
