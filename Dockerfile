FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV SPOOL_DIR=/var/lib/vlan-switch/spool
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py switch.py ./
COPY templates ./templates
COPY scripts ./scripts

# 暂存目录：docker-compose 用命名卷挂载到这里；
# 运行时若不存在会由 SpoolStore 自动创建，启动时自动清空。
RUN mkdir -p /var/lib/vlan-switch/spool

EXPOSE 8000
CMD ["python", "app.py"]
