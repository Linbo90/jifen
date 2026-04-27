# 基于官方Python轻量镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖（避免sqlite3报错）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制全部代码
COPY . .

# 容器启动命令
CMD ["python", "bot.py"]
