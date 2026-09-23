FROM python:3.12-slim

# 国内服务器（九章=百度云）直连 pypi.org 下载大包会超时（cryptography 4.7MB 卡死实测 2 次），
# 默认走清华镜像；海外构建可用 --build-arg PIP_INDEX_URL=https://pypi.org/simple 覆盖
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY homycloud.py honymqtt.py hadiscovery.py bridge.py ./
COPY certs/ ./certs/
# 命名与面板键位定案（2026-09-22 用户定案：随仓库上传作克隆模板）；缺它则 rooms/name_overrides/channels 全空
COPY devices.json ./

CMD ["python", "bridge.py"]
