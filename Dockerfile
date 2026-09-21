FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY homycloud.py honymqtt.py bridge.py ./
COPY certs/ ./certs/
# 项目命名定案（真实设备 ID，不进 git）；缺它则 NAME_OVERRIDES 为空、沿用 APP 房间名
COPY devices.json ./

CMD ["python", "bridge.py"]
