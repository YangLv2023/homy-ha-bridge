"""宏云云端 MQTT 收发层（S7-2）。

协议常量单点维护在 homycloud.py（铁律 12）；本文件只管连接生命周期与消息分发。
关键约束（docs/宏云APP接口文档.md 第五部分）：
- 同 clientId 全局单会话：桥接在线时手机 APP 登录会把桥接踢下线（disconnect rc 变化）
- 高频重连触发 broker 软限流 rc=5：曾成功建连后的 rc=5 按软限流处理，退避 5 分钟
- clientId 必须精确 homyapp_{family.clientId}，改动即 rc=5

用法：
    client = HomyMqttClient(creds, on_message=lambda topic, payload: ...)
    client.start()
    client.wait_ready(timeout=30)
    ...
    client.stop()
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

logger = logging.getLogger("honymqtt")

# broker 使用自建私有 CA（homycloudCA，自签根，实测提取并固定于此）。
# 系统信任库不含该 CA，直接默认校验会 CERTIFICATE_VERIFY_FAILED。
BUNDLED_CA = Path(__file__).parent / "certs" / "homycloudCA.pem"

BACKOFF_INITIAL = 5.0
BACKOFF_MAX = 300.0
# S7-a 实测：互踢高频重连触发 broker 软限流，退避数分钟才能恢复。
# 2026-09-21 用户定案：退避 360s 起 + 随机抖动，避免每次卡在同一个固定点
THROTTLE_BACKOFF = 360.0

CONNACK_ERRORS = {
    1: "协议版本不支持",
    2: "clientId 被拒",
    3: "服务端不可用",
    4: "用户名/密码错误",
    5: "未授权（clientId 错配，或曾频繁重连触发软限流）",
}

# 控制下行（S7-a 实抓定案，docs/宏云APP接口文档.md 5.5）：
# topic = homycloud/{familyUuid}/app/property/set
# payload: type 必须是 newdevReport；data 单字段逐发；温度必须 JSON number
MQTT_TOPIC_SET = "homycloud/{family_uuid}/app/property/set"


def build_set_payload(client_id: str, device_id: str, data: dict) -> dict:
    """构造控制 payload（纯函数，便于 dry-run 验证）。

    client_id = creds["client_id"]（homyapp_{family.clientId}，云端按它识别发送方）。
    data 必须单字段：{field: value}，多字段请逐条调用。
    """
    if len(data) != 1:
        raise ValueError(f"控制 payload 单字段逐发，收到 {len(data)} 个: {list(data)}")
    ts_ms = int(time.time() * 1000)
    return {
        "dId": device_id,
        "data": data,
        "type": "newdevReport",
        "clientId": client_id,
        "code": 0,
        "deviceId": device_id,
        "msgId": f"{ts_ms}{random.randint(0, 9999999):07d}",
        "time": ts_ms,
    }


class HomyMqttClient:
    """云端 8883 MQTT 客户端：后台线程 + 指数退避重连 + 消息分发 + publish。

    - on_message(topic, payload)：payload 已尽力 JSON 解析（失败则传原始 bytes）
    - on_disconnect()：断线通知（含被 APP 接管互踢），供上层决定是否让位/告警
    - start() 非阻塞；wait_ready() 等首次 CONNACK 成功
    """

    def __init__(
        self,
        creds: dict,
        on_message=None,
        on_disconnect=None,
        ca_certs: str | Path | None = BUNDLED_CA,
    ):
        self._creds = creds
        self._ca_certs = str(ca_certs) if ca_certs is not None else None
        self._on_message_cb = on_message
        self._on_disconnect_cb = on_disconnect
        self._client: mqtt.Client | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._connected = threading.Event()
        self._backoff = BACKOFF_INITIAL
        self._ever_connected = False

    # ---------- 对外接口 ----------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            raise RuntimeError("HomyMqttClient 已在运行")
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="honymqtt", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._client:
            try:
                self._client.disconnect()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=10)

    def wait_ready(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout)

    def publish(self, topic: str, payload: dict | str, qos: int = 0) -> None:
        """下发云端消息（S7-6 控制走此口）。未连接时抛错，由上层决定排队或放弃。"""
        if not self._connected.is_set() or self._client is None:
            raise ConnectionError("MQTT 未连接，不能 publish")
        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        info = self._client.publish(topic, payload, qos=qos)
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise TimeoutError(f"publish 未确认: {topic}")

    def publish_set(self, device_id: str, data: dict, family_uuid: str) -> dict:
        """控制下行（S7-6）：按 5.5 定案构造 payload 并 PUBLISH，返回实际发出的 payload。

        data 必须单字段 {field: value}；值类型规则由调用方保证（温度必须 JSON number）。
        """
        payload = build_set_payload(self._creds["client_id"], device_id, data)
        topic = MQTT_TOPIC_SET.format(family_uuid=family_uuid)
        self.publish(topic, payload, qos=0)
        return payload

    # ---------- 内部：连接主循环 ----------
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._connect_and_loop()
            except Exception as e:
                logger.warning("MQTT 连接异常: %s", e)

            self._connected.clear()
            if self._stop.is_set():
                break

            delay = self._backoff
            self._backoff = min(self._backoff * 2, BACKOFF_MAX)
            if getattr(self, "_last_connack_rc", 0) == 5 and self._ever_connected:
                delay = max(delay, THROTTLE_BACKOFF)
                logger.warning("rc=5 且曾成功建连 → 按软限流处理，退避 %.0fs", delay)
            delay *= random.uniform(0.9, 1.1)  # 抖动，避免固定周期撞限流窗口
            logger.info("%.0fs 后重连", delay)
            if self._on_disconnect_cb:
                try:
                    self._on_disconnect_cb()
                except Exception:
                    logger.exception("on_disconnect 回调异常")
            self._stop.wait(delay)

    def _connect_and_loop(self) -> None:
        c = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1,
            client_id=self._creds["client_id"],
            clean_session=True,
        )
        c.tls_set(ca_certs=self._ca_certs)
        c.username_pw_set(self._creds["username"], self._creds["password"])
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.on_subscribe = self._on_subscribe
        self._client = c

        logger.info(
            "连接 %s:%s (clientId=%s)",
            self._creds["host"], self._creds["port"], self._creds["client_id"],
        )
        c.connect(self._creds["host"], self._creds["port"], self._creds["keepalive"])
        c.loop_forever()  # 断开后返回，由 _run 决定退避重连

    # ---------- paho 回调 ----------
    def _on_connect(self, client, userdata, flags, rc):
        self._last_connack_rc = rc
        if rc != 0:
            logger.error("CONNACK 失败 rc=%s (%s)", rc, CONNACK_ERRORS.get(rc, "未知"))
            try:
                client.disconnect()  # 让 loop_forever 返回，走退避
            except Exception:
                pass
            return
        logger.info("MQTT 已连接（CONNACK rc=0）")
        self._ever_connected = True
        self._backoff = BACKOFF_INITIAL
        client.subscribe(self._creds["subscribe_topic"], qos=0)

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        logger.info("订阅成功 topic=%s qos=%s", self._creds["subscribe_topic"], granted_qos)
        self._connected.set()

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc == 0:
            logger.info("MQTT 正常断开")
        else:
            logger.warning("MQTT 意外断开 rc=%s", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            payload = msg.payload
        logger.debug("recv %s: %s", msg.topic, payload)
        if self._on_message_cb:
            try:
                self._on_message_cb(msg.topic, payload)
            except Exception:
                logger.exception("on_message 回调异常")
