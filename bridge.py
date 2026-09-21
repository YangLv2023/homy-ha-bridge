"""宏云 ⇄ Home Assistant 桥接进程（S7-3 骨架）。

启动流程（docs/HA桥接方案设计.md 定案架构）：
  1. 读 .env（账号 + 目标家庭）
  2. 登录云端（token 文件持久化，重启不重登）
  3. 全量设备/房间/在线列表 → 逐台 property/last 初始状态快照（设备断网也有云端最后值）
  4. 启动云端 MQTT 收发层（honymqtt，连接 8883 订阅 auto/subscribe）
  5. 常驻运行；Ctrl+C 优雅退出

S7-4 起补：view/get_response → 本地 MQTT 转发。
S7-7c 补（2026-09-21 用户修订决策 #17）：非温度字段 view 推送实测准确快速，即到即采信直接转发；
温度字段回推不稳定且可能与实际状态不一致，不采信回推值——温度操作后 30s/60s 各拉一次
property/last 校准；取消 30 秒固定轮询（避免频繁拉取对服务器造成压力/被限流）。
自检模式：python bridge.py --probe （启动完成后跑 35 秒自动退出）
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from homycloud import HomyCloudClient
from honymqtt import HomyMqttClient

logger = logging.getLogger("bridge")

LOCAL_STATE_TOPIC = "homy/state/{device_id}"
LOCAL_CMD_TOPIC = "homy/cmd/+"          # HA → 桥接：{"switch": true} / {"temp_set": 26.5}

# 温度必须 JSON number（S7-a 实测：字符串值被设备静默忽略）——这里兜底归一
TEMP_FIELDS = {"temp_set", "heating_temp_set_c"}

# 决策 #17（2026-09-21 用户修订）：温度字段 view 回推不稳定且可能与设备实际状态不一致，
# 不采信回推值——每次温度操作后 30s/60s 各拉一次 property/last 校准；
# 其余字段 view 推送实测准确快速，直接采信转发，不走 HTTP（避免频繁拉取被限流）
TEMP_PULL_DELAYS = (30.0, 60.0)

# 温度校准拉取连续失败达到该次数 → 标 unavailable 上抛（方案决策：连续 3 次失败标 unavailable）
PULL_FAIL_THRESHOLD = 3

# 低频心跳：定期拉一台设备保活 token + 兜底校准，取代已取消的 30s 固定轮询。
# 间隔可经 .env TOKEN_HEARTBEAT_SECONDS（秒）调整，默认 600
TOKEN_HEARTBEAT_SECONDS_DEFAULT = 600.0

# 项目命名定案（2026-09-20 用户拍板）：与 APP houseName 不一致处以 devices.json 为准
# devices.json 含真实设备 ID（随仓库上传，作克隆模板），缺失时回退用 APP 返回的房间/设备名
def load_devices_config() -> dict:
    p = Path(__file__).parent / "devices.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


DEVICES_CONFIG = load_devices_config()
NAME_OVERRIDES = DEVICES_CONFIG.get("name_overrides", {})


ENV_KEYS = ("PHONE", "PASSWORD", "FAMILY_NAME",
            "LOCAL_MQTT_HOST", "LOCAL_MQTT_PORT", "TOKEN_HEARTBEAT_SECONDS",
            "MQTT_USER", "MQTT_PASS")


def load_env() -> dict:
    """本机运行读 .env 文件；容器部署 env_file 注入环境变量，环境变量优先。"""
    env = {}
    p = Path(__file__).parent / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    for k in ENV_KEYS:
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


class LocalMqtt:
    """本地 MQTT 发布端（mosquitto，HA 订阅端）。断线自动重连，发布带 retain。"""

    def __init__(self, host: str, port: int, on_command=None,
                 username: str | None = None, password: str | None = None):
        self._host = host
        self._port = port
        self._on_command_cb = on_command
        self._connected = threading.Event()
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="homy-bridge")
        if username:
            # broker 开启认证后（allow_anonymous false）必须凭账号登录
            c.username_pw_set(username, password or "")
        c.reconnect_delay_set(min_delay=5, max_delay=60)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        self._client = c

    def start(self) -> None:
        self._client.connect_async(self._host, self._port, keepalive=60)
        self._client.loop_start()

    def publish(self, topic: str, payload: dict, retain: bool = False) -> None:
        self._client.publish(
            topic, json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            qos=0, retain=retain,
        )

    def set_command_handler(self, cb) -> None:
        self._on_command_cb = cb

    def stop(self) -> None:
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()

    def wait_ready(self, timeout: float = 15.0) -> bool:
        return self._connected.wait(timeout)

    def publish_state(self, payload: dict) -> None:
        self._client.publish(
            LOCAL_STATE_TOPIC.format(device_id=payload["device_id"]),
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            qos=0,
            retain=True,
        )

    def _on_connect(self, client, userdata, flags, rc):
        logger.info("本地 MQTT 已连接 %s:%s", self._host, self._port)
        self._connected.set()
        client.subscribe(LOCAL_CMD_TOPIC, qos=0)

    def _on_message(self, client, userdata, msg):
        if self._on_command_cb is None or not msg.topic.startswith("homy/cmd/"):
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            logger.warning("本地命令 payload 非 JSON，忽略: %s", msg.topic)
            return
        try:
            self._on_command_cb(msg.topic, payload)
        except Exception:
            logger.exception("命令处理异常: %s", msg.topic)

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc != 0:
            logger.warning("本地 MQTT 断开 rc=%s，自动重连中", rc)


class BridgeState:
    """全设备状态缓存（桥接的单一事实源，供上行转发/轮询/控制共用）。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._devices: dict[str, dict] = {}   # deviceId → 元数据（name/room/template）
        self._props: dict[str, dict] = {}     # deviceId → 最新属性
        self._online: set[str] = set()
        self._unavailable: set[str] = set()   # 轮询连续失败标脏（恢复后清除）

    def register(self, device_id: str, meta: dict) -> None:
        with self._lock:
            self._devices[device_id] = meta

    def apply_snapshot(self, device_id: str, props: dict) -> None:
        with self._lock:
            self._props[device_id] = dict(props)

    def apply_view(self, device_id: str, props: dict) -> list[str]:
        """合并 view 推送，返回发生变化的字段名列表。"""
        with self._lock:
            cur = self._props.setdefault(device_id, {})
            changed = [k for k, v in props.items() if cur.get(k) != v]
            cur.update(props)
            return changed

    def set_online(self, online_ids: list[str]) -> None:
        with self._lock:
            self._online = set(online_ids)

    def is_online(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._online

    def set_unavailable(self, device_id: str, unavailable: bool) -> bool:
        """标/清 unavailable，返回状态是否发生变化。"""
        with self._lock:
            was = device_id in self._unavailable
            if unavailable:
                self._unavailable.add(device_id)
            else:
                self._unavailable.discard(device_id)
            return was != unavailable

    def is_unavailable(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._unavailable

    def device_ids(self) -> list[str]:
        with self._lock:
            return list(self._devices)

    def meta(self, device_id: str) -> dict | None:
        with self._lock:
            return self._devices.get(device_id)

    def props_of(self, device_id: str) -> dict:
        with self._lock:
            return dict(self._props.get(device_id, {}))

    def summary(self) -> str:
        with self._lock:
            lines = []
            for did, meta in self._devices.items():
                lines.append(
                    f"  [{'在线' if did in self._online else '离线'}] "
                    f"{meta['room']:<4} {meta['name']:<8} {meta['template']} "
                    f"{did[:8]}… props={len(self._props.get(did, {}))}"
                )
            return "\n".join(lines)


def build_initial_snapshot(cloud: HomyCloudClient, state: BridgeState, family_uuid: str) -> None:
    rooms = {r["uuid"]: r["name"] for r in (cloud.list_rooms(family_uuid) or [])}
    devices = cloud.list_devices(family_uuid) or []
    online = cloud.get_devices_online(family_uuid) or []
    if isinstance(online, dict):
        online = online.get("deviceIds") or []
    state.set_online(online)

    for d in devices:
        did = d["deviceId"]
        ov = NAME_OVERRIDES.get(did, {})
        state.register(did, {
            "room": ov.get("room") or d.get("houseName") or rooms.get(d.get("houseUuid"), "未分房"),
            "name": ov.get("name") or d.get("deviceName") or did[:8],
            "template": d.get("template", "?"),
        })

    for d in devices:
        did = d["deviceId"]
        try:
            snap = cloud.get_device_property(did) or {}
        except Exception as e:
            logger.warning("快照失败 %s: %s", did[:8], e)
            snap = {}
        state.apply_snapshot(did, snap)


def unwrap_view_payload(payload: dict) -> tuple[str, dict] | None:
    """云端 view/get_response payload → (deviceId, {field: value})。"""
    data = payload.get("data")
    did = payload.get("deviceId")
    if not isinstance(data, dict) or not did:
        return None
    flat = {}
    for k, v in data.items():
        if isinstance(v, dict) and "value" in v:
            flat[k] = v["value"]
        else:
            flat[k] = v
    return did, flat


def heartbeat_loop(cloud: HomyCloudClient, state: BridgeState, bridge: Bridge,
                   stop_event: threading.Event,
                   interval: float = TOKEN_HEARTBEAT_SECONDS_DEFAULT) -> None:
    """低频心跳线程：定期拉一台设备的 property/last，保活 token + 兜底校准。

    30s 固定轮询已取消（用户修订：避免拉取压力）；默认 10 分钟一次频率极低，
    间隔经 .env TOKEN_HEARTBEAT_SECONDS（秒）动态调整。
    """
    while not stop_event.wait(interval):
        dids = state.device_ids()
        if not dids:
            continue
        did = next(
            (d for d in dids if (m := state.meta(d)) and m["template"] == "CGW0001"),
            dids[0],
        )
        try:
            snap = cloud.get_device_property(did) or {}
        except Exception as e:
            logger.warning("心跳失败 %s（不影响运行）: %s", did[:8], e)
            continue
        changed = state.apply_view(did, snap)
        if changed:
            logger.info("心跳校准 %s: 变更 %s", did[:8], changed)
            bridge.publish_device(did)


class TempPullScheduler:
    """温度操作后的固定 HTTP 校准拉取（2026-09-21 用户修订决策 #17）。

    温度字段回推不可信：不把回推值写入状态，只在温度事件后排程 30s/60s 两次
    property/last 拉取校准。同一设备连续温度操作会重置排程（以最后一次为准）。
    连续 PULL_FAIL_THRESHOLD 次拉取失败 → 标 unavailable；任一次成功自动清除。
    """

    def __init__(self, cloud: HomyCloudClient, state: BridgeState, bridge: Bridge,
                 delays: tuple = TEMP_PULL_DELAYS):
        self._cloud = cloud
        self._state = state
        self._bridge = bridge
        self._delays = delays
        self._lock = threading.Lock()
        self._timers: dict[str, list[threading.Timer]] = {}
        self._fail_counts: dict[str, int] = {}

    def request(self, device_id: str) -> None:
        with self._lock:
            for old in self._timers.get(device_id, []):
                old.cancel()
            timers = []
            for delay in self._delays:
                t = threading.Timer(delay, self._pull, args=(device_id,))
                t.daemon = True
                timers.append(t)
            self._timers[device_id] = timers
            for t in timers:
                t.start()
        logger.info("温度校准已排程 %s: +%ss/+%ss 各拉一次", device_id[:8], *self._delays)

    def _pull(self, device_id: str) -> None:
        try:
            snap = self._cloud.get_device_property(device_id) or {}
        except Exception as e:
            self._fail_counts[device_id] = self._fail_counts.get(device_id, 0) + 1
            logger.warning(
                "温度校准拉取失败 %s（连续 %d 次）: %s",
                device_id[:8], self._fail_counts[device_id], e,
            )
            if (self._fail_counts[device_id] >= PULL_FAIL_THRESHOLD
                    and self._state.set_unavailable(device_id, True)):
                logger.error("连续 %d 次失败，标 unavailable: %s", PULL_FAIL_THRESHOLD, device_id[:8])
                self._bridge.publish_device(device_id)
            return
        self._fail_counts.pop(device_id, None)
        recovered = self._state.set_unavailable(device_id, False)
        if recovered:
            logger.info("温度校准恢复，清除 unavailable: %s", device_id[:8])
        changed = self._state.apply_view(device_id, snap)
        if recovered or changed:
            logger.info("温度校准 %s: 变更 %s", device_id[:8], changed)
            self._bridge.publish_device(device_id)

    def stop(self) -> None:
        with self._lock:
            for timers in self._timers.values():
                for t in timers:
                    t.cancel()
            self._timers.clear()


class Bridge:
    """装配：云端 MQTT 消息 → 状态缓存 → 本地 MQTT 转发；本地命令 → 云端 set。"""

    def __init__(self, state: BridgeState, local: LocalMqtt):
        self.state = state
        self.local = local
        self.cloud_mqtt: HomyMqttClient | None = None   # main() 里接线
        self.temp_puller: TempPullScheduler | None = None  # main() 里接线
        self.family_uuid: str | None = None
        self.dry_run = False                            # probe 模式：只构造不发送

    def publish_device(self, device_id: str) -> None:
        meta = self.state.meta(device_id)
        if meta is None:
            return
        self.local.publish_state({
            "device_id": device_id,
            "name": meta["name"],
            "room": meta["room"],
            "template": meta["template"],
            "online": self.state.is_online(device_id),
            "unavailable": self.state.is_unavailable(device_id),
            "props": self.state.props_of(device_id),
            "ts": int(time.time() * 1000),
        })

    def publish_all(self) -> None:
        for did in self.state.device_ids():
            self.publish_device(did)

    def on_cloud_message(self, topic: str, payload) -> None:
        if not isinstance(payload, dict):
            return
        if topic.endswith("/app/property/view") or topic.endswith("/app/property/get_response"):
            parsed = unwrap_view_payload(payload)
            if parsed is None:
                return
            did, flat = parsed
            # 决策 #17（用户修订）：温度字段回推不可信，不写入状态，只排程校准拉取；
            # 其余字段实测准确快速，即到即采信直接转发
            temp_part = {k: v for k, v in flat.items() if k in TEMP_FIELDS}
            real_part = {k: v for k, v in flat.items() if k not in TEMP_FIELDS}
            if real_part:
                changed = self.state.apply_view(did, real_part)
                if changed:
                    logger.info("view 实时推送 %s: %s", did[:8], changed)
                    self.publish_device(did)
            if temp_part and self.temp_puller:
                self.temp_puller.request(did)
        elif topic.endswith("/app/status/view"):
            ids = payload.get("data", {}).get("deviceIds")
            if isinstance(ids, list):
                self.state.set_online(ids)
                logger.info("在线列表更新: %d 台在线", len(ids))
                self.publish_all()

    # ---------- 控制下行（S7-6）----------
    def on_local_command(self, topic: str, payload: dict) -> None:
        """homy/cmd/{deviceId} {"field": value, ...} → 逐字段云端 set（5.5 单字段逐发）。"""
        if not isinstance(payload, dict) or not payload:
            logger.warning("命令 payload 需为非空 JSON 对象，忽略: %s", topic)
            return
        device_id = topic.split("/")[-1]
        if self.state.meta(device_id) is None:
            logger.warning("命令目标设备未注册，忽略: %s", device_id)
            return
        for field, value in payload.items():
            if field in TEMP_FIELDS:
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    logger.warning("命令 %s.%s 温度值非法（需数字）: %r，忽略", device_id[:8], field, value)
                    continue
            if self.dry_run:
                logger.info("[dry-run] set %s: %s=%r", device_id[:8], field, value)
                continue
            try:
                sent = self.cloud_mqtt.publish_set(device_id, {field: value}, self.family_uuid)
                logger.info("已下发 set %s: %s=%r (msgId=%s)", device_id[:8], field, value, sent["msgId"])
                if field in TEMP_FIELDS and self.temp_puller:
                    # 决策 #17（用户修订）：温度 set 无可靠回推，30s/60s 后拉 property/last 校准；
                    # 其余字段 view 回推准确快速，无需再拉
                    self.temp_puller.request(device_id)
            except Exception as e:
                logger.error("下发失败 %s.%s: %s", device_id[:8], field, e)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    env = load_env()

    cloud = HomyCloudClient(env["PHONE"], env["PASSWORD"])
    cloud.login()
    logger.info("云端登录 OK（user=%s）", cloud.user_uuid)

    # 家庭信息运行时解析：.env 只配 FAMILY_NAME，uuid/clientId 从 family/simple 现查
    families = cloud.list_families() or []
    target = next((f for f in families if f.get("familyName") == env["FAMILY_NAME"]), None)
    if target is None:
        logger.error(
            "家庭 %r 不在账号家庭列表中（现有: %s），退出",
            env["FAMILY_NAME"], [f.get("familyName") for f in families],
        )
        sys.exit(1)
    family_uuid = target["familyUuid"]
    family_client_id = target["clientId"]
    logger.info("目标家庭: %s (uuid=%s, clientId=%s)", env["FAMILY_NAME"], family_uuid, family_client_id)

    state = BridgeState()
    build_initial_snapshot(cloud, state, family_uuid)
    logger.info("初始快照完成，共 %d 台：\n%s", len(state.device_ids()), state.summary())

    local = LocalMqtt(
        env.get("LOCAL_MQTT_HOST", "127.0.0.1"), int(env.get("LOCAL_MQTT_PORT", "1883")),
        username=env.get("MQTT_USER") or None, password=env.get("MQTT_PASS") or None,
    )
    bridge = Bridge(state, local)
    temp_puller = TempPullScheduler(cloud, state, bridge)
    bridge.temp_puller = temp_puller
    local.set_command_handler(bridge.on_local_command)
    local.start()
    if not local.wait_ready(timeout=15):
        logger.error("本地 MQTT 15s 未连接（docker compose up -d 起 mosquitto），退出")
        sys.exit(1)

    bridge.family_uuid = family_uuid
    creds = cloud.build_mqtt_credentials(family_uuid, family_client_id)
    mqtt_cli = HomyMqttClient(creds, on_message=bridge.on_cloud_message)
    bridge.cloud_mqtt = mqtt_cli
    mqtt_cli.start()
    if not mqtt_cli.wait_ready(timeout=30):
        logger.error("云端 MQTT 30s 未建连，退出")
        sys.exit(1)

    bridge.publish_all()
    logger.info("桥接就绪：初始状态已发布本地（homy/state/#，retained）")

    stop_event = threading.Event()
    try:
        heartbeat_interval = float(env.get("TOKEN_HEARTBEAT_SECONDS", TOKEN_HEARTBEAT_SECONDS_DEFAULT))
    except ValueError:
        logger.warning("TOKEN_HEARTBEAT_SECONDS 配置非法，使用默认 %.0fs", TOKEN_HEARTBEAT_SECONDS_DEFAULT)
        heartbeat_interval = TOKEN_HEARTBEAT_SECONDS_DEFAULT
    logger.info("心跳间隔 %.0fs（.env TOKEN_HEARTBEAT_SECONDS 可调）", heartbeat_interval)
    threading.Thread(
        target=heartbeat_loop, args=(cloud, state, bridge, stop_event, heartbeat_interval),
        name="homy-heartbeat", daemon=True,
    ).start()

    try:
        if "--probe" in sys.argv:
            # 断网自检：本地注入一条伪造 view（不写云端），验证 view→直接采信→转发管线
            time.sleep(3)
            fake = {
                "code": 0,
                "deviceId": DEVICES_CONFIG.get("gateway_device_id", ""),
                "data": {"ip_addr": {"time": int(time.time() * 1000), "value": f"10.0.{int(time.time()) % 256}.{int(time.time() * 10) % 256}"}},
            }
            bridge.on_cloud_message(
                f"homycloud/{family_uuid}/app/property/view", fake
            )
            logger.info("probe：已注入伪造 view，观察上方 view 变更日志")
            # S7-6 自检：本地命令 → set 构造（dry-run，不真正下发云端）
            bridge.dry_run = True
            hvac = DEVICES_CONFIG.get("probe_hvac_device_id", "")
            if not hvac:
                logger.warning("probe：devices.json 未配置 probe_hvac_device_id，跳过命令注入")
            else:
                local.publish(f"homy/cmd/{hvac}", {"temp_set": "26.5", "switch": True})
                logger.info("probe：已注入本地命令（dry-run），观察 set 构造日志")
            time.sleep(35)
        else:
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("收到退出信号")
    finally:
        stop_event.set()
        temp_puller.stop()
        mqtt_cli.stop()
        local.stop()
        cloud.close()
        logger.info("已退出")


if __name__ == "__main__":
    main()
