"""宏云智能云端 SDK（S5b：登录 + 查询部分）。

协议出处：docs/宏云APP接口文档.md 第〇部分（抓包逆向 + 实测验证）。
本文件是全部协议常量与算法的单点维护处（项目铁律 12）——固件/APP 升级导致接口变化时只改这里。

用法：
    from homycloud import HomyCloudClient
    with HomyCloudClient(phone, password) as c:
        c.login()
        families = c.list_families()
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger("homycloud")

# ============================================================
# 协议常量（单点维护）
# ============================================================
def _read_env_setting(name: str) -> str:
    """从环境变量或同目录 .env 读取配置值（容器用环境变量注入，本机用 .env）。"""
    value = os.environ.get(name, "").strip()
    if value:
        return value
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{name}="):
                value = line.split("=", 1)[1].strip().strip('"').strip("'")
                if value:
                    return value
    return ""


BASE_URL = "https://iot-api.homycloud.com"
ACCESS_KEY = "wogNpZDF"
# 以下两值为宏云 APK 内嵌/抓包所得的固定协议常量（非用户凭据），代码内置默认值；
# .env 可覆盖（一般无需覆盖）。
_DEFAULT_ACCESS_SECRET = "99fdac79244d46cdb6968d264f4a9adf93b9f6de"
_DEFAULT_REG_ID = "1507bfd3f69e38fde66"
ACCESS_SECRET = _read_env_setting("HOMY_ACCESS_SECRET") or _DEFAULT_ACCESS_SECRET
GRANT_TYPE = "appid"
USER_AGENT = "okhttp/4.10.0"
CONTENT_TYPE = "application/json; charset=UTF-8"

# 极光推送 registrationId（登录协议需要该字段占位）。SDK 无推送能力。
DEFAULT_REG_ID = _read_env_setting("HOMY_REG_ID") or _DEFAULT_REG_ID

# ------------------------------------------------------------
# MQTT（控制/状态通道）协议常量（S6：Frida 抓 8883 明文 + 实测破解）
# 出处：docs/宏云APP接口文档.md 第五部分。
# 关键约束（实测）：
#   - broker 走 TLS，8883；MQTT v3.1.1，cleanSession=1，keepalive=20
#   - clientId 必须精确等于 "homyapp_" + family.clientId（family.clientId 为
#     GET /app-api/family/simple 下发的家庭级字段，稳定不轮换）；
#     加后缀或换前缀一律 rc=5(Not authorized)
#   - 同一 clientId 全局单会话：重复登录触发标准"会话接管"，旧连接被踢。
#     故 HA 桥接与手机 APP 不能同时在线（S7 需处理）
#   - username = "{familyUuid}#{13位毫秒时间戳}#HmacSHA256#mode=2"
#   - password = HMAC-SHA256(key=user_secret, msg=username).hexdigest()
#     （user_secret 来自登录响应，稳定；离线核对可复现 APP 抓包密码）
MQTT_HOST = "iot-mqtt.homycloud.com"
MQTT_PORT = 8883
MQTT_KEEPALIVE = 20
MQTT_CLIENT_ID_PREFIX = "homyapp_"
MQTT_USERNAME_TEMPLATE = "{family_uuid}#{ts_ms}#HmacSHA256#mode=2"
MQTT_TOPIC_SUBSCRIBE = "homycloud/{family_uuid}/app/auto/subscribe"

# password 加密公钥：RSA-1024 + PKCS#1 v1.5（login_pub_1024.pem 内嵌，避免文件依赖）
RSA_PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCCyuMRsaJXsRs/IhUBsQeVU87q
pa/ndLmSE0zkEq48K7ADmk/5jlwpP/FLXNn6xCexGOp1MOeSEmOEa6OrLrxS3UI+
U8e/dJNoj+QAe2fQPMi73mZQv497P0Q2WaVeW6PSkuaTRSMDJCN2S8c/y+Y+5Vma
X6uNOA48uSI/WpXAzwIDAQAB
-----END PUBLIC KEY-----"""

# token 有效期 12h（登录响应 expiresIn=43200），提前 10 分钟视为过期
TOKEN_TTL_SECONDS = 43200
TOKEN_EXPIRE_MARGIN = 600

DEFAULT_TOKEN_PATH = Path.home() / ".homycloud" / "token.json"

_PUB_KEY = serialization.load_pem_public_key(RSA_PUBLIC_KEY_PEM)


# ============================================================
# 异常
# ============================================================
class HomyCloudError(Exception):
    """SDK 异常基类。"""


class ApiError(HomyCloudError):
    """云端返回 code != 0000。"""

    def __init__(self, code: str, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"[{code}] {msg}")


class AuthError(ApiError):
    """token 缺失/失效。"""


def _is_auth_failure(code: str, msg: str) -> bool:
    # 4005 = empty Token（已实测）；过期 token 的错误码未知，按 msg 兜底
    return code == "4005" or "token" in (msg or "").lower()


# ============================================================
# 协议算法
# ============================================================
def make_signature(method: str, path: str, body: str = "", timestamp: str | None = None) -> str:
    """HMAC-SHA256 请求签名（接口文档 0.3，smali 还原并实测通过）。

    - signParams 按 key ASCII 升序（等价 Java TreeMap）
    - signString = "METHOD&" + "k=v&" 逐项拼接，末尾 & 保留
    - 整串 URL 编码后再做 HMAC，输出 64 字符 hex
    """
    if timestamp is None:
        timestamp = str(int(time.time() * 1000))

    path_only = path.split("?", 1)[0]
    sign_params = {
        "GrantType": GRANT_TYPE,
        "AccessKey": ACCESS_KEY,
        "Timestamp": timestamp,
        "Path": path_only,
        "ContentMD5": hashlib.md5((body or "").encode("utf-8")).hexdigest(),
    }

    raw = method.upper() + "&"
    for k in sorted(sign_params):
        raw += f"{k}={sign_params[k]}&"

    # 对齐 Java URLEncoder.encode(UTF-8)：空格→%20、*→%2A、~保留
    encoded = urllib.parse.quote(raw, safe="")
    encoded = encoded.replace("+", "%20").replace("*", "%2A").replace("%7E", "~")

    return hmac.new(
        ACCESS_SECRET.encode("utf-8"), encoded.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def encrypt_password(plain: str) -> str:
    """明文密码 → RSA-1024 + PKCS#1 v1.5 → base64（接口文档 0.4）。"""
    enc = _PUB_KEY.encrypt(plain.encode("utf-8"), padding.PKCS1v15())
    return base64.b64encode(enc).decode()


# ============================================================
# 客户端
# ============================================================
class HomyCloudClient:
    """宏云云端 HTTP 客户端（同步 httpx，token 文件持久化）。

    - token 缓存在本地文件，重启不重登（方案决策 9）
    - token 失效时自动重登并重试一次；重试策略（退避等）由上层桥接负责（方案决策 11）
    """

    def __init__(
        self,
        phone: str,
        password: str,
        reg_id: str = DEFAULT_REG_ID,
        token_path: str | Path | None = DEFAULT_TOKEN_PATH,
        timeout: float = 15.0,
    ):
        self.phone = phone
        self.password = password
        self.reg_id = reg_id
        self.token_path = Path(token_path) if token_path is not None else None
        self.token: str | None = None
        self.user_secret: str | None = None
        self.user_uuid: str | None = None
        self._token_expires_at = 0.0
        self._http = httpx.Client(base_url=BASE_URL, timeout=timeout)
        self._load_token()

    # ---------- 会话管理 ----------
    def login(self, force: bool = False) -> None:
        """密码登录。已持有未过期 token 时跳过（force=True 强制重登）。"""
        if not force and self.token and time.time() < self._token_expires_at:
            return
        body = self._json_body(
            {
                "password": encrypt_password(self.password),
                "phone": self.phone,
                "registrationId": self.reg_id,
            }
        )
        data = self._request(
            "POST", "/app-api/user/center/accountLogin", body, with_token=False
        )
        self.token = data["token"]
        self.user_secret = data["userSecret"]
        self._token_expires_at = time.time() + int(data.get("expiresIn", TOKEN_TTL_SECONDS))
        self._save_token()
        logger.info("登录成功，token 有效期至 %s", time.strftime("%H:%M:%S", time.localtime(self._token_expires_at)))

    def logout(self) -> None:
        try:
            self._request("GET", "/app-api/user/center/exitLogin", allow_relogin=False)
        finally:
            self._clear_token()
            logger.info("已退出登录并清除本地 token")

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "HomyCloudClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ---------- 查询接口 ----------
    def get_user(self) -> dict:
        """用户信息（uuid / nickname / phone / userSecret）。"""
        return self._request("GET", "/app-api/user/center/getUserData")

    def list_families(self) -> list:
        """家庭列表。"""
        return self._request("GET", "/app-api/family/simple")

    def list_rooms(self, family_uuid: str) -> list:
        """房间列表。"""
        return self._request("GET", f"/app-api/family/house/getFamilyHouseList/{family_uuid}")

    def list_devices(self, family_uuid: str) -> list:
        """设备列表（含 template/deviceType/bindingGateway，无实时状态）。"""
        return self._request("GET", f"/app-api/family/device/list/{family_uuid}")

    def get_devices_online(self, family_uuid: str):
        """设备在线状态。"""
        return self._request("GET", f"/app-api/family/device/online/{family_uuid}")

    def get_device_property(self, device_id: str) -> dict | None:
        """设备最新状态快照（charles4 实测：键值对，含 IS_ONLINE_<familyUuid>）。

        CTD0001 暖通: mode/temp_set/temp_current/level(空调风速)/switch(空调开关)/
            switch_air(新风)/fan_speed_enum(新风风速)/switch_heating(地暖)/
            heating_temp_set_c/work_mode/type(AH|AN…)
        CLA0001 面板: switch_1..N/countdown_1..N/backlight_switch/relay_status_all
        """
        return self._request("GET", f"/app-api/family/device/property/last/{device_id}")

    def get_device_functions(self, family_uuid: str, device_id: str, view_type: str = "Control") -> dict:
        """设备能力定义（funcItems：funcCode/type/valueInfo/枚举值）。

        注意：该接口同时是 APP 的"浏览埋点"（POST section/device/view），调用会在云端留痕。
        """
        body = self._json_body({"deviceId": device_id, "viewType": view_type})
        return self._request("POST", f"/app-api/section/device/view/{family_uuid}", body)

    # ---------- MQTT（控制/状态通道）----------
    def build_mqtt_credentials(self, family_uuid: str, family_client_id: str) -> dict:
        """生成 MQTT CONNECT 凭据（S6 破解，S7 桥接建连用）。

        family_uuid / family_client_id 均来自 list_families() 运行时结果：
        family_uuid = 家庭 uuid，family_client_id = 家庭级 clientId 字段。
        user_secret 来自登录响应（本方法确保已登录）。

        返回 dict：host/port/keepalive/client_id/username/password/subscribe_topic。
        注意 clientId 必须精确等于 homyapp_{family_client_id}，任何改动都会被拒。
        """
        if not self.user_secret:
            self.login()
        ts_ms = int(time.time() * 1000)
        username = MQTT_USERNAME_TEMPLATE.format(family_uuid=family_uuid, ts_ms=ts_ms)
        password = hmac.new(
            self.user_secret.encode("utf-8"), username.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return {
            "host": MQTT_HOST,
            "port": MQTT_PORT,
            "keepalive": MQTT_KEEPALIVE,
            "client_id": MQTT_CLIENT_ID_PREFIX + family_client_id,
            "username": username,
            "password": password,
            "subscribe_topic": MQTT_TOPIC_SUBSCRIBE.format(family_uuid=family_uuid),
        }

    # ---------- 内部 ----------
    @staticmethod
    def _json_body(obj) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def _request(
        self,
        method: str,
        path: str,
        body: str | None = None,
        with_token: bool = True,
        allow_relogin: bool = True,
    ):
        if with_token and (self.token is None or time.time() >= self._token_expires_at):
            self.login(force=True)

        headers = {
            "granttype": GRANT_TYPE,
            "accesskey": ACCESS_KEY,
            "user-agent": USER_AGENT,
        }
        if body is not None:
            headers["content-type"] = CONTENT_TYPE
        if with_token and self.token:
            headers["token"] = self.token

        ts = str(int(time.time() * 1000))
        headers["timestamp"] = ts
        headers["signature"] = make_signature(method, path, body or "", ts)

        logger.debug("%s %s", method, path)
        resp = self._http.request(
            method,
            path,
            content=body.encode("utf-8") if body is not None else None,
            headers=headers,
        )
        resp.raise_for_status()

        try:
            payload = resp.json()
        except ValueError:
            raise HomyCloudError(f"非 JSON 响应: HTTP {resp.status_code}")

        code = payload.get("code")
        msg = payload.get("msg", "")
        if code == "0000":
            return payload.get("data")

        if allow_relogin and with_token and _is_auth_failure(code, msg):
            logger.warning("token 失效（code=%s msg=%s），自动重登后重试", code, msg)
            self.login(force=True)
            return self._request(method, path, body, with_token, allow_relogin=False)
        raise ApiError(code, msg)

    # ---------- token 文件持久化 ----------
    def _load_token(self) -> None:
        if self.token_path is None or not self.token_path.exists():
            return
        try:
            raw = json.loads(self.token_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("token 文件读取失败，忽略: %s", self.token_path)
            return
        if raw.get("phone") != self.phone:
            return
        self.token = raw.get("token")
        self.user_secret = raw.get("user_secret")
        self.user_uuid = raw.get("user_uuid")
        self._token_expires_at = float(raw.get("expires_at", 0))

    def _save_token(self) -> None:
        if self.token_path is None:
            return
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_path.write_text(
            json.dumps(
                {
                    "phone": self.phone,
                    "token": self.token,
                    "user_secret": self.user_secret,
                    "user_uuid": self.user_uuid,
                    "expires_at": self._token_expires_at,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _clear_token(self) -> None:
        self.token = None
        self.user_secret = None
        self.user_uuid = None
        self._token_expires_at = 0.0
        if self.token_path is not None and self.token_path.exists():
            self.token_path.unlink()
