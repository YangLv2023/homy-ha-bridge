"""HA MQTT discovery 报文生成（S8-2）。

定案见 docs/HA实体映射定义.md（命名/设备/区域/灯组规划）与《任务进度》S8 拷问记录：
- 状态：复用 homy/state/{deviceId} 整设备 JSON（Q1-A），value_template 取字段
- 命令：homy/cmd/{deviceId}/{suffix}，桥接翻译 HA 各平台 payload（Q2-B）
- 联动组只由主面板出实体（_linked_primary）；镜像面板（_mirror_only）与网关不出实体
- availability：online==false 或 unavailable==true → HA「不可用」（Q6-A）
"""

from __future__ import annotations

import json
import re

from pypinyin import lazy_pinyin

DISCOVERY_PREFIX = "homeassistant"


def _object_id(name: str) -> str:
    """中文名 → 拼音 slug，作 HA entity_id 主体（定案：自动拼音、无 homy_ 前缀）。

    entity 显示名唯一（见《HA实体映射定义》§一命名规则），故 slug 亦唯一。
    """
    s = re.sub(r"[^a-z0-9]+", "_", "_".join(lazy_pinyin(name)).lower()).strip("_")
    return s or "homy"


# 面板布尔在 property/last 里可能是 "true"/"false" 字符串（S7-4 观察），模板统一归一
def _bool_tpl(field: str, on: str = "ON", off: str = "OFF") -> str:
    return f"{{{{ 'ON' if value_json.props.{field} in [true,'true'] else 'OFF' }}}}"

AVAILABILITY_ONLINE_TPL = (
    "{{ 'online' if value_json.online and not value_json.unavailable else 'offline' }}"
)

# 空调 mode 枚举 → HA hvac_mode（命令方向的反映射在 bridge.py 命令翻译层）
HVAC_MODE_OUT = {"cold": "cool", "hot": "heat", "auto": "auto", "dry": "dry", "wind": "fan_only"}
HVAC_MODE_IN = {v: k for k, v in HVAC_MODE_OUT.items()}

AC_MODE_TPL = (
    "{% set p = value_json.props %}{% set m = "
    + json.dumps(HVAC_MODE_OUT, ensure_ascii=False)
    + " %}"
    "{{ 'off' if p.switch in [false,'false'] else m.get(p.mode, 'auto') }}"
)


def _availability(did: str) -> dict:
    return {
        "availability_topic": f"homy/state/{did}",
        "availability_template": AVAILABILITY_ONLINE_TPL,
    }


def _temperature_tpl(field: str) -> str:
    return (
        "{% if '" + field + "' in value_json.props %}"
        "{{ value_json.props." + field + " | float(0) }}"
        "{% else %}0{% endif %}"
    )


def _dev(identifiers: str, name: str, area: str, model: str) -> dict:
    return {
        "identifiers": [identifiers],
        "name": name,
        "suggested_area": area,
        "manufacturer": "宏云 HomY",
        "model": model,
    }


def _config(platform: str, node_id: str, object_id: str, payload: dict) -> tuple[str, dict]:
    topic = f"{DISCOVERY_PREFIX}/{platform}/{node_id}/{object_id}/config"
    return topic, payload


def _light_config(did: str, field: str, name: str, dev: dict) -> tuple[str, dict]:
    payload = {
        "name": name,
        "unique_id": f"{did}_{field}",
        "object_id": _object_id(name),
        "state_topic": f"homy/state/{did}",
        "state_value_template": _bool_tpl(field),
        "command_topic": f"homy/cmd/{did}/light_{field.split('_')[1]}",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device": dev,
        **_availability(did),
    }
    return _config("light", "homy", f"{did}_{field}", payload)


def _fan_bool_config(did: str, field: str, name: str, dev: dict) -> tuple[str, dict]:
    payload = {
        "name": name,
        "unique_id": f"{did}_{field}",
        "object_id": _object_id(name),
        "state_topic": f"homy/state/{did}",
        "state_value_template": _bool_tpl(field),
        "command_topic": f"homy/cmd/{did}/fan_{field.split('_')[1]}",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device": dev,
        **_availability(did),
    }
    return _config("fan", "homy", f"{did}_{field}", payload)


def _fan_air_config(did: str, name: str, dev: dict) -> tuple[str, dict]:
    payload = {
        "name": name,
        "unique_id": f"{did}_switch_air",
        "object_id": _object_id(name),
        "state_topic": f"homy/state/{did}",
        "state_value_template": _bool_tpl("switch_air"),
        "command_topic": f"homy/cmd/{did}/air",
        "payload_on": "ON",
        "payload_off": "OFF",
        "preset_mode_state_topic": f"homy/state/{did}",
        "preset_mode_value_template": "{{ value_json.props.fan_speed_enum }}",
        "preset_mode_command_topic": f"homy/cmd/{did}/air",
        "preset_modes": ["low", "middle", "high"],
        "device": dev,
        **_availability(did),
    }
    return _config("fan", "homy", f"{did}_switch_air", payload)


def _climate_config(did: str, name: str, dev: dict, kind: str) -> tuple[str, dict]:
    suffix = "hvac_ac" if kind == "ac" else "hvac_heat"
    payload = {
        "name": name,
        "unique_id": f"{did}_{kind}",
        "object_id": _object_id(name),
        "mode_state_topic": f"homy/state/{did}",
        "mode_state_template": AC_MODE_TPL if kind == "ac"
        else "{{ 'off' if value_json.props.switch_heating in [false,'false'] else 'heat' }}",
        "mode_command_topic": f"homy/cmd/{did}/{suffix}",
        "temperature_state_topic": f"homy/state/{did}",
        "temperature_state_template": _temperature_tpl(
            "temp_set" if kind == "ac" else "heating_temp_set_c"
        ),
        "temperature_command_topic": f"homy/cmd/{did}/{suffix}",
        "current_temperature_topic": f"homy/state/{did}",
        "current_temperature_template": _temperature_tpl("temp_current"),
        "min_temp": 16.0,
        "max_temp": 32.0,
        "temp_step": 0.5,
        "precision": 0.5,
        "modes": ["off", "cool", "heat", "auto", "dry", "fan_only"] if kind == "ac"
        else ["off", "heat"],
        "device": dev,
        **_availability(did),
    }
    if kind == "ac":
        payload["fan_mode_state_topic"] = f"homy/state/{did}"
        payload["fan_mode_state_template"] = "{{ value_json.props.level }}"
        payload["fan_mode_command_topic"] = f"homy/cmd/{did}/{suffix}"
    return _config("climate", "homy", f"{did}_{kind}", payload)


def build_configs(meta_by_did: dict[str, dict], channels: dict) -> list[tuple[str, dict]]:
    """meta_by_did: deviceId → {name, room, template}（来自 BridgeState）。返回 (topic, payload) 列表。"""
    configs: list[tuple[str, dict]] = []
    light_groups = {}  # (did, area) → [entity configs] 仅用于面板灯组统计

    for did, meta in meta_by_did.items():
        template = meta.get("template", "?")
        room = meta.get("room", "")
        if template == "CGW0001":
            continue  # 网关不出实体（reboot/factory_reset 误触风险）
        if template == "CTD0001":
            dev = _dev(f"homy_{did}", meta.get("name") or f"{room}暖通", room, "CTD0001 三合一暖通")
            configs.append(_climate_config(did, f"{room}空调", dev, "ac"))
            configs.append(_climate_config(did, f"{room}地暖", dev, "heat"))
            configs.append(_fan_air_config(did, f"{room}新风", dev))
            continue
        if template != "CLA0001":
            continue
        ch = channels.get(did) or {}
        if ch.get("_mirror_only"):
            continue  # 联动镜像面板不出实体
        for field, entry in ch.items():
            if field.startswith("_") or not isinstance(entry, dict):
                continue
            area = entry.get("area") or room
            light_groups.setdefault((did, area), []).append((field, entry))

    # 灯组设备：同 (面板, 区域) 多实体 → "{区域}灯组"；单实体 → "{区域}灯具"
    for (did, area), entries in light_groups.items():
        model = "CLA0001 智控面板"
        if len(entries) > 1:
            dev = _dev(f"homy_g_{did}_{area}", f"{area}灯组", area, model)
        else:
            field, entry = entries[0]
            dev = _dev(f"homy_g_{did}_{area}", f"{area}灯具", area, model)
        for field, entry in entries:
            name = entry.get("name") or f"{area}{field}"
            if entry.get("platform") == "fan":
                configs.append(_fan_bool_config(did, field, name, dev))
            else:
                configs.append(_light_config(did, field, name, dev))

    return configs
