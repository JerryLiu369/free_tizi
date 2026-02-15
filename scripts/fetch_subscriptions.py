from __future__ import annotations

import argparse
import base64
import binascii
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import yaml

TARGETS = {
    "clash1": {
        "url": "https://yoyapai.com/mianfeijiedian/{date}-clash-vpn-mfjiedian-yoyapai.com.yaml",
        "output": "subscriptions/clash1.yaml",
    },
    "v2ray1": {
        "url": "https://yoyapai.com/mianfeijiedian/{date}-ssr-v2ray-vpn-jiedian-yoyapai.com.txt",
        "output": "subscriptions/v2ray1.txt",
    },
    "clash2": {
        "url": "http://shareclash.cczzuu.top/node/{date}-clash.yaml",
        "output": "subscriptions/clash2.yaml",
    },
    "v2ray2": {
        "url": "http://shareclash.cczzuu.top/node/{date}-v2ray.txt",
        "output": "subscriptions/v2ray2.txt",
    },
    "clash3": {
        "url": "https://static.v2rayshare.net/{year}/{month}/m{date}.yaml",
        "output": "subscriptions/clash3.yaml",
    },
    "v2ray3": {
        "url": "https://static.v2rayshare.net/{year}/{month}/{date}.txt",
        "output": "subscriptions/v2ray3.txt",
    },
    "clash4": {
        "url": "https://node.clashn.net/uploads/{year}/{month}/1-{date}.yaml",
        "output": "subscriptions/clash4.yaml",
    },
    "v2ray4": {
        "url": "https://node.clashn.net/uploads/{year}/{month}/1-{date}.txt",
        "output": "subscriptions/v2ray4.txt",
    },
}

PROXY_SCHEMES = (
    "vmess://",
    "vless://",
    "trojan://",
    "ss://",
    "ssr://",
    "hysteria://",
    "hysteria2://",
    "tuic://",
)

ERROR_MARKERS = (
    "payment required",
    "deployment_disabled",
    "access denied",
    "forbidden",
    "service unavailable",
    "bad gateway",
    "<!doctype html",
    "<html",
)

MERGED_OUTPUTS = {
    "v2ray": "subscriptions/v2ray-all.txt",
    "clash": "subscriptions/clash-all.yaml",
}

CLASH_RULE_TEMPLATE_URL = (
    "https://raw.githubusercontent.com/Kwisma/cf-worker-mihomo/main/template/"
    "Mihomo_ACL4SSR_Online_Full_NoAds.yaml"
)

CLASH_TOP_TEMPLATE_URL = "https://raw.githubusercontent.com/Kwisma/cf-worker-mihomo/main/Config/Mihomo_lite.yaml"


def build_headers(url: str) -> dict[str, str]:
    parts = urlsplit(url)
    referer = f"{parts.scheme}://{parts.netloc}/"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
    }


def has_error_marker(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in ERROR_MARKERS)


def is_valid_clash_content(content: bytes) -> bool:
    text = content.decode("utf-8", errors="ignore").strip()
    if not text or has_error_marker(text):
        return False

    lowered = text.lower()
    return "proxies:" in lowered and "proxy-groups:" in lowered


def contains_proxy_links(text: str) -> bool:
    return any(scheme in text for scheme in PROXY_SCHEMES)


def try_decode_base64_subscription(text: str) -> str:
    compact = "".join(text.split())
    if not compact:
        return ""

    padded = compact + ("=" * (-len(compact) % 4))
    try:
        decoded = base64.b64decode(padded, validate=False)
    except (binascii.Error, ValueError):
        return ""
    return decoded.decode("utf-8", errors="ignore")


def extract_v2ray_links(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line and not line.startswith("#") and contains_proxy_links(line)]


def decode_v2ray_text_to_links(content: bytes) -> list[str]:
    text = content.decode("utf-8", errors="ignore").strip()
    if not text:
        return []

    direct_links = extract_v2ray_links(text)
    if direct_links:
        return direct_links

    decoded_text = try_decode_base64_subscription(text)
    if not decoded_text:
        return []
    return extract_v2ray_links(decoded_text)


def is_valid_v2ray_content(content: bytes) -> bool:
    text = content.decode("utf-8", errors="ignore").strip()
    if not text or has_error_marker(text):
        return False

    if contains_proxy_links(text):
        return True

    decoded_text = try_decode_base64_subscription(text)
    if not decoded_text or has_error_marker(decoded_text):
        return False

    return contains_proxy_links(decoded_text)


def validate_content(name: str, content: bytes) -> None:
    if name.startswith("clash"):
        valid = is_valid_clash_content(content)
    elif name.startswith("v2ray"):
        valid = is_valid_v2ray_content(content)
    else:
        valid = bool(content.strip())

    if not valid:
        raise ValueError(f"{name} returned invalid subscription content")


def fetch_url_bytes(url: str, timeout: int = 30) -> bytes:
    request = Request(url, headers=build_headers(url))
    with urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise HTTPError(url, response.status, "Non-200", response.headers, None)
        return response.read()


def load_clash_yaml(content: bytes) -> dict[str, Any]:
    loaded = yaml.safe_load(content.decode("utf-8", errors="ignore"))
    if not isinstance(loaded, dict):
        raise ValueError("Clash content is not a YAML mapping")
    return loaded


def dump_clash_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False, width=4096)


def reorder_mapping_keys(data: dict[str, Any], preferred_order: list[str]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}
    for key in preferred_order:
        if key in data:
            ordered[key] = data[key]
    for key, value in data.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def normalize_proxy_group_key_order(groups: list[Any]) -> list[Any]:
    preferred_order = [
        "name",
        "type",
        "proxies",
        "use",
        "include-all",
        "filter",
        "url",
        "interval",
        "tolerance",
        "strategy",
        "lazy",
        "timeout",
        "max-failed-times",
        "hidden",
        "icon",
    ]

    normalized: list[Any] = []
    for group in groups:
        if isinstance(group, dict):
            normalized.append(reorder_mapping_keys(group, preferred_order))
        else:
            normalized.append(group)

    return normalized


def make_proxy_fingerprint(proxy: dict[str, Any]) -> str:
    normalized = {key: value for key, value in proxy.items() if key != "name"}
    return yaml.safe_dump(normalized, allow_unicode=True, sort_keys=True, width=4096)


def merge_clash_proxies(contents: list[bytes]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_fingerprints: set[str] = set()
    used_names: set[str] = set()

    for content in contents:
        data = load_clash_yaml(content)
        proxies = data.get("proxies", [])
        if not isinstance(proxies, list):
            continue

        for item in proxies:
            if not isinstance(item, dict):
                continue

            fingerprint = make_proxy_fingerprint(item)
            if fingerprint in seen_fingerprints:
                continue

            seen_fingerprints.add(fingerprint)
            proxy = deepcopy(item)
            original_name = str(proxy.get("name", "")) or "node"
            candidate = original_name
            suffix = 2
            while candidate in used_names:
                candidate = f"{original_name}_{suffix}"
                suffix += 1
            proxy["name"] = candidate
            used_names.add(candidate)
            merged.append(proxy)

    return merged


def load_clash_template(template_url: str) -> dict[str, Any]:
    content = fetch_url_bytes(template_url)
    data = load_clash_yaml(content)
    if "proxy-groups" not in data or "rules" not in data:
        raise ValueError("Clash template missing required fields: proxy-groups/rules")
    return data


def apply_rule_template_on_top(top_data: dict[str, Any], rule_data: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(top_data)
    merged["proxies"] = rule_data.get("proxies", [])
    merged["proxy-groups"] = rule_data.get("proxy-groups", [])
    merged["rules"] = rule_data.get("rules", [])

    sub_rules = rule_data.get("sub-rules")
    if isinstance(sub_rules, dict):
        merged["sub-rules"] = sub_rules

    proxy_providers = rule_data.get("proxy-providers")
    if isinstance(proxy_providers, dict):
        merged["proxy-providers"] = proxy_providers

    merged["rule-providers"] = {
        **(merged.get("rule-providers") if isinstance(merged.get("rule-providers"), dict) else {}),
        **(rule_data.get("rule-providers") if isinstance(rule_data.get("rule-providers"), dict) else {}),
    }

    if merged.get("find-process-mode") is False:
        merged["find-process-mode"] = "off"

    return merged


def prune_groups_by_filter(groups: list[Any], proxy_names: list[str]) -> list[Any]:
    deleted_group_names: list[str] = []
    updated_groups: list[Any] = []

    for group in groups:
        if not isinstance(group, dict):
            updated_groups.append(group)
            continue

        filter_text = group.get("filter")
        if not isinstance(filter_text, str):
            updated_groups.append(group)
            continue

        has_ignore_case = bool(re.search(r"\(\?i\)", filter_text, flags=re.IGNORECASE))
        cleaned_filter = re.sub(r"\(\?i\)", "", filter_text, flags=re.IGNORECASE)

        try:
            pattern = re.compile(cleaned_filter, re.IGNORECASE if has_ignore_case else 0)
        except re.error:
            updated_groups.append(group)
            continue

        has_match = any(pattern.search(name) is not None for name in proxy_names)
        group_proxies = group.get("proxies")
        if not has_match and (not isinstance(group_proxies, list) or len(group_proxies) == 0):
            group_name = group.get("name")
            if isinstance(group_name, str) and group_name:
                deleted_group_names.append(group_name)
            continue

        updated_groups.append(group)

    if not deleted_group_names:
        return updated_groups

    for group in updated_groups:
        if not isinstance(group, dict):
            continue
        group_proxies = group.get("proxies")
        if not isinstance(group_proxies, list):
            continue

        group["proxies"] = [
            item
            for item in group_proxies
            if isinstance(item, str)
            and not any(deleted in item for deleted in deleted_group_names)
        ]

    return updated_groups


def read_valid_target_contents(prefix: str) -> list[bytes]:
    contents: list[bytes] = []

    for name, meta in TARGETS.items():
        if not name.startswith(prefix):
            continue

        path = Path(meta["output"])
        if not path.exists():
            print(f"WARN: {name} output missing, skip merge source: {path}")
            continue

        content = path.read_bytes()
        try:
            validate_content(name, content)
            if prefix == "clash":
                data = load_clash_yaml(content)
                proxies = data.get("proxies")
                if not isinstance(proxies, list) or len(proxies) == 0:
                    raise ValueError("empty or invalid proxies list")
            elif prefix == "v2ray":
                if len(decode_v2ray_text_to_links(content)) == 0:
                    raise ValueError("no valid v2ray links")
        except Exception as exc:
            print(f"WARN: skip {name} in merge: {exc}")
            continue

        contents.append(content)

    return contents


def build_merged_v2ray_output(v2ray_contents: list[bytes]) -> bytes:
    unique_links: list[str] = []
    seen: set[str] = set()

    for content in v2ray_contents:
        for link in decode_v2ray_text_to_links(content):
            if link in seen:
                continue
            seen.add(link)
            unique_links.append(link)

    if not unique_links:
        raise ValueError("No valid v2ray links extracted from sources")

    merged_text = "\n".join(unique_links)
    encoded = base64.b64encode(merged_text.encode("utf-8"))
    return encoded + b"\n"


def build_merged_clash_output(
    clash_contents: list[bytes],
    rule_template_url: str,
    top_template_url: str,
) -> bytes:
    merged_proxies = merge_clash_proxies(clash_contents)
    if not merged_proxies:
        raise ValueError("No valid clash proxies extracted from sources")

    rule_template_data = load_clash_template(rule_template_url)
    rule_template_data["proxies"] = merged_proxies
    proxy_names = [str(proxy.get("name", "")) for proxy in merged_proxies if str(proxy.get("name", ""))]
    groups = rule_template_data.get("proxy-groups")
    if isinstance(groups, list):
        cleaned_groups = prune_groups_by_filter(groups, proxy_names)
        rule_template_data["proxy-groups"] = normalize_proxy_group_key_order(cleaned_groups)

    top_template_data = load_clash_yaml(fetch_url_bytes(top_template_url))
    merged_data = apply_rule_template_on_top(top_template_data, rule_template_data)

    dumped = dump_clash_yaml(merged_data)
    return dumped.encode("utf-8")


def build_merged_outputs(clash_rule_template_url: str, clash_top_template_url: str) -> dict[str, bytes]:
    clash_contents = read_valid_target_contents("clash")
    v2ray_contents = read_valid_target_contents("v2ray")

    merged: dict[str, bytes] = {}
    merged[MERGED_OUTPUTS["v2ray"]] = build_merged_v2ray_output(v2ray_contents)
    merged[MERGED_OUTPUTS["clash"]] = build_merged_clash_output(
        clash_contents,
        clash_rule_template_url,
        clash_top_template_url,
    )
    return merged


def fetch_with_fallback(name: str, url_template: str, tz: str, max_days: int) -> tuple[str, bytes]:
    today = datetime.now(ZoneInfo(tz)).date()
    last_error: Exception | None = None
    attempt_errors: list[str] = []

    for offset in range(max_days):
        day = today - timedelta(days=offset)
        date_str = day.strftime("%Y%m%d")
        year_str = day.strftime("%Y")
        month_str = day.strftime("%m")
        url = url_template.format(date=date_str, year=year_str, month=month_str)
        request = Request(url, headers=build_headers(url))

        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise HTTPError(url, response.status, "Non-200", response.headers, None)
                content = response.read()
            validate_content(name, content)
            return url, content
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            attempt_errors.append(f"{date_str}: {exc}")
            continue
        except Exception as exc:
            last_error = exc
            attempt_errors.append(f"{date_str}: {exc}")
            continue

    details = " | ".join(attempt_errors)
    raise RuntimeError(
        f"Failed to fetch {name} after {max_days} days. Last error: {last_error}. Attempts: {details}"
    )


def write_bytes(path: str, content: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tz", default="Asia/Shanghai")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--clash-template", default=CLASH_RULE_TEMPLATE_URL)
    parser.add_argument("--clash-top-template", default=CLASH_TOP_TEMPLATE_URL)
    args = parser.parse_args()

    successes = 0
    failures: list[str] = []

    for name, meta in TARGETS.items():
        try:
            url, content = fetch_with_fallback(name, meta["url"], args.tz, args.days)
        except Exception as exc:
            failures.append(f"{name}: {exc}")
            print(f"WARN: {name} fetch failed: {exc}")
            continue

        write_bytes(meta["output"], content)
        successes += 1
        print(f"{name} fetched from {url} -> {meta['output']}")

    try:
        for output_path, merged_content in build_merged_outputs(
            args.clash_template,
            args.clash_top_template,
        ).items():
            write_bytes(output_path, merged_content)
            print(f"merged output generated -> {output_path}")
    except Exception as exc:
        print(f"WARN: merged outputs generation failed: {exc}")

    if successes == 0:
        raise RuntimeError("All sources failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
