from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

TARGETS = {
    "clash1": {
        "url": "https://yoyapai.com/mianfeijiedian/{date}-clash-vpnmfjiedian-yoyapai.com.yaml",
        "output": "subscriptions/clash1.yaml",
    },
    "v2ray1": {
        "url": "https://yoyapai.com/mianfeijiedian/{date}-ssr-v2ray-vpnjiedian-yoyapai.com.txt",
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
        "url": "https://node.clashnodes.com/uploads/{year}/{month}/0-{date}.yaml",
        "output": "subscriptions/clash4.yaml",
    },
    "v2ray4": {
        "url": "https://node.clashnodes.com/uploads/{year}/{month}/0-{date}.txt",
        "output": "subscriptions/v2ray4.txt",
    },
}


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


def fetch_with_fallback(name: str, url_template: str, tz: str, max_days: int) -> tuple[str, bytes]:
    today = datetime.now(ZoneInfo(tz)).date()
    last_error: Exception | None = None

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
            return url, content
        except HTTPError as exc:
            last_error = exc
            if exc.code in {404, 406}:
                continue
            break
        except URLError as exc:
            last_error = exc
            break

    raise RuntimeError(
        f"Failed to fetch {name} after {max_days} days. Last error: {last_error}"
    )


def write_bytes(path: str, content: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tz", default="Asia/Shanghai")
    parser.add_argument("--days", type=int, default=3)
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

    if successes == 0:
        raise RuntimeError("All sources failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
