from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
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
}


def fetch_with_fallback(name: str, url_template: str, tz: str, max_days: int) -> tuple[str, bytes]:
    today = datetime.now(ZoneInfo(tz)).date()
    last_error: Exception | None = None

    for offset in range(max_days):
        day = today - timedelta(days=offset)
        date_str = day.strftime("%Y%m%d")
        url = url_template.format(date=date_str)
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise HTTPError(url, response.status, "Non-200", response.headers, None)
                content = response.read()
            return url, content
        except HTTPError as exc:
            last_error = exc
            if exc.code == 404:
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

    for name, meta in TARGETS.items():
        url, content = fetch_with_fallback(name, meta["url"], args.tz, args.days)
        write_bytes(meta["output"], content)
        print(f"{name} fetched from {url} -> {meta['output']}")


if __name__ == "__main__":
    main()
