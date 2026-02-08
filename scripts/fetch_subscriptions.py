from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

BASE_URLS = {
    "clash": "https://yoyapai.com/mianfeijiedian/{date}-clash-vpnmfjiedian-yoyapai.com.yaml",
    "v2ray": "https://yoyapai.com/mianfeijiedian/{date}-ssr-v2ray-vpnjiedian-yoyapai.com.txt",
}

OUTPUT_PATHS = {
    "clash": "subscriptions/clash.yaml",
    "v2ray": "subscriptions/v2ray.txt",
}


def fetch_with_fallback(kind: str, tz: str, max_days: int) -> tuple[str, bytes]:
    today = datetime.now(ZoneInfo(tz)).date()
    last_error: Exception | None = None

    for offset in range(max_days):
        day = today - timedelta(days=offset)
        date_str = day.strftime("%Y%m%d")
        url = BASE_URLS[kind].format(date=date_str)
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
        f"Failed to fetch {kind} after {max_days} days. Last error: {last_error}"
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

    for kind in ("clash", "v2ray"):
        url, content = fetch_with_fallback(kind, args.tz, args.days)
        write_bytes(OUTPUT_PATHS[kind], content)
        print(f"{kind} fetched from {url} -> {OUTPUT_PATHS[kind]}")


if __name__ == "__main__":
    main()
