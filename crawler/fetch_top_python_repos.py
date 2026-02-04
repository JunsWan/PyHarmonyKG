import os
import csv
import time
from datetime import datetime, timezone
from typing import List, Dict

import requests
from tqdm import tqdm

GITHUB_API_BASE = "https://api.github.com"


def get_github_headers() -> Dict[str, str]:
    """
    从环境变量 GITHUB_TOKEN 读取个人令牌，构造请求头。
    如果没有令牌，也返回基础头，但会受到更严格的速率限制。
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "python-top-repos-script",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def rate_limited_get(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    timeout: int = 30,
    max_retries: int = 10,
) -> requests.Response:
    """
    对 GitHub API 的 GET 请求增加速率限制处理：
    - 遇到 403 / 429 且包含 rate limit 相关提示时，自动等待一段时间并重试。
    - 优先使用 Retry-After 或 X-RateLimit-Reset 头信息，否则采用指数回退。
    """
    backoff = 60  # 初始等待 60 秒
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code in (403, 429):
            text_lower = ""
            try:
                text_lower = resp.text.lower()
            except Exception:
                text_lower = ""

            if "rate limit" in text_lower:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = int(retry_after)
                else:
                    reset_ts = resp.headers.get("X-RateLimit-Reset")
                    if reset_ts and reset_ts.isdigit():
                        # 等到官方给出的重置时间
                        wait_seconds = max(int(reset_ts) - int(time.time()), 1)
                    else:
                        # 否则使用指数退避
                        wait_seconds = backoff
                        backoff = min(backoff * 2, 900)  # 上限 15 分钟

                print(
                    f"命中 GitHub rate limit（状态码 {resp.status_code}），"
                    f"等待 {wait_seconds} 秒后重试，第 {attempt}/{max_retries} 次重试……"
                )
                time.sleep(wait_seconds)
                continue

        return resp

    # 超过最大重试次数，返回最后一次响应
    return resp


def fetch_top_python_repos(
    limit: int = 1000,
    per_page: int = 100,
    out_csv: str = "top_python_repos.csv",
) -> None:
    """
    使用 GitHub Search API 获取按 star 排名前 limit 个 Python 仓库，并保存为 CSV。
    CSV 字段：full_name, name, owner, stargazers_count, html_url, fetched_at
    其中 fetched_at 为每条记录抓取时间（UTC ISO8601），可视为时间轴。
    同时使用 tqdm 作为可视化“时间轴”进度条，并内置 rate limit 处理与较低抓取速度。
    """
    headers = get_github_headers()
    collected: List[Dict] = []

    # GitHub Search API 每个 query 只能返回前 1000 条结果。
    # 这里采用分段策略：每次最多拿 1000 条（10 页 * 100），
    # 然后根据本段中最小 star 数，下一轮在 query 中增加条件：stars:<min_star，继续往下抓。
    stars_upper_bound: int | None = None  # 下一段的 "stars:<X" 上界

    with tqdm(total=limit, desc="Fetching top Python repos") as pbar:
        while len(collected) < limit:
            # 单个 query 最多可拿到的上限
            remaining = limit - len(collected)
            batch_target = min(remaining, 1000)
            min_star_in_batch: int | None = None

            for page in range(1, 11):  # 单个 query 最多 10 页
                # 构造 query，必要时加入 stars:<min_star 的条件
                if stars_upper_bound is None:
                    q = "language:Python"
                else:
                    q = f"language:Python stars:<{stars_upper_bound}"

                params = {
                    "q": q,
                    "sort": "stars",
                    "order": "desc",
                    "per_page": per_page,
                    "page": page,
                }

                resp = rate_limited_get(
                    f"{GITHUB_API_BASE}/search/repositories",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                if resp.status_code != 200:
                    print(f"Error fetching page {page}: {resp.status_code} {resp.text}")
                    # 当前 query 出错就终止整体抓取
                    break

                data = resp.json()
                items = data.get("items", [])
                if not items:
                    # 本 query 已经没有更多结果
                    break

                for item in items:
                    stars = item.get("stargazers_count") or 0
                    collected.append(
                        {
                            "full_name": item.get("full_name"),
                            "name": item.get("name"),
                            "owner": (item.get("owner") or {}).get("login"),
                            "stargazers_count": stars,
                            "html_url": item.get("html_url"),
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    pbar.update(1)

                    if min_star_in_batch is None or stars < min_star_in_batch:
                        min_star_in_batch = stars

                    if len(collected) >= limit or (len(collected) % 1000 == 0 and len(collected) % batch_target == 0):
                        break

                if len(collected) >= limit or (len(collected) % 1000 == 0 and len(collected) % batch_target == 0):
                    break

                # 降低整体抓取速度，每页之间额外等待几秒
                time.sleep(5)

            # 如果这一段完全没有拿到任何新仓库，说明已经到底，直接跳出
            if min_star_in_batch is None:
                break

            # 下一次 query 只取 star 小于这一段最小 star 的仓库
            stars_upper_bound = min_star_in_batch

    # 写入 CSV
    fieldnames = [
        "full_name",
        "name",
        "owner",
        "stargazers_count",
        "html_url",
        "fetched_at",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in collected:
            writer.writerow(row)

    print(f"Saved {len(collected)} repositories to {out_csv}")


def main():
    """
    仅执行“任务 1”：生成 top_python_repos.csv。
    """
    out_csv = "top_python_repos_1W.csv"
    print("Fetching top Python repositories and saving to CSV ...")
    fetch_top_python_repos(out_csv=out_csv, limit=10000)


if __name__ == "__main__":
    main()


