import os
import csv
import base64
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        # 包含 topics 所需的 preview 头
        "Accept": "application/vnd.github+json, application/vnd.github.mercy-preview+json",
        "User-Agent": "python-top-repos-script",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def read_repos_from_csv(csv_path: str) -> List[Dict[str, str]]:
    """
    从 CSV 读取仓库列表。
    要求 CSV 中至少包含 full_name, stargazers_count 字段。
    """
    repos: List[Dict[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            repos.append(row)
    return repos


def get_repo_default_branch(full_name: str, headers: Dict[str, str]) -> Optional[str]:
    """
    获取仓库默认分支名称。
    """
    owner, repo = full_name.split("/", 1)
    resp = requests.get(
        f"{GITHUB_API_BASE}/repos/{owner}/{repo}",
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"Warning: failed to get default branch for {full_name}: {resp.status_code}")
        return None
    return resp.json().get("default_branch")


def fetch_requirements_txt(
    full_name: str,
    headers: Dict[str, str],
    prefer_branches: Optional[List[str]] = None,
) -> Optional[str]:
    """
    获取仓库 requirements.txt 文本内容。
    优先尝试 prefer_branches（例如 ['main', 'master']），
    如果失败，再尝试默认分支。
    """
    if prefer_branches is None:
        prefer_branches = ["main", "master"]

    owner, repo = full_name.split("/", 1)

    def _try_get(branch: Optional[str]) -> Optional[str]:
        params = {}
        if branch:
            params["ref"] = branch
        resp = requests.get(
            f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/requirements.txt",
            headers=headers,
            params=params,
            timeout=30,
        )
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            print(
                f"Warning: failed to get requirements.txt for {full_name} "
                f"(branch={branch}): {resp.status_code}"
            )
            return None
        data = resp.json()
        content = data.get("content")
        encoding = data.get("encoding")
        if not content or encoding != "base64":
            print(f"Warning: unexpected content format for {full_name} (branch={branch})")
            return None
        try:
            decoded = base64.b64decode(content).decode("utf-8", errors="replace")
        except Exception as e:
            print(f"Warning: decode error for {full_name} (branch={branch}): {e}")
            return None
        return decoded

    # 1. 优先尝试指定分支（main / master）
    for b in prefer_branches:
        text = _try_get(b)
        if text is not None:
            return text

    # 2. 再尝试默认分支
    default_branch = get_repo_default_branch(full_name, headers)
    if default_branch:
        text = _try_get(default_branch)
        if text is not None:
            return text

    return None


def fetch_readme_excerpt(
    full_name: str,
    headers: Dict[str, str],
    max_chars: int = 1000,
) -> Optional[str]:
    """
    获取仓库 README 的简短介绍（截断版）。
    使用 /repos/{owner}/{repo}/readme 接口，解码后只保留前 max_chars 字符。
    """
    owner, repo = full_name.split("/", 1)
    resp = requests.get(
        f"{GITHUB_API_BASE}/repos/{owner}/{repo}/readme",
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(f"Warning: failed to get README for {full_name}: {resp.status_code}")
        return None

    data = resp.json()
    content = data.get("content")
    encoding = data.get("encoding")
    if not content or encoding != "base64":
        print(f"Warning: unexpected README format for {full_name}")
        return None

    try:
        decoded = base64.b64decode(content).decode("utf-8", errors="replace")
    except Exception as e:
        print(f"Warning: decode README error for {full_name}: {e}")
        return None

    # 只保留前 max_chars 字符，避免 JSONL 过大
    decoded = decoded.strip()
    if len(decoded) > max_chars:
        return decoded[:max_chars] + "..."
    return decoded


def fetch_repo_about(
    full_name: str,
    headers: Dict[str, str],
    max_topics: int = 20,
) -> Tuple[Optional[str], List[str]]:
    """
    获取仓库的 About 信息及其下方的标签（topics）。
    - About 文本来自仓库 description。
    - 标签列表来自仓库 topics 字段。
    """
    owner, repo = full_name.split("/", 1)
    resp = requests.get(
        f"{GITHUB_API_BASE}/repos/{owner}/{repo}",
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"Warning: failed to get repo about for {full_name}: {resp.status_code}")
        return None, []

    try:
        data = resp.json()
    except Exception as e:
        print(f"Warning: parse repo about json error for {full_name}: {e}")
        return None, []

    about = data.get("description")
    topics_raw = data.get("topics") or []
    topics: List[str] = []
    for t in topics_raw:
        if isinstance(t, str):
            topics.append(t)
    if len(topics) > max_topics:
        topics = topics[:max_topics]
    return about, topics


def build_requirements_jsonl(
    csv_path: str,
    out_jsonl: str = "python_repos_requirements_more_info.jsonl",
    max_workers: int = 10
) -> None:
    """
    从 CSV 读取仓库列表，抓取 requirements.txt、README 简短介绍，以及 About 与其下 topics 标签，并写入 JSONL。
    使用 ThreadPoolExecutor 并行调用 GitHub API，加快抓取速度，并通过 tqdm 显示进度条时间轴。

    JSONL 每行字段：
      - full_name: 仓库全名（owner/repo）
      - stargazers_count: star 数
      - requirements: requirements.txt 文本（如果不存在则为 None）
      - readme_excerpt: README 的简短介绍（可能为 None）
      - about: About 文本（仓库 description，可能为 None）
      - about_topics: About 下的标签（topics 名称列表）
      - fetched_at: 抓取该仓库信息的时间（UTC ISO8601），作为时间轴。
    """
    headers = get_github_headers()
    repos = read_repos_from_csv(csv_path)

    def _process_row(row: Dict[str, str]) -> Dict[str, object]:
        full_name = row.get("full_name")
        stars = row.get("stargazers_count")
        if not full_name:
            return {}

        req_text = fetch_requirements_txt(full_name, headers=headers)
        readme_excerpt = fetch_readme_excerpt(full_name, headers=headers)
        about, about_topics = fetch_repo_about(full_name, headers=headers)

        return {
            "full_name": full_name,
            "stargazers_count": int(stars) if stars is not None else None,
            "requirements": req_text,
            "about": about,
            "about_topics": about_topics,
            "readme_excerpt": readme_excerpt,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    with open(out_jsonl, "w", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_process_row, row)
                for row in repos
                if row.get("full_name")
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Fetching requirements & README",
            ):
                record = future.result()
                if not record:
                    continue
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Saved requirements info to {out_jsonl}")


def main():
    """
    仅执行“任务 2”：从 CSV 读取仓库列表并生成 JSONL。
    默认读取 top_python_repos.csv，输出 python_repos_requirements.jsonl。
    """
    csv_path = "top_python_repos.csv"
    out_jsonl = "python_repos_requirements_more_info.jsonl"

    print("Fetching requirements.txt and README excerpts for repositories from CSV ...")
    build_requirements_jsonl(csv_path=csv_path, out_jsonl=out_jsonl)


if __name__ == "__main__":
    main()


