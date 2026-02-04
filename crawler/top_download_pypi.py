from asyncio import LimitOverrunError
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any

import requests
from tqdm import tqdm

limit = 10000
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
TOP_PYPI_URL = (
    "https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
)
TOP_PKGS_JSON_PATH = os.path.join(DATA_DIR, f"top_packages_by_downloads_{limit}.json")
def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


print(f"从 {TOP_PYPI_URL} 获取下载量排行榜...")
resp = requests.get(TOP_PYPI_URL, timeout=30)
resp.raise_for_status()
data = resp.json()
rows = data.get("rows", [])
# rows 中一般包含字段: project, download_count
rows_sorted = sorted(
    rows, key=lambda r: r.get("download_count", 0), reverse=True
)
# 保存完整排行榜到本地，方便后续分析
save_json(TOP_PKGS_JSON_PATH, rows_sorted)
names = [r.get("project") for r in rows_sorted if r.get("project")]
top_names = names[:limit]
print(f"已按下载量获取前 {len(top_names)} 个包名（降序）")