#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于下载榜前 1000 包 + 其依赖，生成精简版知识图谱 CSV（Aura 友好）。
数据来源：
- data/top_packages_by_downloads_10000.json
- data/google_sql_with_pyv_pkg.json        （>2GB，需流式）
- data/python_repos_requirements_more_info.jsonl

输出目录：./out/
- packages.csv
- package_versions.csv
- package_version_requires.csv
- repos.csv
- repo_depends.csv
- topics.csv
- repo_topics.csv
"""

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from tqdm.auto import tqdm
from packaging.version import Version, InvalidVersion

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = Path(__file__).resolve().parent / "out"

TOP_N = 500  # 仅保留下载榜前 1000 的包及其依赖

_SPACE_RE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    return _SPACE_RE.sub("-", name.strip().lower().replace("_", "-"))


def parse_requirement(req: str) -> Tuple[Optional[str], str, str]:
    req = req.strip()
    if not req:
        return None, "", ""
    try:
        from packaging.requirements import Requirement  # type: ignore
    except Exception:
        Requirement = None  # type: ignore

    if Requirement:
        try:
            r = Requirement(req)
            name = normalize_name(r.name)
            spec = str(r.specifier) if r.specifier else ""
            marker = str(r.marker) if r.marker else ""
            return name, spec, marker
        except Exception:
            pass

    marker = ""
    if ";" in req:
        req, marker = req.split(";", 1)
        marker = marker.strip()
    req = req.strip()
    name = normalize_name(req.split()[0].split("[", 1)[0].split("(", 1)[0])
    spec = ""
    if "(" in req and ")" in req:
        spec = req[req.find("(") : req.find(")") + 1]
    return name or None, spec, marker


def ensure_out_dir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_top_packages() -> Dict[str, Dict]:
    path = DATA_DIR / "top_packages_by_downloads_10000.json"
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    result = {}
    for idx, row in enumerate(data[:TOP_N], start=1):
        name = normalize_name(row["project"])
        result[name] = {
            "downloads": int(row["download_count"]),
            "rank": idx,
            "is_top": True,
        }
    return result


def first_pass_collect_keep(path: Path, top_set: Set[str]) -> Set[str]:
    keep: Set[str] = set(top_set)
    with path.open("r", encoding="utf-8") as f:
        for line in tqdm(f, desc=f"扫描 top{TOP_N} 依赖", unit="行", mininterval=1.0):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            pkg = normalize_name(obj.get("package", ""))
            if pkg not in top_set:
                continue
            for req in obj.get("requires_dist") or []:
                dep, _, _ = parse_requirement(req)
                if dep:
                    keep.add(dep)
    return keep


def second_pass_build(
    path: Path, keep_set: Set[str], top_info: Dict[str, Dict]
) -> Tuple[Dict[str, Dict], List[Dict], List[Dict]]:
    packages: Dict[str, Dict] = {}
    versions_out: List[Dict] = []
    requires_edges_out: List[Dict] = []
    versions_map: Dict[str, List[Dict]] = {}

    with path.open("r", encoding="utf-8") as f:
        for line in tqdm(f, desc="生成包/版本/依赖", unit="行", mininterval=1.0):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            pkg = normalize_name(obj.get("package", ""))
            if pkg not in keep_set:
                continue

            packages.setdefault(
                pkg,
                {
                    "name": pkg,
                    "downloads": top_info.get(pkg, {}).get("downloads"),
                    "rank": top_info.get(pkg, {}).get("rank"),
                    "is_top": pkg in top_info,
                    "noise": pkg not in top_info,
                },
            )

            version = str(obj.get("version") or "")
            # 解析上传时间为时间戳，便于排序
            upload_time = obj.get("upload_time")
            upload_ts = None
            if upload_time:
                try:
                    upload_ts = datetime.fromisoformat(str(upload_time).replace("Z", "+00:00")).timestamp()
                except Exception:
                    upload_ts = None
            # 跳过非 PEP 440 合法版本（忽略所有非标准后缀）
            try:
                Version(version)
            except InvalidVersion:
                continue

            name_version = f"{pkg}@{version}"
            item = {
                "name_version": name_version,
                "name": pkg,
                "version": version,
                "requires_python": obj.get("requires_python") or "",
                "is_top_pkg": pkg in top_info,
                "requires": [],
                "upload_ts": upload_ts,
            }
            for req in obj.get("requires_dist") or []:
                dep, spec, marker = parse_requirement(req)
                if dep:
                    item["requires"].append({"dest": dep, "spec": spec, "marker": marker})
            versions_map.setdefault(pkg, []).append(item)

    def version_key(v: str):
        try:
            return (0, Version(v))
        except Exception:
            return (1, v)

    def item_key(x: Dict):
        ts = x.get("upload_ts")
        ts_key = -ts if ts is not None else 0  # 越新越小
        return (ts_key, version_key(x["version"]))

    for pkg, items in versions_map.items():
        items_sorted = sorted(items, key=item_key)
        for item in items_sorted[:30]:  # 保留最新版本（按时间优先，其次语义版本）
            versions_out.append(
                {
                    "name_version": item["name_version"],
                    "name": item["name"],
                    "version": item["version"],
                    "requires_python": item["requires_python"],
                    "is_top_pkg": item["is_top_pkg"],
                    "upload_time": item.get("upload_ts"),
                }
            )
            for dep in item["requires"]:
                requires_edges_out.append(
                    {
                        "src": item["name_version"],
                        "dest": dep["dest"],
                        "spec": dep["spec"],
                        "marker": dep["marker"],
                    }
                )

    return packages, versions_out, requires_edges_out


def parse_repo_requirements(keep_packages: Set[str]):
    repo_path = DATA_DIR / "python_repos_requirements_more_info.jsonl"
    repos: List[Dict] = []
    repo_depends: List[Dict] = []
    repo_topics: List[Dict] = []

    with repo_path.open("r", encoding="utf-8") as f:
        for line in tqdm(f, desc="解析 Repo requirements", unit="行", mininterval=0.5):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            full_name = obj.get("full_name")
            if not full_name:
                continue

            repos.append(
                {
                    "full_name": full_name,
                    "stars": obj.get("stargazers_count", 0),
                    "about": obj.get("about", "") or "",
                }
            )

            topics = obj.get("about_topics") or []
            for t in topics:
                t_norm = t.strip().lower()
                if t_norm:
                    repo_topics.append({"repo": full_name, "topic": t_norm})

            req_text = obj.get("requirements") or ""
            for raw in req_text.splitlines():
                raw = raw.strip()
                if not raw or raw.startswith("#"):
                    continue
                pkg, spec, marker = parse_requirement(raw)
                if not pkg:
                    continue
                if pkg not in keep_packages:
                    continue
                repo_depends.append(
                    {
                        "repo": full_name,
                        "pkg": pkg,
                        "spec": spec,
                        "marker": marker,
                    }
                )

    return repos, repo_depends, repo_topics


def write_csv(path: Path, rows: Iterable[Dict]):
    rows = list(rows)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    ensure_out_dir()
    print(f"使用下载榜前 {TOP_N} 的包构建精简图谱...")

    print("[1/6] 读取下载榜...")
    top_info = load_top_packages()
    top_set = set(top_info.keys())

    google_path = DATA_DIR / "google_sql_with_time_pkg.json"
    print("[2/6] 扫描依赖（首遍）...")
    keep_set = first_pass_collect_keep(google_path, top_set)
    print(f"    保留包数量：{len(keep_set)}")

    print("[3/6] 生成包/版本/依赖...")
    packages, versions, requires_edges = second_pass_build(google_path, keep_set, top_info)
    print(f"    包数：{len(packages)}, 版本数：{len(versions)}, 依赖边：{len(requires_edges)}")

    print("[4/6] 解析 Repo requirements...")
    repos, repo_depends, repo_topics = parse_repo_requirements(set(packages.keys()))
    print(
        f"    仓库数：{len(repos)}, 依赖边：{len(repo_depends)}, 主题关系：{len(repo_topics)}"
    )

    print("[5/6] 写出 CSV...")
    write_csv(OUT_DIR / "packages.csv", packages.values())
    write_csv(OUT_DIR / "package_versions.csv", versions)
    write_csv(OUT_DIR / "package_version_requires.csv", requires_edges)
    write_csv(OUT_DIR / "repos.csv", repos)
    write_csv(OUT_DIR / "repo_depends.csv", repo_depends)
    write_csv(OUT_DIR / "topics.csv", [{"name": t["topic"]} for t in repo_topics])
    write_csv(OUT_DIR / "repo_topics.csv", repo_topics)

    print("[6/6] 完成。")


if __name__ == "__main__":
    main()
