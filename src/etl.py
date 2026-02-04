"""
基于三份数据文件构建 Neo4j 导入所需的 CSV：
- data/top_packages_by_downloads_10000.json           下载量榜（前 1 万）
- data/google_sql_with_pyv_pkg.json                   包版本与 requires_dist（>2GB，逐行流式）
- data/python_repos_requirements_more_info_1w1.jsonl  GitHub 热门仓库 requirements（1w 分片 1）
- data/python_repos_requirements_more_info_1w2.jsonl  GitHub 热门仓库 requirements（1w 分片 2）
- (兼容) data/python_repos_requirements_more_info.jsonl  旧版单文件（若 1w 分片不存在则回退）

输出（写入当前目录下 out/）：
- packages.csv                Package 节点
- package_versions.csv        PackageVersion 节点
- package_version_requires.csv PackageVersion -> Package 依赖
- repos.csv                   Repo 节点
- repo_depends.csv            Repo -> Package 依赖
- topics.csv                  Topic 节点
- repo_topics.csv             Repo -> Topic 关系

导入前请先在 Neo4j 建约束：
  CREATE CONSTRAINT pkg IF NOT EXISTS FOR (p:Package) REQUIRE p.name IS UNIQUE;
  CREATE CONSTRAINT pv IF NOT EXISTS FOR (v:PackageVersion) REQUIRE v.name_version IS UNIQUE;
  CREATE CONSTRAINT repo IF NOT EXISTS FOR (r:Repo) REQUIRE r.full_name IS UNIQUE;
  CREATE CONSTRAINT topic IF NOT EXISTS FOR (t:Topic) REQUIRE t.name IS UNIQUE;
"""

import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = Path(__file__).resolve().parent / "out"

# ----------- 基础工具 ----------- #

_SPACE_RE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """统一包名：小写、下划线转连字符、去空白。"""
    return _SPACE_RE.sub("-", name.strip().lower().replace("_", "-"))


def parse_requirement(req: str) -> Tuple[Optional[str], str, str]:
    """
    解析 requires_dist / requirement 行，返回 (包名, 版本约束, marker)。
    尽量不依赖外部库；若系统已装 packaging.requirements 则优先使用。
    """
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
            pass  # 回退到简单解析

    # 简易回退：截断 ';' marker 与括号约束
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


# ----------- 数据读取 ----------- #

def load_top_packages() -> Dict[str, Dict]:
    """读取下载榜，返回 name -> {downloads, rank, is_top}。"""
    path = DATA_DIR / "top_packages_by_downloads_10000.json"
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    result = {}
    for idx, row in enumerate(data, start=1):
        name = normalize_name(row["project"])
        result[name] = {
            "downloads": int(row["download_count"]),
            "rank": idx,
            "is_top": True,
        }
    return result


def first_pass_collect_keep(path: Path, top_set: Set[str]) -> Set[str]:
    """
    第一遍扫描超大 jsonl，只关注 top 包，收集它们的依赖包名，返回 keep 集合。
    仅解析 requires_dist；不存数据。
    """
    keep: Set[str] = set(top_set)
    tqdm = _maybe_tqdm()
    with path.open("r", encoding="utf-8") as f:
        iterator = f if tqdm is None else tqdm(f, desc="扫描 top 包依赖", unit="行", mininterval=1.0)
        for line in iterator:
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
    """
    第二遍扫描，生成包/版本/依赖。
    返回 (packages, versions, requires_edges)。
    """
    packages: Dict[str, Dict] = {}
    versions: List[Dict] = []
    requires_edges: List[Dict] = []

    tqdm = _maybe_tqdm()
    with path.open("r", encoding="utf-8") as f:
        iterator = f if tqdm is None else tqdm(f, desc="生成包/版本/依赖", unit="行", mininterval=1.0)
        for line in iterator:
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
            # 如果多次出现，保留首次标注的 downloads/rank；不覆盖。

            version = str(obj.get("version") or "")
            name_version = f"{pkg}@{version}"
            versions.append(
                {
                    "name_version": name_version,
                    "name": pkg,
                    "version": version,
                    "requires_python": obj.get("requires_python") or "",
                    "is_top_pkg": pkg in top_info,
                }
            )

            for req in obj.get("requires_dist") or []:
                dep, spec, marker = parse_requirement(req)
                if not dep:
                    continue
                requires_edges.append(
                    {
                        "src": name_version,
                        "dest": dep,
                        "spec": spec,
                        "marker": marker,
                    }
                )

    return packages, versions, requires_edges


def parse_repo_requirements(
    keep_packages: Set[str],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    解析 GitHub 仓库的 requirements，生成 Repo 节点、Repo->Package 依赖、Topic 关系。
    仅保留依赖包名在 keep_packages 中的记录，减少噪声。
    """
    # 优先使用 1w 分片；不存在则回退旧版单文件
    repo_paths = [
        DATA_DIR / "python_repos_requirements_more_info_1w1.jsonl",
        DATA_DIR / "python_repos_requirements_more_info_1w2.jsonl",
    ]
    if not all(p.exists() for p in repo_paths):
        repo_paths = [DATA_DIR / "python_repos_requirements_more_info.jsonl"]

    repos: List[Dict] = []
    repo_depends: List[Dict] = []
    repo_topics: List[Dict] = []
    seen_full_name: Set[str] = set()

    tqdm = _maybe_tqdm()
    for repo_path in repo_paths:
        with repo_path.open("r", encoding="utf-8") as f:
            desc = f"解析 repo requirements: {repo_path.name}"
            iterator = f if tqdm is None else tqdm(f, desc=desc, unit="行", mininterval=0.5)
            for line in iterator:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                full_name = obj.get("full_name")
                if not full_name:
                    continue
                if full_name in seen_full_name:
                    continue
                seen_full_name.add(full_name)

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
                        continue  # 忽略噪声包
                    repo_depends.append(
                        {
                            "repo": full_name,
                            "pkg": pkg,
                            "spec": spec,
                            "marker": marker,
                        }
                    )

    return repos, repo_depends, repo_topics


# ----------- 写 CSV ----------- #

def write_csv(path: Path, rows: Iterable[Dict]):
    rows = list(rows)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ----------- tqdm 兼容 ----------- #

def _maybe_tqdm():
    """有 tqdm 则返回 tqdm 包装，否则返回 None。"""
    try:
        from tqdm.auto import tqdm  # type: ignore
    except Exception:
        return None
    return tqdm


# ----------- 主流程 ----------- #

def main():
    ensure_out_dir()

    print("[1/6] 读取下载榜...")
    top_info = load_top_packages()
    top_set = set(top_info.keys())

    google_path = DATA_DIR / "google_sql_with_pyv_pkg.json"
    print("[2/6] 第一遍扫描，收集 top 包依赖...")
    keep_set = first_pass_collect_keep(google_path, top_set)
    print(f"    需保留包数：{len(keep_set)}")

    print("[3/6] 第二遍扫描，生成包/版本/依赖...")
    packages, versions, requires_edges = second_pass_build(google_path, keep_set, top_info)
    print(f"    包数：{len(packages)}, 版本数：{len(versions)}, 依赖边：{len(requires_edges)}")

    print("[4/6] 解析 repo requirements...")
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

    print("[6/6] 完成。可使用 LOAD CSV / neo4j-admin 导入。")


if __name__ == "__main__":
    main()
