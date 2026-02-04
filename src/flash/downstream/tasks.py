#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下游任务工具：基于已导入的 Neo4j 知识图谱，完成包安装冲突判断与最小升级规划。

支持三类任务：
1) 安装单个新包（指定版本或范围），判断是否冲突。
2) 安装单个新包（>= 版本），若有冲突，计算最小可行升级方案。
3) 安装多个新包（>= 版本），若有冲突，计算最小可行升级方案。

依赖：
    pip install neo4j packaging tqdm
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Set

from neo4j import GraphDatabase
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version, InvalidVersion
from tqdm.auto import tqdm

NEO4J_URI = ""
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DB = "neo4j"

KEEP_VERSIONS = 30  # 与 ETL 保持一致
TIMEOUT_SEC = 20
MAX_QUEUE = 2000
MAX_PACKAGES = 800
RETRIES = 3
VERBOSE = True


# ----------------------- 数据结构 ----------------------- #

@dataclass
class PackageSelection:
    name: str
    version: str
    source: str  # "existing" | "new" | "upgrade"


@dataclass
class ResolutionResult:
    ok: bool
    plan: Dict[str, PackageSelection]
    conflicts: List[str]


# ----------------------- Neo4j 访问 ----------------------- #


class KGClient:
    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD, database=NEO4J_DB):
        self.driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            connection_timeout=20,
            max_connection_lifetime=300,
        )
        self.database = database

    def close(self):
        self.driver.close()

    def _run_query(self, query: str, params: Dict) -> List[Dict]:
        last_err = None
        for attempt in range(1, RETRIES + 1):
            try:
                import time

                t0 = time.time()
                res = self.driver.execute_query(
                    query,
                    parameters_=params,
                    database_=self.database,
                    timeout=TIMEOUT_SEC,
                )
                dt = time.time() - t0
                print(f"[KG] query attempt {attempt} time={dt:.2f}s params_keys={list(params.keys())}")
                return res.records
            except Exception as e:
                last_err = e
                print(f"[KG][warn] attempt {attempt} failed: {e}")
        if last_err:
            raise last_err
        return []

    def get_versions(self, name: str) -> List[str]:
        """返回指定包的版本列表（字符串），未排序由 Neo4j 返回；在 Python 侧再排序。"""
        q = """
        MATCH (p:Package {name:$name})-[:HAS_VERSION]->(v:PackageVersion)
        RETURN v.version AS ver
        """
        records = self._run_query(q, {"name": name})
        vers = [r["ver"] for r in records]
        return sort_versions(vers)

    def get_requires(self, name: str, version: str) -> List[Tuple[str, str, str]]:
        """给定包与版本，返回依赖列表 (dep_name, spec, marker)。"""
        q = """
        MATCH (v:PackageVersion {name_version:$nv})- [r:REQUIRES]->(dep:Package)
        RETURN dep.name AS dep, r.spec AS spec, r.marker AS marker
        """
        nv = f"{name}@{version}"
        records = self._run_query(q, {"nv": nv})
        return [(r["dep"], r["spec"] or "", r["marker"] or "") for r in records]


# ----------------------- 版本工具 ----------------------- #


def sort_versions(versions: List[str]) -> List[str]:
    def key(v: str):
        try:
            return (0, Version(v))
        except InvalidVersion:
            return (1, v)

    return sorted(versions, key=key, reverse=True)


def pick_min_satisfying(versions: List[str], spec: SpecifierSet, prefer_latest_when_any: bool = False) -> Optional[str]:
    """
    选择满足 spec 的版本：
    - 若 prefer_latest_when_any 且 spec 为空，则返回最新（降序列表首个）。
    - 否则选择最低可行版本（升序扫描）。
    """
    if prefer_latest_when_any and (not spec or str(spec) == ""):
        return sort_versions(versions)[0] if versions else None
    vs = list(reversed(sort_versions(versions)))  # 升序
    for v in vs:
        try:
            if not spec or spec.contains(v, prereleases=True):
                return v
        except Exception:
            continue
    return None


def normalize_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def parse_requirements(req_lines: Sequence[str]) -> Dict[str, str]:
    """解析 requirements.txt 行，返回 {name: spec_str}，无版本约束则为空字符串。"""
    result = {}
    for line in req_lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        r = Requirement(line)
        name = normalize_name(r.name)
        spec = str(r.specifier) if r.specifier else ""
        result[name] = spec
    return result


# ----------------------- 核心解析器 ----------------------- #


def resolve_plan(
    kg: KGClient,
    current: Dict[str, str],
    targets: List[Tuple[str, str, str]],
) -> ResolutionResult:
    """
    解析安装/升级计划。
    :param current: 现有环境 {name: version}
    :param targets: [(name, constraint_spec_str, source_tag)]  source_tag= "targetX"
    """
    plan: Dict[str, PackageSelection] = {
        normalize_name(k): PackageSelection(normalize_name(k), v, "existing")
        for k, v in current.items()
    }
    conflicts: List[str] = []

    queue: List[Tuple[str, SpecifierSet, str]] = []
    for name, spec_str, tag in targets:
        queue.append((normalize_name(name), SpecifierSet(spec_str or ""), tag))

    def dfs(queue_state: List[Tuple[str, SpecifierSet, str]], plan_state: Dict[str, PackageSelection], visited: Set[Tuple[str, str]]) -> Tuple[bool, Dict[str, PackageSelection], List[str]]:
        if len(plan_state) > MAX_PACKAGES:
            return False, plan_state, [f"处理的包数超过上限 {MAX_PACKAGES}"]
        if not queue_state:
            return True, plan_state, []
        if len(queue_state) > MAX_QUEUE:
            return False, plan_state, [f"队列超过上限 {MAX_QUEUE}，中止"]

        name, spec, tag = queue_state.pop(0)
        key = (name, str(spec))
        if key in visited:
            return dfs(queue_state, plan_state, visited)
        visited = set(visited)
        visited.add(key)

        versions = kg.get_versions(name)
        if not versions:
            return False, plan_state, [f"{name}: 在图谱中无可用版本"]

        # 构造候选列表
        if not spec or str(spec) == "":
            # 无约束时优先最新，再尝试更低版本
            cands = sort_versions(versions)  # 降序，最新在前
        else:
            cands = [v for v in reversed(sort_versions(versions)) if spec.contains(v, prereleases=True)]  # 升序满足
        if not cands:
            return False, plan_state, [f"{name}: 无法找到满足 {spec} 的版本"]

        # 已有且满足，放在首位保留
        if name in plan_state and spec.contains(plan_state[name].version, prereleases=True):
            cands = [plan_state[name].version] + [v for v in cands if v != plan_state[name].version]

        for cand in cands:
            new_plan = dict(plan_state)
            new_queue = list(queue_state)
            source = "new"
            if name in new_plan:
                if new_plan[name].source == "existing" and new_plan[name].version == cand:
                    source = "existing"
                else:
                    source = "upgrade"
            new_plan[name] = PackageSelection(name, cand, source)
            deps = kg.get_requires(name, cand)
            for dep_name, dep_spec, marker in deps:
                if marker and "extra ==" in marker:
                    continue
                new_queue.append((normalize_name(dep_name), SpecifierSet(dep_spec or ""), f"dep-of-{name}"))
            ok, p2, conf = dfs(new_queue, new_plan, visited)
            if ok:
                return True, p2, conf
        return False, plan_state, [f"{name}: 所有候选版本依赖冲突"]

    ok, plan_res, confs = dfs(queue, plan, set())
    return ResolutionResult(ok=ok, plan=plan_res, conflicts=confs)


# ----------------------- 任务封装 ----------------------- #


def task1_check_single(kg: KGClient, current: Dict[str, str], req_lines: Sequence[str], new_pkg: str, spec: str) -> ResolutionResult:
    req_map = parse_requirements(req_lines)
    merged = {**current}
    for k, s in req_map.items():
        if s:
            # 若已有版本但不满足，尝试视作冲突
            if k in merged and not SpecifierSet(s).contains(merged[k], prereleases=True):
                return ResolutionResult(False, {}, [f"{k}: 已安装 {merged[k]} 不满足 {s}"])
        merged.setdefault(k, None)
    targets = [(new_pkg, spec, "target")]
    return resolve_plan(kg, {k: v for k, v in merged.items() if v}, targets)


def task2_install_single_with_upgrade(kg: KGClient, current: Dict[str, str], req_lines: Sequence[str], new_pkg: str, min_spec: str) -> ResolutionResult:
    req_map = parse_requirements(req_lines)
    merged = {**current}
    # 如果 requirements 指定了版本且不满足，也通过升级规划解决
    targets = [(new_pkg, min_spec, "target")]
    for k, s in req_map.items():
        # 仅对“已在环境中的包”施加约束，避免把整个 requirements 当成安装列表导致爆炸
        if k in merged and s:
            targets.append((k, s, "req"))
    return resolve_plan(kg, merged, targets)


def task3_install_multi_with_upgrade(kg: KGClient, current: Dict[str, str], req_lines: Sequence[str], new_pkgs: List[Tuple[str, str]]) -> ResolutionResult:
    req_map = parse_requirements(req_lines)
    merged = {**current}
    targets = [(name, spec, "target") for name, spec in new_pkgs]
    for k, s in req_map.items():
        if k in merged and s:
            targets.append((k, s, "req"))
    return resolve_plan(kg, merged, targets)


# ----------------------- 示例使用 ----------------------- #


def demo():
    kg = KGClient()
    try:
        current = {"pandas": "1.5.3", "numpy": "1.24.0"}
        req_lines = ["requests>=2.31.0", "pydantic==1.10.9"]

        print("任务1：只检测冲突")
        r1 = task1_check_single(kg, current, req_lines, "transformers", "==4.57.2")
        print("ok:", r1.ok, "conflicts:", r1.conflicts)

        print("任务2：单包安装，允许升级")
        r2 = task2_install_single_with_upgrade(kg, current, req_lines, "fastapi", ">=0.11.0")
        print("ok:", r2.ok, "conflicts:", r2.conflicts)
        if r2.ok:
            print("plan:", {k: (v.version, v.source) for k, v in r2.plan.items() if v.source != "existing"})

        print("任务3：多包安装，允许升级")
        r3 = task3_install_multi_with_upgrade(
            kg,
            current,
            req_lines,
            [("fastapi", ">=0.11.0"), ("uvicorn", ">=0.22.0")]
        )

        print("ok:", r3.ok)
        print("conflicts:", r3.conflicts)

        if r3.ok:
            print("plan:")
            for k, v in r3.plan.items():
                if v.source != "existing":
                    print(f"  {k}=={v.version} ({v.source})")

    finally:
        kg.close()


if __name__ == "__main__":
    demo()
