#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将精简版 CSV（top1000 包及其依赖）并行导入 Neo4j（Aura）。
流程：
1) 分批清空现有数据（1000/批）。
2) 创建唯一约束。
3) 并行批量导入节点与关系（ThreadPoolExecutor + UNWIND）。
"""

import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from neo4j import GraphDatabase
from tqdm.auto import tqdm

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "out"

NEO4J_URI = "neo4j+s://8921557f.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "OLlYz1vaBXPDZeQxFPbN97-U6f922uWJLARPHk5amjE"
NEO4J_DATABASE = "neo4j"

BATCH = 500
WORKERS = 4


# ----------- 工具 ----------- #

def read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def chunked(seq: Sequence[Dict], size: int) -> Iterable[List[Dict]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def execute_query_retry(driver, query: str, params: Dict = None, attempts: int = 5):
    params = params or {}
    delay = 2
    for i in range(attempts):
        try:
            return driver.execute_query(query, **params, database_=NEO4J_DATABASE)
        except Exception as e:
            if i == attempts - 1:
                raise
            print(f"  重试 {i+1}/{attempts-1} ... ({e})")
            time.sleep(delay)
            delay *= 1.5


def parallel_import(driver, query: str, rows: List[Dict], desc: str):
    with ThreadPoolExecutor(max_workers=WORKERS) as exe:
        futures = []
        for chunk in chunked(rows, BATCH):
            futures.append(
                exe.submit(
                    lambda c=chunk: execute_query_retry(
                        driver, query, {"rows": c}, attempts=5
                    )
                )
            )
        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc, unit="批"):
            fut.result()


# ----------- 导入步骤 ----------- #

def clear_database(driver):
    print("清空数据库 ...")
    while True:
        res = execute_query_retry(
            driver,
            "MATCH (n) WITH n LIMIT 1000 DETACH DELETE n RETURN count(n) AS c",
        )
        deleted = res.records[0]["c"] if res.records else 0
        if deleted == 0:
            break
        print(f"  已删除 {deleted} 条，继续 ...")


def create_constraints(driver):
    print("创建约束 ...")
    queries = [
        "CREATE CONSTRAINT pkg IF NOT EXISTS FOR (p:Package) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT pv IF NOT EXISTS FOR (v:PackageVersion) REQUIRE v.name_version IS UNIQUE",
        "CREATE CONSTRAINT repo IF NOT EXISTS FOR (r:Repo) REQUIRE r.full_name IS UNIQUE",
        "CREATE CONSTRAINT topic IF NOT EXISTS FOR (t:Topic) REQUIRE t.name IS UNIQUE",
    ]
    for q in queries:
        execute_query_retry(driver, q)


def import_packages(driver):
    rows = read_csv_dicts(OUT_DIR / "packages.csv")
    query = """
    UNWIND $rows AS row
    MERGE (p:Package {name: row.name})
    SET p.downloads = toInteger(row.downloads),
        p.rank = toInteger(row.rank),
        p.is_top = row.is_top = 'True' OR row.is_top = 'true',
        p.noise = row.noise = 'True' OR row.noise = 'true'
    """
    parallel_import(driver, query, rows, "导入 Package")


def import_package_versions(driver):
    rows = read_csv_dicts(OUT_DIR / "package_versions.csv")
    query = """
    UNWIND $rows AS row
    MERGE (p:Package {name: row.name})
    MERGE (v:PackageVersion {name_version: row.name_version})
    SET v.version = row.version,
        v.requires_python = row.requires_python,
        v.is_top_pkg = row.is_top_pkg = 'True' OR row.is_top_pkg = 'true'
    MERGE (p)-[:HAS_VERSION]->(v)
    """
    parallel_import(driver, query, rows, "导入 PackageVersion")


def import_version_requires(driver):
    rows = read_csv_dicts(OUT_DIR / "package_version_requires.csv")
    query = """
    UNWIND $rows AS row
    MATCH (src:PackageVersion {name_version: row.src})
    MATCH (dst:Package {name: row.dest})
    MERGE (src)-[r:REQUIRES]->(dst)
    SET r.spec = row.spec,
        r.marker = row.marker
    """
    parallel_import(driver, query, rows, "导入 REQUIRES")


def import_repos(driver):
    rows = read_csv_dicts(OUT_DIR / "repos.csv")
    query = """
    UNWIND $rows AS row
    MERGE (r:Repo {full_name: row.full_name})
    SET r.stars = toInteger(row.stars),
        r.about = row.about
    """
    parallel_import(driver, query, rows, "导入 Repo")


def import_repo_depends(driver):
    rows = read_csv_dicts(OUT_DIR / "repo_depends.csv")
    query = """
    UNWIND $rows AS row
    MATCH (r:Repo {full_name: row.repo})
    MATCH (p:Package {name: row.pkg})
    MERGE (r)-[d:DEPENDS_ON]->(p)
    SET d.spec = row.spec,
        d.marker = row.marker
    """
    parallel_import(driver, query, rows, "导入 Repo 依赖")


def import_topics(driver):
    rows = read_csv_dicts(OUT_DIR / "topics.csv")
    query = """
    UNWIND $rows AS row
    MERGE (t:Topic {name: row.name})
    """
    parallel_import(driver, query, rows, "导入 Topic")


def import_repo_topics(driver):
    rows = read_csv_dicts(OUT_DIR / "repo_topics.csv")
    query = """
    UNWIND $rows AS row
    MATCH (r:Repo {full_name: row.repo})
    MATCH (t:Topic {name: row.topic})
    MERGE (r)-[:TAGGED_AS]->(t)
    """
    parallel_import(driver, query, rows, "导入 Repo-Topic")


def main():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
        connection_timeout=20,
        max_connection_lifetime=300,
    )
    driver.verify_connectivity()
    with driver:
        clear_database(driver)
        create_constraints(driver)
        import_packages(driver)
        import_package_versions(driver)
        import_version_requires(driver)
        import_repos(driver)
        import_repo_depends(driver)
        import_topics(driver)
        import_repo_topics(driver)
    print("全部导入完成。")


if __name__ == "__main__":
    main()
