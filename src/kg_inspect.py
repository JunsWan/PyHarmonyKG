#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
kg_inspect_with_edges.py

功能：
- 连接 Neo4j
- 统计节点 / 关系数量
- 查看节点类型、关系类型
- 【新增】明确给出节点间关系结构（from / rel / to）
"""

from neo4j import GraphDatabase
from typing import List, Dict


# -----------------------------
# Neo4j 连接信息
# -----------------------------
NEO4J_URI = ""
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "neo4j"


# -----------------------------
# Neo4j 客户端
# -----------------------------
class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
            connection_timeout=10,
        )

    def close(self):
        self.driver.close()

    def run(self, query: str, params: Dict | None = None) -> List[Dict]:
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]


# -----------------------------
# 图谱检查器
# -----------------------------
class KGInspector:
    def __init__(self, client: Neo4jClient):
        self.client = client

    # ---------- 统计 ----------
    def node_type_stats(self):
        return self.client.run("""
        MATCH (n)
        UNWIND labels(n) AS label
        RETURN label, count(*) AS cnt
        ORDER BY cnt DESC
        """)

    def relationship_type_stats(self):
        return self.client.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS rel, count(*) AS cnt
        ORDER BY cnt DESC
        """)

    # ---------- 关系 schema（非常重要） ----------
    def relationship_schema(self):
        """
        返回：(:LabelA)-[:REL]->(:LabelB) 结构
        """
        return self.client.run("""
        MATCH (a)-[r]->(b)
        RETURN DISTINCT
            labels(a) AS from_labels,
            type(r) AS rel_type,
            labels(b) AS to_labels
        """)

    # ---------- 真实边（抽样） ----------
    def sample_edges(self, limit: int = 20):
        """
        返回真实的边数据（from / rel / to）
        """
        return self.client.run("""
        MATCH (a)-[r]->(b)
        RETURN
            labels(a) AS from_labels,
            a.name AS from_name,
            type(r) AS rel,
            labels(b) AS to_labels,
            b.name AS to_name
        LIMIT $limit
        """, {"limit": limit})

    # ---------- 某个包的依赖子图 ----------
    def package_dependency_edges(self, package: str, limit: int = 50):
        """
        返回某个包的完整依赖关系边
        """
        return self.client.run("""
        MATCH (p:Package {name:$name})-[:HAS_VERSION]->(v)-[:REQUIRES]->(d:Package)
        RETURN
            p.name AS package,
            v.version AS version,
            d.name AS dependency
        LIMIT $limit
        """, {"name": package, "limit": limit})


# -----------------------------
# 主程序
# -----------------------------
def main():
    client = Neo4jClient()
    kg = KGInspector(client)

    try:
        print("\n=== 关系 Schema（从什么到什么） ===")
        for row in kg.relationship_schema():
            print(
                f"{row['from_labels']} -[{row['rel_type']}]-> {row['to_labels']}"
            )

        print("\n=== 关系实例（抽样 10 条） ===")
        for row in kg.sample_edges(10):
            print(
                f"{row['from_labels']}:{row['from_name']} "
                f"-[{row['rel']}]-> "
                f"{row['to_labels']}:{row['to_name']}"
            )

        print("\n=== 示例：transformers 的依赖关系 ===")
        for row in kg.package_dependency_edges("transformers", 20):
            print(
                f"transformers@{row['version']} "
                f"-[REQUIRES]-> {row['dependency']}"
            )

    finally:
        client.close()


if __name__ == "__main__":
    main()
