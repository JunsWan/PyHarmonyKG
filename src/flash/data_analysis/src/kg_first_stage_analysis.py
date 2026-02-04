'''
对数据进行初步分析，包括版本数最多的Top-20包，依赖数最多的包，被依赖数最多的包
'''

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
第一阶段分析：
1. Package 入度 Top-20
2. Package 版本数 Top-20
3. PackageVersion 出度分布

输出：PDF
"""

from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# -----------------------------
# Neo4j 连接信息
# -----------------------------
NEO4J_URI = ""
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DATABASE = "neo4j"


# -----------------------------
# Neo4j 工具
# -----------------------------
class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
        )

    def close(self):
        self.driver.close()

    def run(self, query, params=None):
        with self.driver.session(database=NEO4J_DATABASE) as s:
            return [r.data() for r in s.run(query, params or {})]


# -----------------------------
# 主分析逻辑
# -----------------------------
def main():
    kg = Neo4jClient()

    # ========== 1. Package 入度 Top-20 ==========
    q_indegree = """
    MATCH (p:Package)<-[:REQUIRES]-()
    RETURN p.name AS package, count(*) AS indegree
    ORDER BY indegree DESC
    LIMIT 20
    """
    df_in = pd.DataFrame(kg.run(q_indegree))

    with PdfPages("/Users/junswan/Desktop/研究生课程/知识图谱/知识图谱课程项目_副本/知识图谱/flash/data_analysis/reports/package_indegree_top20.pdf") as pdf:
        plt.figure(figsize=(8, 6))
        plt.barh(df_in["package"], df_in["indegree"])
        plt.xlabel("In-degree (times required)")
        plt.title("Top-20 Most Depended-on Packages")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

    # ========== 3B. Package 出度 Top-20（版本聚合） ==========
    q_pkg_outdeg_top = """
    MATCH (p:Package)-[:HAS_VERSION]->(v:PackageVersion)-[:REQUIRES]->(d:Package)
    RETURN p.name AS package, count(d) AS outdegree
    ORDER BY outdegree DESC
    LIMIT 20
    """
    df_pkg_out = pd.DataFrame(kg.run(q_pkg_outdeg_top))

    with PdfPages(
        "/Users/junswan/Desktop/研究生课程/知识图谱/知识图谱课程项目_副本/知识图谱/flash/data_analysis/reports/package_outdegree_top20.pdf"
    ) as pdf:
        plt.figure(figsize=(8, 6))
        plt.barh(df_pkg_out["package"], df_pkg_out["outdegree"])
        plt.xlabel("Out-degree (#dependencies)")
        plt.title("Top-20 Packages by Total Dependencies")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        pdf.savefig()
        plt.close()
    
    # ========== Topic 入度 Top-20 ==========
    q_topic_top = """
    MATCH (r:Repo)-[:TAGGED_AS]->(t:Topic)
    RETURN t.name AS topic, count(r) AS repo_count
    ORDER BY repo_count DESC
    LIMIT 20
    """
    df_topic = pd.DataFrame(kg.run(q_topic_top))

    print(df_topic)

    # ---------- deep-learning ----------
    q_dl = """
    MATCH (t:Topic {name:"deep-learning"})<-[:TAGGED_AS]-(r:Repo)
    MATCH (r)-[:DEPENDS_ON]->(p:Package)
    RETURN p.name AS package, count(r) AS repo_count
    ORDER BY repo_count DESC
    LIMIT 20
    """
    df_dl = pd.DataFrame(kg.run(q_dl))

    with PdfPages(
        "/Users/junswan/Desktop/研究生课程/知识图谱/知识图谱课程项目_副本/知识图谱/flash/data_analysis/reports/deep_learning_package_top20.pdf"
    ) as pdf:
        plt.figure(figsize=(8, 6))
        plt.barh(df_dl["package"], df_dl["repo_count"])
        plt.xlabel("Number of Repositories")
        plt.title("Top-20 Packages for Topic: Deep Learning")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

    # ---------- machine-learning ----------
    q_ml = """
    MATCH (t:Topic {name:"machine-learning"})<-[:TAGGED_AS]-(r:Repo)
    MATCH (r)-[:DEPENDS_ON]->(p:Package)
    RETURN p.name AS package, count(r) AS repo_count
    ORDER BY repo_count DESC
    LIMIT 20
    """
    df_ml = pd.DataFrame(kg.run(q_ml))

    with PdfPages(
        "/Users/junswan/Desktop/研究生课程/知识图谱/知识图谱课程项目_副本/知识图谱/flash/data_analysis/reports/machine_learning_package_top20.pdf"
    ) as pdf:
        plt.figure(figsize=(8, 6))
        plt.barh(df_ml["package"], df_ml["repo_count"])
        plt.xlabel("Number of Repositories")
        plt.title("Top-20 Packages for Topic: Machine Learning")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        pdf.savefig()
        plt.close()


if __name__ == "__main__":
    main()



