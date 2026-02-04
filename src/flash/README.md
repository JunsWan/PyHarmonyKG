# 知识图谱（精简版，Top 500 包 + 依赖）

## 节点与关系
- `Package`：属性 `name`、`downloads`、`rank`、`is_top`、`noise`（是否非榜单）。
- `PackageVersion`：属性 `name_version`（`pkg@ver`）、`version`、`requires_python`、`is_top_pkg`；关系 `(:Package)-[:HAS_VERSION]->(:PackageVersion)`。
- `REQUIRES`：`(:PackageVersion)-[:REQUIRES {spec, marker}]->(:Package)`，`spec` 为版本约束（PEP 440），`marker` 为环境标记。
- `Repo`：属性 `full_name`、`stars`、`about`；关系 `(:Repo)-[:DEPENDS_ON {spec, marker}]->(:Package)`。
- `Topic`：属性 `name`；关系 `(:Repo)-[:TAGGED_AS]->(:Topic)`。

版本保留策略：每个包仅保留最新 30 个版本（按语义化版本优先，其次字典序）。

## 访问与连接
- Aura URI：`neo4j+s://8921557f.databases.neo4j.io`
- 用户名：`neo4j`
- 密码：`OLlYz1vaBXPDZeQxFPbN97-U6f922uWJLARPHk5amjE`
- 数据库：`neo4j`

推荐安装驱动：
```bash
pip install neo4j packaging tqdm
```

Python 连接示例：
```python
from neo4j import GraphDatabase
driver = GraphDatabase.driver(
    "neo4j+s://8921557f.databases.neo4j.io",
    auth=("neo4j", "OLlYz1vaBXPDZeQxFPbN97-U6f922uWJLARPHk5amjE"),
)
driver.verify_connectivity()
```

## 常用 Cypher
- 查询某包的最新版本及依赖：
```cypher
MATCH (p:Package {name:$name})-[:HAS_VERSION]->(v:PackageVersion)
RETURN v.name_version AS nv, v.version AS ver
ORDER BY ver DESC
LIMIT 1;
```
```cypher
MATCH (v:PackageVersion {name_version:$nv})- [r:REQUIRES]->(dep:Package)
RETURN dep.name AS dep, r.spec AS spec, r.marker AS marker;
```

- 查询一个仓库依赖了哪些 Top 包：
```cypher
MATCH (r:Repo {full_name:$repo})-[d:DEPENDS_ON]->(p:Package)
RETURN p.name, d.spec, p.is_top
ORDER BY p.rank;
```

## 导入流程（已完成）
1. `flash/etl_flash.py` 生成精简 CSV 到 `flash/out/`。
2. `flash/import_flash.py` 并行清空并导入 Aura，自动建约束与进度条。

## 下游使用
- 下游任务脚本位于 `flash/downstream/tasks.py`，提供冲突检测与升级规划。
- 需先确保环境可访问 Aura，并安装 `neo4j`、`packaging`、`tqdm`。
