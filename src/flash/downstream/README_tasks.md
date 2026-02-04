# 下游任务说明与复杂度估算

本文件记录当前三类任务的实现方法、时间复杂度估算，以及使用求解器加速的改进方案。

## 任务描述
1. 任务一：在已有环境（给定已安装包与版本）和 requirements.txt 下，安装一个新包（给定版本/范围），判断是否存在冲突。
2. 任务二：同条件，安装一个新包（>= 某版本）及其所有依赖；若冲突，给出最小可行升级方案（尽量少、尽量低版本）。
3. 任务三：同条件，安装多个新包（>= 某版本）及其依赖；若冲突，给出最小可行升级方案。

## 现有实现（tasks.py）
- 数据来源：Neo4j 精简图谱（Top500 包，每包最新 30 版本）。
- 逻辑：解析环境与目标约束，获取版本候选，按约束尝试版本；递归展开依赖，过滤 extras。若冲突则尝试更高版本（回溯）。
- 适用：快速可行性检查和安装/升级规划。

### 复杂度（上界）
O(N+E) 次图查询（N 包数，E 依赖），版本数 30 为常数。网络/图大小决定实际耗时。

## 使用
```python
from tasks import KGClient, task3_install_multi_with_upgrade
kg = KGClient()
current = {"numpy":"1.23.0"}
targets = [("requests", ">=2.31.0"), ("urllib3", ">=1.26.0")]
res = task3_install_multi_with_upgrade(kg, current, [], targets)
print(res.ok, res.conflicts, {k:(v.version,v.source) for k,v in res.plan.items()})
kg.close()
```
