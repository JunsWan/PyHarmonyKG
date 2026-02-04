# 前端与后端说明

## 功能概览
- 左侧依赖图：输入环境包（pip list 粘贴，每行 name 或 name==version），可一键绘制当前图；多行新增包后自动生成新旧图对比（Plotly，箭头方向表示依赖）。
- 右侧对话：与大模型交流，支持流式输出；按钮可填充“生成安装清单”提示。
- 依赖规划：右侧“安装清单”多行输入 name==version 或带约束（>= 等），调用后端 `/plan`（基于 tasks.py 的 task3）给出可行方案/冲突，并把最新规划上下文传给对话。

## 关键接口
- `POST /graph/old`：`{packages:[{name,version?}]}` → nodes/edges/skipped。
- `POST /graph/new`：`{packages:[…], new_packages:[…]}` → old/new 两张图。
- `POST /plan`：`{env:[{name,version?}], targets:[{name,spec}]}` → 规划结果/冲突。
- `POST /chat`：`{history, env_text, plan_context}` 流式回复；prompt 会优先使用最新规划和环境上下文。
- `GET /health`：健康检查；`GET /`：返回 index.html。

## 运行
```bash
cd flash/html
uvicorn server:app --host 0.0.0.0 --port 8000   # 端口占用可换
# 浏览器访问 http://127.0.0.1:8000/
```
依赖：`pip install fastapi uvicorn neo4j packaging`

## 实现要点
- 后端：FastAPI + Neo4j 实时查询（过滤 extras，限制深度/节点数）；/plan 调用 tasks.py 回溯策略尝试更高版本；大模型懒加载，流式输出。
- 前端：Plotly 绘图（包名去掉版本），状态栏提示节点/边/跳过包；对话流式追加，不重复“助手”前缀；规划成功/失败写入对话和 plan_context 便于后续提问。请确保服务常驻运行，否则前端请求会失败。 
