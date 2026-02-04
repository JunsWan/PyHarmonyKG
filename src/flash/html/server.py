#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简易后端（FastAPI）：
- /graph/old  根据环境包绘制依赖图（忽略版本，默认取最新版本）
- /graph/new  在旧图基础上添加一个新包（==version 可选），返回 old/new 两张图
- /chat       大模型对话（需环境变量 LLM_API_KEY 和 LLM_BASE_URL），内置提示词，会优先查图谱包信息

依赖：pip install fastapi uvicorn neo4j packaging
运行： uvicorn server:app --host 0.0.0.0 --port 8000
"""

import os
import time
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from packaging.specifiers import SpecifierSet
from packaging.version import Version, InvalidVersion
from neo4j import GraphDatabase

# 兼容直接运行，加入上级路径以导入 downstream.tasks
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from downstream.tasks import KGClient as TasksKGClient, task3_install_multi_with_upgrade, normalize_name as norm_task

# ------- Neo4j 连接 -------
NEO4J_URI = ""
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""
NEO4J_DB = "neo4j"

MAX_NODES = 400
MAX_DEPTH = 3
TIMEOUT = 15


def normalize(name: str) -> str:
    return name.strip().lower().replace("_", "-")


class KG:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            connection_timeout=10,
            max_connection_lifetime=300,
        )

    def close(self):
        self.driver.close()

    def _run(self, query: str, params: Dict):
        return self.driver.execute_query(
            query, parameters_=params, database_=NEO4J_DB, timeout=TIMEOUT
        ).records

    def versions(self, name: str) -> List[str]:
        q = """
        MATCH (p:Package {name:$name})-[:HAS_VERSION]->(v:PackageVersion)
        RETURN v.version AS ver
        """
        try:
            recs = self._run(q, {"name": name})
            vers = [r["ver"] for r in recs]
            vers_sorted = sorted(vers, key=lambda v: Version(v) if is_valid(v) else Version("0"))
            return vers_sorted
        except Exception:
            return []

    def deps(self, name: str, version: str) -> List[Tuple[str, str, str]]:
        q = """
        MATCH (v:PackageVersion {name_version:$nv})-[r:REQUIRES]->(d:Package)
        RETURN d.name AS dep, r.spec AS spec, r.marker AS marker
        """
        try:
            recs = self._run(q, {"nv": f"{name}@{version}"})
            return [(r["dep"], r["spec"] or "", r["marker"] or "") for r in recs]
        except Exception:
            return []

    def latest(self, name: str) -> Optional[str]:
        vs = self.versions(name)
        return vs[-1] if vs else None  # 升序，取最后 = 最新


def is_valid(v: str) -> bool:
    try:
        Version(v)
        return True
    except InvalidVersion:
        return False


# ------- FastAPI ------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.kg = KG()
    try:
        yield
    finally:
        try:
            app.state.kg.close()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)
HERE = Path(__file__).resolve().parent
HOME_FILE = HERE / "home.html"
INDEX_FILE = HERE / "index.html"

# CORS，便于本地调试
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 若有静态资源可放到 static/ 目录
static_dir = HERE / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


class PackageInput(BaseModel):
    name: str
    version: Optional[str] = None


class GraphRequest(BaseModel):
    packages: List[PackageInput]
    new_package: Optional[PackageInput] = None  # 兼容单包
    new_packages: Optional[List[PackageInput]] = None  # 支持多包


def build_graph(packages: List[PackageInput]) -> Dict:
    kg = app.state.kg
    nodes = {}
    edges = []
    queue: List[Tuple[str, int]] = []
    visited: Set[str] = set()
    skipped: List[str] = []

    for p in packages:
        name = normalize(p.name)
        ver = p.version
        if not ver:
            ver = kg.latest(name)
        if not ver:
            skipped.append(name)
            continue
        queue.append((f"{name}@{ver}", 0))
        nodes[name] = {"id": name, "label": name}

    while queue and len(nodes) < MAX_NODES:
        nv, depth = queue.pop(0)
        if depth > MAX_DEPTH:
            continue
        name, ver = nv.split("@", 1)
        key = nv
        if key in visited:
            continue
        visited.add(key)
        deps = kg.deps(name, ver)
        for dep, spec, marker in deps:
            if marker and "extra ==" in marker:
                continue
            depn = normalize(dep)
            if depn not in nodes:
                nodes[depn] = {"id": depn, "label": depn}
            # 不在图上展示版本约束，统一置空
            edges.append({"from": name, "to": depn, "spec": ""})
            # 只取最新版本继续展开
            dep_ver = kg.latest(depn)
            if dep_ver:
                queue.append((f"{depn}@{dep_ver}", depth + 1))

    graph = {"nodes": list(nodes.values()), "edges": edges, "skipped": skipped}
    print(f"[graph] packages_in={len(packages)} nodes={len(nodes)} edges={len(edges)} skipped={skipped}")
    return graph


@app.get("/")
def home():
    return FileResponse(HOME_FILE)


@app.get("/app")
def app_page():
    return FileResponse(INDEX_FILE)


@app.post("/graph/old")
def graph_old(req: GraphRequest):
    print(f"[graph_old] pkgs={req.packages}")
    return build_graph(req.packages)


@app.post("/graph/new")
def graph_new(req: GraphRequest):
    print(f"[graph_new] pkgs={req.packages}, new={req.new_package}, new_list={req.new_packages}")
    # 旧图仅环境
    old_graph = build_graph(req.packages)
    # 新图 = 环境 + 新包（去重按包名）
    merged: Dict[str, PackageInput] = {normalize(p.name): p for p in req.packages}
    if req.new_package:
        merged[normalize(req.new_package.name)] = req.new_package
    if req.new_packages:
        for p in req.new_packages:
            merged[normalize(p.name)] = p
    new_graph = build_graph(list(merged.values()))
    return {"old": old_graph, "new": new_graph}


# --------- Chat --------- #
## 需要自己配置
LLM_KEY = "sk-2c09c2d8cc834e7aa32e9a2d1b5c46d1"
LLM_URL = "https://api.deepseek.com"
LLM_MODEL = "deepseek-chat"

try:
    import openai

    llm_client = openai.OpenAI(api_key=LLM_KEY, base_url=LLM_URL) if LLM_KEY else None
except Exception:
    llm_client = None


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    history: List[ChatMessage]
    env_text: Optional[str] = ""  # 用户粘贴 pip list
    plan_context: Optional[str] = ""  # 最新规划结果（供大模型参考）


class PlanRequest(BaseModel):
    env: List[PackageInput]
    targets: List[dict]  # {name, spec}


class PlanTarget(BaseModel):
    name: str
    spec: Optional[str] = ""


class PlanRequest(BaseModel):
    env: List[PackageInput]
    targets: List[PlanTarget]


PROMPT = """你是 Python 依赖助手，必须遵守以下规范：
1) 对话需使用用户提供的环境上下文（env_text）作为已安装列表；不要虚构环境信息。
2) 若有依赖规划上下文（plan_context），视作按时间顺序的【用户请求 input】和【规划 output】；回答时应参考最新一次规划结果。
3) 若用户要求“仅列出安装清单”，只输出 plan_context（或当前规划结果）中的 name==version，每行一个，不加其它文字。
4) 若需要安装包，列表中一行一个包，必须使用 ==精确版本；若图谱缺失，请说明“未在规划/上下文中找到”。
5) 不要臆造不存在的版本号；未找到即明确告知。
请逐步思考后给出结果。"""


@app.post("/chat")
def chat(req: ChatRequest):
    """
    流式输出，提升体验。
    前端需逐块读取并显示。
    """
    global llm_client
    if llm_client is None:
        try:
            import openai
            llm_client = openai.OpenAI(api_key=LLM_KEY, base_url=LLM_URL)
        except Exception as e:
            return StreamingResponse(iter([f"LLM 初始化失败：{e}"]), media_type="text/plain")
    msgs = [{"role": "system", "content": PROMPT}]
    for m in req.history:
        msgs.append({"role": m.role, "content": m.content})
    if req.env_text:
        msgs.append({"role": "user", "content": f"当前环境包列表：\n{req.env_text}"})
    if req.plan_context:
        msgs.append({"role": "user", "content": f"最新的依赖规划结果：\n{req.plan_context}"})

    def gen():
        try:
            stream = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=msgs,
                temperature=0.3,
                stream=True,
            )
            for chunk in stream:
                delta = chunk.choices[0].delta.content or ""
                if delta:
                    yield delta
        except Exception as e:
            yield f"\n[LLM 调用失败]: {e}"

    return StreamingResponse(gen(), media_type="text/plain")


@app.post("/plan")
def plan(req: PlanRequest):
    """
    使用 tasks.py 的 task3_install_multi_with_upgrade 规划安装。
    targets: name/spec（spec 可空则不加约束，支持 >=、== 等）
    """
    kg = TasksKGClient()
    try:
        current = {norm_task(p.name): p.version for p in req.env if p.version}
        targets = []
        for t in req.targets:
            name = norm_task(t.name)
            spec = t.spec or ""
            targets.append((name, spec))
        res = task3_install_multi_with_upgrade(kg, current, [], targets)
        plan_changes = {k: (v.version, v.source) for k, v in res.plan.items()} if res.ok else {}
        # 组装文本，便于前端传递给大模型
        plan_text = "\n".join([f"{k}=={v[0]} ({v[1]})" for k, v in plan_changes.items()]) if res.ok else ""
        return {"ok": res.ok, "conflicts": res.conflicts, "plan": plan_changes, "plan_text": plan_text}
    finally:
        kg.close()


@app.get("/health")
def health():
    return {"status": "ok", "neo4j": True}

@app.get("/")
def index():
    return FileResponse(INDEX_FILE)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
