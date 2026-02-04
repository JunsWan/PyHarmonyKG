# dump_pypi_dependencies_with_python_tqdm.py
import os
import json
from google.cloud import bigquery
from tqdm import tqdm

# 从环境变量读取项目 ID（推荐）
PROJECT_ID = 'pypi-kg'
OUTPUT_PATH = "./data/google_sql_with_time_pkg.json" 

client = bigquery.Client(project=PROJECT_ID)

# 1）统计总行数（和导出时的 GROUP BY 保持一致）
COUNT_SQL = """
SELECT
  COUNT(*) AS num_rows
FROM (
  SELECT
    REGEXP_REPLACE(LOWER(name), r"[-_.]+", "-") AS package,
    version
  FROM `bigquery-public-data.pypi.distribution_metadata`
  GROUP BY package, version
)
"""

# 2）实际导出的查询（五个字段，增加 upload_time）
DUMP_SQL = """
SELECT
  REGEXP_REPLACE(LOWER(name), r"[-_.]+", "-") AS package,
  version,
  ANY_VALUE(requires_dist) AS requires_dist,
  ANY_VALUE(requires_python) AS requires_python,
  MAX(upload_time) AS upload_time
FROM `bigquery-public-data.pypi.distribution_metadata`
GROUP BY package, version
ORDER BY package, version
"""

def get_total_rows():
    """先跑一次 COUNT，用来给 tqdm 提供 total，显示百分比和剩余时间。"""
    print("Running COUNT query to estimate total rows...")
    job = client.query(COUNT_SQL)
    result = list(job)[0]
    total = result["num_rows"]
    print(f"Total (package, version) rows: {total:,}")
    return total

def dump_with_tqdm(total_rows: int):
    """执行导出，并用 tqdm 展示进度和时间估计。"""
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    print("Starting main BigQuery export job...")
    query_job = client.query(DUMP_SQL, job_config=job_config)

    num_rows = 0
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f, \
         tqdm(total=total_rows, unit="rows", desc="Exporting", smoothing=0.1) as pbar:

        # 逐行流式写出
        for row in query_job:
            upload = row["upload_time"]
            if upload is not None:
                # BigQuery 返回 datetime，转 ISO 字符串
                upload = upload.isoformat()
            record = {
                "package": row["package"],
                "version": row["version"],
                "requires_dist": row["requires_dist"],       # 可能是 None 或字符串
                "requires_python": row["requires_python"],   # 可能是 None 或字符串
                "upload_time": upload,                       # ISO 字符串或 None
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            num_rows += 1
            pbar.update(1)

    print(f"\nDone. Total rows written: {num_rows:,}")
    print(f"Output file: {OUTPUT_PATH}")

def main():
    total = get_total_rows()
    dump_with_tqdm(total)

if __name__ == "__main__":
    main()
