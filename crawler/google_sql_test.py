# dump_pypi_dependencies.py
import os
import json
from google.cloud import bigquery

PROJECT_ID = 'pypi-kg'
OUTPUT_PATH = "./data/google_sql_pkg.json"  # 或者改成你想要的路径

client = bigquery.Client(project=PROJECT_ID)

DUMP_SQL = """
SELECT
  REGEXP_REPLACE(LOWER(name), r"[-_.]+", "-") AS package,
  version,
  ANY_VALUE(requires_dist) AS requires_dist
FROM `bigquery-public-data.pypi.distribution_metadata`
GROUP BY package, version
ORDER BY package, version
"""

def main():
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    print("Starting BigQuery job...")
    query_job = client.query(DUMP_SQL, job_config=job_config)

    num_rows = 0
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for row in query_job:
            # row["requires_dist"] 是一个字符串，里面是多行/多条 Requires-Dist
            record = {
                "package": row["package"],
                "version": row["version"],
                "requires_dist": row["requires_dist"],  # 后续你可以自己再解析
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            num_rows += 1
            if num_rows % 100000 == 0:
                print(f"... written {num_rows:,} rows")

    print(f"Done. Total rows written: {num_rows:,}")
    print(f"Output file: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
