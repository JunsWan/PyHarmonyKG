# 爬虫 / 数据抓取脚本说明

## google_sql_with_pyv.py
- 来源：BigQuery `bigquery-public-data.pypi.distribution_metadata`
- 字段：package（规范化）、version、requires_dist、requires_python、upload_time
- 逻辑：先 COUNT 估算行数，再流式导出为 JSON 行文件 `data/google_sql_with_time_pkg.json`
- 运行前需设置 GCP 项目：`PROJECT_ID = 'pypi-kg'`，并确保本地已配置 gcloud 认证
- 输出用于 ETL（etl_flash.py）构建知识图谱

## 其他说明
- 该目录主要放置数据抓取脚本与原始 JSON/密钥文件，不建议提交敏感凭证
- 输出文件大（>2GB），请谨慎存储和传输
