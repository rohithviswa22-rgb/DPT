from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udaf
from pyflink.common import Row
import numpy as np

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

kafka_create_table_sql = f"""
    CREATE TABLE change_log_stream (
        record_time TIMESTAMP(3) METADATA FROM 'timestamp',
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'db_change_log',
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
"""
t_env.execute_sql(kafka_create_table_sql)

normalized_cdc_view_sql = """
    SELECT 
        CAST(JSON_VALUE(payload, '$.after.id') AS INT) AS id,
        CAST(JSON_VALUE(payload, '$.after.score') AS DOUBLE) AS score,
        CAST(JSON_VALUE(payload, '$.after.status') AS STRING) AS status,
        CASE JSON_VALUE(payload, '$.op')
             WHEN 'c' THEN 'INSERT'
             WHEN 'r' THEN 'INSERT'
             WHEN 'u' THEN 'UPDATE_BEFORE'
             WHEN 'd' THEN 'DELETE'
             ELSE NULL
        END AS op_type,
        CAST(JSON_VALUE(payload, '$.after.id') AS INT) AS pk_id 
    FROM change_log_stream
    WHERE JSON_VALUE(payload, '$.op') IS NOT NULL
"""

t_env.create_temporary_view("normalized_changes", t_env.sql_query(normalized_cdc_view_sql))

live_users_table = t_env.sql_query("""
    SELECT 
        pk_id, 
        LAST_VALUE(score) AS latest_score, 
        LAST_VALUE(status) AS latest_status
    FROM normalized_changes
    GROUP BY pk_id
""")

t_env.create_temporary_view("live_users", live_users_table)

model_update_query = """
    SELECT
        COUNT(pk_id) FILTER (WHERE latest_status = 'Active') AS active_user_count,
        AVG(latest_score) FILTER (WHERE latest_status = 'Active') AS mean_active_score,
        CURRENT_WATERMARK(TUMBLE_ROWTIME()) AS processing_time
    FROM live_users
"""

result_table = t_env.sql_query(model_update_query)

statement_set = t_env.create_statement_set()
statement_set.add_insert("print_sink", result_table)

print_sink_ddl = """
    CREATE TABLE print_sink (
        active_user_count BIGINT,
        mean_active_score DOUBLE,
        processing_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
"""
t_env.execute_sql(print_sink_ddl)

print("Starting Flink job for incremental data processing. Outputs will stream to console.")
job_handle = statement_set.execute()