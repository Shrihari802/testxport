from datetime import datetime, timedelta 

 

from airflow import DAG 

from airflow.operators.dummy import DummyOperator 

from airflow.operators.python import PythonOperator, BranchPythonOperator 

from airflow.operators.postgres_operator import PostgresOperator 

from airflow.sensors.external_task_sensor import ExternalTaskSensor 

from airflow.utils.trigger_rule import TriggerRule 

 

# ------------------------------------------------------------------------- 

# Default arguments 

# ------------------------------------------------------------------------- 

default_args = { 

    "owner": "data_eng", 

    "depends_on_past": False, 

    "email": ["data-eng-alerts@example.com"], 

    "email_on_failure": True, 

    "email_on_retry": False, 

    "retries": 2, 

    "retry_delay": timedelta(minutes=10), 

    "sla": timedelta(hours=2), 

} 

 

# ------------------------------------------------------------------------- 

# Helper Python callables 

# ------------------------------------------------------------------------- 

def decide_full_or_incremental(execution_date, **context): 

    """ 

    Very simple branching rule: 

    - On the first of the month, run a full refresh 

    - Otherwise, run incremental 

    """ 

    if execution_date.day == 1: 

        return "full_refresh_start" 

    else: 

        return "incremental_refresh_start" 

 

 

def build_incremental_window(**context): 

    """ 

    Compute a time window for incremental load and push to XCom. 

    Databricks Workflows equivalent: Task parameters / widgets. 

    """ 

    execution_date = context["execution_date"] 

    prev_execution_date = context["prev_execution_date"] 

 

    start_ts = prev_execution_date.replace(microsecond=0).isoformat() 

    end_ts = execution_date.replace(microsecond=0).isoformat() 

 

    context["ti"].xcom_push(key="incremental_start_ts", value=start_ts) 

    context["ti"].xcom_push(key="incremental_end_ts", value=end_ts) 

 

 

def log_quality_results(**context): 

    """Dummy placeholder for reading quality results from Redshift / logs.""" 

    # In real code you might query a metrics table, parse logs, etc. 

    # Here we just log to show PythonOperator usage. 

    ti = context["ti"] 

    failed_rows = ti.xcom_pull(key="failed_row_count", task_ids="dq_check_orders") 

    print(f"[DQ SUMMARY] Failed row count in orders: {failed_rows}") 

 

 

# ------------------------------------------------------------------------- 

# DAG definition 

# ------------------------------------------------------------------------- 

with DAG( 

    dag_id="redshift_orders_etl_pipeline", 

    default_args=default_args, 

    description="Sample Redshift → Databricks migration DAG (orders pipeline)", 

    start_date=datetime(2024, 1, 1), 

    schedule_interval="0 2 * * *",  # daily at 02:00 

    catchup=False, 

    max_active_runs=1, 

    tags=["redshift", "migration", "sample"], 

) as dag: 

 

    # --------------------------------------------------------------------- 

    # Upstream dependency example (e.g. raw data landed elsewhere) 

    # --------------------------------------------------------------------- 

    wait_for_raw_events = ExternalTaskSensor( 

        task_id="wait_for_raw_events", 

        external_dag_id="raw_events_ingestion_dag", 

        external_task_id="raw_events_landed", 

        mode="poke", 

        poke_interval=300, 

        timeout=60 * 60, 

        soft_fail=False, 

    ) 

 

    start = DummyOperator(task_id="start") 

 

    # --------------------------------------------------------------------- 

    # Decide full vs incremental (branching) 

    # --------------------------------------------------------------------- 

    branch_full_or_incremental = BranchPythonOperator( 

        task_id="branch_full_or_incremental", 

        python_callable=decide_full_or_incremental, 

        provide_context=True, 

    ) 

 

    # --------------------------------------------------------------------- 

    # FULL REFRESH PATH 

    # --------------------------------------------------------------------- 

    full_refresh_start = DummyOperator(task_id="full_refresh_start") 

 

    truncate_staging_orders_full = PostgresOperator( 

        task_id="truncate_staging_orders_full", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            TRUNCATE TABLE staging.stg_orders; 

        """, 

    ) 

 

    load_staging_orders_full = PostgresOperator( 

        task_id="load_staging_orders_full", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            -- Example: full reload from raw.orders into staging 

            INSERT INTO staging.stg_orders ( 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                updated_at 

            ) 

            SELECT 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                updated_at 

            FROM raw.orders; 

        """, 

    ) 

 

    merge_orders_full = PostgresOperator( 

        task_id="merge_orders_full", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            BEGIN; 

 

            -- Simple full refresh pattern 

            TRUNCATE TABLE mart.orders; 

 

            INSERT INTO mart.orders ( 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                load_ts 

            ) 

            SELECT 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                GETDATE() AS load_ts 

            FROM staging.stg_orders; 

 

            COMMIT; 

        """, 

    ) 

 

    full_refresh_end = DummyOperator(task_id="full_refresh_end") 

 

    # --------------------------------------------------------------------- 

    # INCREMENTAL PATH 

    # --------------------------------------------------------------------- 

    incremental_refresh_start = DummyOperator(task_id="incremental_refresh_start") 

 

    compute_incremental_window = PythonOperator( 

        task_id="compute_incremental_window", 

        python_callable=build_incremental_window, 

        provide_context=True, 

    ) 

 

    load_staging_orders_incremental = PostgresOperator( 

        task_id="load_staging_orders_incremental", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            -- Incremental load using XCom-based window 

            -- (In practice you'd parameterize with Jinja + XCom pulls) 

            INSERT INTO staging.stg_orders_incr ( 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                updated_at 

            ) 

            SELECT 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                updated_at 

            FROM raw.orders 

            WHERE updated_at >= '{{ ti.xcom_pull(task_ids="compute_incremental_window", key="incremental_start_ts") }}' 

              AND updated_at <  '{{ ti.xcom_pull(task_ids="compute_incremental_window", key="incremental_end_ts") }}'; 

        """, 

    ) 

 

    upsert_orders_incremental = PostgresOperator( 

        task_id="upsert_orders_incremental", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            BEGIN; 

 

            -- Upsert pattern for Redshift 

            DELETE FROM mart.orders 

            USING staging.stg_orders_incr s 

            WHERE mart.orders.order_id = s.order_id; 

 

            INSERT INTO mart.orders ( 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                load_ts 

            ) 

            SELECT 

                order_id, 

                customer_id, 

                order_ts, 

                order_status, 

                order_amount, 

                currency_code, 

                country_code, 

                GETDATE() AS load_ts 

            FROM staging.stg_orders_incr; 

 

            COMMIT; 

        """, 

    ) 

 

    incremental_refresh_end = DummyOperator(task_id="incremental_refresh_end") 

 

    # --------------------------------------------------------------------- 

    # Common downstream tasks (post-merge) 

    # --------------------------------------------------------------------- 

    join_paths = DummyOperator( 

        task_id="join_paths", 

        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, 

    ) 

 

    dq_check_orders = PostgresOperator( 

        task_id="dq_check_orders", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            -- Simple data quality check: no negative amounts, no null order_id 

            -- In real world you'd write to a dq_metrics table 

            WITH violations AS ( 

                SELECT 

                    COUNT(*) AS invalid_rows 

                FROM mart.orders 

                WHERE order_id IS NULL 

                   OR order_amount < 0 

            ) 

            SELECT invalid_rows 

            FROM violations; 

        """, 

    ) 

 

    summarize_dq = PythonOperator( 

        task_id="summarize_dq", 

        python_callable=log_quality_results, 

        provide_context=True, 

    ) 

 

    refresh_orders_daily_snapshot = PostgresOperator( 

        task_id="refresh_orders_daily_snapshot", 

        postgres_conn_id="redshift_default", 

        sql=""" 

            -- Example: refresh a summary table 

            DELETE FROM mart.orders_daily_snapshot 

            WHERE snapshot_date = '{{ ds }}'; 

 

            INSERT INTO mart.orders_daily_snapshot ( 

                snapshot_date, 

                country_code, 

                total_orders, 

                total_revenue, 

                cancelled_orders 

            ) 

            SELECT 

                '{{ ds }}'::date AS snapshot_date, 

                country_code, 

                COUNT(*) AS total_orders, 

                SUM(order_amount) AS total_revenue, 

                SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders 

            FROM mart.orders 

            WHERE order_ts >= '{{ ds }}'::date 

              AND order_ts <  ('{{ ds }}'::date + INTERVAL '1 day') 

            GROUP BY 

                country_code; 

        """, 

    ) 

 

    end = DummyOperator(task_id="end") 

 

    # --------------------------------------------------------------------- 

    # DAG wiring 

    # --------------------------------------------------------------------- 

    wait_for_raw_events >> start >> branch_full_or_incremental 

 

    # full path 

    branch_full_or_incremental >> full_refresh_start 

    full_refresh_start >> truncate_staging_orders_full >> load_staging_orders_full >> merge_orders_full >> full_refresh_end 

 

    # incremental path 

    branch_full_or_incremental >> incremental_refresh_start 

    incremental_refresh_start >> compute_incremental_window >> load_staging_orders_incremental >> upsert_orders_incremental >> incremental_refresh_end 

 

    # join & downstream 

    [full_refresh_end, incremental_refresh_end] >> join_paths 

    join_paths >> dq_check_orders >> summarize_dq >> refresh_orders_daily_snapshot >> end 