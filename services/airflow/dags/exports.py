"""Daily full exports; CDC capture and messaging workers remain independent persistent services."""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG,Param,task


def build(dag_id,dataset,schedule):
    with DAG(dag_id=dag_id,schedule=schedule,
        start_date=pendulum.datetime(2026,10,29,0,0,tz='America/Toronto'),catchup=False,
        max_active_runs=1,max_active_tasks=1,dagrun_timeout=timedelta(minutes=45),
        default_args={'owner':'data-platform','retries':2,'retry_delay':timedelta(seconds=30),
                      'retry_exponential_backoff':True,'max_retry_delay':timedelta(minutes=3),'pool':'export_jobs'},
        params={'fail_after_export_once':Param(False,type='boolean',description='Lab recovery test: fail first export task attempt after publication')},
        tags=['migration','full-export',dataset],
        description='Validate ingestion, export a consistent warehouse snapshot, verify its delivery and track consumption.',
        is_paused_upon_creation=True) as dag:

        @task(execution_timeout=timedelta(minutes=2))
        def check_ingestion():
            from pipeline_client import call
            return call({'action':'preflight','dataset':dataset})

        @task(execution_timeout=timedelta(minutes=17))
        def export_snapshot():
            from airflow.sdk import get_current_context
            from pipeline_client import job,request_id
            context=get_current_context()
            identity=request_id(dataset,context['run_id'])
            result=job(dataset,identity,'export',int(context['ti'].try_number))
            if context['params']['fail_after_export_once'] and int(context['ti'].try_number)==1:
                raise RuntimeError('Intentional recovery test after publication; the retry must reuse this export: '+identity)
            return result

        @task(execution_timeout=timedelta(minutes=8))
        def verify_delivery(exported):
            from airflow.sdk import get_current_context
            from pipeline_client import job
            return job(dataset,exported['request_id'],'verify',int(get_current_context()['ti'].try_number),timeout=420)

        @task(execution_timeout=timedelta(minutes=11))
        def confirm_invoice_application(verified):
            import time
            from pipeline_client import call
            deadline=time.monotonic()+600
            while time.monotonic()<deadline:
                receipt=call({'action':'invoice_receipt','dataset':dataset,'request_id':verified['request_id']})
                if receipt['processed']: return receipt
                time.sleep(10)
            raise TimeoutError('No matching durable invoice application receipt within ten minutes')

        ready=check_ingestion()
        exported=export_snapshot()
        ready >> exported
        verified=verify_delivery(exported)
        if dataset=='invoice_details': confirm_invoice_application(verified)
    return dag


export_invoices=build('export_invoices','invoice_details','0 6 * * *')
export_orders=build('export_orders','order_details','15 6 * * *')
