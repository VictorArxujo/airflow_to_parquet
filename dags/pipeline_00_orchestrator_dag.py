"""
DAG 0 — ORQUESTRADOR (mensal).

Roda 1x por mês e encadeia as DAGs funcionais na ordem correta:

    extract -> transform -> purge

Cada etapa é uma DAG separada (disparada via TriggerDagRunOperator com
wait_for_completion). Se uma etapa falhar, a cadeia para — em especial, o
expurgo NUNCA roda se a transformação falhar.

O período/cutoff é calculado aqui e repassado por `conf` para que as três DAGs
operem exatamente sobre a mesma janela de dados:
- cutoff = início do mês corrente (data_interval_end do agendamento mensal)
- period = rótulo YYYYMM do mês cujos dados estão sendo arquivados
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# conf repassada às DAGs filhas. Os campos são renderizados via Jinja em runtime.
COMMON_CONF = {
    "cutoff": "{{ data_interval_end }}",
    "period": "{{ data_interval_start.strftime('%Y%m') }}",
}


def _trigger(task_id, dag_id):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        conf=COMMON_CONF,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )


with DAG(
    dag_id="pipeline_00_orchestrator",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["telemetria", "orquestrador"],
) as dag:
    extract = _trigger("trigger_extract", "pipeline_01_extract")
    transform = _trigger("trigger_transform", "pipeline_02_transform")
    purge = _trigger("trigger_purge", "pipeline_03_purge")

    extract >> transform >> purge
