"""
DAG 2/3 — TRANSFORM.

Responsabilidade única: ler os documentos brutos do staging, aplicar as regras
de limpeza e gravar o arquivo Parquet em data/dados_parquet/. Nada é lido do
Mongo nem apagado aqui.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from pipeline_common import resolver_periodo, carregar_raw, parquet_name
from utils.json_to_parquet import limpar_dados, converter_para_parquet


@dag(
    dag_id="pipeline_02_transform",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["telemetria", "parquet", "transform"],
)
def pipeline_transform():
    @task
    def transformar(**context):
        conf = context["dag_run"].conf
        periodo, _ = resolver_periodo(conf)

        docs = carregar_raw(periodo)
        if not docs:
            raise AirflowSkipException(
                f"Sem dados no staging [{periodo}] — nada para transformar."
            )

        df = limpar_dados(docs)
        if df is None or df.empty:
            raise AirflowSkipException(
                f"Após a limpeza não restaram linhas válidas [{periodo}]."
            )

        nome = parquet_name(periodo)
        converter_para_parquet(df, nome)
        print(f"✅ Parquet gerado: {nome} ({len(df)} linhas).")
        return {"periodo": periodo, "linhas": len(df), "arquivo": nome}

    transformar()


pipeline_transform()
