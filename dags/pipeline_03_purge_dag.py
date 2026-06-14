"""
DAG 3/3 — PURGE.

Responsabilidade única: apagar do MongoDB exatamente os documentos que foram
extraídos no staging deste período — e SOMENTE depois de confirmar que o
Parquet correspondente já existe em disco.

Segurança:
- Deleta por _id (os mesmos coletados no extract), nunca por janela de tempo.
  Assim, qualquer dado inserido na VPS após a extração permanece intocado.
- Aborta se o Parquet do período não existir (o arquivo é o backup; sem ele,
  não se apaga nada).
"""
from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from pipeline_common import resolver_periodo, carregar_raw, raw_path, parquet_path
from utils.db_config import delete_data


@dag(
    dag_id="pipeline_03_purge",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["telemetria", "mongodb", "purge"],
)
def pipeline_purge():
    @task
    def expurgar(**context):
        conf = context["dag_run"].conf
        periodo, _ = resolver_periodo(conf)

        docs = carregar_raw(periodo)
        if not docs:
            raise AirflowSkipException(
                f"Sem staging [{periodo}] — nada a expurgar."
            )

        # TRAVA DE SEGURANÇA: só apaga se o Parquet (backup) existir.
        if not os.path.exists(parquet_path(periodo)):
            raise RuntimeError(
                f"❌ Parquet do período {periodo} não encontrado. "
                "Expurgo abortado para não perder dados."
            )

        ids = [d["_id"] for d in docs if "_id" in d]
        apagados = delete_data({"_id": {"$in": ids}})
        print(f"🧹 {apagados} documentos expurgados do Mongo [{periodo}].")

        # Limpa o raw.pkl (já não é mais necessário); mantém manifest + Parquet.
        try:
            os.remove(raw_path(periodo))
        except OSError:
            pass

        return {"periodo": periodo, "apagados": apagados}

    expurgar()


pipeline_purge()
