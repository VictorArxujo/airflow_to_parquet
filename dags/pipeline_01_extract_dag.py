"""
DAG 1/3 — EXTRACT.

Responsabilidade única: ler do MongoDB (VPS) todos os documentos ANTERIORES
ao cutoff e gravá-los na área de staging local. Nada é apagado aqui.

Disparada pelo orquestrador (pipeline_00_orchestrator), mas também pode rodar
avulsa (usa o 1º dia do mês atual como cutoff).
"""
from __future__ import annotations

import json

import pendulum
from airflow.decorators import dag, task

from pipeline_common import resolver_periodo, salvar_raw, manifest_path
from utils.db_config import get_data


@dag(
    dag_id="pipeline_01_extract",
    schedule=None,  # só roda quando disparada pelo orquestrador ou manualmente
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["telemetria", "mongodb", "extract"],
)
def pipeline_extract():
    @task
    def extrair(**context):
        conf = context["dag_run"].conf
        periodo, cutoff = resolver_periodo(conf)

        print(f"📥 EXTRACT [{periodo}] -> tudo com received_at < {cutoff.isoformat()}")

        query = {"received_at": {"$lt": cutoff}}
        docs = get_data(query)
        qtd = len(docs) if docs else 0

        salvar_raw(periodo, docs or [])

        ids = [str(d["_id"]) for d in (docs or []) if "_id" in d]
        manifest = {
            "periodo": periodo,
            "cutoff": cutoff.isoformat(),
            "quantidade": qtd,
            "ids": ids,
        }
        with open(manifest_path(periodo), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        print(f"✅ {qtd} documentos extraídos para o staging [{periodo}].")
        return {"periodo": periodo, "quantidade": qtd}

    extrair()


pipeline_extract()
