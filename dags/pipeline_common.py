"""
Funções e caminhos compartilhados pelas DAGs do pipeline de telemetria.

Este módulo NÃO contém nenhuma DAG. Ele centraliza:
- O cálculo do período/cutoff (janela de dados a processar).
- Os caminhos da área de staging (dados brutos extraídos) e dos Parquets.

A área de staging é o "ponto de encontro" entre as DAGs separadas por função:
  extract  -> grava  raw.pkl + manifest.json
  transform-> lê     raw.pkl   e grava o Parquet
  purge    -> lê     raw.pkl   e apaga do Mongo exatamente os _id extraídos
"""
import os
import pickle
from datetime import datetime
from zoneinfo import ZoneInfo

# Raiz do projeto. Funciona tanto local (raiz do repo) quanto no container
# Airflow (onde este arquivo fica em /opt/airflow/dags, logo a raiz é /opt/airflow).
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_ROOT = os.path.join(BASE_DIR, "data", "staging")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "dados_parquet")


def primeiro_dia_mes_atual():
    # Meia-noite (UTC) do primeiro dia do mês corrente.
    agora = datetime.now(ZoneInfo("UTC"))
    return agora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def resolver_periodo(conf):
    """Define o período (rótulo YYYYMM) e o cutoff da janela.

    Tudo com received_at ANTERIOR ao cutoff é processado/expurgado; o mês
    corrente (dados "vivos") é preservado no Mongo.

    O orquestrador injeta cutoff/period via `conf`. Quando a DAG roda avulsa
    (sem conf), assume o primeiro dia do mês atual como cutoff.
    """
    conf = conf or {}

    cutoff_iso = conf.get("cutoff")
    if cutoff_iso:
        cutoff = datetime.fromisoformat(cutoff_iso)
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=ZoneInfo("UTC"))
    else:
        cutoff = primeiro_dia_mes_atual()

    periodo = conf.get("period") or cutoff.strftime("%Y%m")
    return periodo, cutoff


def staging_dir(periodo):
    caminho = os.path.join(STAGING_ROOT, periodo)
    os.makedirs(caminho, exist_ok=True)
    return caminho


def raw_path(periodo):
    return os.path.join(staging_dir(periodo), "raw.pkl")


def manifest_path(periodo):
    return os.path.join(staging_dir(periodo), "manifest.json")


def parquet_name(periodo):
    return f"telemetria_{periodo}.parquet"


def parquet_path(periodo):
    return os.path.join(PARQUET_DIR, parquet_name(periodo))


def salvar_raw(periodo, docs):
    # Persiste os documentos brutos (preservando ObjectId/datetime via pickle).
    with open(raw_path(periodo), "wb") as f:
        pickle.dump(docs, f)


def carregar_raw(periodo):
    caminho = raw_path(periodo)
    if not os.path.exists(caminho):
        return None
    with open(caminho, "rb") as f:
        return pickle.load(f)
