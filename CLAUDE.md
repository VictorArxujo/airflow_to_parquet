# CLAUDE.md

Guia de contexto para o Claude Code (e para humanos) trabalhar neste repositório.

---

## ⚙️ Manutenção automática deste arquivo (LEIA PRIMEIRO)

**No início de cada sessão, antes de qualquer outra tarefa, atualize este arquivo
olhando os commits novos.**

Procedimento:
1. Leia o campo `<!-- ultimo-commit-documentado: HASH -->` no fim deste arquivo.
2. Rode `git log <HASH>..HEAD --oneline` para ver o que mudou desde então.
3. Se houver commits novos, atualize as seções relevantes abaixo (estrutura,
   pipeline, dependências, etc.) para refletir o estado atual do código.
4. Atualize o marcador `ultimo-commit-documentado` para o `HEAD` atual.
5. Se nada mudou (`git log` vazio), não faça nada.

> Esta atualização é responsabilidade do Claude a cada sessão — não há hook
> automático no harness para isso. Trate como o primeiro passo do trabalho.

---

## 🎯 Objetivo do projeto

Orquestrador de arquivamento de telemetria. A cada **1 mês** o pipeline:

1. **Extrai** do MongoDB (hospedado na VPS do usuário) todas as mensagens
   históricas — desde a primeira mensagem até o início do mês corrente.
2. **Transforma** esses dados em arquivos **Parquet** (comprimidos), salvos no
   repositório local em `data/dados_parquet/`.
3. **Expurga** do MongoDB os dados já arquivados, para não sobrecarregar a VPS.

O mês corrente (dados "vivos") é sempre preservado no banco; só o que é anterior
ao primeiro dia do mês atual é arquivado e apagado.

A orquestração é feita com **Apache Airflow**, com **uma DAG separada por
função** (extract / transform / purge) coordenadas por uma DAG orquestradora.

---

## 🗂️ Estrutura do repositório

```
json_to_parquet/
├── CLAUDE.md                 # Este arquivo
├── docker-compose.yml        # Stack do Airflow (postgres + webserver + scheduler)
├── requirements.txt          # Dependências para rodar/local (fora do Airflow)
├── app.py                    # Script ETL standalone original (lotes SEMANAIS)
├── testes.py                 # Script avulso para inspecionar um Parquet gerado
├── utils/
│   ├── db_config.py          # Conexão Mongo: get_db / get_data / delete_data
│   └── json_to_parquet.py    # limpar_dados / converter_para_parquet
├── dags/                     # DAGs do Airflow (montadas em /opt/airflow/dags)
│   ├── pipeline_common.py            # Helpers de período/cutoff e caminhos de staging
│   ├── pipeline_00_orchestrator_dag.py # Orquestrador mensal (encadeia as 3 DAGs)
│   ├── pipeline_01_extract_dag.py      # Mongo -> staging (raw.pkl + manifest.json)
│   ├── pipeline_02_transform_dag.py    # staging -> Parquet
│   └── pipeline_03_purge_dag.py        # apaga do Mongo os _id arquivados
└── data/
    ├── dados_parquet/        # Saída final (Parquets versionados no repo)
    └── staging/              # Área temporária entre DAGs (NÃO versionada)
```

---

## 🔄 Arquitetura do pipeline (Airflow)

DAGs separadas por função, encadeadas pelo orquestrador:

```
pipeline_00_orchestrator  (schedule: @monthly)
        │  calcula period (YYYYMM) e cutoff (1º dia do mês corrente)
        │  e repassa via conf para as 3 DAGs abaixo
        ▼
pipeline_01_extract   Mongo (received_at < cutoff) ──► data/staging/<period>/raw.pkl
                                                       + manifest.json (lista de _id)
        ▼
pipeline_02_transform raw.pkl ──► limpar_dados ──► data/dados_parquet/telemetria_<period>.parquet
        ▼
pipeline_03_purge     confirma que o Parquet existe ──► delete_many({_id: $in [ids]})
                      e remove o raw.pkl do staging
```

### Garantias de segurança do expurgo
- O `purge` deleta **por `_id`** (exatamente os documentos lidos no `extract`),
  **nunca** por janela de tempo. Dados inseridos na VPS após a extração ficam
  intocados.
- O `purge` **aborta** se o Parquet do período não existir (sem backup, não
  apaga nada).
- Se `transform` falhar, o orquestrador interrompe a cadeia e o `purge` não roda.

### Área de staging
É o ponto de encontro entre as DAGs (já que cada DAG é um processo separado):
- `data/staging/<period>/raw.pkl` — documentos brutos (pickle preserva
  `ObjectId`/`datetime`).
- `data/staging/<period>/manifest.json` — período, cutoff, quantidade e lista de
  `_id` extraídos (auditoria).

---

## 🧩 Componentes-chave

### `utils/db_config.py`
- Lê `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION` do `.env`.
- `get_db()`, `get_data(query)`, `delete_data(query)`.

### `utils/json_to_parquet.py`
- `limpar_dados(data)`: remove linhas `cmd == 'send_status'`, descarta `_id`,
  remove `payload` nulo e serializa `payload` (dict/list) em JSON string.
- `converter_para_parquet(df, nome_arquivo)`: grava em `data/dados_parquet/`
  via `pyarrow` e loga a taxa de compressão.

### `app.py` (legado/standalone)
Script ETL original que processa em **lotes semanais** com retenção de 1 dia.
**Não** é usado pelo Airflow; mantido como referência. A lógica equivalente
(mensal, separada por função) vive em `dags/`.

---

## 🔐 Variáveis de ambiente (`.env`, não versionado)

```
MONGO_URI=<string de conexão do MongoDB na VPS>
MONGO_DB=<nome do banco>
MONGO_COLLECTION=<nome da coleção>   # ex.: mensagens
```

O `docker-compose.yml` injeta o `.env` nos contêineres do Airflow e monta
`./dags`, `./utils`, `./data` e `./.env` em `/opt/airflow/`. `PYTHONPATH` aponta
para `/opt/airflow`, então `import utils...` e `import pipeline_common`
funcionam dentro das DAGs.

---

## ▶️ Como rodar

### Airflow (modo orquestrado, recomendado)
```bash
docker compose up -d            # sobe postgres + webserver + scheduler
# UI em http://localhost:8080  (admin / admin)
```
Ative a DAG `pipeline_00_orchestrator` (ou dispare manualmente). As DAGs filhas
ficam `schedule=None` e são disparadas pelo orquestrador; também podem ser
executadas avulsas (usam o 1º dia do mês atual como cutoff).

### Script standalone (legado)
```bash
pip install -r requirements.txt
python app.py
```

---

## 📌 Convenções
- Comentários e logs em **português**; mensagens com emojis para legibilidade.
- Esquema de telemetria esperado nos documentos: `received_at` (datetime),
  `cmd`, `payload`.

---

## 🚧 Pontos de atenção / melhorias futuras
- O `extract` carrega toda a janela em memória (`list(cursor)`). Na **primeira**
  execução (todo o histórico desde a primeira mensagem) isso pode ser pesado;
  considerar paginação/batches por mês se o volume inicial for grande. Execuções
  mensais seguintes são leves (≈1 mês de dados).
- `requirements.txt` é para uso local; dentro do Airflow as libs vêm de
  `_PIP_ADDITIONAL_REQUIREMENTS` no `docker-compose.yml`.

---

<!-- ultimo-commit-documentado: 9067221 -->
