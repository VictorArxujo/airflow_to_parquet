# CLAUDE.md

Guia de contexto para o Claude Code (e para humanos) trabalhar neste repositório.

> **Branch `producao`** — versão enxuta, só com o script, que vai para produção.
> A orquestração com Apache Airflow vive na branch `airflow` (implementação
> futura). Não traga Airflow, DAGs ou `docker-compose.yml` para esta branch.

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

Arquivador de telemetria. O script roda **diariamente** (ex.: via cron) e:

1. **Extrai** do MongoDB (hospedado na VPS do usuário) os dados mais antigos
   que a janela de retenção.
2. **Transforma** esses dados em arquivos **Parquet** (comprimidos, um por dia),
   salvos em `data/dados_parquet/`.
3. **Expurga** do MongoDB os dados já arquivados.

O MongoDB mantém sempre uma janela móvel de **14 dias** de dados "vivos"
(`DIAS_RETENCAO`). Tudo anterior a `(hoje - 14 dias)` é arquivado e apagado,
para não sobrecarregar a VPS.

---

## 🗂️ Estrutura do repositório

```
mongo_to_parquet/
├── CLAUDE.md                 # Este arquivo
├── requirements.txt          # Dependências (pymongo, pandas, pyarrow, dotenv)
├── app.py                    # Script principal: arquivamento DIÁRIO + retenção 14d
├── testes.py                 # Script avulso para inspecionar um Parquet gerado
├── utils/
│   ├── db_config.py          # Conexão Mongo: get_db / get_data / delete_data
│   └── json_to_parquet.py    # limpar_dados / converter_para_parquet
└── data/
    └── dados_parquet/        # Saída final (Parquets versionados no repo)
```

---

## 🔄 Lógica do `app.py`

```
arquivar_telemetria()  (rodar 1x por dia)
   │  acha o registro mais antigo no Mongo
   │  cutoff = hoje - DIAS_RETENCAO (14 dias)
   │  se o mais antigo já é >= cutoff: nada a fazer
   ▼
   para cada DIA, do mais antigo até < cutoff:
     ├─ get_data({received_at: [inicio_dia, proximo_dia)})
     ├─ limpar_dados(...)
     ├─ converter_para_parquet -> data/dados_parquet/telemetria_YYYY-MM-DD.parquet
     └─ delete_data(janela do dia)   # só APÓS gravar o Parquet com sucesso
```

### Garantias de segurança
- O expurgo de cada dia só ocorre **após** o Parquet daquele dia ser gravado
  com sucesso. Se a gravação falhar, a operação aborta e nada é apagado.
- É **idempotente** e faz **backfill**: se uma execução diária for perdida, a
  próxima processa todos os dias pendentes (do mais antigo até o cutoff).
- Dias que contêm apenas ruído (`cmd == 'send_status'` / `payload` nulo) são
  expurgados sem gerar Parquet (não há dado útil para arquivar).

> ⚠️ A **primeira** execução arquiva todo o histórico anterior aos 14 dias,
> dia a dia. Se o volume inicial for muito grande, considere rodar fora do
> horário de pico (cada dia é carregado em memória via `list(cursor)`).

---

## 🧩 Componentes-chave

### `utils/db_config.py`
- Lê `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION` do `.env`.
- `get_db()`, `get_data(query)`, `delete_data(query)`; expõe `NOME_COLECAO`.

### `utils/json_to_parquet.py`
- `limpar_dados(data)`: remove linhas `cmd == 'send_status'`, descarta `_id`,
  remove `payload` nulo e serializa `payload` (dict/list) em JSON string.
- `converter_para_parquet(df, nome_arquivo)`: grava em `data/dados_parquet/`
  via `pyarrow` e loga a taxa de compressão.

### `app.py`
- `DIAS_RETENCAO = 14`: janela mantida viva no Mongo.
- `arquivar_telemetria()`: loop diário descrito acima.
- `arquivar_e_expurgar_dia(inicio, fim)`: arquiva+expurga um único dia.

---

## 🔐 Variáveis de ambiente (`.env`, não versionado)

```
MONGO_URI=<string de conexão do MongoDB na VPS>
MONGO_DB=<nome do banco>
MONGO_COLLECTION=<nome da coleção>   # ex.: mensagens
```

---

## ▶️ Como rodar

```bash
pip install -r requirements.txt
python app.py
```

### Em produção (execução diária via cron)
```cron
# todo dia às 03:00 (ajuste o caminho do projeto e do python)
0 3 * * * cd /caminho/mongo_to_parquet && /caminho/python app.py >> arquivamento.log 2>&1
```

---

## 📌 Convenções
- Comentários e logs em **português**; mensagens com emojis para legibilidade.
- Esquema de telemetria esperado nos documentos: `received_at` (datetime),
  `cmd`, `payload`.
- Datas tratadas em **UTC** (meia-noite como fronteira de cada dia).

---

<!-- ultimo-commit-documentado: c27cc8a -->
