from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.db_config import get_db, get_data, delete_data
from utils.json_to_parquet import limpar_dados, converter_para_parquet

def processar_todo_historico_por_semana():
    db = get_db()
    if db is None:
        print("Erro: Falha na conexão com o banco.")
        return

    # 1. Encontra o registro mais antigo da usina para saber de onde começar
    primeiro_registro = db['mensagens'].find_one({}, sort=[("received_at", 1)])
    if not primeiro_registro or 'received_at' not in primeiro_registro:
        print("Banco de dados vazio ou sem dados cronológicos.")
        return

    # Data de início (meia-noite do primeiro dia do banco)
    data_atual = primeiro_registro['received_at'].replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=ZoneInfo("UTC")
    )    
    # 2. A TRAVA DE SEGURANÇA: Calcula a fronteira de 2 semanas atrás (14 dias)
    hoje = datetime.now(ZoneInfo("UTC")).replace(hour=0, minute=0, second=0, microsecond=0)
    limite_retencao = hoje - timedelta(days=1)

    print(f"🧹 INICIANDO SANEAMENTO HISTÓRICO")
    print(f"-> Ponto de partida (Mais antigo): {data_atual.date()}")
    print(f"-> Fronteira de segurança (Preservar após): {limite_retencao.date()}\n")

    if data_atual >= limite_retencao:
        print("✅ Tudo em ordem! O banco já está limpo. Não há dados velhos para processar hoje.")
        return

    # O loop só roda enquanto a data processada for ANTERIOR ao limite de 2 semanas
    while data_atual < limite_retencao:
        
        proxima_semana = data_atual + timedelta(weeks=1)
        
        # Se o salto de 1 semana invadir a zona protegida, ajustamos o fim do lote para o limite exato
        if proxima_semana > limite_retencao:
            proxima_semana = limite_retencao

        # Formata o nome do arquivo com a janela daquela semana (ex: telemetria_2026-01-01_a_2026-01-08)
        janela_str = f"{data_atual.strftime('%Y%m%d')}_a_{proxima_semana.strftime('%Y%m%d')}"
        print(f"⏳ Processando lote semanal: {data_atual.date()} até {proxima_semana.date()}...")

        query_da_semana = {
            "received_at": {"$gte": data_atual, "$lt": proxima_semana}
        }

        # Extrai o lote semanal do banco
        dados_brutos = get_data(query_da_semana)

        if dados_brutos:
            df_limpo = limpar_dados(dados_brutos)
            
            if df_limpo is not None and not df_limpo.empty:
                nome_arquivo = f"telemetria_semana_{janela_str}.parquet"
                
                # Só apaga do Mongo se o Parquet daquela semana foi gravado com sucesso
                if converter_para_parquet(df_limpo, nome_arquivo):
                    apagados = delete_data(query_da_semana)
                    print(f"✅ Lote [{janela_str}] salvo. {apagados} registros antigos expurgados.")
                else:
                    print(f"❌ Erro ao salvar o arquivo da semana {janela_str}. Operação abortada.")
                    break
        else:
             print(f"skip [{janela_str}] Sem registros nesta janela.")

        # Avança o ponteiro do loop para a próxima semana
        data_atual = proxima_semana

    print(f"\n Saneamento concluído! O MongoDB foi limpo e agora contém apenas as últimas 2 semanas de dados ({limite_retencao.date()} até hoje).")

if __name__ == "__main__":
    processar_todo_historico_por_semana()