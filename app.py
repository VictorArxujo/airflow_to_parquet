from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from utils.db_config import get_db, get_data, delete_data, NOME_COLECAO
from utils.json_to_parquet import limpar_dados, converter_para_parquet

# Quantos dias de telemetria "viva" devem permanecer no MongoDB.
# Tudo anterior a (hoje - DIAS_RETENCAO) é arquivado em Parquet e expurgado.
DIAS_RETENCAO = 14

TZ = ZoneInfo("UTC")


def _meia_noite(dt):
    """Normaliza um datetime para meia-noite UTC (tz-aware)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def arquivar_e_expurgar_dia(inicio_dia, fim_dia):
    """Arquiva em Parquet os dados de um dia e os expurga do MongoDB.

    Retorna True em caso de sucesso (ou se não havia nada a fazer), e False
    se a gravação do Parquet falhar (nesse caso NADA é apagado do banco).
    """
    janela = {"received_at": {"$gte": inicio_dia, "$lt": fim_dia}}
    dia_str = inicio_dia.strftime("%Y-%m-%d")

    dados_brutos = get_data(janela)
    if not dados_brutos:
        print(f"⏭️  [{dia_str}] Sem registros nesta janela.")
        return True

    df_limpo = limpar_dados(dados_brutos)

    # Se sobrou dado útil, só apagamos do Mongo APÓS gravar o Parquet com sucesso.
    if df_limpo is not None and not df_limpo.empty:
        nome_arquivo = f"telemetria_{dia_str}.parquet"
        if not converter_para_parquet(df_limpo, nome_arquivo):
            print(f"❌ [{dia_str}] Falha ao gravar o Parquet. Nada foi apagado.")
            return False
        apagados = delete_data(janela)
        print(f"✅ [{dia_str}] {apagados} registros arquivados em {nome_arquivo} e expurgados.")
    else:
        # O dia só tinha ruído (send_status / payload nulo): expurga sem arquivar.
        apagados = delete_data(janela)
        print(f"🧹 [{dia_str}] {apagados} registros descartados (sem dados úteis para arquivar).")

    return True


def arquivar_telemetria():
    """Mantém no MongoDB apenas os últimos DIAS_RETENCAO dias de telemetria.

    Pensado para rodar diariamente (ex.: via cron). Arquiva, dia a dia, tudo
    que for anterior à janela de retenção e expurga do banco. É idempotente e
    faz backfill automático caso alguma execução diária tenha sido perdida.
    """
    db = get_db()
    if db is None:
        print("Erro: Falha na conexão com o banco.")
        return

    colecao = db[NOME_COLECAO]

    primeiro_registro = colecao.find_one({}, sort=[("received_at", 1)])
    if not primeiro_registro or "received_at" not in primeiro_registro:
        print("Banco de dados vazio ou sem dados cronológicos.")
        return

    inicio = _meia_noite(primeiro_registro["received_at"])
    hoje = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = hoje - timedelta(days=DIAS_RETENCAO)  # arquiva tudo < cutoff

    print("🗄️  INICIANDO ARQUIVAMENTO DIÁRIO DE TELEMETRIA")
    print(f"-> Registro mais antigo:        {inicio.date()}")
    print(f"-> Janela de retenção:          {DIAS_RETENCAO} dias")
    print(f"-> Preservar a partir de:       {cutoff.date()} (inclusive)\n")

    if inicio >= cutoff:
        print(f"✅ Nada a arquivar. O banco já contém apenas os últimos {DIAS_RETENCAO} dias.")
        return

    dia = inicio
    while dia < cutoff:
        proximo_dia = dia + timedelta(days=1)
        if not arquivar_e_expurgar_dia(dia, proximo_dia):
            print("⛔ Operação interrompida.")
            return
        dia = proximo_dia

    print(f"\n🏁 Arquivamento concluído! O MongoDB mantém apenas os últimos "
          f"{DIAS_RETENCAO} dias (a partir de {cutoff.date()}).")


if __name__ == "__main__":
    arquivar_telemetria()
