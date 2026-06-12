import pandas as pd
from pyarrow import parquet as pq
import os
import json

pd.set_option('display.max_colwidth', None)

# Remove o limite de colunas ocultas no meio da tabela
pd.set_option('display.max_columns', None)

# Aumenta a largura virtual da tela para evitar quebra de linha excessiva
pd.set_option('display.width', 1000)

def limpar_dados(data):

    if data is None:
        print("Nenhum dado para converter.")
        return

    df = pd.DataFrame(data)
    linhas_antes = len(df)

    # Imprime uma lista limpa com todos os nomes das colunas
    print("Colunas encontradas no DataFrame:")
    print(df.columns.tolist())
    print("-" * 30)


    # Excluindo dados do tipo 'send_status' da coluna 'cmd'
    if 'cmd' in df.columns:
        df = df[df['cmd'] != 'send_status']
        print('Dados do tipo "send_status" foram excluídos da coluna "cmd".')

    # Tirando os IDS do MongoDB 
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
        print(df.columns.tolist())

    # Removendo a linha que tem todo o payload nulo
    if 'payload' in df.columns:
        linhas_com_payload_nulo = df[df['payload'].isnull()]
        print(f'Número de linhas com payload nulo: {len(linhas_com_payload_nulo)}')
        print(linhas_com_payload_nulo)
        df = df.dropna(subset=['payload'])
        print(f'Número de linhas após remover payload nulo: {len(df)}')

    linhas_depois = len(df)
    print(f'Número de linhas antes da conversão: {linhas_antes}')
    print(f'Número de linhas depois da conversão: {linhas_depois}')

    # Transformando list em json 
    df['payload'] = df['payload'].apply(
        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x)
    )

    return df

def converter_para_parquet(df, nome_arquivo):

    pasta_destino = 'data/dados_parquet'

    os.makedirs(pasta_destino, exist_ok=True)
    
    caminho_final = os.path.join(pasta_destino, 'dados_parquet.parquet')


    tamanho_df_bytes = df.memory_usage(deep=True).sum()
    tamanho_df_mb = tamanho_df_bytes / (1024 * 1024) # Converte bytes para Megabytes

    # Salva o arquivo final usando o motor do PyArrow
    df.to_parquet(
        caminho_final, 
        engine='pyarrow', 
        index=False
    )

    tamanho_arquivo_bytes = os.path.getsize(caminho_final)
    tamanho_arquivo_mb = tamanho_arquivo_bytes / (1024 * 1024)


    if tamanho_df_bytes > 0:
        compressao = (1 - (tamanho_arquivo_bytes / tamanho_df_bytes)) * 100
    else:
        compressao = 0

    # ==========================================
    # 4. RELATÓRIO FINAL
    # ==========================================
    print("-" * 40)
    print("📊 RELATÓRIO DE COMPRESSÃO PARQUET")
    print("-" * 40)
    print(f"Tamanho do DataFrame (RAM): {tamanho_df_mb:.2f} MB")
    print(f"Tamanho do Parquet (Disco): {tamanho_arquivo_mb:.2f} MB")
    print(f"Redução de tamanho:         {compressao:.1f}%")
    print("-" * 40)

    
    return True