import pandas as pd
import os
import json

# Configurações de exibição do terminal
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def limpar_dados(data):
    # Aplica as regras de negócio, remove nulos e vetoriza o JSON
    if not data:
        return None

    df = pd.DataFrame(data)

    if 'cmd' in df.columns:
        df = df[df['cmd'] != 'send_status']

    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    if 'payload' in df.columns:
        df = df.dropna(subset=['payload'])

    df['payload'] = df['payload'].apply(
        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x)
    )

    return df

def converter_para_parquet(df, nome_arquivo):
    # Salva o DataFrame em disco e exibe relatório de compressão
    pasta_destino = 'data/dados_parquet'
    os.makedirs(pasta_destino, exist_ok=True)
    
    caminho_final = os.path.join(pasta_destino, nome_arquivo)
    tamanho_df_bytes = df.memory_usage(deep=True).sum()

    df.to_parquet(caminho_final, engine='pyarrow', index=False)

    tamanho_arquivo_bytes = os.path.getsize(caminho_final)
    
    # Cálculo de métricas
    tamanho_df_mb = tamanho_df_bytes / (1024 * 1024)
    tamanho_arquivo_mb = tamanho_arquivo_bytes / (1024 * 1024)
    compressao = (1 - (tamanho_arquivo_bytes / tamanho_df_bytes)) * 100 if tamanho_df_bytes > 0 else 0

    print(f"📊 {tamanho_df_mb:.2f}MB RAM -> {tamanho_arquivo_mb:.2f}MB Disco | Redução: {compressao:.1f}%")
    return True