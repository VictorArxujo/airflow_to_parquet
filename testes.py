import pandas as pd

# Caminho do arquivo que você gerou (ajuste o nome exato do arquivo aqui)
caminho_arquivo = 'data/dados_parquet/telemetria_semana_20260612_a_20260613.parquet'

df = pd.read_parquet(caminho_arquivo)

df.columns = ['_id', 'received_at', 'send_status', 'payload']
print(df)
print("\n" + "="*50 + "\n")

print("ℹ️ INFORMAÇÕES E TIPAGEM DAS COLUNAS:")
# Mostra o tipo de cada coluna e se o Pandas manteve a estrutura correta
print(df.info())


print(df['payload'][3])