import pandas as pd

# Caminho do arquivo que você gerou (ajuste o nome exato do arquivo aqui)
caminho_arquivo = 'data/dados_parquet/telemetria_semana_20260612_a_20260613.parquet'

# 1. Lê o arquivo Parquet e transforma de volta em DataFrame
df = pd.read_parquet(caminho_arquivo)

print("👀 PRIMEIRAS LINHAS DO ARQUIVO PARQUET:")
print(df.head())

print("\n" + "="*50 + "\n")

print("ℹ️ INFORMAÇÕES E TIPAGEM DAS COLUNAS:")
# Mostra o tipo de cada coluna e se o Pandas manteve a estrutura correta
print(df.info())


print(df['payload'][3])