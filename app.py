from utils.db_config import get_db, get_data, conect_to_db
from data.json_to_parquet import limpar_dados, converter_para_parquet


def main():
    try:
        data = get_data()
    except Exception as e:
        print(f"Erro ao obter dados: {e}")
        return None
    
    df = limpar_dados(data)
    dados_comprimidos = converter_para_parquet(df, 'dados.parquet')
    
    if dados_comprimidos is not None:
        print("Dados convertidos para Parquet com sucesso.")
    else:
        print("Falha ao converter dados para Parquet.")
    
if __name__ == "__main__":
    main()
    
        
