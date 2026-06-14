import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
NOME_BANCO = os.getenv("MONGO_DB")
NOME_COLECAO = os.getenv("MONGO_COLLECTION")

def get_db():
    # Estabelece a conexão base com o banco de dados
    client = MongoClient(MONGO_URI) 
    return client[NOME_BANCO]

def get_data(query=None):
    # Busca dados no MongoDB, aplicando filtros de lote se fornecidos
    db = get_db()
    if db is None:
        return None
        
    collection = db[NOME_COLECAO]
    cursor = collection.find(query or {}) 
    return list(cursor)

def delete_data(query):
    # Exclui documentos permanentemente baseados na regra de tempo
    db = get_db()
    if db is not None:
        resultado = db[NOME_COLECAO].delete_many(query)
        return resultado.deleted_count
    return 0