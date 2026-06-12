from pymongo import MongoClient
import os

MONGO_URI = "mongodb://admin:Metrion%402024@127.0.0.1:27017/metrion?authSource=admin"
NOME_BANCO = "metrion"
NOME_COLECAO = "mensagens"

def get_db():
    client = MongoClient(MONGO_URI) 
    return client[NOME_BANCO] # Retornando o Client

def get_data():
    try:
        db = conect_to_db()
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
    collection = db[NOME_COLECAO]
    cursor = collection.find() 
    data = list(cursor)  # Convertendo o cursor para uma lista
    return data




def conect_to_db():
    print('Tentando se conectar no banco')
    try:
        db = get_db()
        db.command('ping')
        print('Conexão bem sucedida')

    except Exception as e:
        print(f'Erro ao se conectar no banco: {e}')
        return None

    return db # Retornando o banco de dados