# Databricks notebook source
# ---------------------------------------------------------
# PASSO 1: CONFIGURAÇÃO DE ACESSO AO AZURE
# ---------------------------------------------------------
# Preencha com seus dados do Azure (igual ao anterior)
storage_account_name = "f1datalakecarol2026"
storage_account_key = "INSIRA_SUA_CHAVE_AQUI"
container_name = "bronze"

# Configura o Spark (Mounting)
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ---------------------------------------------------------
# PASSO 2: FUNÇÃO DE INGESTÃO (jolpica API)
# ---------------------------------------------------------
import requests
import json
from datetime import datetime

def ingest_jolpica_data(year, endpoint, file_name):
    """
    Busca dados da jolpica API (Gratuita) e salva no Data Lake (Bronze).
    Endpoint ex: "https://api.jolpi.ca/ergast/f1/{year}/{endpoint}/?limit=1000"
    """
    # A jolpica exige um limite alto para trazer tudo de uma vez 
    url = f"https://api.jolpi.ca/ergast/f1/{year}/{endpoint}/?limit=1000"
    
    print(f"🔄 Buscando dados de: {endpoint} ({year})...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # TRANSFORMAÇÃO IMPORTANTE:
        # A jolpica devolve um JSON muito aninhado (MRData -> RaceTable).
        # Para facilitar sua vida no Spark depois, vamos pegar só o "miolo" dos dados.
        # Se for endpoint de resultados/corridas, geralmente está em 'RaceTable'
        
        raw_data_to_save = data # Salvamos o JSON bruto mesmo para ser fiel ao conceito Bronze

        # Cria DataFrame (Gambiarra técnica: Spark precisa de uma lista para criar DF de JSON único)
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw_data_to_save)]))

        # Define caminho no Azure
        timestamp = datetime.now().strftime("%Y%m%d")
        file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/jolpica/{endpoint}_{year}_{timestamp}.json"
        
        # Salva
        df.coalesce(1).write.mode("overwrite").json(file_path)
        
        print(f"✅ Sucesso! Arquivo salvo em: {file_path}")
        return df
        
    except Exception as e:
        print(f"❌ Erro na ingestão: {e}")

# ---------------------------------------------------------
# PASSO 3: EXECUTANDO O PIPELINE (2024)
# ---------------------------------------------------------

print("--- INICIANDO INGESTÃO jolpica ---")

# 1. Baixar o Calendário de Corridas e Resultados
# Esse endpoint 'results' já traz a corrida, o circuito e quem correu. É um "tudo em um".
ingest_jolpica_data(year=2024, endpoint="results", file_name="full_results")

# 2. Baixar tabela de Pilotos (Drivers) - Para fazer a Dimensão depois
ingest_jolpica_data(year=2024, endpoint="drivers", file_name="drivers_list")

# 3. Baixar tabela de Construtores (Equipes)
ingest_jolpica_data(year=2024, endpoint="constructors", file_name="constructors_list")

print("--- FIM DO PIPELINE BRONZE ---")