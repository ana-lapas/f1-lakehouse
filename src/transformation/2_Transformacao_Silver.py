# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Camada Silver: Transformação e Padronização
# MAGIC
# MAGIC Nesta etapa, processamos os dados brutos da camada Bronze para criar tabelas confiáveis e otimizadas.
# MAGIC
# MAGIC **Estratégias aplicadas em todas as pipelines:**
# MAGIC 1.  **Schema Enforcement:** Conversão de tipos (`String` -> `Int`, `Date`, `Float`) para garantir consistência numérica.
# MAGIC 2.  **Deduplicação:** Remoção de registros duplicados baseados em chaves únicas (IDs).
# MAGIC 3.  **Metadados:** Adição de colunas de auditoria (`ingestion_date` e `source_file`).
# MAGIC 4.  **Formato Delta:** Salvamento em formato Delta Lake para permitir *ACID Transactions* e performance.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, explode, current_timestamp, input_file_name 

# --- CONFIGURAÇÃO DE CAMINHOS ---
# PASSO 1: CONFIGURAÇÃO DE ACESSO AO AZURE
# Preencha com seus dados do Azure 
storage_account_name = "f1datalakecarol2026"
storage_account_key = "6FQP+MNUBoy3xCDvI7ZBjUH7IoJqbY7lS82W0seE2S2M5gCpVVhIQfWoZ/FTZAmVXs2E6WR7p2PY+AStKv01ig=="
container_name = "bronze"

# Configura o Spark (Mounting)
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

container_bronze = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"
container_silver = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"

print("🚀 Iniciando Processamento SILVER...")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏎️ Pipeline 1: Drivers (Dimensão)
# MAGIC Tratamento da lista de pilotos. Aplainamento (Flattening) da estrutura aninhada e geração da tabela de dimensão.

# COMMAND ----------


print("Iniciando Inspeção dos PILOTOS...")
# Analisando a estrutura do arquivo Raw para planejar a transformação
try:
    df_drivers_raw = spark.read.json(f"{container_bronze}/jolpica/drivers_*.json")
    
    print("--- Schema Identificado ---")
    df_drivers_raw.printSchema()
    
    print("--- Amostra de Dados ---")
    display(df_drivers_raw.limit(5))
except Exception as e:
    print(f"Dados ainda não disponíveis: {e}")

# COMMAND ----------

print("🚀 Processando Drivers para Silver...")

try:
    if 'df_drivers_raw' not in locals():
        print("⚠️ Leitura raw não encontrada na memória, lendo arquivos agora...")
        df_drivers_raw = spark.read.json(f"{container_bronze}/jolpica/drivers_*.json")

    df_drivers_exploded = df_drivers_raw \
        .select(explode(col("MRData.DriverTable.Drivers")).alias("driver_data"))

    df_drivers_silver = df_drivers_exploded.select(
        col("driver_data.driverId").alias("driver_id"),
        col("driver_data.givenName").alias("driver_name"),
        col("driver_data.familyName").alias("driver_surname"),
        col("driver_data.nationality").alias("driver_nationality"),
        col("driver_data.dateOfBirth").cast("date").alias("driver_dob"),
        col("driver_data.code").alias("driver_code"),
        col("driver_data.permanentNumber").cast("int").alias("driver_number"),
        col("driver_data.url").alias("driver_url"),
        current_timestamp().alias("ingestion_date"),
        input_file_name().alias("source_file")
    )

    # DEDUPLICAÇÃO
    df_drivers_silver = df_drivers_silver.dropDuplicates(['driver_id'])

    df_drivers_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{container_silver}/drivers")
        
    print("✅ Tabela 'drivers' salva e deduplicada!")    
    print("Preview Drivers:")
    display(df_drivers_silver.limit(5))

except Exception as e:
    print(f"❌ Erro em Drivers: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔧 Pipeline 2: Constructors (Dimensão)
# MAGIC Tratamento das equipes construtoras.

# COMMAND ----------

print("🔍 Iniciando Inspeção das EQUIPES...")

try:
    df_constructors_raw = spark.read.json(f"{container_bronze}/jolpica/constructors_*.json")

    print("📜 Schema Constructors:")
    df_constructors_raw.printSchema()

    print("👀 Amostra Raw:")
    display(df_constructors_raw.limit(5))

except Exception as e:
    print(f"⚠️ Erro na leitura de Constructors: {e}")

# COMMAND ----------

print("🚀 Processando Constructors para Silver...")

try:
    df_constructors_exploded = df_constructors_raw \
        .select(explode(col("MRData.ConstructorTable.Constructors")).alias("team_data"))

    df_constructors_silver = df_constructors_exploded.select(
        col("team_data.constructorId").alias("team_id"),
        col("team_data.name").alias("team_name"),
        col("team_data.nationality").alias("team_nationality"),
        col("team_data.url").alias("team_url"),
        current_timestamp().alias("ingestion_date"),
        input_file_name().alias("source_file")
    )
    
    df_constructors_silver = df_constructors_silver.dropDuplicates(['team_id'])

    df_constructors_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{container_silver}/constructors")
        
    print("✅ Tabela 'constructors' salva com sucesso!")
    print("Preview Constructors:")
    display(df_constructors_silver.limit(5))

except Exception as e:
    print(f"❌ Erro em Constructors: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏁 Pipeline 3: Results (Fato)
# MAGIC Tabela principal contendo os resultados das corridas.
# MAGIC * **Nota Técnica:** Aplicamos **Particionamento por Temporada (`season`)** nesta tabela para otimizar a performance de leitura em grandes volumes de dados.
# MAGIC

# COMMAND ----------

print("🔍 Iniciando Inspeção dos Resultados...")

try:
    path_results = f"{container_bronze}/jolpica/results_*.json"
    df_results_raw = spark.read.json(path_results)

    print("📜 Schema do Arquivo JSON:")
    df_results_raw.printSchema()

    print("Visualizando as 5 primeiras linhas do JSON cru:")
    display(df_results_raw.limit(5))

except Exception as e:
    print(f"❌ Erro na leitura dos arquivos: {e}")

# COMMAND ----------

print("🚀 Iniciando Transformação para Camada Prata...")

try:
    # EXPLODE CORRIDAS
    # "Abre" o array de corridas para que cada corrida vire uma linha
    # O schema mostra que 'Races' está dentro de 'MRData.RaceTable'
    df_races = df_results_raw \
        .select(explode(col("MRData.RaceTable.Races")).alias("race_data"))
        
    # EXPLODE RESULTADOS
    # "Abre" o array de resultados que está DENTRO de cada corrida
    # Mantemos 'race_data' para não perder a info de qual corrida aquele resultado pertence
    df_results_exploded = df_races \
        .select(
            col("race_data"),
            explode(col("race_data.Results")).alias("result_data")
        )
        
    # SELEÇÃO FINAL E TIPAGEM (CAST)
    df_results_silver = df_results_exploded.select(
        # --- DADOS DA CORRIDA (Vêm de race_data) ---
        col("race_data.season").cast("int").alias("season"),
        col("race_data.round").cast("int").alias("round"),
        col("race_data.raceName").alias("race_name"),
        col("race_data.date").cast("date").alias("race_date"),
        
        # Dados do Circuito (Note o caminho: Circuit -> circuitId)
        col("race_data.Circuit.circuitId").alias("circuit_id"),
        col("race_data.Circuit.circuitName").alias("circuit_name"),
        # O Schema mostra que country está dentro de Location
        col("race_data.Circuit.Location.country").alias("country"), 
        
        # --- DADOS DO RESULTADO (Vêm de result_data) ---
        col("result_data.position").cast("int").alias("position"),
        col("result_data.points").cast("float").alias("points"),
        col("result_data.laps").cast("int").alias("laps"),
        col("result_data.status").alias("status"),
        col("result_data.FastestLap.Time.time").alias("fastest_lap_time"),
        col("result_data.FastestLap.rank").cast("int").alias("fastest_lap_rank"),
        
        # --- CHAVES ESTRANGEIRAS (IDs) ---
        col("result_data.Driver.driverId").alias("driver_id"), 
        col("result_data.Constructor.constructorId").alias("team_id"), 
        current_timestamp().alias("ingestion_date")
    )

    df_results_silver = df_results_silver.dropDuplicates(['season', 'round', 'driver_id'])
    caminho_silver = f"{container_silver}/results"
    
    df_results_silver.write.format("delta").mode("overwrite").save(caminho_silver)
    
    print(f"✅ Sucesso! Tabela salva em: {caminho_silver}")
    print("Preview Final:")
    display(df_results_silver.limit(5))

except Exception as e:
    print(f"❌ Erro durante a transformação: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Conclusão da Camada Silver
# MAGIC
# MAGIC O processamento da camada Prata foi finalizado com sucesso. Os dados agora estão estruturados, tipados e salvos em formato **Delta Lake**.
# MAGIC
# MAGIC **Resumo dos Entregáveis:**
# MAGIC 1.  **`drivers`**: Tabela Dimensão deduplicada com cadastro de pilotos.
# MAGIC 2.  **`constructors`**: Tabela Dimensão deduplicada com cadastro de equipes.
# MAGIC 3.  **`results`**: Tabela Fato contendo métricas de corrida, particionada por `season` para alta performance.
# MAGIC
# MAGIC **Próximos Passos:**
# MAGIC No próximo notebook (**Camada Gold**), aplicaremos a **Modelagem Dimensional (Star Schema)** para deixar esses dados prontos para análise de negócios e Power BI.

# COMMAND ----------

# Validação Final: Listando os arquivos gerados no Data Lake
print("📂 Verificando arquivos na Camada Silver:")

try:
    # Lista as pastas criadas
    files = dbutils.fs.ls(container_silver)
    
    for f in files:
        print(f"✅ Tabela encontrada: {f.name} | Caminho: {f.path}")
        
except Exception as e:
    print(f"Erro na validação: {e}")