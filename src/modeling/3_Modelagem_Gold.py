# Databricks notebook source
# MAGIC %md
# MAGIC # 🌟 Camada Gold: Modelagem Dimensional (Star Schema)
# MAGIC
# MAGIC Nesta etapa, transformamos os dados técnicos da camada Silver em um modelo de negócios otimizado para Analytics e Power BI.
# MAGIC
# MAGIC **Arquitetura do Data Warehouse:**
# MAGIC Utilizamos o esquema **Star Schema**, composto por:
# MAGIC 1.  **Tabelas Dimensão (`dim_`)**: Contêm os atributos descritivos (Quem? Onde? O que?).
# MAGIC     * `dim_drivers`: Perfil dos pilotos.
# MAGIC     * `dim_constructors`: Perfil das equipes.
# MAGIC     * `dim_races`: Calendário e locais das corridas.
# MAGIC 2.  **Tabela Fato (`fact_`)**: Contém as métricas de negócio e chaves estrangeiras.
# MAGIC     * `fact_results`: Resultados, pontos e classificações de cada corrida.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💼 Visão de Negócios: Transferência de Conhecimento
# MAGIC
# MAGIC Embora este projeto utilize dados de Fórmula 1, a arquitetura **Star Schema** aplicada aqui resolve problemas idênticos aos do setor de Varejo/Assinaturas.
# MAGIC
# MAGIC **Matriz de Equivalência:**
# MAGIC
# MAGIC | Conceito F1 (Neste Projeto) | Conceito Varejo (Aplicação Real) | Problema de Negócio Resolvido |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Tabela Fato (Resultados)** | **Fato Vendas / Assinaturas** | "Qual a receita gerada por transação?" |
# MAGIC | **Dimensão Piloto** | **Dimensão Cliente (Pet/Tutor)** | "Quem são nossos clientes mais fiéis (LTV)?" |
# MAGIC | **Dimensão Equipe (Construtor)** | **Dimensão Marca/Categoria** | "Qual marca de ração vende mais?" |
# MAGIC | **Status "Retired/Collision"** | **Churn (Cancelamento)** | "Por que o cliente parou de comprar?" |

# COMMAND ----------

from pyspark.sql.functions import col, explode, current_timestamp, input_file_name, concat, lit

# --- CONFIGURAÇÃO DE CAMINHOS ---
# PASSO 1: CONFIGURAÇÃO DE ACESSO AO AZURE
# Preencha com seus dados do Azure 
storage_account_name = "f1datalakecarol2026"
storage_account_key = "INSIRA_SUA_CHAVE_AQUI"

# Configura o Spark (Mounting)
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

container_silver = f"abfss://silver@{storage_account_name}.dfs.core.windows.net"
container_gold = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

print("🚀 Iniciando Processamento GOLD...")



# COMMAND ----------

# 2. LEITURA DOS DADOS DA CAMADA SILVER
print("📂 Lendo dados da Silver...")
df_drivers = spark.read.format("delta").load(f"{container_silver}/drivers")
df_constructors = spark.read.format("delta").load(f"{container_silver}/constructors")
df_results = spark.read.format("delta").load(f"{container_silver}/results")

# COMMAND ----------

# 3. CRIAÇÃO DAS DIMENSÕES (Quem? Onde? Quando?)
# --- DIM_DRIVERS (Quem é o piloto?) ---
print("🏎️ Criando Dimensão: dim_drivers")
df_dim_drivers = df_drivers.select(
    col("driver_id"),
    col("driver_name"),
    col("driver_surname"),
    col("driver_nationality"),
    col("driver_dob"),
    col("driver_number")
).distinct()

df_dim_drivers.write.format("delta").mode("overwrite").save(f"{container_gold}/dim_drivers")

# COMMAND ----------

# --- DIM_CONSTRUCTORS (Qual é a equipe?) ---
print("🔧 Criando Dimensão: dim_constructors")
df_dim_constructors = df_constructors.select(
    col("team_id"),
    col("team_name"),
    col("team_nationality")
).distinct()

df_dim_constructors.write.format("delta").mode("overwrite").save(f"{container_gold}/dim_constructors")


# COMMAND ----------

# --- DIM_RACES (Onde e quando foi a corrida?) ---
# Nota: Criamos essa dimensão extraindo os dados únicos de corrida da tabela results
print("🏁 Criando Dimensão: dim_races")

# Criamos uma chave única para a corrida (Ex: "2024-1" para a primeira corrida de 2024)
df_dim_races = df_results.select(

    col("season"),
    col("round"),
    col("race_name"),
    col("race_date"),
    col("circuit_name"),
    col("country")
).distinct().withColumn("race_key", concat(col("season"), lit("-"), col("round")))

df_dim_races.write.format("delta").mode("overwrite").save(f"{container_gold}/dim_races")

# COMMAND ----------

# 4. CRIAÇÃO DA TABELA FATO (O que aconteceu?)
# A tabela fato deve ter apenas IDs (Chaves Estrangeiras) e Números (Métricas)

print("🏆 Criando Tabela Fato: fact_results")

df_fact = df_results.withColumn("race_key", concat(col("season"), lit("-"), col("round"))) \
    .select(
        col("race_key"),     # FK para dim_races
        col("driver_id"),    # FK para dim_drivers
        col("team_id"),      # FK para dim_constructors
        col("position"),     # Métrica
        col("points"),       # Métrica
        col("laps"),         # Métrica
        col("fastest_lap_time"), # Métrica/Atributo
        col("status"),       # Degenerate Dimension (Atributo na Fato)
        current_timestamp().alias("processed_at")
    )

df_fact.write.format("delta").mode("overwrite").save(f"{container_gold}/fact_results")

print("✅ SUCESSO! Modelagem Gold finalizada.")

# COMMAND ----------

# --- VALIDAÇÃO ANALÍTICA (SQL) ---
# Registra tabelas no Spark SQL para consulta
spark.read.format("delta").load(f"{container_gold}/fact_results").createOrReplaceTempView("fact_results")
spark.read.format("delta").load(f"{container_gold}/dim_drivers").createOrReplaceTempView("dim_drivers")
spark.read.format("delta").load(f"{container_gold}/dim_constructors").createOrReplaceTempView("dim_constructors")
spark.read.format("delta").load(f"{container_gold}/dim_races").createOrReplaceTempView("dim_races")

print("📊 Relatório: Top 5 Pilotos com mais pontos")

# Query SQL Clássica de BI
df_report = spark.sql("""
    SELECT 
        d.driver_name,
        d.driver_surname,
        SUM(f.points) as total_pontos
    FROM fact_results f
    JOIN dim_drivers d ON f.driver_id = d.driver_id
    GROUP BY d.driver_name, d.driver_surname
    ORDER BY total_pontos DESC
    LIMIT 5
""")

display(df_report)

# COMMAND ----------

# CRIAÇÃO DOS WIDGETS
# Ajustei a lista de anos apenas para 2024, já que é o único dado que temos carregado
dbutils.widgets.dropdown("ano_selecionado", "2024", ["2024"]) 
dbutils.widgets.dropdown("equipe_foco", "Red Bull", ["Red Bull", "Ferrari", "McLaren", "Mercedes", "Aston Martin"])

# CAPTURA DOS VALORES ESCOLHIDOS
ano = dbutils.widgets.get("ano_selecionado")
equipe = dbutils.widgets.get("equipe_foco")

print(f"📊 Analisando Performance: {equipe} na Temporada {ano}")

# QUERY (Agora vai funcionar porque dim_races foi registrada)
query = f"""
    SELECT 
        d.driver_name,
        r.race_name,
        f.position,
        f.points,
        f.status
    FROM fact_results f
    JOIN dim_drivers d ON f.driver_id = d.driver_id
    JOIN dim_constructors c ON f.team_id = c.team_id
    JOIN dim_races r ON f.race_key = r.race_key
    WHERE c.team_name LIKE '%{equipe}%' 
      AND r.season = {ano}
    ORDER BY f.points DESC
"""

display(spark.sql(query))