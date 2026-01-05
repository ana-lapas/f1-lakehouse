# Databricks notebook source
# --- SETUP DO DASHBOARD ---
# Configuração de acesso (Igual aos notebooks anteriores)
storage_account_name = "f1datalakecarol2026"
storage_account_key = "INSIRA_AQUI_SUA_CHAVE_DE_ACESSO"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

container_gold = f"abfss://gold@{storage_account_name}.dfs.core.windows.net"

print("🚀 Carregando tabelas para o SQL...")

# Lendo os arquivos Delta e registrando como Views Temporárias
spark.read.format("delta").load(f"{container_gold}/fact_results").createOrReplaceTempView("fact_results")
spark.read.format("delta").load(f"{container_gold}/dim_drivers").createOrReplaceTempView("dim_drivers")
spark.read.format("delta").load(f"{container_gold}/dim_constructors").createOrReplaceTempView("dim_constructors")
spark.read.format("delta").load(f"{container_gold}/dim_races").createOrReplaceTempView("dim_races")

print("✅ Tabelas registradas! Agora o %sql vai funcionar.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.driver_name, 
# MAGIC     SUM(f.points) as total_pontos
# MAGIC FROM fact_results f
# MAGIC JOIN dim_drivers d ON f.driver_id = d.driver_id
# MAGIC GROUP BY d.driver_name
# MAGIC ORDER BY total_pontos DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.team_name, 
# MAGIC     SUM(f.points) as total_pontos
# MAGIC FROM fact_results f
# MAGIC JOIN dim_constructors c ON f.team_id = c.team_id
# MAGIC GROUP BY c.team_name
# MAGIC HAVING total_pontos > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ROUND((SUM(CASE WHEN f.status LIKE 'Finished' THEN 0 ELSE 1 END) / COUNT(*)) * 100, 2) AS taxa_churn
# MAGIC FROM fact_results f