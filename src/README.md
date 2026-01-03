# 🏎️ F1 Lakehouse Analytics

Projeto de Engenharia de Dados construindo um Lakehouse completo (Bronze, Silver, Gold) utilizando Azure Databricks e Spark.

## 📅 Diário de Bordo - Dia 1: Ingestão de Dados (Bronze)

**Objetivo:** Consumir dados da F1 via API pública e armazenar no Data Lake (Azure Gen2).

**O que foi feito:**

- Configuração do ambiente Azure (Resource Group, Storage Account, Databricks Cluster).
- Desenvolvimento de script de ingestão em Python/PySpark.
- Integração com a **Jolpica API** (substituindo a Ergast para maior estabilidade).
- Implementação de gravação no formato JSON na camada Bronze.

**Desafios:**

- A API original (Ergast) estava instável/descontinuada, migrei para Jolpica.
- Configuração de acesso (Mounting) do Spark ao Azure Blob Storage.

**Stack Tecnológica:** Python, Spark, Azure Databricks, REST APIs.
