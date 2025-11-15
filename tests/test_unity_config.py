# Databricks notebook source
# MAGIC %md
# MAGIC # Test UnityConfig

# COMMAND ----------

# Import
from config import UnityConfig

# COMMAND ----------

# Test 1: Inizializzazione
config = UnityConfig()
print(f"✅ Catalog: {config.catalog_name}")
print(f"✅ Schema: {config.schema_name}")
print(f"✅ Volume: {config.volume_name}")

# COMMAND ----------

# Test 2: Paths
print(f"Volume path: {config.volume_path}")
print(f"Full schema: {config.full_schema}")

# COMMAND ----------

# Test 3: Setup tutto
results = config.setup_all(spark)

print(f"Catalog: {results['catalog']['status']}")
print(f"Schema: {results['schema']['status']}")
print(f"Volume: {results['volume']['status']}")
print(f"\nStatus: {results['overall_status']}")