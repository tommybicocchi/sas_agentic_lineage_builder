# Databricks notebook source
# MAGIC %md
# MAGIC # Test TableConfig

# COMMAND ----------

from config import UnityConfig, TableConfig, Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: TableConfig initialization

# COMMAND ----------

table_config = TableConfig()

print("📋 All tables:")
for table in table_config.get_all_tables():
    print(f"  - {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Full table names

# COMMAND ----------

unity_config = UnityConfig()
table_config = TableConfig()

print("📋 Full table names:")
for table in table_config.get_all_tables():
    full_name = table_config.get_full_table_name(table, unity_config)
    print(f"  - {full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Create all tables

# COMMAND ----------

unity_config = UnityConfig()
table_config = TableConfig()

# Setup Unity Catalog first
unity_results = unity_config.setup_all(spark)
print(f"Unity setup: {unity_results['overall_status']}")

# Create tables
results = table_config.create_all_tables(spark, unity_config)

print("\n" + "="*60)
print("TABLE CREATION RESULTS:")
print("="*60)

for table_name, result in results.items():
    if table_name != "overall_status":
        status_icon = "✅" if result["status"] == "success" else "❌"
        print(f"{status_icon} {table_name}: {result['status']}")

print(f"\nOverall Status: {results['overall_status']}")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Verify tables exist

# COMMAND ----------

spark.sql("USE CATALOG demo")
spark.sql("USE SCHEMA lineage_data")

print("📊 Tables in demo.lineage_data:")
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Complete setup with Config class

# COMMAND ----------

config = Config()
results = config.setup_all(spark)

print("\n" + "="*60)
print("COMPLETE SETUP RESULTS:")
print("="*60)
print(f"Unity Catalog: {results['unity']['overall_status']}")
print(f"Tables: {results['tables']['overall_status']}")
print(f"\nOverall: {results['overall_status']}")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Success!
# MAGIC 
# MAGIC All tables created:
# MAGIC - ✅ raw_dependencies
# MAGIC - ✅ resolved_dependencies
# MAGIC - ✅ expanded_includes
# MAGIC - ✅ dataset_classifications
# MAGIC - ✅ lineage_edges
# MAGIC - ✅ lineage_nodes
# MAGIC - ✅ file_metadata
# MAGIC - ✅ workflow_state