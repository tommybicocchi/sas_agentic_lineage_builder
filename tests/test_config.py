# Databricks notebook source
# MAGIC %md
# MAGIC # Test UnityConfig

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# Test 1: Default initialization
print("Test 1: Default initialization")
config = UnityConfig()

assert config.catalog_name == "demo", f"Expected 'demo', got '{config.catalog_name}'"
assert config.schema_name == "lineage_data", f"Expected 'lineage_data', got '{config.schema_name}'"
assert config.volume_name == "sas_files", f"Expected 'sas_files', got '{config.volume_name}'"

print("✅ PASSED")
print(f"   catalog_name: {config.catalog_name}")
print(f"   schema_name: {config.schema_name}")
print(f"   volume_name: {config.volume_name}")

# COMMAND ----------

# Test 2: Custom initialization
print("Test 2: Custom initialization")
config = UnityConfig(
    catalog_name="custom_catalog",
    schema_name="custom_schema",
    volume_name="custom_volume"
)

assert config.catalog_name == "custom_catalog"
assert config.schema_name == "custom_schema"
assert config.volume_name == "custom_volume"

print("✅ PASSED")

# COMMAND ----------

# Test 3: Volume path default
print("Test 3: Volume path default")
config = UnityConfig()

expected = "/Volumes/demo/lineage_data/sas_files"
actual = config.volume_path

assert actual == expected, f"Expected '{expected}', got '{actual}'"

print("✅ PASSED")
print(f"   volume_path: {actual}")

# COMMAND ----------

# Test 4: Volume path custom
print("Test 4: Volume path custom")
config = UnityConfig(
    catalog_name="my_cat",
    schema_name="my_sch",
    volume_name="my_vol"
)

expected = "/Volumes/my_cat/my_sch/my_vol"
actual = config.volume_path

assert actual == expected, f"Expected '{expected}', got '{actual}'"

print("✅ PASSED")

# COMMAND ----------

# Test 5: Full schema default
print("Test 5: Full schema default")
config = UnityConfig()

expected = "demo.lineage_data"
actual = config.full_schema

assert actual == expected, f"Expected '{expected}', got '{actual}'"

print("✅ PASSED")
print(f"   full_schema: {actual}")

# COMMAND ----------

# Test 6: Full schema custom
print("Test 6: Full schema custom")
config = UnityConfig(
    catalog_name="test_catalog",
    schema_name="test_schema"
)

expected = "test_catalog.test_schema"
actual = config.full_schema

assert actual == expected, f"Expected '{expected}', got '{actual}'"

print("✅ PASSED")

# COMMAND ----------

# Test 7: Setup catalog (REAL)
print("Test 7: Setup catalog")
config = UnityConfig()
result = config.setup_catalog(spark)

print(f"   Status: {result['status']}")
print(f"   Message: {result['message']}")

assert result['status'] in ['created', 'exists'], f"Unexpected status: {result['status']}"

print("✅ PASSED")

# COMMAND ----------

# Test 8: Setup schema (REAL)
print("Test 8: Setup schema")
config = UnityConfig()
result = config.setup_schema(spark)

print(f"   Status: {result['status']}")
print(f"   Message: {result['message']}")

assert result['status'] in ['created', 'exists'], f"Unexpected status: {result['status']}"

print("✅ PASSED")

# COMMAND ----------

# Test 9: Setup volume (REAL)
print("Test 9: Setup volume")
config = UnityConfig()
result = config.setup_volume(spark)

print(f"   Status: {result['status']}")
print(f"   Message: {result['message']}")

assert result['status'] in ['created', 'exists'], f"Unexpected status: {result['status']}"

print("✅ PASSED")

# COMMAND ----------

# Test 10: Setup all
print("Test 10: Setup all")
config = UnityConfig()
results = config.setup_all(spark)

print("\n" + "="*60)
print("SETUP RESULTS:")
print("="*60)
print(f"Catalog: {results['catalog']['status']} - {results['catalog']['message']}")
print(f"Schema:  {results['schema']['status']} - {results['schema']['message']}")
print(f"Volume:  {results['volume']['status']} - {results['volume']['message']}")
print(f"\nOverall Status: {results['overall_status']}")
print("="*60)

assert results['overall_status'] == 'success'

print("\n✅ PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 ALL TESTS PASSED!

# COMMAND ----------

print("""
╔════════════════════════════════════════╗
║   ALL TESTS PASSED ✅                  ║
╚════════════════════════════════════════╝

Created Resources:
  📁 Catalog: demo
  📁 Schema: demo.lineage_data
  📁 Volume: /Volumes/demo/lineage_data/sas_files

Next Step:
  → Proceed to TableConfig
""")