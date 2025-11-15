# utils/delta_helpers.py

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from config import UnityConfig, TableConfig


def _ensure_catalog_schema(spark: SparkSession, unity_config: UnityConfig) -> None:
    """
    Ensure we are on the correct catalog and schema.
    Non crea nulla: assume che catalog/schema esistano già.
    """
    spark.sql(f"USE CATALOG {unity_config.catalog_name}")
    spark.sql(f"USE SCHEMA {unity_config.schema_name}")


def table_exists(
    spark: SparkSession,
    table_name: str,
    unity_config: UnityConfig,
    table_config: Optional[TableConfig] = None,
) -> bool:
    """
    Check whether a table exists.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
        Logical table name (e.g. 'workflow_state').
    unity_config : UnityConfig
    table_config : TableConfig, optional

    Returns
    -------
    bool
    """
    if table_config is None:
        table_config = TableConfig()

    full_name = table_config.get_full_table_name(table_name, unity_config)
    try:
        _ensure_catalog_schema(spark, unity_config)
        spark.table(full_name)  # raises AnalysisException if not exists
        return True
    except AnalysisException:
        return False


def create_table(
    spark: SparkSession,
    table_name: str,
    unity_config: UnityConfig,
    table_config: Optional[TableConfig] = None,
) -> None:
    """
    Create a Delta table using the DDL defined in TableConfig, if it does not exist.
    """
    if table_config is None:
        table_config = TableConfig()

    schemas = table_config.get_table_schemas()
    if table_name not in schemas:
        raise ValueError(f"Unknown table_name '{table_name}'")

    full_name = table_config.get_full_table_name(table_name, unity_config)
    ddl_schema = schemas[table_name]

    _ensure_catalog_schema(spark, unity_config)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {ddl_schema}
        )
        USING DELTA
        """
    )


def write_dataframe(
    df: DataFrame,
    table_name: str,
    unity_config: UnityConfig,
    mode: str = "append",
    table_config: Optional[TableConfig] = None,
) -> None:
    """
    Write a DataFrame to a Delta table (saveAsTable).
    """
    if table_config is None:
        table_config = TableConfig()

    _ensure_catalog_schema(df.sparkSession, unity_config)
    full_name = table_config.get_full_table_name(table_name, unity_config)

    # opzionale: crea la tabella se non esiste
    if not table_exists(df.sparkSession, table_name, unity_config, table_config):
        create_table(df.sparkSession, table_name, unity_config, table_config)

    df.write.format("delta").mode(mode).saveAsTable(full_name)


def read_table(
    spark: SparkSession,
    table_name: str,
    unity_config: UnityConfig,
    table_config: Optional[TableConfig] = None,
) -> DataFrame:
    """
    Read a Delta table as DataFrame.
    """
    if table_config is None:
        table_config = TableConfig()

    _ensure_catalog_schema(spark, unity_config)
    full_name = table_config.get_full_table_name(table_name, unity_config)
    return spark.table(full_name)