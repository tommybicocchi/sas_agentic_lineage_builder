"""
Central configuration for SAS Agentic Lineage Builder
"""
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class UnityConfig:
    """Unity Catalog configuration"""
    catalog_name: str = "demo"
    schema_name: str = "lineage_data"
    volume_name: str = "sas_files"
    
    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog_name}/{self.schema_name}/{self.volume_name}"
    
    @property
    def full_schema(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}"
    
    def setup_catalog(self, spark) -> dict:
        """Create catalog if not exists"""
        try:
            catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
            
            if self.catalog_name not in catalogs:
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
                return {"status": "created", "message": f"Catalog '{self.catalog_name}' created"}
            else:
                return {"status": "exists", "message": f"Catalog '{self.catalog_name}' already exists"}
        except Exception as e:
            return {"status": "error", "message": f"Error: {str(e)}"}
    
    def setup_schema(self, spark) -> dict:
        """Create schema if not exists"""
        try:
            catalog_result = self.setup_catalog(spark)
            if catalog_result["status"] == "error":
                return catalog_result
            
            spark.sql(f"USE CATALOG {self.catalog_name}")
            schemas = [row.databaseName for row in spark.sql("SHOW SCHEMAS").collect()]
            
            if self.schema_name not in schemas:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_schema}")
                return {"status": "created", "message": f"Schema '{self.full_schema}' created"}
            else:
                return {"status": "exists", "message": f"Schema '{self.full_schema}' already exists"}
        except Exception as e:
            return {"status": "error", "message": f"Error: {str(e)}"}
    
    def setup_volume(self, spark) -> dict:
        """Create volume if not exists"""
        try:
            schema_result = self.setup_schema(spark)
            if schema_result["status"] == "error":
                return schema_result
            
            spark.sql(f"USE CATALOG {self.catalog_name}")
            spark.sql(f"USE SCHEMA {self.schema_name}")
            
            volumes = [row.volume_name for row in spark.sql("SHOW VOLUMES").collect()]
            
            if self.volume_name not in volumes:
                spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.catalog_name}.{self.schema_name}.{self.volume_name}")
                return {"status": "created", "message": f"Volume created at {self.volume_path}"}
            else:
                return {"status": "exists", "message": f"Volume exists at {self.volume_path}"}
        except Exception as e:
            return {"status": "error", "message": f"Error: {str(e)}"}
    
    def setup_all(self, spark) -> dict:
        """Setup catalog, schema, and volume"""
        results = {
            "catalog": self.setup_catalog(spark),
            "schema": self.setup_schema(spark),
            "volume": self.setup_volume(spark)
        }
        
        has_error = any(r["status"] == "error" for r in results.values())
        results["overall_status"] = "error" if has_error else "success"
        
        return results


@dataclass
class TableConfig:
    """Delta table definitions"""
    
    # Table names
    RAW_DEPENDENCIES: str = "raw_dependencies"
    RESOLVED_DEPENDENCIES: str = "resolved_dependencies"
    EXPANDED_INCLUDES: str = "expanded_includes"
    DATASET_CLASSIFICATIONS: str = "dataset_classifications"
    LINEAGE_EDGES: str = "lineage_edges"
    LINEAGE_NODES: str = "lineage_nodes"
    FILE_METADATA: str = "file_metadata"
    WORKFLOW_STATE: str = "workflow_state"
    
    def get_full_table_name(self, table_name: str, unity_config: UnityConfig) -> str:
        """Get fully qualified table name"""
        return f"{unity_config.full_schema}.{table_name}"
    
    def get_all_tables(self) -> List[str]:
        """Get list of all table names"""
        return [
            self.RAW_DEPENDENCIES,
            self.RESOLVED_DEPENDENCIES,
            self.EXPANDED_INCLUDES,
            self.DATASET_CLASSIFICATIONS,
            self.LINEAGE_EDGES,
            self.LINEAGE_NODES,
            self.FILE_METADATA,
            self.WORKFLOW_STATE
        ]
    
    def get_table_schemas(self) -> Dict[str, str]:
        """Get DDL for all tables"""
        return {
            self.RAW_DEPENDENCIES: """
                file_path STRING,
                file_name STRING,
                input_datasets ARRAY<STRING>,
                output_datasets ARRAY<STRING>,
                macro_calls ARRAY<STRING>,
                include_files ARRAY<STRING>,
                has_dynamic_deps BOOLEAN,
                proc_types ARRAY<STRING>,
                created_at TIMESTAMP
            """,
            
            self.RESOLVED_DEPENDENCIES: """
                file_path STRING,
                original_inputs ARRAY<STRING>,
                original_outputs ARRAY<STRING>,
                resolved_inputs ARRAY<STRING>,
                resolved_outputs ARRAY<STRING>,
                resolution_confidence FLOAT,
                macro_definitions MAP<STRING, STRING>,
                resolution_notes STRING,
                created_at TIMESTAMP
            """,
            
            self.EXPANDED_INCLUDES: """
                file_path STRING,
                include_file STRING,
                resolved_path STRING,
                include_content STRING,
                expansion_level INT,
                created_at TIMESTAMP
            """,
            
            self.DATASET_CLASSIFICATIONS: """
                dataset_name STRING,
                business_domain STRING,
                data_category STRING,
                sensitivity_level STRING,
                description STRING,
                unity_catalog_match STRING,
                match_confidence FLOAT,
                created_at TIMESTAMP
            """,
            
            self.LINEAGE_EDGES: """
                source_node STRING,
                target_node STRING,
                edge_type STRING,
                confidence FLOAT,
                file_path STRING,
                created_at TIMESTAMP
            """,
            
            self.LINEAGE_NODES: """
                node_id STRING,
                node_type STRING,
                node_name STRING,
                in_degree INT,
                out_degree INT,
                betweenness_centrality FLOAT,
                pagerank FLOAT,
                created_at TIMESTAMP
            """,
            
            self.FILE_METADATA: """
                file_path STRING,
                file_name STRING,
                file_size_bytes BIGINT,
                line_count INT,
                last_modified TIMESTAMP,
                created_at TIMESTAMP
            """,
            
            self.WORKFLOW_STATE: """
                workflow_id STRING,
                stage STRING,
                status STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message STRING,
                checkpoint_data STRING,
                created_at TIMESTAMP
            """
        }
    
    def create_table(self, spark, table_name: str, unity_config: UnityConfig) -> dict:
        """Create a single table"""
        try:
            full_name = self.get_full_table_name(table_name, unity_config)
            schema = self.get_table_schemas()[table_name]
            
            ddl = f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    {schema}
                )
                USING DELTA
            """
            
            spark.sql(ddl)
            return {"status": "success", "table": full_name, "message": f"Table {full_name} created"}
        except Exception as e:
            return {"status": "error", "table": table_name, "message": str(e)}
    
    def create_all_tables(self, spark, unity_config: UnityConfig) -> dict:
        """Create all tables"""
        results = {}
        
        for table_name in self.get_all_tables():
            results[table_name] = self.create_table(spark, table_name, unity_config)
        
        has_error = any(r["status"] == "error" for r in results.values())
        results["overall_status"] = "error" if has_error else "success"
        
        return results
    
    def drop_table(self, spark, table_name: str, unity_config: UnityConfig) -> dict:
        """Drop a single table"""
        try:
            full_name = self.get_full_table_name(table_name, unity_config)
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            return {"status": "success", "table": full_name, "message": f"Table {full_name} dropped"}
        except Exception as e:
            return {"status": "error", "table": table_name, "message": str(e)}
    
    def drop_all_tables(self, spark, unity_config: UnityConfig) -> dict:
        """Drop all tables"""
        results = {}
        
        for table_name in self.get_all_tables():
            results[table_name] = self.drop_table(spark, table_name, unity_config)
        
        has_error = any(r["status"] == "error" for r in results.values())
        results["overall_status"] = "error" if has_error else "success"
        
        return results


@dataclass
class LLMConfig:
    """LLM configuration"""
    model_name: str = "databricks-meta-llama-3-1-70b-instruct"
    temperature: float = 0.1
    max_tokens: int = 4000
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class Config:
    """Main configuration container"""
    unity: UnityConfig = None
    tables: TableConfig = None
    llm: LLMConfig = None
    
    def __post_init__(self):
        if self.unity is None:
            self.unity = UnityConfig()
        if self.tables is None:
            self.tables = TableConfig()
        if self.llm is None:
            self.llm = LLMConfig()
    
    def setup_all(self, spark) -> dict:
        """Setup everything: catalog, schema, volume, and tables"""
        # Setup Unity Catalog
        unity_results = self.unity.setup_all(spark)
        
        if unity_results["overall_status"] == "error":
            return unity_results
        
        # Create tables
        table_results = self.tables.create_all_tables(spark, self.unity)
        
        return {
            "unity": unity_results,
            "tables": table_results,
            "overall_status": table_results["overall_status"]
        }