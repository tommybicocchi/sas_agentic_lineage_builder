# agents/parser_agent.py

from typing import Any, Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import (
    StringType,
    ArrayType,
    BooleanType,
    StructType,
    StructField,
)

from config import Config
from agents.base_agent import BaseAgent
from utils import delta_helpers
from utils import sas_parser
from pyspark.sql.utils import AnalysisException


class ParserAgent(BaseAgent):
    """
    Agent responsabile della parsificazione statica dei file SAS.

    Compiti (secondo specifica):
      - Leggere i file .sas da un volume Unity Catalog.
      - Estrarre dipendenze sintattiche (input/output datasets, macro calls, include files, ecc.)
      - Scrivere i risultati nella tabella Delta 'raw_dependencies'.

    In questo scheletro:
      - L'estrazione delle dipendenze è ancora un placeholder.
      - Usiamo una logica minimale per popolare 'raw_dependencies' con colonne base.
    """

    def __init__(
        self,
        config: Config,
        spark: SparkSession,
        state_manager: Any,
        workflow_id: str,
    ) -> None:
        super().__init__(
            config=config,
            spark=spark,
            state_manager=state_manager,
            llm_client=None,  # Parser non usa LLM
            workflow_id=workflow_id,
        )

        self.unity = config.unity
        self.tables = config.tables

    def execute(self, volume_path: str) -> Dict[str, Any]:
        """
        Entry point principale dell'agent.

        Parameters
        ----------
        volume_path : str
            Percorso del volume che contiene i file .sas.

        Returns
        -------
        dict
            Informazioni di sintesi sull'esecuzione (status, numero file, ecc.).
        """
        stage = "PARSE"
        print(f"[ParserAgent.execute] START, volume_path={volume_path}")

        try:
            self.log_execution(stage=stage, status="STARTED")

            result = self.parse_repository(volume_path=volume_path)
            print(f"[ParserAgent.execute] parse_repository result = {result}")

            # log di successo con info sintetiche
            self.log_execution(
                stage=stage,
                status="SUCCESS",
                extra_info={"volume_path": volume_path, "summary": result},
            )

            print("[ParserAgent.execute] END SUCCESS")
            return {
                "status": "success",
                "stage": stage,
                "details": result,
            }

        except Exception as e:
            # usa lo standard di BaseAgent
            print(f"[ParserAgent.execute] EXCEPTION: {type(e).__name__} - {e}")
            return self.handle_error(stage=stage, error=e)

    def parse_repository(self, volume_path: str) -> Dict[str, Any]:
        """
        Legge tutti i file .sas sotto volume_path, estrae le dipendenze
        e scrive la tabella raw_dependencies.

        Questa versione:
          - usa dbutils.fs.ls per trovare i file .sas (no glob **),
          - gestisce volume/directory vuota senza eccezioni.
        """
        print(f"[ParserAgent.parse_repository] volume_path = {volume_path}")

        # 1) Troviamo tutti i file .sas sotto volume_path usando dbutils
        try:
            entries = dbutils.fs.ls(volume_path)
        except Exception as e:
            # directory non esistente o non accessibile
            print("[ParserAgent.parse_repository] dbutils.fs.ls failed:", e)
            return {
                "num_files": 0,
                "message": f"No .sas files found (path not found or unreadable: {volume_path})",
            }

        sas_files = []
        for e in entries:
            if e.isDir():
                # opzionale: si potrebbe ricorsivamente scendere nelle sottodirectory
                # per ora ignoriamo sottodirectory per semplicità
                continue
            if e.path.lower().endswith(".sas"):
                sas_files.append(e.path)

        print(f"[ParserAgent.parse_repository] Found {len(sas_files)} .sas files")

        if not sas_files:
            return {"num_files": 0, "message": "No .sas files found"}

        # 2) Leggiamo i file .sas trovati
        try:
            df = (
                self.spark.read.text(sas_files)
                .withColumn("file_path", input_file_name())
                .withColumn("file_name", col("file_path"))
            )
        except AnalysisException as e:
            print("[ParserAgent.parse_repository] AnalysisException during read:", e)
            return {
                "num_files": 0,
                "message": f"Failed to read .sas files: {str(e)}",
            }

        # 3) Applichiamo la UDF di estrazione dipendenze
        extract_udf = self._get_extract_dependencies_udf()

        parsed_df = df.withColumn(
            "parsed",
            extract_udf(col("value")),
        )

        result_df = (
            parsed_df
            .withColumn("input_datasets", col("parsed.input_datasets"))
            .withColumn("output_datasets", col("parsed.output_datasets"))
            .withColumn("macro_calls", col("parsed.macro_calls"))
            .withColumn("include_files", col("parsed.include_files"))
            .withColumn("has_dynamic_deps", col("parsed.has_dynamic_deps"))
            .withColumn("proc_types", col("parsed.proc_types"))
            .drop("parsed")
        )

        result_df = result_df.withColumn("created_at", lit(None).cast("timestamp"))

        print("[ParserAgent.parse_repository] Writing to raw_dependencies")

        delta_helpers.write_dataframe(
            df=result_df,
            table_name=self.tables.RAW_DEPENDENCIES,
            unity_config=self.unity,
            mode="append",
            table_config=self.tables,
        )

        num_files = result_df.select("file_path").distinct().count()
        print(f"[ParserAgent.parse_repository] Done, num_files = {num_files}")

        return {
            "num_files": num_files,
            "message": "Parsed with sas_parser on explicit .sas file list",
        }

    def parse_files(self, sas_files: list[str]) -> Dict[str, Any]:
        """
        Esegue il parsing su una lista esplicita di path di file .sas.

        Parameters
        ----------
        sas_files : list[str]

        Returns
        -------
        dict
            Statistiche minime: numero di file processati.
        """
        if not sas_files:
            return {"num_files": 0, "message": "No .sas files provided"}

        print(f"[ParserAgent.parse_files] Parsing {len(sas_files)} files")

        try:
            df = (
                self.spark.read.text(sas_files)
                .withColumn("file_path", col("_metadata.file_path"))
                .withColumn("file_name", col("file_path"))
            )
        except AnalysisException as e:
            print("[ParserAgent.parse_files] AnalysisException during read:", e)
            return {
                "num_files": 0,
                "message": f"Failed to read .sas files: {str(e)}",
            }

        # UDF per estrarre le dipendenze
        extract_udf = self._get_extract_dependencies_udf()

        parsed_df = df.withColumn(
            "parsed",
            extract_udf(col("value")),
        )

        result_df = (
            parsed_df
            .withColumn("input_datasets", col("parsed.input_datasets"))
            .withColumn("output_datasets", col("parsed.output_datasets"))
            .withColumn("macro_calls", col("parsed.macro_calls"))
            .withColumn("include_files", col("parsed.include_files"))
            .withColumn("has_dynamic_deps", col("parsed.has_dynamic_deps"))
            .withColumn("proc_types", col("parsed.proc_types"))
            .drop("parsed")
            .drop("value")
        )

        result_df = result_df.withColumn("created_at", lit(None).cast("timestamp"))

        print("[ParserAgent.parse_files] Writing to raw_dependencies")

        delta_helpers.write_dataframe(
            df=result_df,
            table_name=self.tables.RAW_DEPENDENCIES,
            unity_config=self.unity,
            mode="append",
            table_config=self.tables,
        )

        num_files = result_df.select("file_path").distinct().count()
        print(f"[ParserAgent.parse_files] Done, num_files = {num_files}")

        return {
            "num_files": num_files,
            "message": "Parsed with sas_parser on explicit file list",
        }

    def extract_dependencies(self, code: str) -> Dict[str, Any]:
        """
        Estrae le dipendenze da uno snippet di codice SAS usando utils.sas_parser.

        Parameters
        ----------
        code : str

        Returns
        -------
        dict
            Dict con le chiavi:
              - input_datasets: List[str]
              - output_datasets: List[str]
              - macro_calls: List[str]
              - include_files: List[str]
              - has_dynamic_deps: bool
              - proc_types: List[str]
        """
        return sas_parser.parse_dependencies(code or "")

    def _get_extract_dependencies_udf(self):
        """
        Crea una UDF Spark che wrappa extract_dependencies(code: str) -> dict,
        restituendo una struct con i campi necessari.

        In questa versione, usa il placeholder di extract_dependencies.
        """

        schema = StructType(
            [
                StructField("input_datasets", ArrayType(StringType()), nullable=True),
                StructField("output_datasets", ArrayType(StringType()), nullable=True),
                StructField("macro_calls", ArrayType(StringType()), nullable=True),
                StructField("include_files", ArrayType(StringType()), nullable=True),
                StructField("has_dynamic_deps", BooleanType(), nullable=True),
                StructField("proc_types", ArrayType(StringType()), nullable=True),
            ]
        )

        def _udf_impl(code: str) -> Dict[str, Any]:
            return self.extract_dependencies(code or "")

        return udf(_udf_impl, schema)