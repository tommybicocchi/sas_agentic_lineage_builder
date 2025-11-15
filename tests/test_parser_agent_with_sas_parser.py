# tests/test_parser_agent_with_sas_parser.py

from config import Config
from orchestration.state_manager import StateManager
from utils import delta_helpers
from agents.parser_agent import ParserAgent


def run_tests(spark):
    """
    Test integrato ParserAgent + sas_parser:
    - crea un DataFrame con un finto file .sas (usando creato inline),
    - scrive il file in un path temporaneo nel volume,
    - esegue ParserAgent.parse_repository su quel path,
    - verifica che raw_dependencies contenga input/output coerenti.
    """

    print("=== Test ParserAgent + sas_parser: INIZIO ===")

    config = Config()
    setup_result = config.setup_all(spark)
    assert setup_result["overall_status"] == "success"
    print(" - Config/setup_all OK")

    unity = config.unity
    tables = config.tables

    state_manager = StateManager(spark, config)
    workflow_id = "test_workflow_parser_agent_sas_parser_001"
    state_manager.create_workflow(workflow_id, initial_status="CREATED")
    print(f" - Workflow creato: {workflow_id}")

    agent = ParserAgent(
        config=config,
        spark=spark,
        state_manager=state_manager,
        workflow_id=workflow_id,
    )
    print(" - ParserAgent inizializzato")

    # 1. Creiamo un file .sas di test nel volume
    volume_base = unity.volume_path  # es: /Volumes/demo/lineage_data/sas_files
    test_dir = f"{volume_base}/unit_test_parser_agent"
    test_file_path = f"{test_dir}/test_sample.sas"

    print(f" - Creazione directory: {test_dir}")
    dbutils.fs.mkdirs(test_dir)

    sas_code = """
    %macro mymacro(param);
      %put &=param;
    %mend;

    data work.out_ds;
      set work.in_ds1 work.in_ds2;
    run;

    proc sort data=work.out_ds out=work.sorted_ds;
      by id;
    run;
    """

    print(f" - Scrittura file SAS: {test_file_path}")
    dbutils.fs.put(test_file_path, sas_code, overwrite=True)

    # 2. Eseguiamo ParserAgent.parse_repository sul path specifico (non sul volume root)
    result = agent.parse_repository(volume_path=test_dir)
    print(" - ParserAgent.parse_repository() chiamato")
    print("   result:", result)

    # 3. Leggiamo raw_dependencies e verifichiamo
    raw_df = delta_helpers.read_table(
        spark=spark,
        table_name=tables.RAW_DEPENDENCIES,
        unity_config=unity,
        table_config=tables,
    )

    from pyspark.sql.functions import col

    rows = raw_df.filter(col("file_path").endswith("test_sample.sas")).collect()
    assert len(rows) == 1, f"Mi aspetto 1 riga per test_sample.sas, trovate {len(rows)}"

    row = rows[0]
    print("   raw_dependencies row:", row)

    inputs = row["input_datasets"]
    outputs = row["output_datasets"]
    proc_types = row["proc_types"]

    assert "work.in_ds1" in inputs
    assert "work.in_ds2" in inputs
    assert "work.out_ds" in outputs
    assert "work.sorted_ds" in outputs
    assert "sort" in proc_types

    print("=== Test ParserAgent + sas_parser: TUTTO OK ✅ ===")


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    run_tests(spark)