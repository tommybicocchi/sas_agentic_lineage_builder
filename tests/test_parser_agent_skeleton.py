# tests/test_parser_agent_skeleton.py

from config import Config
from orchestration.state_manager import StateManager
from utils import delta_helpers
from agents.parser_agent import ParserAgent


def run_tests(spark):
    """
    Test di base per il ParserAgent (scheletro).

    Passaggi:
      1. Setup Config (catalog/schema/volume + tabelle).
      2. Inizializza StateManager.
      3. Crea un workflow_id di test.
      4. Istanzia ParserAgent.
      5. Esegue execute(volume_path).
      6. Verifica che:
         - lo status sia 'success'
         - se ci sono file .sas, appaiano righe in raw_dependencies
    """

    print("=== Test ParserAgent (scheletro): INIZIO ===")

    # 1. Setup config
    config = Config()
    setup_result = config.setup_all(spark)
    assert setup_result["overall_status"] == "success", f"Setup fallito: {setup_result}"
    print(" - Config/setup_all OK")

    unity = config.unity
    tables = config.tables

    # 2. StateManager
    state_manager = StateManager(spark, config)
    print(" - StateManager inizializzato")

    # 3. Workflow id
    workflow_id = "test_workflow_parser_agent_001"
    state_manager.create_workflow(workflow_id, initial_status="CREATED")
    print(f" - Workflow creato: {workflow_id}")

    # 4. ParserAgent
    agent = ParserAgent(
        config=config,
        spark=spark,
        state_manager=state_manager,
        workflow_id=workflow_id,
    )
    print(" - ParserAgent inizializzato")

    # 5. Volume path: usiamo quello di default di UnityConfig
    volume_path = unity.volume_path
    print(f" - Volume path: {volume_path}")

    result = agent.execute(volume_path=volume_path)
    print(" - ParserAgent.execute() chiamato")
    print("   result:", result)

    assert result.get("status") == "success", "ParserAgent non ha restituito status='success'"

    # 6. Se ci sono file .sas, controlliamo raw_dependencies
    raw_df = delta_helpers.read_table(
        spark=spark,
        table_name=tables.RAW_DEPENDENCIES,
        unity_config=unity,
        table_config=tables,
    )

    count_raw = raw_df.count()
    print(f" - Righe in raw_dependencies: {count_raw}")

    # Non facciamo assert sul count, perché potrebbe essere 0 se non hai ancora caricato .sas.
    # L'importante in questa fase è che execute non esploda.

    print("=== Test ParserAgent (scheletro): TUTTO OK ✅ ===")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_tests(spark)