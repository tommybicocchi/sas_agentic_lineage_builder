# tests/test_base_agent_dummy.py

from config import Config
from orchestration.state_manager import StateManager
from utils import delta_helpers
from tests.dummy_agent import DummyAgent


def run_tests(spark):
    """
    Test di integrazione BaseAgent + DummyAgent + StateManager.

    Passaggi:
      1. Setup Config (catalog/schema/volume + tabelle).
      2. Inizializza StateManager.
      3. Crea un workflow_id di test.
      4. Istanzia DummyAgent con workflow_id.
      5. Chiama execute().
      6. Verifica che in workflow_state compaiano righe con stage='DUMMY'.
    """

    print("=== Test BaseAgent + DummyAgent: INIZIO ===")

    # 1. Setup config
    config = Config()
    setup_result = config.setup_all(spark)
    assert setup_result["overall_status"] == "success", f"Setup fallito: {setup_result}"
    print(" - Config/setup_all OK")

    # 2. StateManager
    state_manager = StateManager(spark, config)
    print(" - StateManager inizializzato")

    # 3. Crea workflow_id di test
    workflow_id = "test_workflow_dummy_agent_001"
    state_manager.create_workflow(workflow_id, initial_status="CREATED")
    print(f" - Workflow creato: {workflow_id}")

    # 4. Istanzia DummyAgent
    agent = DummyAgent(
        config=config,
        spark=spark,
        state_manager=state_manager,
        workflow_id=workflow_id,
    )
    print(" - DummyAgent inizializzato")

    # 5. Esegui agent
    result = agent.execute()
    print(" - DummyAgent.execute() chiamato")
    print("   result:", result)

    assert result.get("status") == "success", "DummyAgent non ha restituito status='success'"

    # 6. Verifica righe in workflow_state per stage='DUMMY'
    unity = config.unity
    tables = config.tables

    ws_df = delta_helpers.read_table(
        spark=spark,
        table_name=tables.WORKFLOW_STATE,
        unity_config=unity,
        table_config=tables,
    )

    rows = ws_df.filter(ws_df.workflow_id == workflow_id).collect()
    assert len(rows) > 0, "Nessuna riga trovata per questo workflow_id in workflow_state"

    dummy_rows = [r for r in rows if r["stage"] == "DUMMY"]
    assert len(dummy_rows) >= 2, "Mi aspetto almeno due righe per stage='DUMMY' (STARTED, SUCCESS o CHECKPOINT)"

    statuses = {r["status"] for r in dummy_rows}
    assert "STARTED" in statuses, f"Status STARTED non trovato per DUMMY: {statuses}"
    assert "SUCCESS" in statuses, f"Status SUCCESS non trovato per DUMMY: {statuses}"

    print(" - workflow_state contiene le righe attese per stage='DUMMY'")
    print("=== Test BaseAgent + DummyAgent: TUTTO OK ✅ ===")


if __name__ == "__main__":
    # Esecuzione diretta come script Python (utile fuori da Databricks)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    run_tests(spark)