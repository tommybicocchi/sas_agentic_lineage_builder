# tests/test_state_manager.py

from config import Config
from orchestration.state_manager import StateManager
from utils import delta_helpers


def run_tests(spark):
    """
    Esegue una serie di test base sullo StateManager.
    Pensata per essere chiamata direttamente da un notebook Databricks
    o da riga di comando (con Spark inizializzato).
    """

    print("=== Test StateManager: INIZIO ===")

    # 1. Setup config (catalog, schema, volume, tabelle)
    config = Config()
    setup_result = config.setup_all(spark)
    assert setup_result["overall_status"] == "success", f"Setup fallito: {setup_result}"

    print(" - Config/setup_all OK")

    # 2. Inizializza StateManager
    state_manager = StateManager(spark, config)
    print(" - StateManager inizializzato")

    # 3. Crea un workflow di test
    workflow_id = "test_workflow_state_manager_001"
    state_manager.create_workflow(workflow_id, initial_status="CREATED")
    print(f" - Workflow creato: {workflow_id}")

    # 4. Aggiorna stato stage PARSE -> STARTED
    state_manager.update_stage_status(workflow_id, stage="PARSE", status="STARTED")
    print(" - Stage PARSE -> STARTED")

    # 5. Salva checkpoint per PARSE
    checkpoint_data = {"step": 1, "info": "dummy_info"}
    state_manager.save_checkpoint(workflow_id, stage="PARSE", data=checkpoint_data)
    print(" - Checkpoint salvato per PARSE")

    # 6. Aggiorna stato stage PARSE -> SUCCESS
    state_manager.update_stage_status(workflow_id, stage="PARSE", status="SUCCESS")
    print(" - Stage PARSE -> SUCCESS")

    # 7. Carica il checkpoint più recente
    loaded_checkpoint = state_manager.load_checkpoint(workflow_id, stage="PARSE")
    assert loaded_checkpoint is not None, "Checkpoint non trovato"
    assert loaded_checkpoint.get("step") == 1
    assert loaded_checkpoint.get("info") == "dummy_info"
    print(" - Checkpoint caricato e verificato")

    # 8. Verifica righe in workflow_state
    unity = config.unity
    tables = config.tables

    ws_df = delta_helpers.read_table(
        spark=spark,
        table_name=tables.WORKFLOW_STATE,
        unity_config=unity,
        table_config=tables,
    )

    workflow_rows = ws_df.filter(ws_df.workflow_id == workflow_id).collect()
    assert len(workflow_rows) >= 4, "Mi aspetto almeno 4 righe (INIT/CREATED, STARTED, CHECKPOINT, SUCCESS)"

    statuses = {row["status"] for row in workflow_rows}
    # Ci aspettiamo almeno questi
    assert "STARTED" in statuses, f"Status STARTED non trovato: {statuses}"
    assert "CHECKPOINT" in statuses, f"Status CHECKPOINT non trovato: {statuses}"
    assert "SUCCESS" in statuses, f"Status SUCCESS non trovato: {statuses}"

    print(" - workflow_state contiene le righe attese")
    print("=== Test StateManager: TUTTO OK ✅ ===")


if __name__ == "__main__":
    # Questo ramo serve se un giorno lo esegui come script Python classico.
    # In Databricks, di solito userai run_tests(spark) da un notebook.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_tests(spark)