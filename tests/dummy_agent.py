# tests/dummy_agent.py

from typing import Any, Dict

from pyspark.sql import SparkSession

from config import Config
from orchestration.state_manager import StateManager
from agents.base_agent import BaseAgent


class DummyAgent(BaseAgent):
    """
    Agente finto usato solo per testare BaseAgent + StateManager.

    Comportamento di execute():
      - logga STARTED per lo stage 'DUMMY'
      - (opzionale) salva un checkpoint con info banali
      - logga SUCCESS per lo stage 'DUMMY'
      - ritorna un dict con status='success'
    """

    def __init__(
        self,
        config: Config,
        spark: SparkSession,
        state_manager: StateManager,
        workflow_id: str,
    ) -> None:
        super().__init__(
            config=config,
            spark=spark,
            state_manager=state_manager,
            llm_client=None,
            workflow_id=workflow_id,
        )

    def execute(self, *args, **kwargs) -> Dict[str, Any]:
        stage = "DUMMY"
        try:
            # Log di inizio
            self.log_execution(stage=stage, status="STARTED")

            # Eseguiamo qualcosa di banale, ad esempio prepariamo un po' di info
            result_info = {"message": "dummy agent executed", "result": 42}

            # Salviamo un checkpoint (facoltativo, giusto per testare il flusso)
            self.log_execution(
                stage=stage,
                status="CHECKPOINT",
                extra_info={"intermediate": result_info},
            )

            # Log di successo
            self.log_execution(stage=stage, status="SUCCESS")

            return {
                "status": "success",
                "stage": stage,
                "data": result_info,
            }

        except Exception as e:
            # Usa l'helper di BaseAgent per gestire l'errore
            return self.handle_error(stage=stage, error=e)