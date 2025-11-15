# agents/base_agent.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from config import Config


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the SAS Agentic Lineage Builder.

    Responsibilities:
    - Hold shared dependencies (config, spark, state_manager, llm_client)
    - Provide a standard 'execute' interface to be implemented by subclasses
    - Provide helper methods for logging and error handling
    """

    def __init__(
        self,
        config: Config,
        spark: SparkSession,
        state_manager: Any,
        llm_client: Optional[Any] = None,
        workflow_id: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        config : Config
            Global configuration object.
        spark : SparkSession
            Active Spark session.
        state_manager : Any
            Object responsible for workflow state persistence.
            Expected to implement methods like:
            - create_workflow
            - update_stage_status
            - save_checkpoint
            - load_checkpoint
        llm_client : Any, optional
            LLM client wrapper; not all agents will need it.
        workflow_id : str, optional
            Identifier for the current workflow run.
        """
        self.config = config
        self.spark = spark
        self.state_manager = state_manager
        self.llm_client = llm_client
        self.workflow_id = workflow_id

    @abstractmethod
    def execute(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Main entry point for the agent.

        Subclasses must implement this method.

        Returns
        -------
        Dict[str, Any]
            A dictionary summarizing the execution (e.g. status, metrics).
        """
        raise NotImplementedError

    def log_execution(
        self,
        stage: str,
        status: str,
        error_message: Optional[str] = None,
        extra_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log the execution status of a given stage to the workflow_state table
        via the state_manager.

        For now this is just a thin wrapper around state_manager.update_stage_status
        and state_manager.save_checkpoint (we can refine the details later).

        Parameters
        ----------
        stage : str
            Logical name of the stage (e.g. 'PARSE', 'RESOLVE').
        status : str
            Execution status (e.g. 'STARTED', 'SUCCESS', 'FAILED').
        error_message : str, optional
            Error details if any.
        extra_info : dict, optional
            Additional metadata to store as checkpoint_data or similar.
        """
        if self.state_manager is None or self.workflow_id is None:
            # In modalità minimale, se non abbiamo state_manager/workflow_id non facciamo nulla
            return

        try:
            # Aggiorna lo stato base
            self.state_manager.update_stage_status(
                workflow_id=self.workflow_id,
                stage=stage,
                status=status,
                error_message=error_message,
            )

            # Se c'è extra_info, la salviamo come checkpoint (serializzazione la definiremo dopo)
            if extra_info is not None:
                self.state_manager.save_checkpoint(
                    workflow_id=self.workflow_id,
                    stage=stage,
                    data=extra_info,
                )
        except Exception:
            # In base agent non vogliamo esplodere per problemi di logging.
            # Eventuale logging su stdout/log4j potrà essere aggiunto più avanti.
            pass

    def handle_error(
        self,
        stage: str,
        error: Exception,
        extra_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Standardized error handling for agents.

        - Log the error via log_execution
        - Return a structured error payload that callers can use

        Parameters
        ----------
        stage : str
            Logical name of the stage where the error occurred.
        error : Exception
            The exception raised.
        extra_info : dict, optional
            Additional context to log.

        Returns
        -------
        Dict[str, Any]
            A standardized error dict with message and type.
        """
        error_message = f"{type(error).__name__}: {str(error)}"

        print(f"[BaseAgent.handle_error] stage={stage}, error_type={type(error).__name__}")
        print(f"[BaseAgent.handle_error] error_message={error_message}")

        # Log sul workflow_state tramite state_manager (se disponibile)
        self.log_execution(
            stage=stage,
            status="FAILED",
            error_message=error_message,
            extra_info=extra_info,
        )

        # Ritorno di un dizionario standard che gli orchestrator possono usare
        return {
            "status": "error",
            "stage": stage,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "extra_info": extra_info or {},
        }