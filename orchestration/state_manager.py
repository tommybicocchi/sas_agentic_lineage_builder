# orchestration/state_manager.py

from typing import Any, Dict, Optional
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from pyspark.sql import Row

from config import Config
from utils import delta_helpers


# Schema esplicito per workflow_state
WORKFLOW_STATE_SCHEMA = StructType(
    [
        StructField("workflow_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("status", StringType(), False),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("checkpoint_data", StringType(), True),
        StructField("created_at", TimestampType(), False),
    ]
)


class StateManager:
    """
    Workflow state persistence using the workflow_state Delta table.
    """

    def __init__(self, spark: SparkSession, config: Config) -> None:
        self.spark = spark
        self.config = config
        self.unity = config.unity
        self.tables = config.tables

        # Assicuriamoci che la tabella workflow_state esista
        delta_helpers.create_table(
            spark=self.spark,
            table_name=self.tables.WORKFLOW_STATE,
            unity_config=self.unity,
            table_config=self.tables,
        )

    def _now(self):
        """Helper to get current timestamp as Python datetime."""
        return datetime.utcnow()

    def _make_df(self, rows: list[dict]):
        """
        Crea un DataFrame a partire da una lista di dict usando
        lo schema esplicito WORKFLOW_STATE_SCHEMA.
        """
        # trasformiamo dict -> lista ordinata secondo schema
        data = []
        for r in rows:
            data.append(
                [
                    r.get("workflow_id"),
                    r.get("stage"),
                    r.get("status"),
                    r.get("start_time"),
                    r.get("end_time"),
                    r.get("error_message"),
                    r.get("checkpoint_data"),
                    r.get("created_at"),
                ]
            )
        return self.spark.createDataFrame(data, schema=WORKFLOW_STATE_SCHEMA)

    def create_workflow(self, workflow_id: str, initial_status: str = "CREATED") -> str:
        """
        Initialize a new workflow entry.
        """
        now = self._now()

        row_dict = {
            "workflow_id": workflow_id,
            "stage": "INIT",
            "status": initial_status,
            "start_time": now,
            "end_time": None,
            "error_message": None,
            "checkpoint_data": None,
            "created_at": now,
        }

        df = self._make_df([row_dict])

        delta_helpers.write_dataframe(
            df=df,
            table_name=self.tables.WORKFLOW_STATE,
            unity_config=self.unity,
            mode="append",
            table_config=self.tables,
        )

        return workflow_id

    def update_stage_status(
        self,
        workflow_id: str,
        stage: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update the status of a given workflow stage.
        """
        now = self._now()

        row_dict = {
            "workflow_id": workflow_id,
            "stage": stage,
            "status": status,
            "start_time": now if status == "STARTED" else None,
            "end_time": now if status in ("SUCCESS", "FAILED") else None,
            "error_message": error_message,
            "checkpoint_data": None,
            "created_at": now,
        }

        df = self._make_df([row_dict])

        delta_helpers.write_dataframe(
            df=df,
            table_name=self.tables.WORKFLOW_STATE,
            unity_config=self.unity,
            mode="append",
            table_config=self.tables,
        )

    def save_checkpoint(
        self,
        workflow_id: str,
        stage: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Save checkpoint data for a given workflow stage.
        """
        now = self._now()
        checkpoint_json = json.dumps(data, default=str)

        row_dict = {
            "workflow_id": workflow_id,
            "stage": stage,
            "status": "CHECKPOINT",
            "start_time": None,
            "end_time": None,
            "error_message": None,
            "checkpoint_data": checkpoint_json,
            "created_at": now,
        }

        df = self._make_df([row_dict])

        delta_helpers.write_dataframe(
            df=df,
            table_name=self.tables.WORKFLOW_STATE,
            unity_config=self.unity,
            mode="append",
            table_config=self.tables,
        )

    def load_checkpoint(
        self,
        workflow_id: str,
        stage: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Load the most recent checkpoint for a given workflow and stage.
        """
        df = delta_helpers.read_table(
            spark=self.spark,
            table_name=self.tables.WORKFLOW_STATE,
            unity_config=self.unity,
            table_config=self.tables,
        )

        filtered = (
            df.filter(
                (df.workflow_id == workflow_id)
                & (df.stage == stage)
                & (df.status == "CHECKPOINT")
            )
            .orderBy(df.created_at.desc())
            .limit(1)
        )

        rows = filtered.collect()
        if not rows:
            return None

        checkpoint_str = rows[0]["checkpoint_data"]
        if not checkpoint_str:
            return None

        try:
            return json.loads(checkpoint_str)
        except Exception:
            return None