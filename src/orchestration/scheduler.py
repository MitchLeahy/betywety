"""Scheduler to coordinate all pipeline jobs."""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

from src.orchestration.bronze_job import BronzeIngestionJob
from src.orchestration.silver_job import SilverTransformationJob
from src.orchestration.gold_job import GoldAggregationJob

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Schedule and coordinate all data pipeline jobs."""
    
    def __init__(
        self,
        bronze_interval_minutes: int = 5,
        silver_interval_minutes: int = 10,
        gold_interval_minutes: int = 15
    ):
        """
        Initialize pipeline scheduler.
        
        Args:
            bronze_interval_minutes: Minutes between bronze ingestion runs
            silver_interval_minutes: Minutes between silver transformation runs
            gold_interval_minutes: Minutes between gold aggregation runs
        """
        self.scheduler = BlockingScheduler()
        self.bronze_job = BronzeIngestionJob()
        self.silver_job = SilverTransformationJob()
        self.gold_job = GoldAggregationJob()
        
        # Schedule jobs
        self.scheduler.add_job(
            self._run_bronze,
            trigger=IntervalTrigger(minutes=bronze_interval_minutes),
            id="bronze_ingestion",
            name="Bronze Ingestion",
            replace_existing=True
        )
        
        self.scheduler.add_job(
            self._run_silver,
            trigger=IntervalTrigger(minutes=silver_interval_minutes),
            id="silver_transformation",
            name="Silver Transformation",
            replace_existing=True
        )
        
        self.scheduler.add_job(
            self._run_gold,
            trigger=IntervalTrigger(minutes=gold_interval_minutes),
            id="gold_aggregation",
            name="Gold Aggregation",
            replace_existing=True
        )
    
    def _run_bronze(self):
        """Run bronze ingestion job."""
        try:
            logger.info("Running scheduled bronze ingestion job")
            self.bronze_job.run()
        except Exception as e:
            logger.error(f"Error in bronze job: {e}", exc_info=True)
    
    def _run_silver(self):
        """Run silver transformation job."""
        try:
            logger.info("Running scheduled silver transformation job")
            self.silver_job.run()
        except Exception as e:
            logger.error(f"Error in silver job: {e}", exc_info=True)
    
    def _run_gold(self):
        """Run gold aggregation job."""
        try:
            logger.info("Running scheduled gold aggregation job")
            self.gold_job.run()
        except Exception as e:
            logger.error(f"Error in gold job: {e}", exc_info=True)
    
    def start(self):
        """Start the scheduler."""
        logger.info("Starting pipeline scheduler")
        self.scheduler.start()
    
    def shutdown(self):
        """Shutdown the scheduler."""
        logger.info("Shutting down pipeline scheduler")
        self.scheduler.shutdown()
