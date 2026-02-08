"""Run the complete data pipeline."""

import sys
import os
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestration.bronze_job import BronzeIngestionJob
from src.orchestration.silver_job import SilverTransformationJob
from src.orchestration.gold_job import GoldAggregationJob

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_full_pipeline():
    """Run the complete pipeline: bronze -> silver -> gold."""
    logger.info("=" * 60)
    logger.info("Starting full pipeline run")
    logger.info("=" * 60)
    
    # Step 1: Bronze ingestion
    logger.info("\n[1/3] Running Bronze Ingestion...")
    bronze_job = BronzeIngestionJob()
    bronze_results = bronze_job.run()
    logger.info(f"Bronze ingestion completed: {sum(1 for r in bronze_results if r)} records")
    
    # Step 2: Silver transformation
    logger.info("\n[2/3] Running Silver Transformation...")
    silver_job = SilverTransformationJob()
    silver_count = silver_job.run(limit=100)
    logger.info(f"Silver transformation completed: {silver_count} lines")
    
    # Step 3: Gold aggregation
    logger.info("\n[3/3] Running Gold Aggregation...")
    gold_job = GoldAggregationJob()
    gold_results = gold_job.run()
    logger.info(f"Gold aggregation completed: {gold_results}")
    
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline run completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        run_full_pipeline()
    except KeyboardInterrupt:
        logger.info("\nPipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
