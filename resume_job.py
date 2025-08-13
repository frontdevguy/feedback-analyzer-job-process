#!/usr/bin/env python3
try:
    import sys
    from sqlalchemy.orm import Session
    from database.config import get_db
    from models.job_config import JobConfig
    from logger import get_logger

    logger = get_logger("job_resumption")
except ImportError as e:
    print(f"Failed to import required modules: {e}")
    sys.exit(0)


def resume_job_processing() -> int:
    logger.info("Attempting to resume job processing")

    try:
        db: Session = next(get_db())
    except Exception as e:
        logger.error("Failed to connect to database", error=e)
        return 1

    try:
        config = db.query(JobConfig).filter(JobConfig.id == 1).first()
        if config:
            logger.info(
                "Found existing job config",
                config_id=config.id,
                current_config=config.config,
            )
            old_config = dict(config.config)  # Store old config for logging
            config.config = {**config.config, "run_next_job": True}
            logger.info(
                "Updated job config",
                config_id=config.id,
                old_config=old_config,
                new_config=config.config,
            )
        else:
            logger.info("No existing job config found, creating new config")
            config = JobConfig(
                id=1, config={"run_next_job": True, "batch_size": 10, "num_workers": 1}
            )
            db.add(config)
            logger.info(
                "Created new job config", config_id=config.id, config=config.config
            )

        db.commit()
        logger.info("Successfully resumed job processing")
        return 0

    except Exception as e:
        logger.error(
            "Failed to update job config", error=e, error_type=type(e).__name__
        )
        db.rollback()
        return 1
    finally:
        db.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    sys.exit(resume_job_processing())
