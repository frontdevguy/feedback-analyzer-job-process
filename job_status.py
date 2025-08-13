#!/usr/bin/env python3
try:
    import sys
    from sqlalchemy.orm import Session
    from database.config import get_db
    from models.job import Job, JobStatus
    from logger import get_logger

    logger = get_logger("job_status")
except ImportError as e:
    print(f"Failed to import required modules: {e}")
    sys.exit(0)


def check_incomplete_jobs() -> int:
    try:
        db: Session = next(get_db())
    except Exception as e:
        logger.error("Failed to connect to database", error=e)
        return 0

    try:
        incomplete_job = db.query(Job).filter(Job.status != JobStatus.COMPLETED).first()
        return 1 if incomplete_job else 0

    except Exception as e:
        logger.error("Failed to query incomplete jobs", error=e)
        return 2
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(check_incomplete_jobs())
