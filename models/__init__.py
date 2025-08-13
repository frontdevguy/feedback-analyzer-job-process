# Import all models here to ensure they are registered with SQLAlchemy
# This is important for Alembic to detect all models

from models.user import User
from models.feedback import Feedback
from models.job import Job
from models.job_config import JobConfig
from models.topics import Topic

__all__ = ["User", "Feedback", "Job", "JobConfig", "Topic"]
