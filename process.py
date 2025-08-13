import pandas as pd
import torch
from transformers import pipeline
import nltk
from nltk.corpus import stopwords
from opensearchpy import OpenSearch, helpers
from logger import get_logger
from config import get_config
import gc
import psutil
import shutil
import uuid
import schedule
import time
from concurrent.futures import ThreadPoolExecutor
from dask.distributed import Client, LocalCluster
from typing import List, Dict
from sqlalchemy import select
from database.config import get_db
from models.job import Job, JobStatus
from models.feedback import Feedback
from models.job_config import JobConfig
from models.topics import Topic

config = get_config()
logger = get_logger("feedback_processor")

try:
    stop_words = set(stopwords.words("english"))
except LookupError:
    nltk.download("stopwords")
    stop_words = set(stopwords.words("english"))


def process_topics_standalone(messages, sentiment_pipeline, topic_pipeline, topics):
    pdf = pd.DataFrame(messages)
    result = process_partition_static(pdf, sentiment_pipeline, topic_pipeline, topics)
    return pdf.join(result)


def index_feedbacks_standalone(feedbacks_df, client: OpenSearch, index_name):
    actions = [
        {
            "_index": index_name,
            "_id": str(row["id"]),
            "_source": {
                "feedback_id": row["id"],
                "product_name": row["product_name"],
                "feedback_text": row["feedback_text"],
                "media_urls": row.get("media_urls", []),
                "sentiment": row["sentiment"],
                "topics": row["topics"],
            },
        }
        for _, row in feedbacks_df.iterrows()
    ]
    success, failed = helpers.bulk(client, actions, stats_only=True)
    logger.info(
        "Feedback indexing completed",
        extra={"success": success, "failed": failed, "index": index_name},
    )


def index_word_counts_standalone(word_counts, client: OpenSearch, index_name):
    nested_word_counts = [{"word": k, "count": v} for k, v in word_counts.items()]
    body = {
        "script": {
            "source": """
                for (item in params.new_counts) {
                    boolean found = false;
                    for (wc in ctx._source.word_counts) {
                        if (wc.word == item.word) {
                            wc.count += item.count;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.word_counts.add(item);
                    }
                }
            """,
            "lang": "painless",
            "params": {"new_counts": nested_word_counts},
        },
        "upsert": {"word_counts": nested_word_counts},
    }
    response = client.update(
        index=index_name,
        id="accumulated_word_counts",
        body=body,
    )
    logger.info(
        "Word count indexing completed",
        extra={"result": response.get("result", "unknown"), "index": index_name},
    )


def process_wordcount_standalone(messages):
    from collections import Counter
    import re

    texts = [m["feedback_text"] for m in messages if m["feedback_text"]]
    words = re.findall(r"\w+", " ".join(texts).lower())
    filtered_words = [
        word for word in words if word not in stop_words and len(word) > 2
    ]
    word_counts = Counter(filtered_words)
    return dict(sorted(word_counts.items(), key=lambda x: x[1], reverse=True))


def process_partition_static(
    df, sentiment_pipeline, topic_pipeline, topics, model_batch_size=16
):
    texts = df["feedback_text"].fillna("").tolist()
    mask_empty = [not isinstance(t, str) or not t.strip() for t in texts]

    sentiments = ["__UNKNOWN__"] * len(texts)
    if any(not is_empty for is_empty in mask_empty):
        valid_texts = [
            t if not is_empty else "neutral" for t, is_empty in zip(texts, mask_empty)
        ]
        sentiment_results = sentiment_pipeline(
            valid_texts, batch_size=model_batch_size, truncation=True
        )
        sentiments = [
            "__UNKNOWN__" if is_empty else res["label"].capitalize()
            for is_empty, res in zip(mask_empty, sentiment_results)
        ]

    topic_labels = [t["label"] for t in topics]
    topics_out = []
    for idx, text in enumerate(texts):
        if mask_empty[idx]:
            topics_out.append(["__UNKNOWN__"])
            continue
        try:
            result = topic_pipeline(
                text,
                candidate_labels=topic_labels,
                hypothesis_template="This feedback is about {}.",
                multi_label=True,
            )
            threshold = 0.7
            filtered_topics = [
                (label, score)
                for label, score in zip(result["labels"], result["scores"])
                if score > threshold
            ]
            top_topics = sorted(filtered_topics, key=lambda x: x[1], reverse=True)[:2]
            selected_topics = [label for label, _ in top_topics] or [
                result["labels"][0]
            ]
            topics_out.append(selected_topics)
        except Exception as e:
            logger.error("Topic classification failed", extra={"error": str(e)})
            topics_out.append(["__UNKNOWN__"])

    return pd.DataFrame({"sentiment": sentiments, "topics": topics_out}, index=df.index)

def pick_device_and_dtype():
    if torch.cuda.is_available():
        return 0, "auto"
    return -1, None


dev, dtype = pick_device_and_dtype()
common = {"device": dev}
if dtype is not None:
    common["torch_dtype"] = dtype
    
class FeedbackWorker:
    def __init__(self):
        logger.info("Initializing Dask cluster...")
        self.cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True)
        self.client = Client(self.cluster)

        logger.info("Loading ML models...")
        logger.info(f"Using device: {dev}, dtype: {dtype}")
        
        self.sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            **common,
        )

        self.topic_pipeline = pipeline(
            "zero-shot-classification",
            model="typeform/distilbert-base-uncased-mnli",
            **common,
        )

        endpoint = config.get("OPENSEARCH_ENDPOINT")
        username = config.get("OPENSEARCH_USERNAME")
        password = config.get("OPENSEARCH_PASSWORD")
        
        self.opensearch_client = None
        if all([endpoint, username, password]):
            self.opensearch_client = OpenSearch(
                hosts=[endpoint],
                http_auth=(username, password),
                use_ssl=True,
                verify_certs=True,
                maxsize=20,
            )
        self.search_feedback_analysis_index = "feedback-analysis"
        self.search_word_counts_index = "wordcount-analysis"

        self.db = next(get_db())

    def get_topics(self):
        topics = (
            self.db.query(Topic)
            .filter(Topic.is_active == True)
            .order_by(Topic.label)
            .all()
        )
        return [
            {"label": t.label, "description": t.description or t.label} for t in topics
        ]

    def get_last_job(self):
        return self.db.query(Job).order_by(Job.id.desc()).first()

    def create_new_active_job(self, start_from_id=0):
        job_name = f"feedback_analysis_{uuid.uuid4()}"
        job = Job(
            job_name=job_name,
            status=JobStatus.PROCESSING,
            last_processed_id=start_from_id,
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        logger.info(
            "Created new job", extra={"job_id": job.id, "start_from_id": start_from_id}
        )
        return job

    def check_feedback_availability(self, last_processed_id=0):
        max_feedback_id = (
            self.db.query(Feedback.id).order_by(Feedback.id.desc()).first()
        )
        if not max_feedback_id:
            logger.info("No feedbacks found in database")
            return False, 0
        max_id = max_feedback_id[0]
        return (last_processed_id < max_id, max_id)

    def get_next_feedbacks(self, last_processed_id=0, config=None):
        has_feedbacks, max_id = self.check_feedback_availability(last_processed_id)
        if not has_feedbacks:
            logger.info(
                "No new feedbacks available",
                extra={"last_processed_id": last_processed_id},
            )
            return []

        batch_size = config.config.get("batch_size", 10)
        query = (
            select(Feedback)
            .where(Feedback.id > last_processed_id)
            .order_by(Feedback.id)
            .limit(batch_size)
        )
        feedbacks = self.db.execute(query).scalars().all()

        logger.info(
            "Fetched feedbacks",
            extra={
                "num_feedbacks": len(feedbacks),
                "start_id": feedbacks[0].id if feedbacks else None,
                "end_id": feedbacks[-1].id if feedbacks else None,
                "remaining": max_id - last_processed_id,
            },
        )

        return [
            {
                "id": f.id,
                "feedback_text": f.feedback_text,
                "product_name": f.product_name,
                "sender_id": f.sender_id,
                "media_urls": f.media_urls or [],
            }
            for f in feedbacks
        ]

    def run_job(self):
        start_time = time.time()
        logger.info("Starting job processing cycle")

        config = self.db.query(JobConfig).first()
        if not config:
            config = JobConfig(config={"batch_size": 10, "run_next_job": True})
            self.db.add(config)
            self.db.commit()

        if not config.config.get("run_next_job", True):
            logger.info("Job processing disabled")
            return

        topics = self.get_topics()
        if not topics:
            logger.info("No topics found, skipping processing")
            return

        last_job = self.get_last_job()
        last_processed_id = last_job.last_processed_id if last_job else 0
        has_feedbacks, _ = self.check_feedback_availability(last_processed_id)
        if not has_feedbacks:
            logger.info(
                "No new feedbacks to process",
                extra={"last_processed_id": last_processed_id},
            )
            return

        job = (
            self.create_new_active_job(last_processed_id)
            if not last_job or last_job.status == JobStatus.COMPLETED
            else last_job
        )

        feedbacks = self.get_next_feedbacks(job.last_processed_id, config)
        if not feedbacks:
            return

        logger.info(
            "Starting feedback batch processing",
            extra={
                "job_id": job.id,
                "job_name": job.job_name,
                "num_feedbacks": len(feedbacks),
                "start_id": feedbacks[0]["id"],
                "end_id": feedbacks[-1]["id"],
            },
        )

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                topic_future = executor.submit(
                    process_topics_standalone,
                    feedbacks,
                    self.sentiment_pipeline,
                    self.topic_pipeline,
                    topics,
                )
                wordcount_future = executor.submit(
                    process_wordcount_standalone, feedbacks
                )
                result_df = topic_future.result()
                word_counts = wordcount_future.result()

            if self.opensearch_client:
                # Sequential indexing to avoid connection pool exhaustion
                index_feedbacks_standalone(
                    result_df,
                    self.opensearch_client,
                    self.search_feedback_analysis_index,
                )
                index_word_counts_standalone(
                    word_counts, self.opensearch_client, self.search_word_counts_index
                )

            job.status = JobStatus.COMPLETED
            job.last_processed_id = feedbacks[-1]["id"]
            self.db.commit()

            duration = round(time.time() - start_time, 2)
            logger.info(
                "Completed feedback batch processing",
                extra={
                    "job_id": job.id,
                    "job_name": job.job_name,
                    "last_processed_id": job.last_processed_id,
                    "num_feedbacks": len(feedbacks),
                    "processing_time_seconds": duration,
                },
            )

        finally:
            # Memory cleanup
            logger.info("Cleaning up job memory...")
            del feedbacks, result_df, word_counts
            gc.collect()

            mem = psutil.virtual_memory()
            disk = shutil.disk_usage("/")
            logger.info(
                "Resource usage after job cleanup",
                extra={
                    "memory_percent": mem.percent,
                    "memory_used_MB": round(mem.used / (1024 * 1024), 2),
                    "disk_total_GB": round(disk.total / (1024**3), 2),
                    "disk_used_GB": round(disk.used / (1024**3), 2),
                    "disk_free_GB": round(disk.free / (1024**3), 2),
                },
            )


def cleanup(worker: FeedbackWorker):
    logger.info("Cleaning up resources...")
    try:
        if worker.client:
            worker.client.close()
        if worker.cluster:
            worker.cluster.close()
            worker.cluster.shutdown()
    except Exception as e:
        logger.error("Cleanup error", extra={"error": str(e)})


def main():
    worker = FeedbackWorker()
    try:
        schedule.every(1).seconds.do(worker.run_job)
        logger.info("Worker started. Scheduled job every second.")
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        cleanup(worker)


if __name__ == "__main__":
    main()
