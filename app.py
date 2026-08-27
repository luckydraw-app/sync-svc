import time
import threading
import json

from flask import Flask, jsonify, Response
from flask_restx import Api
from sqlalchemy.orm import Session

from prometheus_client import generate_latest

from opentelemetry import trace

from opentelemetry.trace import (
    SpanContext,
    TraceFlags,
    NonRecordingSpan,
    set_span_in_context
)

from shared.database.redis_client import redis_client
from shared.database.postgres import SessionLocal, engine
from shared.models.base import Base
from shared.models.participant import Participant

from shared.telemetry.tracing import setup_tracing

from shared.telemetry.metrics import (
    participants_synced_total,
    sync_jobs_total,
    sync_failures_total
)

from shared.telemetry.logger import (
    get_logger
)

Base.metadata.create_all(bind=engine)

app = Flask(__name__)

setup_tracing(
    app,
    "sync-service",
    engine
)

logger = get_logger(
    "sync-service"
)

tracer = trace.get_tracer(
    "sync-service"
)

api = Api(
    app,
    version="1.0",
    title="Sync Service",
    description="Automated Redis to PostgreSQL Sync",
    doc="/swagger"
)

SYNC_INTERVAL_SECONDS = 5


@app.route("/health")
def health():

    return jsonify(
        {
            "status": "UP",
            "service": "sync-service"
        }
    )


@app.route("/metrics")
def metrics():

    return Response(
        generate_latest(),
        mimetype="text/plain"
    )


def sync_worker():

    logger.info(
        "Background sync worker started"
    )

    while True:

        db: Session = SessionLocal()

        processed = 0

        try:

            sync_jobs_total.inc()

            item = redis_client.lpop(
                "participation_queue"
            )

            if not item:

                logger.info(
                    "No participants found in Redis queue",
                    extra={
                        "service": "sync-service",
                        "extra_data": {
                            "processed_count": 0,
                            "status": "empty"
                        }
                    }
                )

                time.sleep(
                    SYNC_INTERVAL_SECONDS
                )

                continue

            payload = json.loads(item)

            parent_context = SpanContext(
                trace_id=int(
                    payload["trace_id"],
                    16
                ),
                span_id=int(
                    payload["span_id"],
                    16
                ),
                is_remote=True,
                trace_flags=TraceFlags(0x01),
                trace_state={}
            )

            ctx = set_span_in_context(
                NonRecordingSpan(parent_context)
            )

            with tracer.start_as_current_span(
                "redis-to-postgres-sync",
                context=ctx
            ) as span:

                participant_data = payload["participant"]

                participant = Participant(
                    name=participant_data["name"],
                    phone=participant_data["phone"],
                    email=participant_data["email"]
                )

                db.add(participant)

                processed += 1

                db.commit()

                participants_synced_total.inc(
                    processed
                )

                span.set_attribute(
                    "processed_count",
                    processed
                )

                span.set_attribute(
                    "queue.name",
                    "participation_queue"
                )

                logger.info(
                    "Participants synced to PostgreSQL",
                    extra={
                        "service": "sync-service",
                        "extra_data": {
                            "processed_count": processed,
                            "status": "success",
                            "queue": "participation_queue"
                        }
                    }
                )

        except Exception as ex:

            sync_failures_total.inc()

            db.rollback()

            logger.error(
                "Batch sync to PostgreSQL failed",
                extra={
                    "service": "sync-service",
                    "extra_data": {
                        "processed_count": processed,
                        "status": "failed",
                        "error": str(ex)
                    }
                }
            )

        finally:

            db.close()

        time.sleep(
            SYNC_INTERVAL_SECONDS
        )


worker_thread = threading.Thread(
    target=sync_worker,
    daemon=True
)

worker_thread.start()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000
    )
