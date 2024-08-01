from __future__ import absolute_import

import logging

from celery import Celery

from processor import BROKER_URL

logger = logging.getLogger(__name__)

# we use celery to support parallel processing of stories in need of classification
app = Celery(
    "feminicide-story-processor",
    broker=BROKER_URL,
    backend="db+sqlite:///celery-backend.db",
    include=[
        "processor.tasks",
        "processor.tasks.classification",
        "processor.tasks.alerts",
        "processor.tasks.delete_old_data",
    ],
)
