import datetime as dt
import logging
import os
import time

from celery.schedules import crontab
from sqlalchemy import func, select
from sqlalchemy.orm.session import Session

import processor.database as database
from processor import get_email_config, get_slack_config, path_to_log_dir
from processor.celery import app
from processor.database.models import Story
from processor.notifications import send_email, send_slack_msg

STORY_COUNT_WINDOW_SIZE = 4
STORY_COUNT_THRESHOLD = 40000
logger = logging.getLogger(__name__)  # get_task_logger(__name__)
logFormatter = logging.Formatter(
    "[%(levelname)s %(threadName)s] - %(asctime)s - %(name)s - : %(message)s"
)
fileHandler = logging.FileHandler(
    os.path.join(path_to_log_dir, "tasks-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)


def get_total_stories_over_n_days(session: Session, days: int = 4) -> int:
    # Get total stories processed over the past 4 days
    earliest_date = dt.datetime.now() - dt.timedelta(days=days)
    result = session.execute(
        select(func.count(Story.id)).where(Story.processed_date >= earliest_date)
    )
    total_stories = result.scalar()
    return total_stories


def send_alert(total_stories: int, days: int, threshold: float):
    # Get average stories over n days, in our case n=4
    average_stories_per_day = total_stories / days
    logger.info(
        f"Average stories processed per day in the last {days} days: {average_stories_per_day:.1f}"
    )

    # Check if it's below threshold and send messages/emails
    if average_stories_per_day < threshold:
        warning_message = (
            f"Warning: Average stories per day in the last {days} days is {average_stories_per_day:.1f}, "
            f"which is far below the expected value of >= {threshold}."
        )
        logger.warning(warning_message)

        slack_config = get_slack_config()
        send_slack_msg(
            slack_config["channel_id"],
            slack_config["bot_token"],
            "Story Fetcher",
            "Low Story Count Alert",
            warning_message,
        )

        email_config = get_email_config()
        send_email(
            email_config["notify_emails"], "Low Story Count Alert", warning_message
        )


@app.task
def check_story_count():
    """
    Task to check the average number of stories processed over the last 4 days and send alerts if below threshold.
    """

    # Create a session
    session = database.get_session_maker()
    with session() as session:
        total_stories = get_total_stories_over_n_days(
            session, days=STORY_COUNT_WINDOW_SIZE
        )
        send_alert(
            total_stories, days=STORY_COUNT_WINDOW_SIZE, threshold=STORY_COUNT_THRESHOLD
        )


# Configuration to schedule the check_story_count task
app.conf.beat_schedule = {
    "check_story_count": {
        "task": "processor.tasks.alerts.check_story_count",
        "schedule": crontab(hour="2", minute="0", day_of_month="*"),
    },
}

app.conf.timezone = "UTC"
