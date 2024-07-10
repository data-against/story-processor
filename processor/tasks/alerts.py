import datetime as dt
import logging
import os
import time

from celery.schedules import crontab
from sqlalchemy import func, select

import processor.database as database
from processor import (
    get_email_config,
    get_slack_config,
    is_email_configured,
    is_slack_configured,
    path_to_log_dir,
)
from processor.celery import app
from processor.database.models import Story
from processor.notifications import send_email, send_slack_msg

logger = logging.getLogger(__name__)  # get_task_logger(__name__)
logFormatter = logging.Formatter(
    "[%(levelname)s %(threadName)s] - %(asctime)s - %(name)s - : %(message)s"
)
fileHandler = logging.FileHandler(
    os.path.join(path_to_log_dir, "tasks-{}.log".format(time.strftime("%Y%m%d-%H%M%S")))
)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)


def get_total_stories_over_n_days(
    session: database.get_session_maker(), days: int = 4
) -> int:
    # Get total stories processed over the past 4 days
    earliest_date = dt.datetime.now() - dt.timedelta(days=days)
    result = session.execute(
        select(func.count(Story.id)).where(Story.processed_date >= earliest_date)
    )
    total_stories = result.scalar()
    return total_stories


def calculate_average_stories(total_stories: int, days: int) -> float:
    return total_stories / days


def send_alert(total_stories: int, days: int, threshold: float):
    # Get average stories over n days, in our case n=4
    average_stories_per_day = calculate_average_stories(total_stories, days)
    logger.info(
        f"Average stories processed per day in the last {days} days: {average_stories_per_day:.1f}"
    )

    warning_message = (
        f"Warning: Average stories per day in the last {days} days is {average_stories_per_day:.1f}, "
        f"which is far below the expected value of >= {threshold}."
    )
    # Check if it's below threshold and send messages/emails
    if average_stories_per_day < threshold:
        logger.warning(warning_message)

        # Check if Slack is configured before sending message
        if is_slack_configured():
            slack_config = get_slack_config()
            send_slack_msg(
                slack_config["channel_id"],
                slack_config["bot_token"],
                "Story Fetcher",
                "Low Story Count Alert",
                warning_message,
            )
        else:
            logger.warning("Slack is not configured.")

        # Check if email is configured before sending
        if is_email_configured():
            email_config = get_email_config()
            send_email(
                email_config["notify_emails"], "Low Story Count Alert", warning_message
            )
        else:
            logger.warning("Email is not configured.")


@app.task
def check_story_count():
    """
    Task to check the average number of stories processed over the last 4 days and send alerts if below threshold.
    """

    days = 4
    threshold = 40000

    # Create a session
    session = database.get_session_maker()
    with session() as session:
        total_stories = get_total_stories_over_n_days(session, days)
        send_alert(total_stories, days, threshold)


# Configuration to schedule the check_story_count task
app.conf.beat_schedule = {
    "check_story_count": {
        "task": "processor.tasks.alerts.check_story_count",
        "schedule": crontab(hour="2", minute="0", day_of_month="*/4"),
    },
}

app.conf.timezone = "UTC"
