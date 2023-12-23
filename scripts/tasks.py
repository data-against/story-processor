import copy
import logging
import time
from functools import lru_cache
from typing import Dict, List, Optional

import dateutil.parser
import mcmetadata as metadata

import processor.database as database
import processor.notifications as notifications
import processor.tasks as celery_tasks
from processor import VERSION, get_email_config, get_slack_config, is_email_configured
from processor.database import projects_db as projects_db
from processor.database import stories_db as stories_db

logger = logging.getLogger(__name__)


@lru_cache(maxsize=50000)
def _cached_metadata_extract(url: str) -> dict:
    # Smart to cache here, because this URL might be in multiple projects!
    return metadata.extract(url)


def fetch_text_task(story: Dict) -> Optional[Dict]:
    try:
        parsed = _cached_metadata_extract(story["url"])
        updated_story = copy.copy(story)
        updated_story["story_text"] = parsed["text_content"]
        updated_story["publish_date"] = parsed[
            "publication_date"
        ]  # this is a date object
        return updated_story
    except Exception as _:
        # this is probably an HTTP, or content parsing error
        pass
    return None


def send_combined_email(summary: Dict, data_source: str, start_time: float):
    email_message = _get_combined_text(
        summary["project_count"], summary["email_text"], summary["stories"], data_source
    )
    _send_email(data_source, summary["stories"], start_time, email_message)


def send_project_list_email(
    project_details: List[Dict], data_source: str, start_time: float
):
    # :param project_details: array of dicts per project, each with 'email_text', 'stories', and 'pages' keys
    total_new_stories = sum([p["stories"] for p in project_details])
    # total_pages = sum([p['pages'] for p in project_details])
    combined_email_text = ""
    for p in project_details:
        combined_email_text += p["email_text"]
    email_message = _get_combined_text(
        len(project_details), combined_email_text, total_new_stories, data_source
    )
    _send_email(data_source, total_new_stories, start_time, email_message)


def _send_email(
    data_source: str, story_count: int, start_time: float, email_message: str
):
    duration_secs = time.time() - start_time
    duration_mins = str(round(duration_secs / 60, 2))
    if is_email_configured():
        email_config = get_email_config()
        notifications.send_email(
            email_config["notify_emails"],
            "Feminicide {} Update: {} stories ({} mins) - v{}".format(
                data_source, story_count, duration_mins, VERSION
            ),
            email_message,
        )
    else:
        logger.info("Not sending any email updates")


def _get_combined_text(
    project_count: int, email_text: str, story_count: int, data_source: str
) -> str:
    email_message = ""
    email_message += "Checking {} projects.\n\n".format(project_count)
    email_message += email_text
    email_message += (
        "\nDone - pulled {} stories.\n\n"
        "(An automated email from your friendly neighborhood {} story processor)".format(
            story_count, data_source
        )
    )
    return email_message


def send_combined_slack_message(summary: Dict, data_source: str, start_time: float):
    # :param project_details: array of dicts per project, each with 'email_text', 'stories', and 'pages' keys
    message = _get_combined_text(
        summary["project_count"], summary["email_text"], summary["stories"], data_source
    )
    _send_slack_message(data_source, summary["stories"], start_time, message)


def send_project_list_slack_message(
    project_details: List[Dict], data_source: str, start_time: float
):
    # :param summary: has keys 'project_count', 'email_text', 'stories'
    total_new_stories = sum([p["stories"] for p in project_details])
    # total_pages = sum([p['pages'] for p in project_details])
    combined_text = ""
    for p in project_details:
        combined_text += p["email_text"]
    message = _get_combined_text(
        len(project_details), combined_text, total_new_stories, data_source
    )
    _send_slack_message(data_source, total_new_stories, start_time, message)


def _send_slack_message(
    data_source: str, story_count: int, start_time: float, slack_message: str
):
    duration_secs = time.time() - start_time
    duration_mins = str(round(duration_secs / 60, 2))
    if get_slack_config():
        slack_config = get_slack_config()
        notifications.send_slack_msg(
            slack_config["channel_id"],
            slack_config["bot_token"],
            data_source,
            "Feminicide {} Update: {} stories ({} mins) - v{}".format(
                data_source, story_count, duration_mins, VERSION
            ),
            slack_message,
        )
    else:
        logger.info("Not sending any slack updates")


def queue_stories_for_classification(
    project_list: List[Dict], stories: List[Dict], datasource: str
) -> Dict:
    total_stories = 0
    email_message = ""
    for p in project_list:
        project_stories = [
            s for s in stories if (s is not None) and (s["project_id"] == p["id"])
        ]
        email_message += "Project {} - {}: {} stories\n".format(
            p["id"], p["title"], len(project_stories)
        )
        total_stories += len(project_stories)
        if len(project_stories) > 0:
            # External source has guessed dates (Newscatcher/Google), so use that
            for s in project_stories:
                if "source_publish_date" in s:
                    s["publish_date"] = s["source_publish_date"]
            # and log that we got and queued them all (this might happen a loooooong time after we last used the DB,
            # so lets be careful here and reset the engine before using the session)
            Session = database.get_session_maker(reset_pool=True)
            with Session() as session:
                project_stories = stories_db.add_stories(
                    session, project_stories, p, datasource
                )
                if len(project_stories) > 0:  # don't queue up unnecessary tasks
                    # important to do this *after* we add the stories_id here
                    celery_tasks.classify_and_post_worker.delay(p, project_stories)
                    # important to write this update now, because we have queued up the task to process these stories
                    # the task queue will manage retrying with the stories if it fails with this batch
                    publish_dates = [
                        dateutil.parser.parse(s["source_publish_date"])
                        for s in project_stories
                    ]
                    # TODO: for MC need to get indexed date, not published_date
                    latest_date = max(
                        publish_dates
                    )  # we use latest pub_date to filter in our queries tomorrow
                    projects_db.update_history(
                        session, p["id"], latest_date, datasource
                    )
        logger.info(
            "  queued {} stories for project {}/{}".format(
                len(project_stories), p["id"], p["title"]
            )
        )
    return dict(
        email_text=email_message, project_count=len(project_list), stories=total_stories
    )
