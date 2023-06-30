import copy
import time
from typing import Dict, Optional, List
import logging
import dateutil.parser
import mcmetadata as metadata
from prefect import task
from functools import lru_cache

from processor import is_email_configured, get_email_config, get_slack_config
import processor.tasks as celery_tasks
import processor.notifications as notifications
from processor.database import stories_db as stories_db
from processor.database import projects_db as projects_db
logger = logging.getLogger(__name__)


@lru_cache(maxsize=50000)
def _cached_metadata_extract(url: str) -> dict:
    # Smart to cache here, because this URL might be in multiple projects!
    return metadata.extract(url)


@task(name='fetch_text')
def fetch_text_task(story: Dict) -> Optional[Dict]:
    try:
        parsed = _cached_metadata_extract(story['url'])
        updated_story = copy.copy(story)
        updated_story['story_text'] = parsed['text_content']
        updated_story['publish_date'] = parsed['publication_date']  # this is a date object
        return updated_story
    except Exception as _:
        # this is probably an HTTP, or content parsing error
        pass
    return None


@task(name='send_email')
def send_email_task(summary: Dict, data_source: str, start_time: float):
    duration_secs = time.time() - start_time
    duration_mins = str(round(duration_secs / 60, 2))
    email_message = ""
    email_message += "Checking {} projects.\n\n".format(summary['project_count'])
    email_message += summary['email_text']
    email_message += "\nDone - pulled {} stories.\n\n" \
                     "(An automated email from your friendly neighborhood {} story processor)" \
        .format(summary['stories'], data_source)
    if is_email_configured():
        email_config = get_email_config()
        notifications.send_email(email_config['notify_emails'],
                                 "Feminicide {} Update: {} stories ({} mins)".format(data_source,
                                                                                     summary['stories'],
                                                                                     duration_mins),
                                 email_message)
    else:
        logger.info("Not sending any email updates")

@task(name='send_slack_msg')
def send_slack_message_task(summary: Dict, data_source: str, start_time: float):
    duration_secs = time.time() - start_time
    duration_mins = str(round(duration_secs / 60, 2))
    slack_message = ""
    slack_message += "Checking {} projects.\n\n".format(summary['project_count'])
    slack_message += summary['email_text']
    slack_message += "\nDone - pulled {} stories.\n\n" \
                     "(An automated message from your friendly neighborhood {} story processor)" \
        .format(summary['stories'], data_source)
    if get_slack_config():
        slack_config = get_slack_config()
        notifications.send_slack_msg(slack_config['channel_id'],slack_config['bot_token'],"Feminicide {} Update: {} stories ({} mins)".format(data_source,
                                                                                     summary['stories'],
                                                                                     duration_mins), slack_message)
    else:
        logger.info("Not sending any slack updates")


@task(name='queue_stories_for_classification')
def queue_stories_for_classification_task(project_list: List[Dict], stories: List[Dict], datasource: str) -> Dict:
    total_stories = 0
    email_message = ""
    for p in project_list:
        project_stories = [s for s in stories if (s is not None) and (s['project_id'] == p['id'])]
        email_message += "Project {} - {}: {} stories\n".format(p['id'], p['title'], len(project_stories))
        total_stories += len(project_stories)
        if len(project_stories) > 0:
            # External source has guess dates (Newscatcher/Google), so use that
            for s in project_stories:
                if 'source_publish_date' in s:
                    s['publish_date'] = s['source_publish_date']
            # and log that we got and queued them all
            project_stories = stories_db.add_stories(project_stories, p, datasource)
            if len(project_stories) > 0:  # don't queue up unnecessary tasks
                # important to do this *after* we add the stories_id here
                celery_tasks.classify_and_post_worker.delay(p, project_stories)
                # important to write this update now, because we have queued up the task to process these stories
                # the task queue will manage retrying with the stories if it fails with this batch
                publish_dates = [dateutil.parser.parse(s['source_publish_date']) for s in project_stories]
                latest_date = max(publish_dates)
                projects_db.update_history(p['id'], last_publish_date=latest_date, last_url=project_stories[0]['url'])
        logger.info("  queued {} stories for project {}/{}".format(total_stories, p['id'], p['title']))
    return dict(
        email_text=email_message,
        project_count=len(project_list),
        stories=total_stories
    )
