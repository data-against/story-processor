import sys
from typing import List, Dict
import feedparser
import time
import mcmetadata.urls as urls
from prefect import flow, task, get_run_logger
from urllib.parse import urlparse, parse_qs
from prefect_dask.task_runners import DaskTaskRunner
import processor
import processor.database as database
import processor.database.projects_db as projects_db
from processor.classifiers import download_models
import processor.projects as projects
import scripts.tasks as prefect_tasks

DEFAULT_STORIES_PER_PAGE = 150  # I found this performs poorly if set too high
DEFAULT_MAX_STORIES_PER_PROJECT = 200  # 40 * 1000  # make sure we don't do too many stories each cron run (for testing)
WORKER_COUNT = 16

DaskTaskRunner(
    cluster_kwargs={
        "image": "prefecthq/prefect:latest",
        "n_workers":WORKER_COUNT,
    },
)

def _url_from_google_alert_link(link: str) -> str:
    o = urlparse(link)
    query = parse_qs(o.query)
    return query['url'][0]


@task(name='load_projects')
def load_projects_task() -> List[Dict]:
    logger = get_run_logger()
    project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
    logger.info("  Checking {} projects".format(len(project_list)))
    return [p for p in project_list if p['rss_url'] and (len(p['rss_url']) > 0)]


@task(name='fetch_project_stories')
def fetch_project_stories_task(project_list: Dict, data_source: str) -> List[Dict]:
    logger = get_run_logger()
    combined_stories = []
    for p in project_list:
        feed = feedparser.parse(p['rss_url'])
        project_stories = []
        valid_stories = 0
        logger.info("Project {}/{} - {} stories".format(p['id'], p['title'], len(feed.entries)))
        Session = database.get_session_maker()
        with Session() as session:
            history = projects_db.get_history(session, p['id'])
        for item in feed.entries:
            # only process stories published after the last check we ran?
            #published_date = dateutil.parser.parse(item['published'])
            #if history.last_publish_date and (published_date < history.last_publish_date):
            #    continue
            # Stop when we've hit a url we've processed already?
            real_url = _url_from_google_alert_link(item['link'])
            if history.last_url == real_url:
                logger.info("  Found last_url on {}, skipping the rest".format(p['id']))
                break
            # story was published more recently than latest one we saw, so process it
            info = dict(
                url=real_url,
                source_publish_date=item['published'],
                title=item['title'],
                source=data_source,
                project_id=p['id'],
                language=p['language'],
                media_url=urls.canonical_domain(real_url),
                media_name=urls.canonical_domain(real_url)
            )
            project_stories.append(info)
            valid_stories += 1
        logger.info("  project {} - {} valid stories (after {})".format(p['id'], valid_stories,
                                                                        history.last_publish_date))
        combined_stories += project_stories
    return combined_stories


if __name__ == '__main__':

    @flow(name="googlealerts_stories",task_runner = DaskTaskRunner())
    def googlealerts_stories_flow(data_source: str):
        logger = get_run_logger()
        logger.info("Starting {} story fetch job".format(processor.SOURCE_GOOGLE_ALERTS))

        # important to do because there might be new models on the server!
        logger.info("  Checking for any new models we need")
        models_downloaded = download_models()
        logger.info(f"    models downloaded: {models_downloaded}")
        if not models_downloaded:
            sys.exit(1)
        data_source_name = data_source
        start_time = time.time()
        # 1. list all the project we need to work on
        projects_list = load_projects_task()
        # 2. fetch all the urls from Google Alerts RSS feeds
        all_stories = fetch_project_stories_task(projects_list, data_source_name)
        # 3. fetch webpage text and parse all the stories (will happen in parallel by story)
        stories_with_text = prefect_tasks.fetch_text_task.map(all_stories)
        # 4. post batches of stories for classification
        results_data = prefect_tasks.queue_stories_for_classification_task(projects_list, stories_with_text,
                                                                           data_source_name)
        # 5. send email/slack_msg with results of operations
        prefect_tasks.send_combined_slack_message_task(results_data, data_source_name, start_time)
        prefect_tasks.send_combined_email_task(results_data, data_source_name, start_time)
        

    # run the whole thing
    googlealerts_stories_flow(processor.SOURCE_GOOGLE_ALERTS)

