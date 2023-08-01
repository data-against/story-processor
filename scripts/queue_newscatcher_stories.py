#import prefect  #Will be required if get_run_context is implemented
import math
from typing import List, Dict
import time
import sys
import mcmetadata.urls as urls
import datetime as dt
from prefect import flow, task, get_run_logger
import newscatcherapi
import newscatcherapi.newscatcherapi_exception
from prefect_dask.task_runners import DaskTaskRunner
import requests.exceptions

import processor
import processor.database as database
import processor.database.projects_db as projects_db
from processor.classifiers import download_models
import processor.projects as projects
import scripts.tasks as prefect_tasks

PAGE_SIZE = 100
DEFAULT_DAY_WINDOW = 5
WORKER_COUNT = 16
MAX_CALLS_PER_SEC = 5
MAX_STORIES_PER_PROJECT = 5000
DELAY_SECS = 1 / MAX_CALLS_PER_SEC

nc_api_client = newscatcherapi.NewsCatcherApiClient(x_api_key=processor.NEWSCATCHER_API_KEY)

DaskTaskRunner(
    cluster_kwargs={
        "image": "prefecthq/prefect:latest",
        "n_workers":WORKER_COUNT,
    },
)

@task(name='load_projects')
def load_projects_task() -> List[Dict]:
    logger = get_run_logger()
    project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
    projects_with_countries = projects.with_countries(project_list)
    if len(projects_with_countries) == 0:
        raise RuntimeError("No projects with countries ({} projects total) - bailing".format(len(project_list)))
    logger.info("  Found {} projects, checking {} with countries set".format(len(project_list),
                                                                             len(projects_with_countries)))
    #return [p for p in projects_with_countries if p['id'] == 166]
    return projects_with_countries


def _fetch_results(project: Dict, start_date: dt.datetime, end_date: dt.datetime, page: int = 1) -> Dict:
    results = dict(total_hits=0) # start of with a mockup of no results, so we can handle transient errors bette
    logger = get_run_logger()
    try:
        terms_no_curlies = project['search_terms'].replace('“', '"').replace('”', '"')
        results = nc_api_client.get_search(
            q=terms_no_curlies,
            lang=project['language'],
            countries=[p.strip() for p in project['newscatcher_country'].split(",")],
            page_size=PAGE_SIZE,
            from_=start_date.strftime("%Y-%m-%d"),
            to_=end_date.strftime("%Y-%m-%d"),
            page=page
        )
    # try to ignore project-level exceptions so we can keep going and process other projects
    except newscatcherapi.newscatcherapi_exception.NewsCatcherApiException as ncae:
        logger.error("Couldn't get Newscatcher results for project {} - check query ({})".format(project['id'], ncae))
        logger.exception(ncae)
    except requests.exceptions.RequestException as re:
        logger.error("Fetch-related response on project {} - {}".format(project['id'], re))
        logger.exception(re)
    return results


@task(name='fetch_project_stories')
def fetch_project_stories_task(project_list: Dict, data_source: str) -> List[Dict]:
    logger = get_run_logger()
    combined_stories = []
    end_date = dt.datetime.now()
    for p in project_list:
        project_stories = []
        valid_stories = 0
        Session = database.get_session_maker()
        with Session() as session:
            history = projects_db.get_history(session, p['id'])
        page_number = 1
        # only search stories since the last search (if we've done one before)
        start_date = end_date - dt.timedelta(days=DEFAULT_DAY_WINDOW)
        if history.last_publish_date is not None:
            # make sure we don't accidentally cut off a half day we haven't queried against yet
            # this is OK because duplicates will get screened out later in the pipeline
            local_start_date = history.last_publish_date - dt.timedelta(days=1)
            start_date = min(local_start_date, start_date)
        current_page = _fetch_results(p, start_date, end_date, page_number)
        total_hits = current_page['total_hits']
        logger.info("Project {}/{} - {} total stories (since {})".format(p['id'], p['title'], total_hits, start_date))
        if total_hits > 0:
            page_count = math.ceil(total_hits / PAGE_SIZE)
            keep_going = True
            while keep_going:
                logger.debug("  {} - page {}: {} stories".format(p['id'], page_number, len(current_page['articles'])))
                for item in current_page['articles']:
                    real_url = item['link']
                    # removing this check for now, because I'm not sure if stories are ordered consistently
                    """
                    # stop when we've hit a url we've processed already
                    if history.last_url == real_url:
                        logger.info("  Found last_url on {}, skipping the rest".format(p['id']))
                        keep_going = False
                        break  # out of the for loop of all articles on page, back to while "more pages"
                    # story was published more recently than latest one we saw, so process it
                    """
                    info = dict(
                        url=real_url,
                        source_publish_date=item['published_date'],
                        title=item['title'],
                        source=data_source,
                        project_id=p['id'],
                        language=p['language'],
                        authors=item['authors'],
                        media_url=urls.canonical_domain(real_url),
                        media_name=urls.canonical_domain(real_url)
                        # too bad there isn't somewhere we can store the `id` (string)
                    )
                    project_stories.append(info)
                    valid_stories += 1
                if keep_going:  # check after page is processed
                    keep_going = (page_number < page_count) and (len(project_stories) <= MAX_STORIES_PER_PROJECT)
                    if keep_going:
                        page_number += 1
                        time.sleep(DELAY_SECS)
                        current_page = _fetch_results(p, start_date, end_date, page_number)
                        # stay below rate limiting
            logger.info("  project {} - {} valid stories (after {})".format(p['id'], valid_stories,
                                                                            history.last_publish_date))
            combined_stories += project_stories
            #print("Task run context:")# Just for testing purposes can be removed if not needed.
            #print(prefect.context.get_run_context().task_run.dict())# Just for testing purposes can be removed if not needed.
    return combined_stories


if __name__ == '__main__':

    @flow(name="newscatcher_stories", task_runner=DaskTaskRunner())
    def newscatcher_stories_flow(data_source: str):
        logger = get_run_logger()
        logger.info("Starting {} story fetch job".format(processor.SOURCE_NEWSCATCHER))
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
        # 2. fetch all the urls from for each project from newscatcher (not mapped, so we can respect rate limiting)
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
    newscatcher_stories_flow(processor.SOURCE_NEWSCATCHER)
