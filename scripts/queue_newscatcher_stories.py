import math
from typing import List, Dict
import time
import mcmetadata.urls as urls
import datetime as dt
import newscatcherapi
import newscatcherapi.newscatcherapi_exception
import requests.exceptions
import logging
import itertools
from multiprocessing import Pool
import sys

import processor
import processor.database as database
import processor.database.projects_db as projects_db
from processor.classifiers import download_models
import processor.projects as projects
import processor.fetcher as fetcher
import scripts.tasks as tasks


POOL_SIZE = 16
PAGE_SIZE = 100
DEFAULT_DAY_WINDOW = 3
MAX_STORIES_PER_PROJECT = 200  #5000
MAX_CALLS_PER_SEC = 5
DELAY_SECS = 1 / MAX_CALLS_PER_SEC

nc_api_client = newscatcherapi.NewsCatcherApiClient(x_api_key=processor.NEWSCATCHER_API_KEY)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_projects() -> List[Dict]:
    project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
    projects_with_countries = projects.with_countries(project_list)
    if len(projects_with_countries) == 0:
        raise RuntimeError("No projects with countries ({} projects total) - bailing".format(len(project_list)))
    logger.info("  Found {} projects, checking {} with countries set".format(len(project_list),
                                                                             len(projects_with_countries)))
    #return [p for p in projects_with_countries if p['id'] == 166]
    #return projects_with_countries[3:5]
    return projects_with_countries


def _fetch_results(project: Dict, start_date: dt.datetime, end_date: dt.datetime, page: int = 1) -> Dict:
    results = dict(total_hits=0)  # start of with a mockup of no results, so we can handle transient errors bette
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


def _project_story_worker(p: Dict) -> List[Dict]:
    end_date = dt.datetime.now()
    project_stories = []
    valid_stories = 0
    try:  # be careful here so database errors don't mess us up
        Session = database.get_session_maker()
        with Session() as session:
            history = projects_db.get_history(session, p['id'])
    except Exception as e:
        logger.error("Couldn't get history for project {} - {}".format(p['id'], e))
        logger.exception(e)
        history = None
    page_number = 1
    # only search stories since the last search (if we've done one before)
    start_date = end_date - dt.timedelta(days=DEFAULT_DAY_WINDOW)
    if history and (history.last_publish_date is not None):
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
                    source=processor.SOURCE_NEWSCATCHER,
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
    return project_stories


def fetch_project_stories(project_list: List[Dict]) -> List[Dict]:
    """
    Fetch stories in parallel by project. Efficient because it is mostly IO-bound. Would be smarter to use async calls
    here but I don't think the underlying API call is async compatible yet.
    :param project_list:
    :return:
    """
    with Pool(POOL_SIZE) as p:
        lists_of_stories = p.map(_project_story_worker, project_list)
    # flatten list of lists of stories into one big list
    combined_stories = [s for s in itertools.chain.from_iterable(lists_of_stories)]
    logger.info("Fetched {} total URLs from {}".format(len(combined_stories), processor.SOURCE_NEWSCATCHER))
    return combined_stories


def fetch_text(stories: List[Dict]) -> List[Dict]:
    stories_to_return = []

    def handle_parse(response_data: Dict):
        # called for each story that successfully is fetched
        nonlocal stories, stories_to_return
        matching_input_stories = [s for s in stories if s['url'] == response_data['original_url']]
        for s in matching_input_stories:
            s['story_text'] = response_data['html_content']
            stories_to_return.append(s)
    # download them all in parallel... will take a while
    fetcher.fetch_all_html([s['url'] for s in stories], handle_parse)
    logger.info("Fetched text for {} stories (failed on {})".format(len(stories_to_return),
                                                                    len(stories) - len(stories_to_return)))
    return stories_to_return


if __name__ == '__main__':

    logger.info("Starting {} story fetch job".format(processor.SOURCE_NEWSCATCHER))

    # important to do because there might be new models on the server!
    logger.info("  Checking for any new models we need")
    models_downloaded = download_models()
    logger.info(f"    models downloaded: {models_downloaded}")
    if not models_downloaded:
        sys.exit(1)

    start_time = time.time()

    # 1. list all the project we need to work on
    projects_list = load_projects()

    # 2. fetch all the urls from for each project from newscatcher (in parallel)
    all_stories = fetch_project_stories(projects_list)

    # 3. fetch webpage text and parse all the stories (use scrapy to do this in parallel, dropping stories that fail)
    stories_with_text = fetch_text(all_stories)

    # 4. post batches of stories for classification
    results_data = tasks.queue_stories_for_classification(projects_list, stories_with_text,
                                                          processor.SOURCE_NEWSCATCHER, logger)

    # 5. send email/slack_msg with results of operations
    tasks.send_combined_slack_message(results_data, processor.SOURCE_NEWSCATCHER, start_time, logger)
    tasks.send_combined_email(results_data, processor.SOURCE_NEWSCATCHER, start_time, logger)
