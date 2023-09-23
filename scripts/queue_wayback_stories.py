from typing import List, Dict, Optional
import time
import sys
import copy
import threading
import numpy as np
import mcmetadata.urls as urls
import datetime as dt
import logging
import itertools
from multiprocessing import Pool
import sys
# from prefect import flow, task, get_run_logger, unmapped
from waybacknews.searchapi import SearchApiClient
# from prefect_dask.task_runners import DaskTaskRunner
import requests.exceptions
import processor
import processor.database as database
import processor.database.projects_db as projects_db
from processor.classifiers import download_models
import processor.projects as projects
import processor.fetcher as fetcher
import scripts.tasks as tasks

POOL_SIZE = 2 #Tried with 3 and 4 as well.
PAGE_SIZE = 1000
DEFAULT_DAY_OFFSET = 4
DEFAULT_DAY_WINDOW = 3
MAX_STORIES_PER_PROJECT = 5000
TEXT_FETCH_TIMEOUT = 5  # seconds to wait for full text fetch to complete

wm_api = SearchApiClient("mediacloud")

logger = logging.getLogger(__name__)

def load_projects_task() -> List[Dict]:
    project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
    logger.info("  Found {} projects".format(len(project_list)))
    #return [p for p in project_list if p['id'] == 166]
    return project_list


# Wacky memory solution here for caching sources in collections because file-based cache failed on prod server 😖
collection2sources_lock = threading.Lock()
collection2sources = {}
def _sources_are_cached(cid: int) -> bool:
    collection2sources_lock.acquire()
    is_cached = cid in collection2sources
    collection2sources_lock.release()
    return is_cached
def _sources_set(cid: int, domains: List[Dict]):
    collection2sources_lock.acquire()
    collection2sources[cid] = domains.copy()
    collection2sources_lock.release()
def _sources_get(cid: int) -> List[Dict]:
    collection2sources_lock.acquire()
    domains = collection2sources[cid]
    collection2sources_lock.release()
    return domains


def _domains_for_collection(cid: int) -> List[str]:
    limit = 1000
    offset = 0
    sources = []
    mc_api = processor.get_mc_client()
    while True:
        response = mc_api.source_list(collection_id=cid, limit=limit, offset=offset)
        sources += response['results']
        if response['next'] is None:
            break
        offset += limit
    return [s['name'] for s in sources if s['name'] is not None]  # grab just the domain names


def _cached_domains_for_collection(cid: int) -> List[str]:
    # fetch info if it isn't cached
    if not _sources_are_cached(cid):
        logger.debug(f'Collection {cid}: sources not cached, fetching')
        sources = _domains_for_collection(cid)
        _sources_set(cid, sources)
    else:
        # otherwise, load up cache to reduce server queries and runtime overall
        sources = _sources_get(cid)
        logger.debug(f'Collection {cid}: found sources cached {len(sources)}')
    return [s['name'] for s in sources if s['name'] is not None]


def _domains_for_project(collection_ids: List[int]) -> List[str]:
    all_domains = []
    for cid in collection_ids:  # fetch all the domains in each collection
        all_domains += _domains_for_collection(cid)  # remove cache because Prefect failing to serials with thread lock error :-(
    return list(set(all_domains))  # make them unique



def fetch_domains_for_projects(project: Dict) -> Dict:
    domains = _domains_for_project(project['media_collections'])
    logger.info(f"Project {project['id']}/{project['title']}: found {len(domains)} domains")
    updated_project = copy.copy(project)
    updated_project['domains'] = domains
    return updated_project

def _query_builder(terms: str, language: str, domains: list) -> str:
    terms_no_curlies = terms.replace('“', '"').replace('”', '"')
    return "({}) AND (language:{}) AND ({})".format(terms_no_curlies, language, " OR ".join(
        [f"domain:{d}" for d in domains]))

def _project_story_worker(p: Dict) -> List[Dict]:
    project_stories = []
    end_date = dt.datetime.now() - dt.timedelta(days=DEFAULT_DAY_OFFSET)  # stories don't get processed for a few days
    valid_stories = 0
    Session = database.get_session_maker()
    with Session() as session:
        history = projects_db.get_history(session, p['id'])
    page_number = 1
    start_date = end_date - dt.timedelta(days=DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW)
    if history.last_publish_date is not None:
        # make sure we don't accidentally cut off a half day we haven't queried against yet
        # this is OK because duplicates will get screened out later in the pipeline
        local_start_date = history.last_publish_date - dt.timedelta(days=1)
        start_date = min(local_start_date, start_date)
    # if query is too big we need to split it up
    # print(type(p))
    # print(p['domains'])
    full_project_query = _query_builder(p['search_terms'], p['language'], p['domains'])
    project_queries = [full_project_query]
    domain_divisor = 2
    queries_too_big = len(full_project_query) > pow(2, 14)
    if queries_too_big:
        while queries_too_big:
            chunked_domains = np.array_split(p['domains'], domain_divisor)
            project_queries = [_query_builder(p['search_terms'], p['language'], d) for d in chunked_domains]
            queries_too_big = any(len(pq) > pow(2, 14) for pq in project_queries)
            domain_divisor *= 2
        logger.info('Project {}/{}: split query into {} parts'.format(p['id'], p['title'], len(project_queries)))
    # now run all queries
    try:
        for project_query in project_queries:
            if valid_stories > MAX_STORIES_PER_PROJECT:
                break
            total_hits = wm_api.count(project_query, start_date, end_date)
            logger.info("Project {}/{} - {} total stories (since {})".format(p['id'], p['title'],
                                                                             total_hits, start_date))
            for page in wm_api.all_articles(project_query, start_date, end_date, page_size=PAGE_SIZE):
                if valid_stories > MAX_STORIES_PER_PROJECT:
                    break
                logger.debug("  {} - page {}: {} stories".format(p['id'], page_number, len(page)))
                for item in page:
                    media_url = item['domain'] if len(item['domain']) > 0 else urls.canonical_domain(item['url'])
                    info = dict(
                        url=item['url'],
                        source_publish_date=item['publication_date'],
                        title=item['title'],
                        source=processor.SOURCE_WAYBACK_MACHINE,
                        project_id=p['id'],
                        language=item['language'],
                        authors=None,
                        media_url=media_url,
                        media_name=media_url,
                        article_url=item['article_url']
                    )
                    project_stories.append(info)
                    valid_stories += 1
        logger.info("  project {} - {} valid stories (after {})".format(p['id'], valid_stories,
                                                                        history.last_publish_date))
    except RuntimeError as re:
        # perhaps a query syntax error? log it, but keep going so other projects succeed
        logger.error(f"  project {p['id']} - failed to fetch stories (likely a query syntax error)")
        logger.exception(re)
    return project_stories


def fetch_project_stories_task(project_list: List[Dict]) -> List[Dict]:
    with Pool(POOL_SIZE) as p:
        lists_of_stories = p.map(_project_story_worker, project_list)
    # flatten list of lists of stories into one big list
    combined_stories = [s for s in itertools.chain.from_iterable(lists_of_stories)]
    logger.info("Fetched {} total URLs from {}".format(len(combined_stories), processor.SOURCE_WAYBACK_MACHINE))
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
    logger.info("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))

    # important to do because there might be new models on the server!
    logger.info("  Checking for any new models we need")
    models_downloaded = download_models()
    logger.info(f"    models downloaded: {models_downloaded}")
    if not models_downloaded:
        sys.exit(1)
    start_time = time.time()
    # 1. list all the project we need to work on
    projects_list = load_projects_task()
    # 2. figure out domains to query for each project (There are 2 methods below both work fine.)
    #projects_with_domains = [fetch_domains_for_projects(project) for project in projects_list] 
    with Pool(POOL_SIZE) as p:
        projects_with_domains = p.map(fetch_domains_for_projects, projects_list)
    # 3. fetch all the urls from for each project from wayback machine (serially so we don't have to flatten 😖)
    all_stories = fetch_project_stories_task(projects_with_domains)
    # 4. fetch pre-parsed content (will happen in parallel by story)
    stories_with_text = fetch_text(all_stories)
    # 5. post batches of stories for classification
    results_data = tasks.queue_stories_for_classification(projects_list, stories_with_text,
                                                                       processor.SOURCE_WAYBACK_MACHINE)
    # 6. send email/slack_msg with results of operations
 
    tasks.send_combined_slack_message(results_data, processor.SOURCE_WAYBACK_MACHINE, start_time, logger)
    tasks.send_combined_email(results_data, processor.SOURCE_WAYBACK_MACHINE, start_time, logger)



