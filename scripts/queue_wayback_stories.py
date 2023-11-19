# ruff: noqa: E402

import copy
import datetime as dt
import itertools
import json
import logging
import sys
import threading
import time
from multiprocessing import Pool
from typing import Dict, List

# Disable loggers prior to package imports
import processor

processor.disable_package_loggers()

from mc_providers.onlinenews import OnlineNewsWaybackMachineProvider

import processor.database as database
import processor.database.projects_db as projects_db
import processor.fetcher as fetcher
import processor.projects as projects
import scripts.tasks as tasks
from processor.classifiers import download_models

POOL_SIZE = 8  # used for fetching project domains and listing stories in parallel
DEFAULT_DAY_OFFSET = 4  # stories don't get processed for a few days
DEFAULT_DAY_WINDOW = 3  # don't look for stories too old (DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW at most)
PAGE_SIZE = 1000
MAX_STORIES_PER_PROJECT = (
    5000  # we can't process all the stories for queries that are too big
)

wm_provider = OnlineNewsWaybackMachineProvider()


logger = logging.getLogger(__name__)


def load_projects() -> List[Dict]:
    project_list = projects.load_project_list(
        force_reload=True, overwrite_last_story=False
    )
    logger.info("  Found {} projects".format(len(project_list)))
    # return project_list[20:25]
    # return [p for p in project_list if p['id'] == 166]
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
    mc_directory_api = processor.get_mc_directory_client()
    while True:
        response = mc_directory_api.source_list(
            collection_id=cid, limit=limit, offset=offset
        )
        # for now we need to remove any sources that have a url_search_string because they are not supported in the API
        # (wildcard search bug on the IA side)
        sources += [r for r in response["results"] if r["url_search_string"] is None]
        if response["next"] is None:
            break
        offset += limit
    return [
        s["name"] for s in sources if s["name"] is not None
    ]  # grab just the domain names


def _cached_domains_for_collection(cid: int) -> List[str]:
    # fetch info if it isn't cached
    if not _sources_are_cached(cid):
        logger.debug(f"Collection {cid}: sources not cached, fetching")
        sources = _domains_for_collection(cid)
        _sources_set(cid, sources)
    else:
        # otherwise, load up cache to reduce server queries and runtime overall
        sources = _sources_get(cid)
        logger.debug(f"Collection {cid}: found sources cached {len(sources)}")
    return [s["name"] for s in sources if s["name"] is not None]


def _domains_for_project(collection_ids: List[int]) -> List[str]:
    all_domains = []
    for cid in collection_ids:  # fetch all the domains in each collection
        all_domains += _domains_for_collection(
            cid
        )  # remove cache because Prefect failing to serials with thread lock error :-(
    return list(set(all_domains))  # make them unique


def fetch_domains_for_projects(project: Dict) -> Dict:
    domains = _domains_for_project(project["media_collections"])
    logger.info(
        f"Project {project['id']}/{project['title']}: found {len(domains)} domains"
    )
    updated_project = copy.copy(project)
    updated_project["domains"] = domains
    return updated_project


def _query_builder(terms: str, language: str) -> str:
    terms_no_curlies = terms.replace("“", '"').replace("”", '"')
    return "({}) AND (language:{})".format(terms_no_curlies, language)


def _project_story_worker(p: Dict) -> List[Dict]:
    end_date = dt.datetime.now() - dt.timedelta(
        days=DEFAULT_DAY_OFFSET
    )  # stories don't get processed for a few days
    project_stories = []
    Session = database.get_session_maker()
    with Session() as session:
        history = projects_db.get_history(session, p["id"])
    page_number = 1
    # only search stories since the last search (if we've done one before)
    start_date = end_date - dt.timedelta(days=DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW)
    if history.last_publish_date is not None:
        # make sure we don't accidentally cut off a half day we haven't queried against yet
        # this is OK because duplicates will get screened out later in the pipeline
        local_start_date = history.last_publish_date - dt.timedelta(days=1)
        start_date = max(local_start_date, start_date)
    full_project_query = _query_builder(p["search_terms"], p["language"])
    try:
        total_hits = wm_provider.count(
            full_project_query, start_date, end_date, domains=p["domains"]
        )
        time.sleep(0.2)  # don't hammer the API
        logger.info(
            "Project {}/{} - {} total stories from {} domains (since {})".format(
                p["id"], p["title"], total_hits, len(p["domains"]), start_date
            )
        )
        if total_hits > 0:  # don't bother querying if no results to page through
            # using the provider wrapper so this does the chunking into smaller queries for us
            for page in wm_provider.all_items(
                full_project_query,
                start_date,
                end_date,
                domains=p["domains"],
                page_size=PAGE_SIZE,
            ):
                if len(project_stories) > MAX_STORIES_PER_PROJECT:
                    break
                logger.debug(
                    "  {} - page {}: {} stories".format(p["id"], page_number, len(page))
                )
                for item in page:
                    if len(project_stories) > MAX_STORIES_PER_PROJECT:
                        break
                    info = dict(
                        id=item[
                            "id"
                        ],  # unique ID that can be used to fetch the pre-parsed text from the WM archive
                        # path to pre-parsed content JSON - so we don't have to fetch and parse the HTML ourselves
                        extracted_content_url=item["article_url"],
                        url=item["url"],
                        # the rest of the pipeline expects a str, @see tasks.queue_stories_for_classification
                        source_publish_date=str(item["publish_date"]),
                        title=item["title"],
                        source=processor.SOURCE_WAYBACK_MACHINE,
                        project_id=p["id"],
                        language=item["language"],
                        authors=None,
                        media_url=item["media_url"],
                        media_name=item["media_name"],  # same as item['media_url']
                        archived_url=item[
                            "archived_url"
                        ],  # the URL to the Wayback Machine provided HTML copy
                    )
                    project_stories.append(info)
                time.sleep(0.2)  # don't hammer the API
            logger.info(
                "  project {} - {} stories (after {})".format(
                    p["id"], len(project_stories), history.last_publish_date
                )
            )
    except Exception as e:
        # perhaps a query syntax error? log it, but keep going so other projects succeed
        logger.error(
            f"  project {p['id']} - failed to fetch stories (likely a query syntax or connection error)"
        )
        logger.exception(e)
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
    logger.info(
        "Fetched {} total URLs from {}".format(
            len(combined_stories), processor.SOURCE_NEWSCATCHER
        )
    )
    return combined_stories


def fetch_text(stories: List[Dict]) -> List[Dict]:
    stories_to_return = []

    def handle_parse(response_data: Dict):
        # called for each story that successfully is fetched by Scrapy
        nonlocal stories, stories_to_return
        try:
            story_details = json.loads(response_data["content"])
            # find matching stories in the original input list based on `extracted_content_url`, because we are
            # fetching from that and not he actual story URL
            matching_input_stories = [
                s
                for s in stories
                if s["extracted_content_url"] == response_data["original_url"]
            ]
            for s in matching_input_stories:
                s["story_text"] = story_details["snippet"]
                stories_to_return.append(s)
        except Exception as e:
            # this just happens occasionally so it is a normal case
            logger.warning(
                f"Skipping story - failed to fetch due to {e} - from {response_data['original_url']}"
            )

    # download them all in parallel... will take a while (note that we're fetching the extracted content JSON here,
    # NOT the archived or original HTML because that saves us the parsing and extraction step)
    urls = [s["extracted_content_url"] for s in stories]
    fetcher.fetch_all_html(urls, handle_parse)
    logger.info(
        "Fetched text for {} stories (failed on {})".format(
            len(stories_to_return), len(stories) - len(stories_to_return)
        )
    )
    return stories_to_return


if __name__ == "__main__":

    def wayback_stories_flow(data_source: str):
        logger.info(
            "Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE)
        )

        # important to do because there might be new models on the server!
        logger.info("  Checking for any new models we need")
        models_downloaded = download_models()
        logger.info(f"    models downloaded: {models_downloaded}")
        if not models_downloaded:
            sys.exit(1)
        start_time = time.time()

        # 1. list all the project we need to work on
        projects_list = load_projects()
        logger.info("Working with {} projects".format(len(projects_list)))

        # 2. figure out domains to query for each project
        with Pool(POOL_SIZE) as p:
            projects_with_domains = p.map(fetch_domains_for_projects, projects_list)

        # 3. fetch all the urls from for each project from wayback machine (serially so we don't have to flatten 😖)
        all_stories = fetch_project_stories(projects_with_domains)
        logger.info("Discovered {} total story URLs".format(len(all_stories)))

        # 4. fetch pre-parsed content (will happen in parallel by story)
        stories_with_text = fetch_text(all_stories)
        logger.info("Fetched {} stories with text".format(len(stories_with_text)))

        # 5. post batches of stories for classification
        results_data = tasks.queue_stories_for_classification(
            projects_list, stories_with_text, processor.SOURCE_WAYBACK_MACHINE
        )

        # 6. send email/slack_msg with results of operations
        tasks.send_combined_slack_message(
            results_data, processor.SOURCE_WAYBACK_MACHINE, start_time
        )
        tasks.send_combined_email(
            results_data, processor.SOURCE_WAYBACK_MACHINE, start_time
        )

    # run the whole thing
    wayback_stories_flow(processor.SOURCE_WAYBACK_MACHINE)
