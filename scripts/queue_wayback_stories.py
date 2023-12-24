# ruff: noqa: E402

import datetime as dt
import itertools
import json
import logging
import sys
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
import processor.mcdirectory as mcdirectory
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


logger = logging.getLogger(__name__)


def load_projects() -> List[Dict]:
    project_list = projects.load_project_list(
        force_reload=True, overwrite_last_story=False
    )
    logger.info("  Found {} projects".format(len(project_list)))
    # return project_list[20:25]
    # return [p for p in project_list if p['id'] == 166]
    return project_list


def _query_builder(terms: str, language: str) -> str:
    terms_no_curlies = terms.replace("“", '"').replace("”", '"')
    return "({}) AND (language:{})".format(terms_no_curlies, language)


def _project_story_worker(p: Dict) -> List[Dict]:
    Session = database.get_session_maker()
    start_date, end_date, history = projects.query_start_end_dates(
        p,
        Session,
        DEFAULT_DAY_OFFSET,
        DEFAULT_DAY_WINDOW,
        processor.SOURCE_WAYBACK_MACHINE,
        False,
    )
    project_stories = []
    page_number = 1
    full_project_query = _query_builder(p["search_terms"], p["language"])
    try:
        wm_provider = OnlineNewsWaybackMachineProvider()
        total_hits = wm_provider.count(
            full_project_query, start_date, end_date, domains=p["domains"]
        )
        time.sleep(0.1)  # don't hammer the API
        logger.info(
            "Project {}/{} - {} total stories from {} domains (since {})".format(
                p["id"], p["title"], total_hits, len(p["domains"]), start_date
            )
        )
        if total_hits > 0:  # don't bother querying if no results to page through
            # using the provider wrapper so this does the chunking into smaller queries for us
            latest_pub_date = dt.datetime.now() - dt.timedelta(
                weeks=50
            )  # a long, long time ago
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
                # track most recent story across all pages (seems to be sorted default by surt_url asc)
                try:
                    page_latest_pub_date = max([s["publish_date"] for s in page])
                    latest_pub_date = max(latest_pub_date, page_latest_pub_date)
                except Exception:  # maybe no stories on this page?
                    pass
                # prep all stories
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
                time.sleep(0.1)  # don't hammer the API
            logger.info(
                "  project {} - {} stories (after {})".format(
                    p["id"], len(project_stories), start_date
                )
            )
            # after all pages done, update latest pub date so we start at that next time
            with Session() as session:
                projects_db.update_history(
                    session, p["id"], latest_pub_date, processor.SOURCE_WAYBACK_MACHINE
                )
    except Exception as e:
        # perhaps a query syntax error? log it, but keep going so other projects succeed
        logger.error(
            f"  project {p['id']} - failed to fetch stories (likely a query syntax or connection error)"
        )
        # logger.exception(e) #Ignore Sentry Logging
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
    logger.info("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))

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
        projects_with_domains = p.map(
            mcdirectory.fetch_domains_for_projects, projects_list
        )

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
