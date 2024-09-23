# ruff: noqa: E402

import datetime as dt
import logging
import multiprocessing
import os
import sys
import time
from typing import Dict, List

import pytz

# Disable loggers prior to package imports
import processor

processor.disable_package_loggers()

import mc_providers

import processor.database as database
import processor.database.projects_db as projects_db
import processor.database.stories_db as stories_db
import processor.projects as projects
import processor.tasks.classification as classification_tasks
import scripts.tasks as tasks
from processor import get_mc_client
from processor.classifiers import download_models

POOL_SIZE = int(os.environ.get("MC_POOL_SIZE", 3))
DAY_OFFSET = 1  # stories are ingested within a day of discovery
DAY_WINDOW = 4  # don't look for stories too old (DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW at most)
STORIES_PER_PAGE = 1000
MAX_STORIES_PER_PROJECT = 5000

INCLUSIVE_RANGE_START = "{"
EXCLUSIVE_RANGE_END = "]"

logger = logging.getLogger(__name__)

MC_PLATFORM_NAME = mc_providers.provider_name(
    mc_providers.PLATFORM_ONLINE_NEWS, mc_providers.PLATFORM_SOURCE_MEDIA_CLOUD
)


def load_projects_task() -> List[Dict]:
    project_list = projects.load_project_list(
        force_reload=True, overwrite_last_story=False
    )
    logger.info("  Checking {} projects".format(len(project_list)))
    # return [p for p in project_list if p['id'] == 177]
    return project_list


def fetch_stories(
    mc_client,
    query,
    pub_start_date,
    pub_end_date,
    project,
    page_size,
    platform,
    max_stories,
):
    all_stories = []
    page_token = None
    more_stories = True
    story_count = 0
    page_count = 0

    while more_stories and (story_count < max_stories):
        try:
            page_of_stories, page_token = mc_client.story_list(
                query,
                pub_start_date,
                pub_end_date,
                collection_ids=project["media_collections"],
                pagination_token=page_token,
                page_size=page_size,
                sort_order="desc",
                platform=platform,
                expanded=True,
            )

            if page_of_stories:
                all_stories.extend(page_of_stories)
                story_count += len(page_of_stories)
                page_count += 1
                logger.info(
                    "    {} - page {}: ({}) stories".format(
                        project["id"], page_count, len(page_of_stories)
                    )
                )
                more_stories = page_token is not None
            else:
                more_stories = False

        except Exception as e:
            logger.error(
                "  Story list error on project {}. Skipping project for now. {}".format(
                    project["id"], e
                )
            )
            more_stories = False
            continue

    return all_stories


def get_total_story_count(
    mc_client, query, pub_start_date, pub_end_date, collection_ids, platform, project
):
    try:
        total_stories = mc_client.story_count(
            query,
            pub_start_date,
            pub_end_date,
            collection_ids=collection_ids,
            platform=platform,
        )["relevant"]
        logger.info(
            "  Project {}: {} total stories".format(project["id"], total_stories)
        )
        return total_stories, None
    except Exception as e:
        logger.error(
            "  Couldn't count stories in project {}. Skipping project for now. {}".format(
                project["id"], e
            )
        )
        return e, None


def _process_project_task(args: Dict) -> Dict:
    project, page_size, max_stories = args
    Session = database.get_session_maker()
    # here confusingly start_date is a useful indexed_date, but end_date is a useful publication_date
    start_date, end_date = projects.query_start_end_dates(
        project,
        Session,
        DAY_OFFSET,
        DAY_WINDOW,
        processor.SOURCE_MEDIA_CLOUD,
    )
    utc = pytz.UTC
    indexed_start = start_date
    indexed_end = utc.localize(dt.datetime.now())
    pub_start_date = dt.date.today() - dt.timedelta(days=(DAY_OFFSET + DAY_WINDOW))
    pub_end_date = end_date.date()
    project_email_message = ""
    logger.info("Checking project {}/{}".format(project["id"], project["title"]))
    logger.debug("  {} stories/page up to {}".format(page_size, max_stories))
    indexed_date_query_clause = f"indexed_date:{INCLUSIVE_RANGE_START}{indexed_start.isoformat()} TO {indexed_end.isoformat()}{EXCLUSIVE_RANGE_END}"
    q = f"({project['search_terms']}) AND language:{project['language']} AND {indexed_date_query_clause}"
    mc = get_mc_client()
    # see how many stories
    total_stories, e = get_total_story_count(
        mc,
        q,
        pub_start_date,
        pub_end_date,
        project["media_collections"],
        MC_PLATFORM_NAME,
        project,
    )

    if total_stories is None:
        project_email_message += "    failed to count with {}\n\n".format(e)
        return dict(
            email_text=project_email_message,
            stories=0,
            pages=0,
        )

    # fetch stories
    stories = fetch_stories(
        mc,
        q,
        max_stories,
        pub_start_date,
        pub_end_date,
        project,
        page_size,
        MC_PLATFORM_NAME,
    )
    if not stories:
        return dict(email_text="", stories=0, pages=0)
    latest_indexed_date = dt.datetime.today() - dt.timedelta(weeks=2)
    page_latest_indexed_date = max([s["indexed_date"] for s in stories])
    latest_indexed_date = max(latest_indexed_date, page_latest_indexed_date)
    for s in stories:
        s["source"] = processor.SOURCE_MEDIA_CLOUD
        s["source_publish_date"] = str(s["publish_date"])
        s["indexed_date"] = s["indexed_date"]
        s["project_id"] = project["id"]
        s["story_text"] = s["text"]

    # process stories
    with Session() as session:
        stories_to_queue = stories_db.add_stories(
            session, stories, project, processor.SOURCE_MEDIA_CLOUD
        )
        story_count = len(stories_to_queue)
        classification_tasks.classify_and_post_worker.delay(project, stories_to_queue)

        projects_db.update_history(
            session,
            project["id"],
            latest_indexed_date,
            processor.SOURCE_MEDIA_CLOUD,
        )
        session.commit()

    # return results
    page_count = len(stories) // STORIES_PER_PAGE
    logger.info(
        "  queued {} stories for project {}/{} (in {} pages)".format(
            story_count, project["id"], project["title"], page_count
        )
    )
    warnings = ""
    if story_count > (max_stories * 0.8):
        warnings += "(⚠️️️ query might be too broad)"

    project_email_message = f"Project {project['id']} - {project['title']}:\n"
    project_email_message += (
        f"    found {story_count} new stories (over {page_count} pages) {warnings}\n\n"
    )

    return dict(
        email_text=project_email_message,
        stories=story_count,
        pages=page_count,
    )


def process_projects_in_parallel(projects_list: List[Dict], pool_size: int):
    args_list = [(p, STORIES_PER_PAGE, MAX_STORIES_PER_PROJECT) for p in projects_list]
    with multiprocessing.Pool(pool_size) as pool:
        results = pool.map(_process_project_task, args_list)
    return results[:MAX_STORIES_PER_PROJECT]


if __name__ == "__main__":
    logger.info("Starting {} story fetch job".format(processor.SOURCE_MEDIA_CLOUD))

    # important to do because there might be new models on the server!
    logger.info("  Checking for any new models we need")
    models_downloaded = download_models()
    logger.info(f"    models downloaded: {models_downloaded}")
    if not models_downloaded:
        sys.exit(1)
    start_time = time.time()
    # logger.info("    will request {} stories/page (up to {})".format(STORIES_PER_PAGE, MAX_STORIES_PER_PROJECT))

    # 1. list all the project we need to work on
    projects_list = load_projects_task()

    # 2. process all the projects (in parallel)
    logger.info(f"Processing project in parallel {POOL_SIZE}")
    project_results = process_projects_in_parallel(projects_list, POOL_SIZE)

    # 3. send email/slack_msg with results of operations
    logger.info(f"Total stories queued: {sum([p['stories'] for p in project_results])}")
    tasks.send_project_list_slack_message(
        project_results,
        processor.SOURCE_MEDIA_CLOUD,
        start_time,
    )
    tasks.send_project_list_email(
        project_results,
        processor.SOURCE_MEDIA_CLOUD,
        start_time,
    )
