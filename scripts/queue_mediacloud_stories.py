# ruff: noqa: E402

import datetime as dt
import logging
import multiprocessing
import sys
import time
from typing import Dict, List

# Disable loggers prior to package imports
import processor

processor.disable_package_loggers()

import processor.database as database
import processor.database.projects_db as projects_db
import processor.database.stories_db as stories_db
import processor.projects as projects
import processor.tasks as p_tasks
import scripts.tasks as tasks
from processor import get_mc_legacy_client
from processor.classifiers import download_models

DEFAULT_STORIES_PER_PAGE = 150  # I found this performs poorly if set too high
# use this to make sure we don't fall behind on recent stories, even if a project query is producing more than
# DEFAULT_MAX_STORIES_PER_PROJECT stories a day
DEFAULT_DAY_WINDOW = 5
DEFAULT_MAX_STORIES_PER_PROJECT = (
    10000  # make sure we don't do too many stories each cron run
)

logger = logging.getLogger(__name__)


def load_projects_task() -> List[Dict]:
    project_list = projects.load_project_list(
        force_reload=True, overwrite_last_story=False
    )
    logger.info("  Checking {} projects".format(len(project_list)))
    # return [p for p in project_list if p['id'] == 166]
    return project_list


def _process_project_task(args: Dict) -> Dict:
    project, page_size, max_stories = args
    mc = get_mc_legacy_client()
    project_last_processed_stories_id = project["local_processed_stories_id"]
    project_email_message = ""
    logger.info(
        "Checking project {}/{} (last processed_stories_id={})".format(
            project["id"], project["title"], project_last_processed_stories_id
        )
    )
    logger.debug("  {} stories/page up to {}".format(page_size, max_stories))
    project_email_message += "Project {} - {}:\n".format(
        project["id"], project["title"]
    )
    # setup queries to filter by language too so we only get stories the model can process
    q = "({}) AND language:{} AND tags_id_media:({})".format(
        project["search_terms"],
        project["language"],
        " ".join([str(tid) for tid in project["media_collections"]]),
    )
    now = dt.datetime.now()
    start_date = now - dt.timedelta(
        days=DEFAULT_DAY_WINDOW
    )  # err on the side of recentness over completeness
    fq = mc.dates_as_query_clause(start_date, now)
    story_count = 0
    page_count = 0
    more_stories = True
    while more_stories and (story_count < max_stories):
        try:
            page_of_stories = mc.storyList(
                q,
                fq,
                last_processed_stories_id=project_last_processed_stories_id,
                text=True,
                rows=page_size,
            )
            logger.info(
                "    {} - page {}: ({}) stories".format(
                    project["id"], page_count, len(page_of_stories)
                )
            )
        except Exception as e:
            logger.error(
                "  Story list error on project {}. Skipping for now. {}".format(
                    project["id"], e
                )
            )
            more_stories = False
            continue  # fail gracefully by going to the next project; maybe next cron run it'll work?
        if len(page_of_stories) > 0:
            for s in page_of_stories:
                s["source"] = processor.SOURCE_MEDIA_CLOUD
            page_count += 1
            story_count += len(page_of_stories)
            # and log that we got and queued them all
            Session = database.get_session_maker()
            with Session() as session:
                stories_to_queue = stories_db.add_stories(
                    session, page_of_stories, project, processor.SOURCE_MEDIA_CLOUD
                )
                p_tasks.classify_and_post_worker.delay(project, stories_to_queue)
                project_last_processed_stories_id = stories_to_queue[-1][
                    "processed_stories_id"
                ]
                # important to write this update now, because we have queued up the task to process these stories
                # the task queue will manage retrying with the stories if it fails with this batch
                projects_db.update_history(
                    session,
                    project["id"],
                    last_processed_stories_id=project_last_processed_stories_id,
                )
        else:
            more_stories = False
    logger.info(
        "  queued {} stories for project {}/{} (in {} pages)".format(
            story_count, project["id"], project["title"], page_count
        )
    )
    #  add a summary to the email we are generating
    warnings = ""
    if story_count > (max_stories * 0.8):  # try to get our attention in the email
        warnings += "(⚠️️️ query might be too broad)"
    project_email_message += (
        "    found {} new stories past {} (over {} pages) {}\n\n".format(
            story_count, project_last_processed_stories_id, page_count, warnings
        )
    )
    return dict(
        email_text=project_email_message,
        stories=story_count,
        pages=page_count,
    )


def process_projects_in_parallel(projects_list: List[Dict], pool_size: int):
    args_list = [
        (p, DEFAULT_STORIES_PER_PAGE, DEFAULT_MAX_STORIES_PER_PROJECT)
        for p in projects_list
    ]
    with multiprocessing.Pool(pool_size) as pool:
        results = pool.map(_process_project_task, args_list)
    return results


if __name__ == "__main__":
    logger.info("Starting {} story fetch job".format(processor.SOURCE_MEDIA_CLOUD))
    # important to do because there might new models on the server!
    logger.info("  Checking for any new models we need")
    models_downloaded = download_models()
    logger.info(f"    models downloaded: {models_downloaded}")
    if not models_downloaded:
        sys.exit(1)
    start_time = time.time()
    # logger.info("    will request {} stories/page (up to {})".format(stories_per_page, max_stories_per_project))
    # 1. list all the project we need to work on
    projects_list = load_projects_task()
    # 2. process all the projects (in parallel)
    pool_size = 4
    project_results = process_projects_in_parallel(projects_list, pool_size)
    # 3. send email/slack_msg with results of operations
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
