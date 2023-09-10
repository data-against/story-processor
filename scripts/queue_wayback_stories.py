Original Approach
# from typing import List, Dict, Optional
# import time
# import sys
# import copy
# import threading
# import numpy as np
# import mcmetadata.urls as urls
# import datetime as dt
# from prefect import flow, task, get_run_logger, unmapped
# from waybacknews.searchapi import SearchApiClient
# from prefect_dask.task_runners import DaskTaskRunner
# import requests.exceptions

# import processor
# import processor.database as database
# import processor.database.projects_db as projects_db
# from processor.classifiers import download_models
# import processor.projects as projects
# import scripts.tasks as prefect_tasks

# PAGE_SIZE = 1000
# DEFAULT_DAY_OFFSET = 4
# DEFAULT_DAY_WINDOW = 3
# MAX_STORIES_PER_PROJECT = 5000
# TEXT_FETCH_TIMEOUT = 5  # seconds to wait for full text fetch to complete

# wm_api = SearchApiClient("mediacloud")


# @task(name='load_projects')
# def load_projects_task() -> List[Dict]:
#     logger = get_run_logger()
#     project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
#     logger.info("  Found {} projects".format(len(project_list)))
#     #return [p for p in project_list if p['id'] == 166]
#     return project_list


# # Wacky memory solution here for caching sources in collections because file-based cache failed on prod server 😖
# collection2sources_lock = threading.Lock()
# collection2sources = {}
# def _sources_are_cached(cid: int) -> bool:
#     collection2sources_lock.acquire()
#     is_cached = cid in collection2sources
#     collection2sources_lock.release()
#     return is_cached
# def _sources_set(cid: int, domains: List[Dict]):
#     collection2sources_lock.acquire()
#     collection2sources[cid] = domains.copy()
#     collection2sources_lock.release()
# def _sources_get(cid: int) -> List[Dict]:
#     collection2sources_lock.acquire()
#     domains = collection2sources[cid]
#     collection2sources_lock.release()
#     return domains


# def _domains_for_collection(cid: int) -> List[str]:
#     limit = 1000
#     offset = 0
#     sources = []
#     mc_api = processor.get_mc_client()
#     while True:
#         response = mc_api.source_list(collection_id=cid, limit=limit, offset=offset)
#         sources += response['results']
#         if response['next'] is None:
#             break
#         offset += limit
#     return [s['name'] for s in sources if s['name'] is not None]  # grab just the domain names


# def _cached_domains_for_collection(cid: int) -> List[str]:
#     logger = get_run_logger()
#     # fetch info if it isn't cached
#     if not _sources_are_cached(cid):
#         logger.debug(f'Collection {cid}: sources not cached, fetching')
#         sources = _domains_for_collection(cid)
#         _sources_set(cid, sources)
#     else:
#         # otherwise, load up cache to reduce server queries and runtime overall
#         sources = _sources_get(cid)
#         logger.debug(f'Collection {cid}: found sources cached {len(sources)}')
#     return [s['name'] for s in sources if s['name'] is not None]


# def _domains_for_project(collection_ids: List[int]) -> List[str]:
#     all_domains = []
#     for cid in collection_ids:  # fetch all the domains in each collection
#         all_domains += _domains_for_collection(cid)  # remove cache because Prefect failing to serials with thread lock error :-(
#     return list(set(all_domains))  # make them unique


# @task(name='domains_for_project')
# def fetch_domains_for_projects(project: Dict) -> Dict:
#     logger = get_run_logger()
#     domains = _domains_for_project(project['media_collections'])
#     logger.info(f"Project {project['id']}/{project['title']}: found {len(domains)} domains")
#     updated_project = copy.copy(project)
#     updated_project['domains'] = domains
#     return updated_project


# def _query_builder(terms: str, language: str, domains: list) -> str:
#     terms_no_curlies = terms.replace('“', '"').replace('”', '"')
#     return "({}) AND (language:{}) AND ({})".format(terms_no_curlies, language, " OR ".join(
#         [f"domain:{d}" for d in domains]))


# @task(name='fetch_project_stories')
# def fetch_project_stories_task(project_list: Dict, data_source: str) -> List[Dict]:
#     logger = get_run_logger()
#     combined_stories = []
#     end_date = dt.datetime.now() - dt.timedelta(days=DEFAULT_DAY_OFFSET)  # stories don't get processed for a few days
#     for p in project_list:
#         project_stories = []
#         valid_stories = 0
#         Session = database.get_session_maker()
#         with Session() as session:
#             history = projects_db.get_history(session, p['id'])
#         page_number = 1
#         # only search stories since the last search (if we've done one before)
#         start_date = end_date - dt.timedelta(days=DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW)
#         if history.last_publish_date is not None:
#             # make sure we don't accidentally cut off a half day we haven't queried against yet
#             # this is OK because duplicates will get screened out later in the pipeline
#             local_start_date = history.last_publish_date - dt.timedelta(days=1)
#             start_date = min(local_start_date, start_date)
#         # if query is too big we need to split it up
#         full_project_query = _query_builder(p['search_terms'], p['language'], p['domains'])
#         project_queries = [full_project_query]
#         domain_divisor = 2
#         queries_too_big = len(full_project_query) > pow(2, 14)
#         if queries_too_big:
#             while queries_too_big:
#                 chunked_domains = np.array_split(p['domains'], domain_divisor)
#                 project_queries = [_query_builder(p['search_terms'], p['language'], d) for d in chunked_domains]
#                 queries_too_big = any(len(pq) > pow(2, 14) for pq in project_queries)
#                 domain_divisor *= 2
#             logger.info('Project {}/{}: split query into {} parts'.format(p['id'], p['title'], len(project_queries)))
#         # now run all queries
#         try:
#             for project_query in project_queries:
#                 if valid_stories > MAX_STORIES_PER_PROJECT:
#                     break
#                 total_hits = wm_api.count(project_query, start_date, end_date)
#                 logger.info("Project {}/{} - {} total stories (since {})".format(p['id'], p['title'],
#                                                                                  total_hits, start_date))
#                 for page in wm_api.all_articles(project_query, start_date, end_date, page_size=PAGE_SIZE):
#                     if valid_stories > MAX_STORIES_PER_PROJECT:
#                         break
#                     logger.debug("  {} - page {}: {} stories".format(p['id'], page_number, len(page)))
#                     for item in page:
#                         media_url = item['domain'] if len(item['domain']) > 0 else urls.canonical_domain(item['url'])
#                         info = dict(
#                             url=item['url'],
#                             source_publish_date=item['publication_date'],
#                             title=item['title'],
#                             source=data_source,
#                             project_id=p['id'],
#                             language=item['language'],
#                             authors=None,
#                             media_url=media_url,
#                             media_name=media_url,
#                             article_url=item['article_url']
#                         )
#                         project_stories.append(info)
#                         valid_stories += 1
#             logger.info("  project {} - {} valid stories (after {})".format(p['id'], valid_stories,
#                                                                             history.last_publish_date))
#             combined_stories += project_stories
#         except RuntimeError as re:
#             # perhaps a query syntax error? log it, but keep going so other projects succeed
#             logger.error(f"  project {p['id']} - failed to fetch stories (likely a query syntax error)")
#             logger.exception(re)
#     return combined_stories


# @task(name='fetch_archived_text')
# def fetch_archived_text_task(story: Dict) -> Optional[Dict]:
#     logger = get_run_logger()
#     try:
#         story_details = requests.get(story['article_url'], timeout=TEXT_FETCH_TIMEOUT).json()
#         updated_story = copy.copy(story)
#         updated_story['story_text'] = story_details['snippet']
#         return updated_story
#     except Exception as e:
#         # this just happens occasionally so it is a normal case
#         logger.warning(f"Skipping story - failed to fetch due to {e} - from {story['article_url']}")


# if __name__ == '__main__':

#     @flow(name="wayback_stories", task_runner=DaskTaskRunner())
#     def wayback_stories_flow(data_source: str):
#         logger = get_run_logger()
#         logger.info("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))

#         # important to do because there might be new models on the server!
#         logger.info("  Checking for any new models we need")
#         models_downloaded = download_models()
#         logger.info(f"    models downloaded: {models_downloaded}")
#         if not models_downloaded:
#             sys.exit(1)
#         data_source_name = data_source
#         start_time = time.time()
#         # 1. list all the project we need to work on
#         projects_list = load_projects_task()
#         # 2. figure out domains to query for each project
#         projects_with_domains = fetch_domains_for_projects.map(projects_list)
#         # 3. fetch all the urls from for each project from wayback machine (serially so we don't have to flatten 😖)
#         all_stories = fetch_project_stories_task(projects_with_domains, unmapped(data_source_name))
#         # 4. fetch pre-parsed content (will happen in parallel by story)
#         stories_with_text = fetch_archived_text_task.map(all_stories)
#         # 5. post batches of stories for classification
#         results_data = prefect_tasks.queue_stories_for_classification_task(projects_list, stories_with_text,
#                                                                            data_source_name)
#         # 6. send email/slack_msg with results of operations
#         prefect_tasks.send_combined_slack_message_task(results_data, data_source_name, start_time)
#         prefect_tasks.send_combined_email_task(results_data, data_source_name, start_time)

#     # run the whole thing
#     wayback_stories_flow(processor.SOURCE_WAYBACK_MACHINE)


Approach 1 (Using ThreadPoolExecutor and beautifulsoup)
# from typing import List, Dict, Optional
# import time
# import sys
# import copy
# import threading
# import logging
# import dateutil.parser
# import numpy as np
# import mcmetadata.urls as urls
# import datetime as dt
# from waybacknews.searchapi import SearchApiClient
# import requests.exceptions
# from concurrent.futures import ThreadPoolExecutor
# import processor
# import processor.database as database
# import processor.database.projects_db as projects_db
# from processor.classifiers import download_models
# import processor.projects as projects
# import scripts.tasks as prefect_tasks
# from processor import  get_slack_config
# import processor.tasks as celery_tasks
# import processor.notifications as notifications
# import processor.database as database
# from processor.database import stories_db as stories_db


# PAGE_SIZE = 1000
# DEFAULT_DAY_OFFSET = 4
# DEFAULT_DAY_WINDOW = 3
# MAX_STORIES_PER_PROJECT = 50
# TEXT_FETCH_TIMEOUT = 5  # seconds to wait for full text fetch to complete

# wm_api = SearchApiClient("mediacloud")



# def load_projects_task() -> List[Dict]:
#     logger = logging.getLogger(__name__)
#     project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
#     logger.info("  Found {} projects".format(len(project_list)))
#     #return [p for p in project_list if p['id'] == 166]
#     return project_list


# # Wacky memory solution here for caching sources in collections because file-based cache failed on prod server 😖
# collection2sources_lock = threading.Lock()
# collection2sources = {}
# def _sources_are_cached(cid: int) -> bool:
#     collection2sources_lock.acquire()
#     is_cached = cid in collection2sources
#     collection2sources_lock.release()
#     return is_cached
# def _sources_set(cid: int, domains: List[Dict]):
#     collection2sources_lock.acquire()
#     collection2sources[cid] = domains.copy()
#     collection2sources_lock.release()
# def _sources_get(cid: int) -> List[Dict]:
#     collection2sources_lock.acquire()
#     domains = collection2sources[cid]
#     collection2sources_lock.release()
#     return domains


# def _domains_for_collection(cid: int) -> List[str]: #same or change
#     limit = 1000
#     offset = 0
#     sources = []
#     mc_api = processor.get_mc_client()
#     while True:
#         response = mc_api.source_list(collection_id=cid, limit=limit, offset=offset)
#         sources += response['results']
#         if response['next'] is None:
#             break
#         offset += limit
#     return [s['name'] for s in sources if s['name'] is not None]  # grab just the domain names


# def _cached_domains_for_collection(cid: int) -> List[str]:
#     logger = logging.getLogger(__name__)
#     # fetch info if it isn't cached
#     if not _sources_are_cached(cid):
#         logger.debug(f'Collection {cid}: sources not cached, fetching')
#         sources = _domains_for_collection(cid)
#         _sources_set(cid, sources)
#     else:
#         # otherwise, load up cache to reduce server queries and runtime overall
#         sources = _sources_get(cid)
#         logger.debug(f'Collection {cid}: found sources cached {len(sources)}')
#     return [s['name'] for s in sources if s['name'] is not None]


# def _domains_for_project(collection_ids: List[int]) -> List[str]:
#     all_domains = []
#     for cid in collection_ids:  # fetch all the domains in each collection
#         all_domains += _domains_for_collection(cid)  # remove cache because Prefect failing to serials with thread lock error :-(
#     return list(set(all_domains))  # make them unique


# def fetch_domains_for_projects(project: Dict) -> Dict:
#     logger = logging.getLogger(__name__)
#     domains = _domains_for_project(project['media_collections'])
#     logger.info(f"Project {project['id']}/{project['title']}: found {len(domains)} domains")
#     updated_project = copy.copy(project)
#     updated_project['domains'] = domains
#     return updated_project


# def _query_builder(terms: str, language: str, domains: list) -> str:
#     terms_no_curlies = terms.replace('“', '"').replace('”', '"')
#     return "({}) AND (language:{}) AND ({})".format(terms_no_curlies, language, " OR ".join(
#         [f"domain:{d}" for d in domains]))

# def fetch_project_stories_task(project_list: Dict, data_source: str) -> List[Dict]:
#     logger = logging.getLogger(__name__)
#     combined_stories = []
#     end_date = dt.datetime.now() - dt.timedelta(days=DEFAULT_DAY_OFFSET)  # stories don't get processed for a few days
#     project_stories = []
#     valid_stories = 0
#     Session = database.get_session_maker()
#     with Session() as session:
#         history = projects_db.get_history(session, project_list['id'])
#     page_number = 1
#     # only search stories since the last search (if we've done one before)
#     start_date = end_date - dt.timedelta(days=DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW)
#     if history.last_publish_date is not None:
#         # make sure we don't accidentally cut off a half day we haven't queried against yet
#         # this is OK because duplicates will get screened out later in the pipeline
#         local_start_date = history.last_publish_date - dt.timedelta(days=1)
#         start_date = min(local_start_date, start_date)
#     # if query is too big we need to split it up
#     full_project_query = _query_builder(project_list['search_terms'], project_list['language'], project_list['domains'])
#     project_queries = [full_project_query]
#     domain_divisor = 2
#     queries_too_big = len(full_project_query) > pow(2, 14)
#     if queries_too_big:
#         while queries_too_big:
#             chunked_domains = np.array_split(project_list['domains'], domain_divisor)
#             project_queries = [_query_builder(project_list['search_terms'], project_list['language'], d) for d in chunked_domains]
#             queries_too_big = any(len(pq) > pow(2, 14) for pq in project_queries)
#             domain_divisor *= 2
#         logger.info('Project {}/{}: split query into {} parts'.format(project_list['id'], project_list['title'], len(project_queries)))
#     # now run all queries
#     try:
#         for project_query in project_queries:
#             if valid_stories > MAX_STORIES_PER_PROJECT:
#                 break
#             total_hits = wm_api.count(project_query, start_date, end_date)
#             logger.info("Project {}/{} - {} total stories (since {})".format(project_list['id'], project_list['title'],
#                                                                              total_hits, start_date))
#             for page in wm_api.all_articles(project_query, start_date, end_date, page_size=PAGE_SIZE):
#                 if valid_stories > MAX_STORIES_PER_PROJECT:
#                     break
#                 logger.debug("  {} - page {}: {} stories".format(project_list['id'], page_number, len(page)))
#                 for item in page:
#                     media_url = item['domain'] if len(item['domain']) > 0 else urls.canonical_domain(item['url'])
#                     info = dict(
#                         url=item['url'],
#                         source_publish_date=item['publication_date'],
#                         title=item['title'],
#                         source=data_source,
#                         project_id=project_list['id'],
#                         language=item['language'],
#                         authors=None,
#                         media_url=media_url,
#                         media_name=media_url,
#                         article_url=item['article_url']
#                     )
#                     project_stories.append(info)
#                     valid_stories += 1
#         logger.info("  project {} - {} valid stories (after {})".format(project_list['id'], valid_stories,
#                                                                         history.last_publish_date))
#         combined_stories += project_stories
#     except RuntimeError as re:
#         # perhaps a query syntax error? log it, but keep going so other projects succeed
#         logger.error(f"  project {project_list['id']} - failed to fetch stories (likely a query syntax error)")
#         logger.exception(re)
#     return combined_stories


# def fetch_archived_text_task(story: Dict) -> Optional[Dict]:
#     logger = logging.getLogger(__name__)
#     try:
#         story_details = requests.get(story['article_url'], timeout=TEXT_FETCH_TIMEOUT).json()
#         updated_story = copy.copy(story)
#         updated_story['story_text'] = story_details['snippet']
#         return updated_story
#     except Exception as e:
#         # this just happens occasionally so it is a normal case
#         logger.warning(f"Skipping story - failed to fetch due to {e} - from {story['article_url']}")

# def _get_combined_text(project_count: int, email_text: str, story_count: int, data_source: str) -> str:
#     email_message = ""
#     email_message += "Checking {} projects.\n\n".format(project_count)
#     email_message += email_text
#     email_message += "\nDone - pulled {} stories.\n\n" \
#                      "(An automated email from your friendly neighborhood {} story processor)" \
#         .format(story_count, data_source)
#     return email_message

# def send_combined_slack_message_task(summary: Dict, data_source: str, start_time: float):
#     # :param project_details: array of dicts per project, each with 'email_text', 'stories', and 'pages' keys
#     message = _get_combined_text(summary['project_count'], summary['email_text'], summary['stories'], data_source)
#     _send_slack_message(data_source, summary['stories'], start_time, message)


# def send_project_list_slack_message_task(project_details: List[Dict], data_source: str, start_time: float):
#     # :param summary: has keys 'project_count', 'email_text', 'stories'
#     total_new_stories = sum([p['stories'] for p in project_details])
#     # total_pages = sum([p['pages'] for p in project_details])
#     combined_text = ""
#     for p in project_details:
#         combined_text += p['email_text']
#     message = _get_combined_text(len(project_details), combined_text, total_new_stories, data_source)
#     _send_slack_message(data_source, total_new_stories, start_time, message)


# def _send_slack_message(data_source: str, story_count: int, start_time: float, slack_message: str):
#     logger = logging.getLogger(__name__)
#     duration_secs = time.time() - start_time
#     duration_mins = str(round(duration_secs / 60, 2))
#     if get_slack_config():
#         slack_config = get_slack_config()
#         notifications.send_slack_msg(slack_config['channel_id'], slack_config['bot_token'],data_source,
#                                      "Feminicide {} Update: {} stories ({} mins)".format(
#                                          data_source, story_count, duration_mins),
#                                      slack_message)
#     else:
#         logger.info("Not sending any slack updates")


# def queue_stories_for_classification_task(project_list: List[Dict], stories: List[Dict], datasource: str) -> Dict:
#     logger = logging.getLogger(__name__)
#     total_stories = 0
#     email_message = ""
#     for p in project_list:
#         project_stories = [s for s in stories if (s is not None) and (s['project_id'] == p['id'])]
#         email_message += "Project {} - {}: {} stories\n".format(p['id'], p['title'], len(project_stories))
#         total_stories += len(project_stories)
#         if len(project_stories) > 0:
#             # External source has guess dates (Newscatcher/Google), so use that
#             for s in project_stories:
#                 if 'source_publish_date' in s:
#                     s['publish_date'] = s['source_publish_date']
#             # and log that we got and queued them all
#             Session = database.get_session_maker()
#             with Session() as session:
#                 project_stories = stories_db.add_stories(session, project_stories, p, datasource)
#                 if len(project_stories) > 0:  # don't queue up unnecessary tasks
#                     # important to do this *after* we add the stories_id here
#                     celery_tasks.classify_and_post_worker.delay(p, project_stories)
#                     # important to write this update now, because we have queued up the task to process these stories
#                     # the task queue will manage retrying with the stories if it fails with this batch
#                     publish_dates = [dateutil.parser.parse(s['source_publish_date']) for s in project_stories]
#                     latest_date = max(publish_dates)
#                     projects_db.update_history(session, p['id'], last_publish_date=latest_date, last_url=project_stories[0]['url'])
#         logger.info("  queued {} stories for project {}/{}".format(total_stories, p['id'], p['title']))
#     return dict(
#         email_text=email_message,
#         project_count=len(project_list),
#         stories=total_stories
#     )


# if __name__ == '__main__':
#     # Setup logger
#     logger =  logging.getLogger(__name__)

#     # Start logging
#     logger.info("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))
#     logger.info("Checking for any new models we need")
    
#     models_downloaded = download_models()
#     logger.info(f"models downloaded: {models_downloaded}")

#     if not models_downloaded:
#         sys.exit(1)

#     data_source_name = processor.SOURCE_WAYBACK_MACHINE
#     start_time = time.time()

#     # Initialize an empty list to store future results
#     projects_with_domains = []
#     all_stories = []
#     stories_with_text = []

#     with ThreadPoolExecutor() as executor:
#         # Step 1: List all the projects we need to work on
#         projects_list = load_projects_task()
        
#         # Step 2: Figure out domains to query for each project
#         for project in projects_list:
#             future = executor.submit(fetch_domains_for_projects, project)
#             projects_with_domains.append(future.result())
#         # with open('output.txt', 'w', encoding='utf-8') as f:
#         #     f.write(str(projects_with_domains))

#         # Step 3: Fetch all the URLs for each project from the Wayback Machine
#         for project in projects_with_domains:
#             future = executor.submit(fetch_project_stories_task, project, data_source_name)
#             all_stories.extend(future.result())

#         # Step 4: Fetch pre-parsed content (happening in parallel by story)
#         for story in all_stories:
#             future = executor.submit(fetch_archived_text_task, story)
#             fetched_story = future.result()
#             if fetched_story:
#                 stories_with_text.append(fetched_story)
                
#     # Steps 5 and 6: These are dependent on all threads completing, so they are outside the ThreadPoolExecutor block
#     # Step 5: Post batches of stories for classification
#     results_data = queue_stories_for_classification_task(projects_list, stories_with_text, data_source_name)
    
#     # Step 6: Send email/Slack messages with results of operations
#     send_combined_slack_message_task(results_data, data_source_name, start_time)
#     #prefect_tasks.send_combined_email_task(results_data, data_source_name, start_time)


Approach 2 (using Multiprocessingpool and Spyder) [Got an API Error for this]
# from typing import List, Dict, Optional
# import time
# import sys
# import copy
# import threading
# import logging
# import dateutil.parser
# import numpy as np
# import mcmetadata.urls as urls
# import datetime as dt
# from waybacknews.searchapi import SearchApiClient
# import requests.exceptions
# from concurrent.futures import ThreadPoolExecutor
# import processor
# import processor.database as database
# import processor.database.projects_db as projects_db
# from processor.classifiers import download_models
# import processor.projects as projects
# import scripts.tasks as prefect_tasks
# from processor import  get_slack_config
# import processor.tasks as celery_tasks
# import processor.notifications as notifications
# import processor.database as database
# from processor.database import stories_db as stories_db
# from multiprocessing import Pool



# PAGE_SIZE = 1000
# DEFAULT_DAY_OFFSET = 4
# DEFAULT_DAY_WINDOW = 3
# MAX_STORIES_PER_PROJECT = 50
# TEXT_FETCH_TIMEOUT = 5  # seconds to wait for full text fetch to complete

# wm_api = SearchApiClient("mediacloud")



# def load_projects_task() -> List[Dict]:
#     logger = logging.getLogger(__name__)
#     project_list = projects.load_project_list(force_reload=True, overwrite_last_story=False)
#     logger.info("  Found {} projects".format(len(project_list)))
#     #return [p for p in project_list if p['id'] == 166]
#     return project_list


# # Wacky memory solution here for caching sources in collections because file-based cache failed on prod server 😖
# collection2sources_lock = threading.Lock()
# collection2sources = {}
# def _sources_are_cached(cid: int) -> bool:
#     collection2sources_lock.acquire()
#     is_cached = cid in collection2sources
#     collection2sources_lock.release()
#     return is_cached
# def _sources_set(cid: int, domains: List[Dict]):
#     collection2sources_lock.acquire()
#     collection2sources[cid] = domains.copy()
#     collection2sources_lock.release()
# def _sources_get(cid: int) -> List[Dict]:
#     collection2sources_lock.acquire()
#     domains = collection2sources[cid]
#     collection2sources_lock.release()
#     return domains


# def _domains_for_collection(cid: int) -> List[str]: #same or change
#     limit = 1000
#     offset = 0
#     sources = []
#     mc_api = processor.get_mc_client()
#     while True:
#         response = mc_api.source_list(collection_id=cid, limit=limit, offset=offset)
#         sources += response['results']
#         if response['next'] is None:
#             break
#         offset += limit
#     return [s['name'] for s in sources if s['name'] is not None]  # grab just the domain names


# def _cached_domains_for_collection(cid: int) -> List[str]:
#     logger = logging.getLogger(__name__)
#     # fetch info if it isn't cached
#     if not _sources_are_cached(cid):
#         logger.debug(f'Collection {cid}: sources not cached, fetching')
#         sources = _domains_for_collection(cid)
#         _sources_set(cid, sources)
#     else:
#         # otherwise, load up cache to reduce server queries and runtime overall
#         sources = _sources_get(cid)
#         logger.debug(f'Collection {cid}: found sources cached {len(sources)}')
#     return [s['name'] for s in sources if s['name'] is not None]


# def _domains_for_project(collection_ids: List[int]) -> List[str]:
#     all_domains = []
#     for cid in collection_ids:  # fetch all the domains in each collection
#         all_domains += _domains_for_collection(cid)  # remove cache because Prefect failing to serials with thread lock error :-(
#     return list(set(all_domains))  # make them unique


# def fetch_domains_for_projects(project: Dict) -> Dict:
#     logger = logging.getLogger(__name__)
#     domains = _domains_for_project(project['media_collections'])
#     logger.info(f"Project {project['id']}/{project['title']}: found {len(domains)} domains")
#     updated_project = copy.copy(project)
#     updated_project['domains'] = domains
#     return updated_project


# def _query_builder(terms: str, language: str, domains: list) -> str:
#     terms_no_curlies = terms.replace('“', '"').replace('”', '"')
#     return "({}) AND (language:{}) AND ({})".format(terms_no_curlies, language, " OR ".join(
#         [f"domain:{d}" for d in domains]))

# def fetch_project_stories_task(project_list: Dict, data_source: str) -> List[Dict]:
#     logger = logging.getLogger(__name__)
#     combined_stories = []
#     end_date = dt.datetime.now() - dt.timedelta(days=DEFAULT_DAY_OFFSET)  # stories don't get processed for a few days
#     for p in project_list:
#         project_stories = []
#         valid_stories = 0
#         Session = database.get_session_maker()
#         with Session() as session:
#             history = projects_db.get_history(session, p['id'])
#         page_number = 1
#         # only search stories since the last search (if we've done one before)
#         start_date = end_date - dt.timedelta(days=DEFAULT_DAY_OFFSET + DEFAULT_DAY_WINDOW)
#         if history.last_publish_date is not None:
#             # make sure we don't accidentally cut off a half day we haven't queried against yet
#             # this is OK because duplicates will get screened out later in the pipeline
#             local_start_date = history.last_publish_date - dt.timedelta(days=1)
#             start_date = min(local_start_date, start_date)
#         # if query is too big we need to split it up
#         full_project_query = _query_builder(p['search_terms'], p['language'], p['domains'])
#         project_queries = [full_project_query]
#         domain_divisor = 2
#         queries_too_big = len(full_project_query) > pow(2, 14)
#         if queries_too_big:
#             while queries_too_big:
#                 chunked_domains = np.array_split(p['domains'], domain_divisor)
#                 project_queries = [_query_builder(p['search_terms'], p['language'], d) for d in chunked_domains]
#                 queries_too_big = any(len(pq) > pow(2, 14) for pq in project_queries)
#                 domain_divisor *= 2
#             logger.info('Project {}/{}: split query into {} parts'.format(p['id'], p['title'], len(project_queries)))
#         # now run all queries
#         try:
#             for project_query in project_queries:
#                 if valid_stories > MAX_STORIES_PER_PROJECT:
#                     break
#                 total_hits = wm_api.count(project_query, start_date, end_date)
#                 logger.info("Project {}/{} - {} total stories (since {})".format(p['id'], p['title'],
#                                                                                  total_hits, start_date))
#                 for page in wm_api.all_articles(project_query, start_date, end_date, page_size=PAGE_SIZE):
#                     if valid_stories > MAX_STORIES_PER_PROJECT:
#                         break
#                     logger.debug("  {} - page {}: {} stories".format(p['id'], page_number, len(page)))
#                     for item in page:
#                         media_url = item['domain'] if len(item['domain']) > 0 else urls.canonical_domain(item['url'])
#                         info = dict(
#                             url=item['url'],
#                             source_publish_date=item['publication_date'],
#                             title=item['title'],
#                             source=data_source,
#                             project_id=p['id'],
#                             language=item['language'],
#                             authors=None,
#                             media_url=media_url,
#                             media_name=media_url,
#                             article_url=item['article_url']
#                         )
#                         project_stories.append(info)
#                         valid_stories += 1
#             logger.info("  project {} - {} valid stories (after {})".format(p['id'], valid_stories,
#                                                                             history.last_publish_date))
#             combined_stories += project_stories
#         except RuntimeError as re:
#             # perhaps a query syntax error? log it, but keep going so other projects succeed
#             logger.error(f"  project {p['id']} - failed to fetch stories (likely a query syntax error)")
#             logger.exception(re)
#     return combined_stories

# def fetch_archived_text_task(story: Dict) -> Optional[Dict]:
#     logger = logging.getLogger(__name__)
#     try:
#         story_details = requests.get(story['article_url'], timeout=TEXT_FETCH_TIMEOUT).json()
#         updated_story = copy.copy(story)
#         updated_story['story_text'] = story_details['snippet']
#         return updated_story
#     except Exception as e:
#         # this just happens occasionally so it is a normal case
#         logger.warning(f"Skipping story - failed to fetch due to {e} - from {story['article_url']}")

# def _get_combined_text(project_count: int, email_text: str, story_count: int, data_source: str) -> str:
#     email_message = ""
#     email_message += "Checking {} projects.\n\n".format(project_count)
#     email_message += email_text
#     email_message += "\nDone - pulled {} stories.\n\n" \
#                      "(An automated email from your friendly neighborhood {} story processor)" \
#         .format(story_count, data_source)
#     return email_message

# def send_combined_slack_message_task(summary: Dict, data_source: str, start_time: float):
#     # :param project_details: array of dicts per project, each with 'email_text', 'stories', and 'pages' keys
#     message = _get_combined_text(summary['project_count'], summary['email_text'], summary['stories'], data_source)
#     _send_slack_message(data_source, summary['stories'], start_time, message)


# def send_project_list_slack_message_task(project_details: List[Dict], data_source: str, start_time: float):
#     # :param summary: has keys 'project_count', 'email_text', 'stories'
#     total_new_stories = sum([p['stories'] for p in project_details])
#     # total_pages = sum([p['pages'] for p in project_details])
#     combined_text = ""
#     for p in project_details:
#         combined_text += p['email_text']
#     message = _get_combined_text(len(project_details), combined_text, total_new_stories, data_source)
#     _send_slack_message(data_source, total_new_stories, start_time, message)


# def _send_slack_message(data_source: str, story_count: int, start_time: float, slack_message: str):
#     logger = logging.getLogger(__name__)
#     duration_secs = time.time() - start_time
#     duration_mins = str(round(duration_secs / 60, 2))
#     if get_slack_config():
#         slack_config = get_slack_config()
#         notifications.send_slack_msg(slack_config['channel_id'], slack_config['bot_token'],data_source,
#                                      "Feminicide {} Update: {} stories ({} mins)".format(
#                                          data_source, story_count, duration_mins),
#                                      slack_message)
#     else:
#         logger.info("Not sending any slack updates")


# def queue_stories_for_classification_task(project_list: List[Dict], stories: List[Dict], datasource: str) -> Dict:
#     logger = logging.getLogger(__name__)
#     total_stories = 0
#     email_message = ""
#     for p in project_list:
#         project_stories = [s for s in stories if (s is not None) and (s['project_id'] == p['id'])]
#         email_message += "Project {} - {}: {} stories\n".format(p['id'], p['title'], len(project_stories))
#         total_stories += len(project_stories)
#         if len(project_stories) > 0:
#             # External source has guess dates (Newscatcher/Google), so use that
#             for s in project_stories:
#                 if 'source_publish_date' in s:
#                     s['publish_date'] = s['source_publish_date']
#             # and log that we got and queued them all
#             Session = database.get_session_maker()
#             with Session() as session:
#                 project_stories = stories_db.add_stories(session, project_stories, p, datasource)
#                 if len(project_stories) > 0:  # don't queue up unnecessary tasks
#                     # important to do this *after* we add the stories_id here
#                     celery_tasks.classify_and_post_worker.delay(p, project_stories)
#                     # important to write this update now, because we have queued up the task to process these stories
#                     # the task queue will manage retrying with the stories if it fails with this batch
#                     publish_dates = [dateutil.parser.parse(s['source_publish_date']) for s in project_stories]
#                     latest_date = max(publish_dates)
#                     projects_db.update_history(session, p['id'], last_publish_date=latest_date, last_url=project_stories[0]['url'])
#         logger.info("  queued {} stories for project {}/{}".format(total_stories, p['id'], p['title']))
#     return dict(
#         email_text=email_message,
#         project_count=len(project_list),
#         stories=total_stories
#     )


# if __name__ == '__main__':
#     # Setup logger
#     logger =  logging.getLogger(__name__)

#     # Start logging
#     logger.info("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))
#     logger.info("Checking for any new models we need")
    
#     models_downloaded = download_models()
#     logger.info(f"models downloaded: {models_downloaded}")

#     if not models_downloaded:
#         sys.exit(1)

#     data_source_name = processor.SOURCE_WAYBACK_MACHINE
#     start_time = time.time()
#     # Mimicking what Prefect does
#     print("Starting {} story fetch job".format(processor.SOURCE_WAYBACK_MACHINE))

#     # 1. Load all the projects
#     projects_list = load_projects_task()

#     # 2. Figure out domains to query for each project
#     with Pool(processes=4) as pool:
#         projects_with_domains = pool.map(fetch_domains_for_projects, projects_list)
        
#     # 3. Fetch all stories for each project
#     # all_stories = fetch_project_stories_task(projects_with_domains, data_source_name)
#     with Pool(processes=4) as pool:
#         results = pool.starmap(fetch_project_stories_task,[(p, data_source_name) for p in projects_with_domains])

#     # results is a list of list of dicts, flatten it if you want a single list
#     flat_results = [item for sublist in results for item in sublist]
#         # all_stories = fetch_project_stories_task(projects_with_domains, data_source_name)

#     # 4. Fetch pre-parsed content (in parallel by story)
#     with Pool(processes=4) as pool:
#         stories_with_text = pool.map(fetch_archived_text_task, flat_results)

#     # 5. Post batches of stories for classification
#     results_data = queue_stories_for_classification_task(projects_list, stories_with_text, data_source_name)

#     # Step 6: Send email/Slack messages with results of operations
#     send_combined_slack_message_task(results_data, data_source_name, start_time)
#     #prefect_tasks.send_combined_email_task(results_data, data_source_name, start_time)


Spyder Class Implementation
# import scrapy
# class MySpider(scrapy.Spider):
#     name = 'my_spider'
#     start_urls = []  # We will dynamically populate this

#     def __init__(self, flattened_stories=None, *args, **kwargs):
#         super(MySpider, self).__init__(*args, **kwargs)
#         if flattened_stories is not None:
#             self.start_urls = [story['article_url'] for story in flattened_stories]
#         self.stories_with_text = []

#     def start_requests(self):
#         for url in self.start_urls:
#             yield scrapy.Request(url, self.parse)
#     def parse(self, response):
#     #Your parsing logic here. Extracting snippet from page.
#         try:
#             snippet = response.css('div.snippet::text').get()
#             if snippet:
#                 # Here, we identify the original story based on URL.
#                 original_story = next((story for story in self.start_urls if story['article_url'] == response.url), None)

#                 if original_story is not None:
#                     # Update your story dict as before
#                     original_story['story_text'] = snippet

#                     # Append it to stories_with_text for later use
#                     self.stories_with_text.append(original_story)
#             else:
#                 self.logger.warning(f"Skipping story - Snippet not found - from {response.url}")

#         except Exception as e:
#             # Log exceptions as warnings
#             self.logger.warning(f"Skipping story - failed to fetch due to {e} - from {response.url}")

