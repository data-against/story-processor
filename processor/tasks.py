import os
import json
import requests
from typing import List, Dict
import logging
import time
from json.decoder import JSONDecodeError
# from celery.utils.log import get_task_logger
import processor.database.stories_db
from processor import path_to_log_dir
from processor.celery import app
import processor.projects as projects
import processor.database as db
import processor.entities as entities

logger = logging.getLogger(__name__)  # get_task_logger(__name__)
logFormatter = logging.Formatter("[%(levelname)s %(threadName)s] - %(asctime)s - %(name)s - : %(message)s")
fileHandler = logging.FileHandler(os.path.join(path_to_log_dir, "tasks-{}.log".format(time.strftime("%Y%m%d-%H%M%S"))))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

ACCEPTED_ENTITY_TYPES = ["PERSON", "PER", "GPE", "LOC", "FAC", "DATE", "TIME", "C_DATE", "C_AGE"]


def _add_confidence_to_stories(project: Dict, stories: List[Dict]) -> List[Dict]:
    probabilities = projects.classify_stories(project, stories)
    for idx, s in enumerate(stories):
        s['confidence'] = probabilities[idx]
    results = projects.prep_stories_for_posting(project, stories)
    if projects.LOG_LAST_POST_TO_FILE:  # helpful for debugging (the last project post will written to a file)
        with open(os.path.join(path_to_log_dir,
                               '{}-all-stories-{}.json'.format(project['id'], time.strftime("%Y%m%d-%H%M%S"))),
                  'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
    return results


def _add_entities_to_stories(stories: List[Dict]):
    for s in stories:
        story_entities = None
        if entities.server_address_set():
            try:
                response = entities.from_content(s['title'] + " " + s['story_text'], s['language'])
                story_entities = [item['text'].lower() for item in response['results']
                                  if item['type'] in ACCEPTED_ENTITY_TYPES]
            except JSONDecodeError:
                # the entity extractor failed, so don't return any entities
                story_entities = None
            except Exception as e:
                # something else happened, so log it but also continue
                logger.error(e)
                story_entities = None
        s['entities'] = story_entities
    return stories


@app.task(serializer='json', bind=True)
def classify_and_post_worker(self, project: Dict, stories: List[Dict]):
    """
    Take a page of stories matching a project and run them through the classifier for that project.
    :param self:
    :param project:
    :param stories:
    """
    try:
        logger.debug('{}: classify {} stories (model {})'.format(project['id'], len(stories),
                                                                 project['language_model_id']))
        # first add in multi-lingual entities
        #stories_with_entities = _add_entities_to_stories(stories)
        # now classify the stories again the model specified for the project
        stories_with_confidence = _add_confidence_to_stories(project, stories)
        for s in stories_with_confidence:
            logger.debug("  classify: {}/{} - {} - {}".format(s['project_id'], s['language_model_id'],
                                                              s['stories_id'], s['confidence']))
        # keep an auditable log in our own local database
        processor.database.stories_db.update_stories_processed_date_score(stories_with_confidence, project['id'])
        # only stories above project score threshold should be posted
        stories_to_send = projects.remove_low_confidence_stories(project.get('min_confidence', 0),
                                                                 stories_with_confidence)
        # mark the stories in the local DB that we intend to send
        processor.database.stories_db.update_stories_above_threshold(stories_to_send, project['id'])
        # now actually post them
        logger.debug('{}: {} stories to post'.format(project['id'], len(stories_to_send)))
        projects.post_results(project, stories_to_send)
        for s in stories_to_send:  # for auditing, keep a log in the container of the results posted to main server
            logger.debug("  post: {}/{} - {} - {}".format(s['project_id'], s['language_model_id'],
                                                          s['stories_id'], s['confidence']))
        # and track that we posted the stories that we did in our local debug DB
        processor.database.stories_db.update_stories_posted_date(stories_to_send, project['id'])
    except requests.exceptions.HTTPError as err:
        # on failure requeue to try again
        logger.warning("{}: Failed to post {} results".format(project['id'], len(stories)))
        logger.exception(err)
        raise self.retry(exc=err)
    except Exception as exc:
        # only failure here is the classifier not loading? probably we should try again... feminicide server holds state
        # and can handle any duplicate results based on stories_id+model_id synthetic unique key
        logger.warning("{}: Failed to label {} stories".format(project['id'], len(stories)))
        logger.exception(exc)
        raise self.retry(exc=exc)
