import datetime as dt
from typing import List, Dict
import logging
import copy
from sqlalchemy.sql import func
from sqlalchemy import text, update, select
from sqlalchemy.orm.session import Session

import processor
from processor.database.models import Story


logger = logging.getLogger(__name__)


def add_stories(session: Session, source_story_list: List[Dict], project: Dict, source: str) -> List[Dict]:
    """
    Logging: Track metadata about all the stories we process we, so we can audit it later (like a log file).
    :param session:
    :param source_story_list:
    :param project:
    :param source:
    :return: list of ids of objects inserted
    """
    new_source_story_list = copy.copy(source_story_list)
    now = dt.datetime.now()
    for discovered_story in new_source_story_list:
        db_story = Story.from_source(discovered_story, source)
        db_story.project_id = project['id']
        db_story.model_id = project['language_model_id']
        db_story.queued_date = now
        db_story.above_threshold = False
        discovered_story['db_story'] = db_story
    # now insert in batch to the database
    session.add_all([s['db_story'] for s in new_source_story_list])
    session.commit()
    # only keep ones that inserted correctly
    new_source_story_list = [s for s in new_source_story_list if ('db_story' in s) and s['db_story'].id]
    for s in new_source_story_list:
        s['log_db_id'] = s['db_story'].id  # keep track of the db id, so we can use it later to update this story
    if source != processor.SOURCE_MEDIA_CLOUD:
        # these stories don't have a stories_id, which we use later, so set it to the local-database id and save
        for s in new_source_story_list:
            s['stories_id'] = s['db_story'].id  # since these don't have a stories_id, set it to the database PK id
            session.execute(update(Story).where(Story.id == s['log_db_id']).values(stories_id=s['log_db_id']))
        session.commit()
    for s in new_source_story_list:  # free the DB objects back for GC
        if 'db_story' in s:  # a little extra safety
            del s['db_story']
    return new_source_story_list


def update_stories_processed_date_score(session: Session, stories: List) -> None:
    """
    Logging: Once we have run the stories through the classifier models we want to save the scores.
    :param session:
    :param stories:
    :return:
    """
    now = dt.datetime.now()
    for s in stories:
        if 'log_db_id' in s:  # more gracefully fail in test scenarios
            session.execute(update(Story)
            .where(Story.id == s['log_db_id'])
            .values(
                model_score=s['model_score'],
                model_1_score=s['model_1_score'],
                model_2_score=s['model_2_score'],
                processed_date=now
            ))#[updated]
    session.commit()


def update_stories_above_threshold(session: Session, stories: List) -> None:
    """
    Logging: Also keep track which stories were above the classifier score threshold on the project right now.
    Ones above should be sent to the server.
    :param session:
    :param stories:
    :return:
    """
    for s in stories:
        session.execute(update(Story).where(Story.id == s['log_db_id']).values(above_threshold=True))#[updated]
    session.commit()


def update_stories_posted_date(session: Session, stories: List) -> None:
    """
    Logging: Keep track of when we sent stories above threshold to the main server.
    :param session:
    :param stories:
    :return:
    """
    now = dt.datetime.now()
    for s in stories:
        session.execute(update(Story).where(Story.id == s['log_db_id']).values(posted_date=now))#[updated]
    session.commit()


def recent_stories(session: Session, project_id: int, above_threshold: bool, limit: int = 5) -> List[Story]:
    """
    UI: show a list of the most recent stories we have processed
    :param session:
    :param project_id:
    :param above_threshold:
    :param limit:
    :return:
    """
    earliest_date = dt.date.today() - dt.timedelta(days=7)
    result = session.execute(
        select(Story)
        .where(
            (Story.project_id == project_id) &
            (Story.above_threshold == above_threshold) &
            (Story.published_date > earliest_date)
        )
        .order_by(func.random())
        .limit(limit)
        )
    return result.scalars().all()


def _stories_by_date_col(session: Session, column_name: str, project_id: int = None, platform: str = None,
                         above_threshold: bool = None, is_posted: bool = None, limit: int = 30) -> List:
    earliest_date = dt.date.today() - dt.timedelta(days=limit)
    clauses = []
    if project_id is not None:
        clauses.append("(project_id={})".format(project_id))
    if platform is not None:
        clauses.append("(source='{}')".format(platform))
    if above_threshold is not None:
        clauses.append("(above_threshold is {})".format('True' if above_threshold else 'False'))
    if is_posted is not None:
        clauses.append("(posted_date {} Null)".format("is not" if is_posted else "is"))
    query = "select "+column_name+"::date as day, count(1) as stories from stories " \
            "where ("+column_name+" is not Null) and ("+column_name+" >= '{}'::DATE) AND {} " \
            "group by 1 order by 1 DESC".format(earliest_date, " AND ".join(clauses))
    return _run_query(session, query)


def stories_by_posted_day(session: Session, project_id: int = None, platform: str = None, above_threshold: bool = True,
                          is_posted: bool = None, limit: int = 45) -> List:
    return _stories_by_date_col(session, 'processed_date', project_id, platform, above_threshold, is_posted, limit)


def stories_by_processed_day(session: Session, project_id: int = None, platform: str = None, above_threshold: bool = None,
                             is_posted: bool = None, limit: int = 45) -> List:
    return _stories_by_date_col(session, 'processed_date', project_id, platform, above_threshold, is_posted, limit)


def stories_by_published_day(session: Session, project_id: int = None, platform: str = None, above_threshold: bool = None,
                             is_posted: bool = None, limit: int = 30) -> List:
    return _stories_by_date_col(session, 'published_date', project_id, platform, above_threshold, is_posted, limit)


def _run_query(session: Session, query: str) -> List:
    results = session.execute(text(query))
    data = []
    for row in results:
        data.append(row._mapping)
    return data


def _run_count_query(session: Session, query: str) -> int:
    data = _run_query(session, query)
    return data[0]['count']


def unposted_above_story_count(session: Session, project_id: int, limit: int = None) -> int:
    """
    UI: How many stories about threshold have *not* been sent to main server (should be zero!).
    """
    date_clause = "(posted_date is not Null)"
    if limit:
        earliest_date = dt.date.today() - dt.timedelta(days=limit)
        date_clause += " AND (posted_date >= '{}'::DATE)".format(earliest_date)
    query = "select count(1) from stories where project_id={} and above_threshold is True and {}".\
        format(project_id, date_clause)
    return _run_count_query(session, query)


def posted_above_story_count(session: Session, project_id: int) -> int:
    """
    UI: How many stories above threshold have we sent to the main server (like all should be)
    :param project_id:
    :return:
    """
    query = "select count(1) from stories " \
            "where project_id={} and posted_date is not Null and above_threshold is True".\
        format(project_id)
    return _run_count_query(session, query)


def below_story_count(session: Session, project_id: int) -> int:
    """
    UI: How many stories total were below threshold (should be same as uposted_stories)
    :param project_id:
    :return:
    """
    query = "select count(1) from stories where project_id={} and above_threshold is False".\
        format(project_id)
    return _run_count_query(session, query)


def unposted_stories(session: Session, project_id: int, limit: int):
    """
    How many stories were not posted to hte main server (should be same as below_story_count)
    :return:
    """
    earliest_date = dt.date.today() - dt.timedelta(days=limit)
    query = "select * from stories " \
            "where project_id={} and posted_date is Null and (posted_date >= '{}'::DATE) and above_threshold is True".\
        format(project_id, earliest_date)
    """
    session = Session()
    q = session.query(Story). \
        filter(Story.project_id == project_id). \
        filter(Story.above_threshold is True). \
        filter(Story.posted_date is None)
    return q.all()
    """
    return _run_query(session, query)


def project_binned_model_scores(session: Session, project_id: int) -> List:
    query = """
        select ROUND(CAST(model_score as numeric), 1) as value, count(1) as frequency
        from stories
        where project_id={} and model_score is not NULL
        group by 1
        order by 1
    """.format(project_id)
    return _run_query(session, query)
