import datetime as dt
from typing import Optional

from sqlalchemy.orm.session import Session

import processor
from processor.database.models import ProjectHistory


def add_history(session: Session, project_id: int) -> None:
    """
    We store project history to keep track of the last story we processed for each project. This lets us optimize
    our queries so that we don't re-process stories within a project.
    :param session:
    :param project_id:
    :return:
    """
    p = ProjectHistory()
    p.id = project_id
    now = dt.datetime.now()
    p.created_at = now
    p.updated_at = now
    session.add(p)
    session.commit()


def update_history(
    session: Session, project_id: int, last_date: dt.datetime, source: str
) -> None:
    """
    Once we've processed a batch of stories, use this to save in the database the id of the latest story
    we've processed (so that we don't redo stories we have already done).
    :param session:
    :param project_id:
    :param last_date:
    :param source
    :return:
    """
    project_history = session.get(ProjectHistory, project_id)
    if source == processor.SOURCE_MEDIA_CLOUD:
        project_history.latest_date_mc = last_date
    elif source == processor.SOURCE_NEWSCATCHER:
        project_history.latest_date_nc = last_date
    elif source == processor.SOURCE_WAYBACK_MACHINE:
        project_history.latest_date_wm = last_date
    project_history.updated_at = dt.datetime.now()
    session.commit()


def get_history(session: Session, project_id: int) -> Optional[ProjectHistory]:
    """
    Find out info about the stories we have processed for this project alerady.
    :param session
    :param project_id:
    :return:
    """
    project_history = session.get(ProjectHistory, project_id)
    return project_history
