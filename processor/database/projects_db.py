import datetime as dt

from sqlalchemy.orm.session import Session

from processor.database.models import ProjectHistory


def add_history(
    session: Session, project_id: int, last_processed_stories_id: int
) -> None:
    """
    We store project history to keep track of the last story we processed for each project. This lets us optimize
    our queries so that we don't re-process stories within a project.
    :param session:
    :param project_id:
    :param last_processed_stories_id:
    :return:
    """
    p = ProjectHistory()
    p.id = project_id
    p.last_processed_id = (
        last_processed_stories_id if last_processed_stories_id is not None else 0
    )
    now = dt.datetime.now()
    p.created_at = now
    p.updated_at = now
    session.add(p)
    session.commit()


def update_history(
    session: Session,
    project_id: int,
    last_processed_stories_id: int = None,
    last_publish_date: dt.datetime = None,
    last_url: str = None,
) -> None:
    """
    Once we've processed a batch of stories, use this to save in the database the id of the lastest story
    we've processed (so that we don't redo stories we have already done).
    :param session:
    :param project_id:
    :param last_processed_stories_id:
    :param last_publish_date:
    :param last_url:
    :return:
    """
    project_history = session.get(
        ProjectHistory, project_id
    )  # session.query(ProjectHistory).get(project_id)[update]
    project_history.last_processed_id = last_processed_stories_id
    project_history.last_publish_date = last_publish_date
    project_history.last_url = last_url
    project_history.updated_at = dt.datetime.now()
    session.commit()


def get_history(session: Session, project_id: int) -> ProjectHistory:
    """
    Find out info about the stories we have processed for this project alerady.
    :param session
    :param project_id:
    :return:
    """
    project_history = session.get(ProjectHistory, project_id)
    return project_history
