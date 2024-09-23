from sqlalchemy import func, select

from processor.database import stories_db
from processor.database.models import ProjectHistory, Story


def verify_stories_table(db_session):
    """
    Verify that the expected number of rows in stories exist with their initial fields before classification.

    :param db_session: The database session to use for querying.
    :raises AssertionError: If any of the checks fail.
    """
    # 1. verify that the stories table has 100 rows
    row_count = db_session.execute(select(func.count()).select_from(Story)).scalar()
    assert row_count == 100, f"Expected 100 rows, found {row_count} rows"

    # 2. verify that each row has 9 non-null values and 5 null values
    stories = db_session.execute(select(Story)).scalars().all()
    for story in stories:
        # Non_null are attributes that should be inserted directly after the stories are fetched
        non_null_count = sum(
            [
                1
                for col in story.__table__.columns
                if getattr(story, col.name) is not None
            ]
        )
        # Null values are attributes that are updated after classification
        null_count = sum(
            [1 for col in story.__table__.columns if getattr(story, col.name) is None]
        )

        assert (
            non_null_count == 9
        ), f"Expected 9 non-null values, found {non_null_count}"
        assert null_count == 5, f"Expected 5 null values, found {null_count}"


def verify_project_history(db_session, project_id="1111"):
    """
    Verify that the project history exists and has valid created_at and updated_at fields.

    :param db_session: The database session to use for querying.
    :param project_id: The project id.
    :raises AssertionError: If any of the checks fail.
    """
    stmt = select(ProjectHistory).where(ProjectHistory.id == project_id)
    project = db_session.execute(stmt).scalars().first()

    assert project is not None, f"No project found with id '{project_id}'"
    assert (
        project.created_at is not None
    ), f"created_at is NULL for project with id '{project_id}'"
    assert (
        project.updated_at is not None
    ), f"updated_at is NULL for project with id '{project_id}'"


def verify_story_threshold_counts(db_session, project_id, expected_count=50):
    """
    Verify that the number of above-threshold and below-threshold stories are equal to the expected count.

    :param db_session: The database session to use for querying.
    :param project_id: The ID of the project to verify.
    :param expected_count: The expected count for above-threshold and below-threshold stories.
    :raises AssertionError: If the counts do not match the expected values.
    """
    above_threshold_stories = stories_db.posted_above_story_count(
        db_session, project_id=project_id
    )
    below_threshold_stories = stories_db.below_story_count(
        db_session, project_id=project_id
    )

    assert (
        above_threshold_stories == expected_count
    ), f"Expected {expected_count} above-threshold stories, found {above_threshold_stories}"
    assert (
        below_threshold_stories == expected_count
    ), f"Expected {expected_count} below-threshold stories, found {below_threshold_stories}"
    assert above_threshold_stories == below_threshold_stories, (
        "Above-threshold and below-threshold stories counts do " "not match"
    )
