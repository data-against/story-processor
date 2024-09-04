import json
import os
from unittest.mock import patch

import pytest
from celery import Celery
from fixtures.sample_mc_stories import stories as mc_stories
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import processor.classifiers as classifiers
from processor import SOURCE_NEWSCATCHER, SOURCE_WAYBACK_MACHINE
from processor.database import projects_db
from processor.database.models import Base, ProjectHistory, Story

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def celery_config():
    # Use in-memory broker and backend - No need to change broker_url env variable
    return {
        "broker_url": "memory://",
        "result_backend": "memory://",
        "task_always_eager": True,
    }


@pytest.fixture(scope="session")
def celery_includes():
    # Our tasks associated with our fetch -> queue -> classify -> post pipeline
    return ["processor.tasks.classification.classify_and_post_worker"]


@pytest.fixture(scope="session")
def celery_app(celery_config):
    # intialize test celery app, with memory broker and classification tasks
    app = Celery("test")
    app.config_from_object(celery_config)
    app.autodiscover_tasks(celery_includes)
    return app


# Fixtures
@pytest.fixture(scope="module")
def db_engine():
    # modify the column definitions to allow null values, will be helpful in initializing rows in our tables
    ProjectHistory.__table__.columns["latest_date_mc"].nullable = True
    ProjectHistory.__table__.columns["latest_date_nc"].nullable = True
    ProjectHistory.__table__.columns["latest_date_wm"].nullable = True

    Story.__table__.columns["model_score"].nullable = True
    Story.__table__.columns["model_1_score"].nullable = True
    Story.__table__.columns["model_2_score"].nullable = True
    Story.__table__.columns["processed_date"].nullable = True
    Story.__table__.columns["queued_date"].nullable = True
    Story.__table__.columns["posted_date"].nullable = True

    # create sqlite database
    engine = create_engine("sqlite:///:memory:")

    # create tables
    Base.metadata.create_all(engine)

    yield engine

    engine.dispose()


@pytest.fixture(scope="module")
def db_session_maker(db_engine):
    Session = sessionmaker(bind=db_engine)
    # mock the session maker
    with patch("processor.database.get_session_maker", return_value=Session):
        yield Session


@pytest.fixture(scope="module")
def mock_load_projects(db_session_maker):
    # create project_list with one sample project
    mock_project_list = [
        {
            "id": 1111,
            "title": "Test project",
            "language": "en",
            "start_date": "2023-01-08T16:03:00.000Z",
            "end_date": "2024-01-08T16:03:00.000Z",
            "search_terms": '(("climate change"))',
            "rss_url": None,
            "country": "US",
            "newscatcher_country": "US",
            "media_collections": [34412234, 38379429],
            "min_confidence": 0.75,
            "language_model": "stubbed model",
            "language_model_id": 1111,
        }
    ]

    # Add/get history for our mock_project_list
    with patch("processor.projects.load_project_list", return_value=mock_project_list):
        Session = db_session_maker
        with Session() as session:
            for project in mock_project_list:
                project_history = projects_db.get_history(session, project["id"])
                if project_history is None:
                    projects_db.add_history(session, project["id"])
        yield mock_project_list


@pytest.fixture(scope="module")
def mock_fetch_project_stories(db_session_maker, mock_load_projects):
    from fixtures.sample_stories import stories

    with patch(
        "scripts.queue_newscatcher_stories.fetch_project_stories"
    ) as mock_nc_fetch, patch(
        "scripts.queue_wayback_stories.fetch_project_stories"
    ) as mock_wb_fetch, patch(
        "scripts.queue_mediacloud_stories.fetch_stories"
    ) as mock_mc_fetch:
        mock_nc_fetch.return_value = [
            {
                **{
                    key: story.pop(key)
                    for key in list(story)
                    if key not in ["extracted_content_url", "archived_url", "id"]
                },
                "source": SOURCE_NEWSCATCHER,
            }
            for story in stories
        ]
        mock_wb_fetch.return_value = [
            {**story, "source": SOURCE_WAYBACK_MACHINE} for story in stories
        ]
        mock_mc_fetch.return_value = mc_stories

        yield {
            "newscatcher": mock_nc_fetch,
            "wayback": mock_wb_fetch,
            "mediacloud": mock_mc_fetch,
        }


@pytest.fixture(scope="module")
def mock_model_list():
    # load the mock_model_list from  our test language_models.json file
    with open(
        os.path.join(base_dir, "test", "fixtures", "language_models.json"), "r"
    ) as f:
        fake_model_list = json.load(f)
    with patch("processor.classifiers.get_model_list", return_value=fake_model_list):
        with patch("processor.classifiers.download_models") as mock_download_models:
            mock_download_models.return_value = True
            # now calling get_model_list() will return our fake model list
            model_list = classifiers.get_model_list()
            mock_model_list.return_value = model_list
            assert model_list == fake_model_list
            assert len(model_list) == 1
            assert model_list[0]["id"] == "1111"

            yield fake_model_list


def patch_update_stories_above_threshold(session: Session) -> None:
    # select stories with confidence >= 0.75 and set above_threshold to True
    # update_stories_above_threshold doesn't run properly with our in-session db so we are replicating the behavior
    session.execute(
        text(
            """
            UPDATE stories
            SET above_threshold = True
            WHERE model_score >= 0.75
            """
        )
    )
    session.commit()
