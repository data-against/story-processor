import os
from unittest.mock import patch

import pytest

import scripts.queue_wayback_stories as wayback
import scripts.tasks as tasks
from processor import SOURCE_WAYBACK_MACHINE, projects
from processor.tasks.classification import classify_and_post_worker
from scripts.test.utils import (
    verify_project_history,
    verify_stories_table,
    verify_story_threshold_counts,
)

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(base_dir, "test", "models")


@pytest.fixture
def sample_stories():
    from scripts.test.fixtures.sample_wb_text import stories

    return stories


@pytest.mark.singlethread
@pytest.mark.celery()
def test_queue_wayback_stories(
    mock_load_projects,
    mock_fetch_project_stories,
    mock_model_list,
    celery_app,
    db_session_maker,
    sample_stories,
):
    # pickle has trouble without import
    from scripts.test.models.stubbed_1_model import NoOpLogisticRegression  # noqa: F401

    projects.REALLY_POST = False  # We don't want to post any results to the main server

    # create session for our test
    Session = db_session_maker
    db_session = Session()

    # 1. list the sample project and get models
    projects_list = wayback.load_projects()
    assert projects_list == mock_load_projects

    # verify history got added to database
    verify_project_history(db_session)

    # 2. fetch all the sample stories for sample project
    all_stories = mock_fetch_project_stories["wayback"].return_value
    unique_url_count = len(set([s["extracted_content_url"] for s in all_stories]))
    assert unique_url_count == 100
    assert len(all_stories) == 100

    # 3. fetch webpage text and parse all the stories (No stories should fail) - using fixture here bc of test env scrapy issues
    # to get data i used a real-time fetch_text

    # 4. post stories for classification, verify expected results
    results_data = tasks.queue_stories_for_classification(
        projects_list, sample_stories, SOURCE_WAYBACK_MACHINE
    )

    verify_stories_table(db_session)
    assert results_data["stories"] == 100

    with patch("processor.classifiers.MODEL_DIR", MODEL_DIR):
        classify_and_post_worker.apply(
            args=[projects_list[0], sample_stories]
        )  # Tasks don't get run
        # urgently, would be useful not to have to call the task directly

        # verify threshold counts
        verify_story_threshold_counts(db_session, project_id=1111)
