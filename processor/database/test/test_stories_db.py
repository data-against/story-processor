import unittest

import processor
import processor.database as database
import processor.database.models as models
import processor.database.stories_db as stories_db
from processor.test import sample_stories

TEST_EN_PROJECT = dict(id=0, language="en", language_model_id=1)


class TestStoriesDb(unittest.TestCase):
    def setUp(self):
        Session = database.get_session_maker()
        with Session() as session:
            history = models.ProjectHistory(id=TEST_EN_PROJECT["id"])
            session.add(history)
        self._remove_all_stories()

    def tearDown(self):
        Session = database.get_session_maker()
        with Session() as session:
            session.query(models.Story).delete()
            session.query(models.ProjectHistory).delete()

    def _story_count(self):
        Session = database.get_session_maker()
        with Session() as session:
            total_stories = session.query(models.Story).count()
        return total_stories

    def _remove_all_stories(self):
        # delete all stories
        Session = database.get_session_maker()
        with Session() as session:
            session.query(models.Story).delete()
            session.commit()
        assert self._story_count() == 0

    def test_add_stories(self):
        assert self._story_count() == 0
        # load samples and make sure some duplicates
        page_of_stories = sample_stories()
        assert len(page_of_stories) > 0
        unique_urls = len(set([s["url"] for s in page_of_stories]))
        assert unique_urls < len(page_of_stories)
        # verify inserting skips duplicates right
        Session = database.get_session_maker()
        with Session() as session:
            stories_to_queue = stories_db.add_stories(
                session, page_of_stories, TEST_EN_PROJECT, processor.SOURCE_MEDIA_CLOUD
            )
            session.commit()
            assert len(stories_to_queue) == unique_urls
        # try to add them again
        assert self._story_count() == len(stories_to_queue)
        self._remove_all_stories()
