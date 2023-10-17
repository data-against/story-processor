import json
import os
import unittest

import processor.database as database
import processor.tasks as tasks
from processor import SOURCE_MEDIA_CLOUD
from processor.test import test_fixture_dir
from processor.test.test_projects import TEST_EN_PROJECT


class TestTasks(unittest.TestCase):

    def _sample_story_ids(self):
        # this loads read data from a real request from a log file on the real server
        with open(os.path.join(test_fixture_dir, "aapf_samples.json"), encoding="utf-8") as f:
            sample_stories = json.load(f)
        story_ids = [s['stories_id'] for s in sample_stories]
        return story_ids

    def _classify_story_ids(self, project, story_ids):
        stories_with_text = []
        for sid in story_ids:
            with open(os.path.join(test_fixture_dir, f'mc-story-{sid}.json'), 'r', encoding="utf-8") as f:
                s = json.load(f)
                s['source'] = SOURCE_MEDIA_CLOUD
                stories_with_text.append(s)
        Session = database.get_session_maker()
        with Session() as session:
            classified_stories = tasks._add_confidence_to_stories(session, project, stories_with_text)
        assert len(classified_stories) == len(stories_with_text)
        return classified_stories

    def test_add_probability_to_stories(self):
        project = TEST_EN_PROJECT.copy()
        project['language_model_id'] = 3
        story_ids = self._sample_story_ids()
        classified_stories = self._classify_story_ids(project, story_ids)
        matching_stories = [s for s in classified_stories if s['stories_id'] == 1957814773]
        assert len(matching_stories) == 1
        assert round(matching_stories[0]['confidence'], 5) == 0.35770

    def test_stories_by_id(self):
        project = TEST_EN_PROJECT.copy()
        project['language_model_id'] = 3
        story_ids = []
        if len(story_ids) > 0:
            classified_stories = self._classify_story_ids(project, story_ids)
            for s in classified_stories:
                assert 'story_text' not in s
                assert s['confidence'] > 0.75
            # assert round(classified_stories[0]['confidence'], 5) == 0.78392

    def test_add_entities_to_stories(self):
        story_ids = self._sample_story_ids()
        stories_with_text = []
        for sid in story_ids:
            with open(os.path.join(test_fixture_dir, f'mc-story-{sid}.json'), 'r', encoding="utf-8") as f:
                s = json.load(f)
                s['source'] = SOURCE_MEDIA_CLOUD
                stories_with_text.append(s)
        stories_with_entities = tasks.add_entities_to_stories(stories_with_text)
        for s in stories_with_entities:
            assert 'entities' in s


if __name__ == "__main__":
    unittest.main()
