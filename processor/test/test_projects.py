import json
import os
import unittest

import processor.projects as projects
from processor.test import test_fixture_dir

TEST_EN_PROJECT = dict(id=0, language="en", language_model_id=1)


class TestProjects(unittest.TestCase):
    def test_update_list(self):
        project_list = projects.load_project_list(True)
        assert len(project_list) > 0
        for p in project_list:
            assert "id" in p
            assert "language_model_id" in p
            assert "rss_url" in p

    def test_classify_stories(self):
        # english
        project = TEST_EN_PROJECT.copy()
        # load test inputs
        with open(
            os.path.join(test_fixture_dir, "usa_sample_stories.json"), encoding="utf-8"
        ) as f:
            sample_text = json.load(f)
        sample_stories = [dict(story_text=t) for t in sample_text]
        # check results
        feminicide_probs = projects.classify_stories(project, sample_stories)[
            "model_scores"
        ]
        assert round(feminicide_probs[0], 5) == 0.36395
        assert round(feminicide_probs[1], 5) == 0.32298
        assert round(feminicide_probs[2], 5) == 0.33297

    def test_newscatcher_countries(self):
        project_list = projects.load_project_list(True)
        assert len(project_list) > 0
        for p in project_list:
            assert "newscatcher_country" in p
        projects_with_countries = projects.with_countries(project_list)
        assert len(projects_with_countries) > 0

    def test_remove_low_confidence_stories(self):
        project = TEST_EN_PROJECT.copy()
        project["min_confidence"] = 0.5
        stories = [
            dict(id=0, confidence=0),
            dict(id=1, confidence=0.1),
            dict(id=2, confidence=0.2),
            dict(id=3, confidence=0.3),
            dict(id=4, confidence=0.4),
            dict(id=5, confidence=0.5),
            dict(id=6, confidence=0.6),
            dict(id=7, confidence=0.7),
            dict(id=8, confidence=0.8),
            dict(id=9, confidence=0.9),
            dict(id=10, confidence=1),
        ]
        trimmed_stories = projects.remove_low_confidence_stories(
            project["min_confidence"], stories
        )
        assert len(trimmed_stories) < len(stories)
        assert len(trimmed_stories) == 6
        for s in trimmed_stories:
            assert s["confidence"] >= project["min_confidence"]


if __name__ == "__main__":
    unittest.main()
