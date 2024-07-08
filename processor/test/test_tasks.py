import json
import os
import unittest
from unittest.mock import MagicMock, patch

import processor.database as database
import processor.tasks as tasks
from processor import SOURCE_MEDIA_CLOUD
from processor.test import sample_story_ids, test_fixture_dir
from processor.test.test_projects import TEST_EN_PROJECT


class TestTasks(unittest.TestCase):
    def _classify_story_ids(self, project, story_ids):
        stories_with_text = []
        for sid in story_ids:
            with open(
                os.path.join(test_fixture_dir, f"mc-story-{sid}.json"),
                "r",
                encoding="utf-8",
            ) as f:
                s = json.load(f)
                s["source"] = SOURCE_MEDIA_CLOUD
                stories_with_text.append(s)
        Session = database.get_session_maker()
        with Session() as session:
            classified_stories = tasks._add_confidence_to_stories(
                session, project, stories_with_text
            )
        assert len(classified_stories) == len(stories_with_text)
        return classified_stories

    def test_add_probability_to_stories(self):
        project = TEST_EN_PROJECT.copy()
        project["language_model_id"] = 3
        story_ids = sample_story_ids()
        classified_stories = self._classify_story_ids(project, story_ids)
        matching_stories = [s for s in classified_stories if s["id"] == 1957814773]
        assert len(matching_stories) == 1
        assert round(matching_stories[0]["confidence"], 5) == 0.35770

    def test_stories_by_id(self):
        project = TEST_EN_PROJECT.copy()
        project["language_model_id"] = 3
        story_ids = []
        if len(story_ids) > 0:
            classified_stories = self._classify_story_ids(project, story_ids)
            for s in classified_stories:
                assert "story_text" not in s
                assert s["confidence"] > 0.75
            # assert round(classified_stories[0]['confidence'], 5) == 0.78392

    def test_add_entities_to_stories(self):
        story_ids = sample_story_ids()
        stories_with_text = []
        for sid in story_ids:
            with open(
                os.path.join(test_fixture_dir, f"mc-story-{sid}.json"),
                "r",
                encoding="utf-8",
            ) as f:
                s = json.load(f)
                s["source"] = SOURCE_MEDIA_CLOUD
                stories_with_text.append(s)
        stories_with_entities = tasks.add_entities_to_stories(stories_with_text)
        for s in stories_with_entities:
            assert "entities" in s

    def test_calculate_average_stories(self):
        # may need to cover more cases
        result = tasks.calculate_average_stories(20, 5)
        self.assertEqual(result, 4.0)

    @patch("processor.database.get_session_maker")
    def test_get_total_stories_over_n_days(self, mock_get_session_maker):
        # Mock the session and query result
        mock_session = MagicMock()
        mock_get_session_maker.return_value = mock_session
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result
        mock_result.scalar.return_value = 20000

        days = 4
        total_stories = tasks.get_total_stories_over_n_days(mock_session, days)
        self.assertEqual(total_stories, 20000)

    @patch("processor.tasks.send_email")
    @patch("processor.tasks.send_slack_msg")
    @patch("processor.tasks.logger")
    @patch("processor.tasks.is_email_configured")
    @patch("processor.tasks.is_slack_configured")
    @patch("processor.tasks.get_slack_config")
    @patch("processor.tasks.get_email_config")
    def test_send_alert(
        self,
        mock_get_email_config,
        mock_get_slack_config,
        mock_is_slack_configured,
        mock_is_email_configured,
        mock_logger,
        mock_send_slack_msg,
        mock_send_email,
    ):
        # Configure mocks
        mock_is_slack_configured.return_value = True
        mock_is_email_configured.return_value = True
        mock_get_slack_config.return_value = {
            "channel_id": "dummy_channel",
            "bot_token": "dummy_token",
        }
        mock_get_email_config.return_value = {"notify_emails": ["dummy@example.com"]}

        total_stories = 20000
        days = 4
        threshold = 40000

        # Run the method
        tasks.send_alert(total_stories, days, threshold)

        # Check if logs and messages are sent correctly
        mock_logger.info.assert_called_with(
            f"Average stories processed per day in the last {days} days: 5000.0"
        )
        mock_logger.warning.assert_called_with(
            f"Warning: Average stories per day in the last {days} days is 5000.0, "
            f"which is far below the expected value of >= {threshold}."
        )
        mock_send_slack_msg.assert_called_once()
        mock_send_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
