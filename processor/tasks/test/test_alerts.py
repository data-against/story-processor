import unittest
from unittest.mock import MagicMock, patch

from processor.tasks import alerts


class TestAlerts(unittest.TestCase):

    def test_calculate_average_stories(self):
        result = alerts.calculate_average_stories(20, 5)
        self.assertEqual(result, 4.0)

    @patch("processor.database.get_session_maker")
    def test_get_total_stories_over_n_days(self, mock_get_session_maker):
        mock_session = MagicMock()
        mock_get_session_maker.return_value = mock_session
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result
        mock_result.scalar.return_value = 20000

        days = 4
        total_stories = alerts.get_total_stories_over_n_days(mock_session, days)
        self.assertEqual(total_stories, 20000)

    @patch("processor.tasks.alerts.send_email")
    @patch("processor.tasks.alerts.send_slack_msg")
    @patch("processor.tasks.alerts.logger")
    @patch("processor.tasks.alerts.is_email_configured")
    @patch("processor.tasks.alerts.is_slack_configured")
    @patch("processor.tasks.alerts.get_slack_config")
    @patch("processor.tasks.alerts.get_email_config")
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

        alerts.send_alert(total_stories, days, threshold)

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
