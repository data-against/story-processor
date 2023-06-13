import unittest
import schedule
import processor.notifications as notifications


class TestNotifications(unittest.TestCase):

    # def test_send_email(self):
    #     notifications.send_email(["r.bhargava@northeastern.edu"],
    #                              "Feminicide MC Story Processor Test",
    #                              "Is this working? ⚠️")

    def test_send_slack_msg(self):
        notifications.send_slack_msg()

if __name__ == "__main__":
    unittest.main()
