import unittest

import processor.notifications as notifications


class TestNotifications(unittest.TestCase):

    def test_send_email(self):
        notifications.send_email(["r.bhargava@northeastern.edu"],
                                 "Feminicide MC Story Processor Test",
                                 "Is this working? ⚠️")


if __name__ == "__main__":
    unittest.main()
