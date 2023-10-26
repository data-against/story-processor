import json
import os
import random
import unittest

from processor import base_dir
from scripts.queue_wayback_stories import _domains_for_project


class TestQueueWaybackStories(unittest.TestCase):
    def test_get_projects_domains(self):
        with open(os.path.join(base_dir, "config", "projects.json"), "r") as f:
            project_list = json.load(f)
        # try it on 3 because going through all takes too long
        random_projects = random.choices(project_list, k=3)
        for p in random_projects:
            assert len(p["media_collections"]) > 0
            domains = _domains_for_project(p["media_collections"])
            assert len(domains) > 0


if __name__ == "__main__":
    unittest.main()
