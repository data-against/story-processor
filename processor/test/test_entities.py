import json
import os
import unittest

import processor.entities as entities
from processor.test import test_fixture_dir


class TestEntities(unittest.TestCase):
    def test_entities_from_url(self):
        with open(
            os.path.join(test_fixture_dir, "aapf_samples.json"), encoding="utf-8"
        ) as f:
            samples = json.load(f)
        for s in samples[:5]:  # don't test too many of them
            response = entities.from_url(s["url"])
            assert "status" in response
            if response["status"] == "ok":
                assert "results" in response
                assert "entities" in response["results"]
                assert len(response["results"]["entities"]) > 0

    def test_ko_supported_langauge(self):
        with open(
            os.path.join(test_fixture_dir, "ko_sample_stories.json"), encoding="utf-8"
        ) as f:
            samples = json.load(f)
        for s in samples:
            response = entities.from_content(
                s, "ko", "http://www.shinmoongo.net/159703"
            )
            assert "status" in response
            assert response["status"] == "ok"
            assert "entities" in response["results"]
            assert len(response["results"]["entities"]) > 0

    def test_entities_from_content(self):
        with open(
            os.path.join(test_fixture_dir, "aapf_samples.json"), encoding="utf-8"
        ) as f:
            samples = json.load(f)
        for s in samples[:5]:  # don't test to many of them
            response1 = entities.content_from_url(s["url"])
            assert "status" in response1
            if response1["status"] == "ok":
                assert "results" in response1
                assert len(response1["results"]["text"]) > 0
                response2 = entities.from_content(
                    response1["results"]["title"] + ". " + response1["results"]["text"],
                    s["language"],
                    s["url"],
                )
                assert "status" in response2
                if response2["status"] == "ok":
                    assert "results" in response2
                    assert "entities" in response2["results"]
                    assert len(response2["results"]["entities"]) > 0


if __name__ == "__main__":
    unittest.main()
