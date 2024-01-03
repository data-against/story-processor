import json
import os

from processor import base_dir

test_fixture_dir = os.path.join(base_dir, "processor", "test", "fixtures")


def sample_stories():
    # this loads read data from a real request from a log file on the real server
    with open(
        os.path.join(test_fixture_dir, "aapf_samples.json"), encoding="utf-8"
    ) as f:
        sample_stories = json.load(f)
    return sample_stories


def sample_story_ids():
    story_ids = [s["id"] for s in sample_stories()]
    return story_ids
