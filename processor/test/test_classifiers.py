import os
import unittest
import json

from processor import base_dir
import processor.classifiers as classifiers

test_fixture_dir = os.path.join(base_dir, "processor", "test", "fixtures")


class TestModelDownload(unittest.TestCase):

    def test_model_download(self):
        classifiers.download_models()


class TestClassifiers(unittest.TestCase):

    def test_classify_en(self):
        project = dict(id=0, language='en', title='', language_model_id=1)
        classifier = classifiers.for_project(project)
        sample_texts = json.load(open(os.path.join(test_fixture_dir, "usa_sample_stories.json")))
        sample_texts = [dict(story_text=t) for t in sample_texts]
        results = classifier.classify(sample_texts)
        assert round(results[0], 5) == 0.36395
        assert round(results[1], 5) == 0.32298
        assert round(results[2], 5) == 0.33297

    def test_classify_en_aapf(self):
        project = dict(id=0, language='en', title='stories for AApf', language_model_id=3)
        classifier = classifiers.for_project(project)
        sample_texts = json.load(open(os.path.join(test_fixture_dir, "more_sample_stories.json")))
        sample_texts = [dict(story_text=t) for t in sample_texts]
        results = classifier.classify(sample_texts)
        assert round(results[0], 5) == 0.88928
        assert round(results[1], 5) == 0.24030
        assert round(results[2], 5) == 0.23219


if __name__ == "__main__":
    unittest.main()
