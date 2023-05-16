import unittest

import processor.apiclient as apiclient
from processor.classifiers import MODEL_TYPES, VECTORIZER_TYPES


class TestApiClient(unittest.TestCase):

    def test_get_projects_list(self):
        project_list = apiclient.get_projects_list()
        assert isinstance(project_list, list)
        assert len(project_list) > 0
        for p in project_list:
            assert 'search_terms' in p
            assert 'start_date' in p
            assert 'media_collections' in p
            assert 'min_confidence' in p
            assert 'language_model_id' in p
            assert 'last_processed_stories_id' in p
            assert 'update_post_url' in p

    def test_get_language_models_list(self):
        models_list = apiclient.get_language_models_list()
        assert isinstance(models_list, list)
        assert len(models_list) > 0
        prefixes = []
        for p in models_list:
            assert 'id' in p
            assert p['filename_prefix'] is not None, "{} has no filename_prefix".format(p['id'])
            assert p['filename_prefix'] not in prefixes,\
                "{} has a non-unique filename_prefix of {}".format(p['id'], p['filename_prefix'])
            # has to include 1 model
            prefixes.append(p['filename_prefix'])
            assert p['model_type_1'] in MODEL_TYPES
            assert p['vectorizer_type_1'] in VECTORIZER_TYPES
            # can potentially include a second
            assert 'chained_models' in p
            if p['chained_models']:
                assert p['model_type_2'] in MODEL_TYPES
                assert p['vectorizer_type_1'] in VECTORIZER_TYPES


if __name__ == "__main__":
    unittest.main()
