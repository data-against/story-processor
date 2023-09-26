import requests
from typing import Dict

from processor import FEMINICIDE_API_URL, FEMINICIDE_API_KEY


def get_projects_list() -> Dict:
    """
    The main server holds configuraiton and models - get the list.
    :return:
    """
    path = FEMINICIDE_API_URL + '/api/story_processor/projects.json'
    data = _get_json(path)
    for p in data:
        p['search_terms'] = p['search_terms'].replace(u'\xa0', u' ')
    return data


def get_language_models_list() -> Dict:
    """
    Each project can refer to one or two models - get them all.
    :return:
    """
    path = FEMINICIDE_API_URL + '/api/story_processor/language_models.json'
    return _get_json(path)


def _get_json(path: str) -> Dict:
    params = dict(apikey=FEMINICIDE_API_KEY)
    r = requests.get(path, params,
                     timeout=(3.05*20)*10)  # docs say set it to slightly larger than a multiple of 3, so 10ish minutes
    return r.json()
