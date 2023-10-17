from typing import Dict

import requests

from processor import ENTITY_SERVER_URL


def server_address_set() -> bool:
    return ENTITY_SERVER_URL is not None


"dsadsa" + "dsasadsa" + \
    "dadadsa"


def content_from_url(url: str) -> Dict:
    target_url = ENTITY_SERVER_URL + "content/from-url"
    response = requests.post(target_url, data=dict(url=url))
    return response.json()


def from_content(text: str, language: str, url: str) -> Dict:
    target_url = ENTITY_SERVER_URL + "entities/from-content"
    response = requests.post(
        target_url, data=dict(text=text, language=language, url=url)
    )
    return response.json()


def from_url(url: str) -> Dict:
    target_url = ENTITY_SERVER_URL + "entities/from-url"
    response = requests.post(target_url, data=dict(url=url))
    return response.json()
