import copy
import logging
from typing import Dict, List

from processor import get_mc_directory_client

logger = logging.getLogger(__name__)


def _domains_for_collection(cid: int) -> List[str]:
    limit = 1000
    offset = 0
    sources = []
    mc_directory_api = get_mc_directory_client()
    while True:
        response = mc_directory_api.source_list(
            collection_id=cid, limit=limit, offset=offset
        )
        # for now we need to remove any sources that have a url_search_string because they are not supported in the API
        # (wildcard search bug on the IA side)
        sources += [r for r in response["results"] if r["url_search_string"] is None]
        if response["next"] is None:
            break
        offset += limit
    return [
        s["name"] for s in sources if s["name"] is not None
    ]  # grab just the domain names


def _domains_for_project(collection_ids: List[int]) -> List[str]:
    all_domains = []
    for cid in collection_ids:  # fetch all the domains in each collection
        all_domains += _domains_for_collection(
            cid
        )  # remove cache because Prefect failing to serials with thread lock error :-(
    return list(set(all_domains))  # make them unique


def fetch_domains_for_projects(project: Dict) -> Dict:
    domains = _domains_for_project(project["media_collections"])
    logger.info(
        f"Project {project['id']}/{project['title']}: found {len(domains)} domains"
    )
    updated_project = copy.copy(project)
    updated_project["domains"] = domains
    return updated_project
