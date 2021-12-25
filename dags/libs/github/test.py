import itertools
import requests
import time
from opensearchpy import OpenSearch
from ..util.base import github_headers
from ..util.log import logger

def test_github_headers():
    logger.warning(github_headers)

    return "End load_github_profile-return"
