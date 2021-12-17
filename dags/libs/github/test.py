import itertools
import requests
import time
from opensearchpy import OpenSearch
from ..util.base import github_headers
from ..a.b.demo import hello

def test_github_headers():
    print(github_headers)
    hello()

    return "End load_github_profile-return"
