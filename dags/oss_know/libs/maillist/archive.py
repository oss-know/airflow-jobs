from datetime import datetime
import requests
import shutil
import gzip
import json
from os import makedirs, remove, path
from opensearchpy import OpenSearch
from datetime import datetime
from grimoire_elk.raw.mbox import MBoxOcean
from grimoire_elk.enriched.mbox import MBoxEnrich
from grimoire_elk.raw.pipermail import PipermailOcean
from grimoire_elk.enriched.pipermail import PipermailEnrich
from grimoire_elk.utils import get_elastic
from perceval.backends.core.mbox import MBox
import os

from dateutil.relativedelta import relativedelta
from perceval.backends.core.mbox import MBox
from perceval.backends.core.pipermail import Pipermail
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.opensearch_index import NSEARCH_INDEX_MAILLISTS



class EmailArchive:
    def __init__(self, project_name, list_name, url_prefix, start_since=None):
        self.project_name = project_name
        self.dirpath = path.join(project_name, list_name)  # TODO Is this proper?(dirpath should just be temp)
        self.url_prefix = url_prefix
        self.start_since = start_since
        self.list_name = list_name

    def get_res(self, since=None, until=None):
        pass

class FileArchive(EmailArchive):
    def __init__(self, project_name, list_name, url_prefix, months=[], url_format='', start_since=None, file_ext=None):
        super().__init__(project_name, list_name, url_prefix, start_since)

        self.url_format = url_format
        self.file_ext = file_ext
        self.months = months
        self.file_paths = []
        # TODO Init date is needed

    def download_files(self, urls):
        for url in urls:
            filename = url.split('/')[-1]
            filepath = f'{self.dirpath}/{filename}'
            print(f'Downloading {url} to {filepath}')
            self.file_paths.append(filepath)
            makedirs(self.dirpath, exist_ok=True)
            with requests.get(url, stream=True) as res:
                with open(filepath, 'wb') as f:
                    shutil.copyfileobj(res.raw, f)

    def process_downloaded_file(self):
        pass

    def get_res(self, since=None, until=None):
        urls = self.generate_urls(since, until)
        self.download_files(urls)
        self.process_downloaded_file()

    def generate_urls(self, since=None, until=None):
        start, end = since, until
        if not since:
            start = self.start_since
        if not until:
            end = datetime.now()

        index = start
        urls = []
        while index <= end:
            date_qs = index.strftime(self.url_format)

            # TODO Generate qs according to self.months
            # Notes: python datetime format can cover full month name with %B
            # Add rules & code to handle more complex situation
            # if self.months:
            #     month_name = self.months[index.month-1]
            # do something with month_name
            url = f'{self.url_prefix}/{date_qs}'
            if self.file_ext:
                url = f'{url}.{self.file_ext}'
            urls.append(url)

            # TODO FileArchive should take an 'interval' param to customize the archive interval
            index += relativedelta(months=1)
        return urls


class GZipArchive(FileArchive):
    def process_downloaded_file(self):
        # When downloaded, the filepath points to the gzip file
        for gzip_filepath in self.file_paths:
            with gzip.open(gzip_filepath, 'rb') as gzip_in:
                # Forget about striping the '.tgz', 'gzip'
                # just add a postfix 'raw' to make life easy!
                with open(f'{gzip_filepath}.raw', 'wb') as raw_out:
                    shutil.copyfileobj(gzip_in, raw_out)
            remove(gzip_filepath)


def sync_archive(opensearch_conn_info, **maillist_params):
    archive_type = maillist_params['archive_type']

    kwargs = {}
    for essential_key in ['project_name', 'list_name','url_prefix']:
        kwargs[essential_key] = maillist_params[essential_key]

    repo = None
    archive = None
    # TODO Receive 'since' and 'until' param from the caller
    if archive_type == 'txt' or archive_type == 'gzip':
        date_format = maillist_params['date_format']
        start_since = datetime.strptime(maillist_params['start_since'], maillist_params['date_format'])
        kwargs['url_format'] = date_format
        kwargs['start_since'] = start_since

        if 'file_ext' in maillist_params and maillist_params['file_ext']:
            kwargs['file_ext'] = maillist_params['file_ext']
        archive = FileArchive(**kwargs)
        archive.get_res()
        repo = MBox(uri=archive.url_prefix, dirpath=archive.dirpath)
        ocean_backend = MBoxOcean(None)
        enrich_backend = MBoxEnrich()
    elif archive_type == 'pipermail':
        archive = EmailArchive(**kwargs)
        archive.get_res()
        repo = Pipermail(url=archive.url_prefix, dirpath=archive.dirpath)
        ocean_backend = PipermailOcean(None)
        enrich_backend = PipermailEnrich()

    # Store original data to raw opensearch index
    data2es(repo.fetch(), ocean_backend)
    OS_USER = opensearch_conn_info["USER"]
    OS_PASS = opensearch_conn_info["PASSWD"]
    OS_HOST = opensearch_conn_info["HOST"]
    OS_PORT = opensearch_conn_info["PORT"]
    OS_URL = f'https://{OS_USER}:{OS_PASS}@{OS_HOST}:{OS_PORT}'
    elastic_ocean = get_elastic(OS_URL, OPENSEARCH_INDEX_MAILLISTS, True, ocean_backend, [])
    ocean_backend.set_elastic(elastic_ocean)

    # TODO What does the param [sortinghat] and [projects] do here?
    num_enriched = enrich_backend.enrich_items(ocean_backend)
    print(f'num_enriched: {num_enriched}')

def data2es(items, ocean):
    def ocean_item(item):
        # Hack until we decide when to drop this field
        if 'updated_on' in item:
            updated = datetime.fromtimestamp(item['updated_on'])
            item['metadata__updated_on'] = updated.isoformat()
        if 'timestamp' in item:
            ts = datetime.fromtimestamp(item['timestamp'])
            item['metadata__timestamp'] = ts.isoformat()

        # the _fix_item does not apply to the test data for Twitter
        try:
            ocean._fix_item(item)
        except KeyError:
            pass

        if ocean.anonymize:
            ocean.identities.anonymize_item(item)

        return item

    items_pack = []  # to feed item in packs

    for item in items:
        item = ocean_item(item)
        if len(items_pack) >= ocean.elastic.max_items_bulk:
            ocean._items_to_es(items_pack)
            items_pack = []
        items_pack.append(item)
    inserted = ocean._items_to_es(items_pack)

    return inserted

