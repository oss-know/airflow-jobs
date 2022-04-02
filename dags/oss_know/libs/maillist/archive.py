import gzip
import http
import shutil
from datetime import datetime
from os import makedirs, remove, path

import requests
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from grimoire_elk.enriched.mbox import MBoxEnrich
from grimoire_elk.enriched.pipermail import PipermailEnrich
from grimoire_elk.raw.mbox import MBoxOcean
from grimoire_elk.raw.pipermail import PipermailOcean
from grimoire_elk.utils import get_elastic
from perceval.backends.core.mbox import MBox
from perceval.backends.core.pipermail import Pipermail

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_MAILLISTS
from oss_know.libs.util.base import now_timestamp
from oss_know.libs.util.log import logger


class OSSKnowMBoxEnrich(MBoxEnrich):
    ESSENTIAL_KEYS = ['References', 'In-Reply-To', 'From', 'To']

    def __init__(self, project_name, list_name, **kwargs):
        super().__init__(**kwargs)
        self.project_name = project_name
        self.mail_list_name = list_name

    def get_rich_item(self, item):
        eitem = super().get_rich_item(item)
        eitem['search_key'] = {
            'message_id': item['data']['Message-ID'],
            'project_name': self.project_name,
            'mail_list_name': self.mail_list_name,
            'updated_at': now_timestamp()
        }
        # TODO Might be KeyError or TypeError(refering None)?
        eitem['Date_tz'] = item['data']['Date_tz']

        for key in ['email_date', 'metadata__enriched_on', 'grimoire_creation_date']:
            formatted_date_str, tz_num = _convert_date_str(eitem[key])
            eitem[key] = formatted_date_str
            eitem[f'{key}_tz'] = tz_num

        for essential_key in OSSKnowMBoxEnrich.ESSENTIAL_KEYS:
            if essential_key in item['data']:
                eitem[essential_key] = item['data'][essential_key]
        return eitem

    def get_connector_name(self):
        return 'mbox'


# PipermailEnrich also inherits from MBoxEnrich
# This makes a 'diamond inherits', which brings complexity
# TODO Make sure OSSKnowPipermailEnrich performs as expected
class OSSKnowPipermailEnrich(OSSKnowMBoxEnrich, PipermailEnrich):
    def get_connector_name(self):
        return 'pipermail'


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
            self.file_paths.append(filepath)
            makedirs(self.dirpath, exist_ok=True)
            # TODO 404 is not an error(there might not be archive for a particular month)
            # So we should extend do_get_result and replace the logic here
            # res = do_get_result(requests.Session(), url)
            res = requests.get(url, stream=True)
            if res.status_code == http.HTTPStatus.NOT_FOUND:
                logger.info(f'{url} not found, might not be archive for that period')
            elif res.status_code >= 300:
                logger.error(f'Failed to fetch archive from {url}: {res.status_code}, {res.text}')
            else:  # Only when status_code is within [200, 299], save file
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
    project_name = maillist_params['project_name']
    list_name = maillist_params['list_name']

    clear_existing_indices = False
    if 'clear_exist' in maillist_params:
        clear_existing_indices = bool(maillist_params['clear_exist'])

    kwargs = {}
    for essential_key in ['project_name', 'list_name', 'url_prefix']:
        kwargs[essential_key] = maillist_params[essential_key]

    repo = None
    enrich_backend = None
    ocean_backend = None
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
        enrich_backend = OSSKnowMBoxEnrich(project_name, list_name)
    elif archive_type == 'pipermail':
        archive = EmailArchive(**kwargs)
        archive.get_res()
        repo = Pipermail(url=archive.url_prefix, dirpath=archive.dirpath, ssl_verify=False)
        ocean_backend = PipermailOcean(None)
        enrich_backend = OSSKnowPipermailEnrich(project_name, list_name)
    elif archive_type == 'mbox':
        project_name = maillist_params['project_name']
        list_name = maillist_params['list_name']
        url_prefix = maillist_params['url_prefix']
        if 'mbox_path' in maillist_params and maillist_params['mbox_path']:
            dirpath = maillist_params['mbox_path']
        else:
            dirpath = path.join(project_name, list_name)
        repo = MBox(uri=url_prefix, dirpath=dirpath)
        ocean_backend = MBoxOcean(None)
        enrich_backend = OSSKnowMBoxEnrich(project_name, list_name)

    ocean_backend.set_filter_raw(f'enrich_filter_raw.keyword: {project_name}_{list_name}')

    os_user = opensearch_conn_info["USER"]
    os_pass = opensearch_conn_info["PASSWD"]
    os_host = opensearch_conn_info["HOST"]
    os_port = opensearch_conn_info["PORT"]
    os_url = f'https://{os_user}:{os_pass}@{os_host}:{os_port}'
    # grimoire_elk will create mapping automatically, which is not necessary
    # And mapping creation will fail
    ocean_backend.mapping = None
    elastic_ocean = get_elastic(os_url, OPENSEARCH_INDEX_MAILLISTS, clear_existing_indices, ocean_backend)
    ocean_backend.set_elastic(elastic_ocean)

    num_raw = _data2es(repo.fetch(), ocean_backend, project_name, list_name)
    logger.info(f'{num_raw} records for mail list {project_name}/{list_name}')

    enrich_backend.mapping = None
    elastic_enrich = get_elastic(os_url, f'{OPENSEARCH_INDEX_MAILLISTS}_enriched', clear_existing_indices,
                                 enrich_backend)
    enrich_backend.set_elastic(elastic_enrich)

    # TODO What does the param [sortinghat] and [projects] do here?
    num_enriched = enrich_backend.enrich_items(ocean_backend)
    logger.info(f'{num_enriched} enriched records for mail list {project_name}/{list_name}')


# The 2 helpers are yanked from:
# https://github.com/chaoss/grimoirelab-elk/blob/6fa82bd1550257f74c13bb11bcae58f895291ca9/tests/base.py#L54
def _ocean_item(item, ocean):
    # TODO By <juzhen@huawei.com>: We may not need the expansions here
    # Hack until we decide when to drop this field
    if 'updated_on' in item:
        item['metadata__updated_on'] = datetime.utcfromtimestamp(int(item['updated_on'])).strftime('%Y-%m-%dT%H:%M:%SZ')

    if 'timestamp' in item:
        item['metadata__timestamp'] = datetime.utcfromtimestamp(int(item['timestamp'])).strftime('%Y-%m-%dT%H:%M:%SZ')

    formatted_date_str, tz_num = _convert_date_str(item['data']['Date'])
    item['data']['Date'] = formatted_date_str
    item['data']['Date_tz'] = tz_num

    # the _fix_item does not apply to the test data for Twitter
    try:
        ocean._fix_item(item)
    except KeyError:
        pass

    if ocean.anonymize:
        ocean.identities.anonymize_item(item)

    return item


def _data2es(items, ocean, project_name, mail_list_name):
    items_pack = []  # to feed item in packs
    inserted = 0

    for item in items:
        item = _ocean_item(item, ocean)
        # By <juzhen@huawei.com> Insert the customized search_key:
        item['search_key'] = {
            'message_id': item['data']['Message-ID'],
            'project_name': project_name,
            'mail_list_name': mail_list_name,
            'updated_at': now_timestamp()
        }
        item['data']['enrich_filter_raw'] = f'{project_name}_{mail_list_name}'
        items_pack.append(item)
        if len(items_pack) >= ocean.elastic.max_items_bulk:
            inserted += ocean._items_to_es(items_pack)
            items_pack = []

    if items_pack:
        inserted += ocean._items_to_es(items_pack)
    return inserted


def _convert_date_str(date_str):
    """Get the formatted date string(%Y-%m-%dT%H:%M:%SZ) and time zone number(like: +8)
        Parameters:
            date_str (str): A date string, expected to be parsed by dateutil lib

        Returns:
            formatted_date_str (str): Date string formatted in "%Y-%m-%dT%H:%M:%SZ"
            tz_num (int): The time zone number, an integer like +8, -3, 0
    """
    datetime_obj = parse(date_str, fuzzy=True)
    formatted_date_str = datetime_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    tz_num = 0
    utc_offset = datetime_obj.utcoffset()
    if utc_offset:
        tz_num = int(utc_offset.total_seconds() / 3600)
    return formatted_date_str, tz_num
