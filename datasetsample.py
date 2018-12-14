import argparse
import json
import logging
from datetime import datetime
from os import getenv, mkdir
from os.path import basename, join
from shutil import rmtree
from urllib.parse import urlsplit

import pygsheets
from google.oauth2 import service_account
from hdx.data.dataset import Dataset
from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.utilities.database import Database
from hdx.utilities.dictandlist import args_to_dict, dict_of_lists_add
from hdx.utilities.downloader import Download, DownloadError
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.loader import load_yaml
from hdx.utilities.session import get_session
from requests.adapters import HTTPAdapter
from sqlalchemy import and_
from urllib3 import Retry

setup_logging()
logger = logging.getLogger(__name__)


def main(file_path, hdx_key, user_agent, preprefix, hdx_site, db_url, db_params, gsheet_auth):
    if db_params:
        params = args_to_dict(db_params)
    elif db_url:
        params = Database.get_params_from_sqlalchemy_url(db_url)
    else:
        params = {'driver': 'sqlite', 'database': 'freshness.db'}
    logger.info('> Database parameters: %s' % params)
    with Database(**params) as session:
        info = json.loads(gsheet_auth)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        gc = pygsheets.authorize(custom_credentials=credentials)
        configuration = load_yaml('project_configuration.yml')
        spreadsheet = gc.open_by_url(configuration['spreadsheet_url'])
        sheet = spreadsheet.worksheet_by_title('datasets')
        sheet.clear()
        rows = [['update freq', 'fresh', 'no days', 'title', 'run date', 'last modified', 'dataset date', 'dataset end date', 'org title', 'URL', 'id', 'org id', 'maintainer', 'what updated', 'resources']]
        run_number, run_date = session.query(DBRun.run_number, DBRun.run_date).order_by(DBRun.run_number.desc()).first()
        logger.info('Run number is %d' % run_number)

        datasetcolumns = [DBDataset.update_frequency, DBDataset.fresh, DBInfoDataset.title, DBDataset.last_modified,
                          DBDataset.dataset_date, DBOrganization.title.label('organization_title'), DBInfoDataset.name,
                          DBDataset.id, DBOrganization.id.label('organization_id'), DBInfoDataset.maintainer, DBDataset.what_updated]

        resourcecolumns = [DBDataset.id, DBResource.url]

        def get_datasets(update_frequency, fresh):
            filters = [DBDataset.run_number == run_number, DBDataset.id == DBInfoDataset.id,
                       DBInfoDataset.organization_id == DBOrganization.id,
                       DBDataset.fresh == fresh, DBDataset.update_frequency == update_frequency]
            return session.query(*datasetcolumns).filter(and_(*filters))

        def get_resources(dataset_ids):
            filters = [DBDataset.run_number == run_number, DBResource.run_number == run_number,
                       DBDataset.id == DBResource.dataset_id, DBDataset.id.in_(dataset_ids)]
            return session.query(*resourcecolumns).filter(and_(*filters))

        fresh_values = [0, 1, 2, 3]
        update_frequencies = [1, 7, 14, 30, 180, 365]

        repobase = '%s/tree/master/datasets/' % configuration['repo']
        dir = join(file_path, 'datasets')
        rmtree(dir, ignore_errors=True)
        mkdir(dir)

        with Download(user_agent=user_agent, preprefix=preprefix) as downloader:
            status_forcelist = [429, 500, 502, 503, 504]
            method_whitelist = frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE'])
            retries = Retry(total=1, backoff_factor=0.4, status_forcelist=status_forcelist,
                            method_whitelist=method_whitelist,
                            raise_on_redirect=True,
                            raise_on_status=True)
            downloader.session.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
            downloader.session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))

            for update_frequency in update_frequencies:
                for fresh in fresh_values:
                    org_ids = list()
                    results = get_datasets(update_frequency, fresh)
                    datasets = list()
                    ids = list()
                    datasets_urls = dict()
                    for dataset in results:
                        dataset = list(dataset)
                        datasets.append(dataset)
                        ids.append(dataset[7])
                    for result in get_resources(ids):
                        resource = list(result)
                        dict_of_lists_add(datasets_urls, resource[0], resource[1])
                    for dataset in datasets:
                        org_id = dataset[8]
                        if org_id in org_ids:
                            continue
                        dataset = list(dataset)
                        dataset[0] = Dataset.transform_update_frequency(str(update_frequency))
                        fresh = dataset[1]
                        if fresh == 0:
                            dataset[1] = 'fresh'
                        elif fresh == 1:
                            dataset[1] = 'due'
                        elif fresh == 2:
                            dataset[1] = 'overdue'
                        elif fresh == 3:
                            dataset[1] = 'delinquent'
                        last_modified = dataset[3]
                        dataset[3] = last_modified.isoformat()
                        nodays = (run_date - last_modified).days
                        dataset.insert(2, nodays)
                        dataset.insert(4, run_date.isoformat())
                        dataset_date = dataset[6]
                        if '-' in dataset_date:
                            dataset_date = dataset_date.split('-')
                            dataset[6] = datetime.strptime(dataset_date[0], '%m/%d/%Y').date().isoformat()
                            dataset.insert(7, datetime.strptime(dataset_date[1], '%m/%d/%Y').date().isoformat())
                        else:
                            dataset[6] = datetime.strptime(dataset_date, '%m/%d/%Y').date().isoformat()
                            dataset.insert(7, '')
                        dataset_name = dataset[9]
                        dataset[9] = 'https://data.humdata.org/dataset/%s' % dataset_name
                        org_ids.append(org_id)
                        if len(org_ids) == 6:
                            break
                        urls = datasets_urls[dataset[10]]
                        if len(urls) != 0:
                            datasetdir = join(dir, dataset_name)
                            mkdir(datasetdir)
                            for url in urls:
                                urlpath = urlsplit(url).path
                                filename = basename(urlpath)
                                try:
                                    downloader.download_file(url, datasetdir, filename)
                                except DownloadError as ex:
                                    with open(join(datasetdir, filename), 'w') as text_file:
                                        text_file.write(str(ex))
                            dataset.append('%s%s' % (repobase, dataset_name))
                        else:
                            dataset.append('')
                        rows.append(dataset)
                        logger.info('Added dataset %s' % dataset_name)
            sheet.update_values('A1', rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DatasetSample')
    parser.add_argument('-fp', '--file_path', default=None, help='Path to store files')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters. Overrides --db_url.')
    parser.add_argument('-gh', '--github_auth', default=None, help='Credentials for accessing GitHub')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    args = parser.parse_args()
    file_path = args.file_path
    if file_path is None:
        file_path = getenv('FILE_PATH')
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'dataset-sampler'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    db_url = args.db_url
    if db_url is None:
        db_url = getenv('DB_URL')
    if db_url and '://' not in db_url:
        db_url = 'postgresql://%s' % db_url
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    main(file_path, hdx_key, user_agent, preprefix, hdx_site, db_url, args.db_params, gsheet_auth)
