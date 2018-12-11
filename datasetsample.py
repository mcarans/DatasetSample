import argparse
import json
import logging
from datetime import datetime
from os import getenv
from os.path import basename
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
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import temp_dir
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import FileNotUploadedError
from sqlalchemy import and_

setup_logging()
logger = logging.getLogger(__name__)


def main(hdx_key, user_agent, preprefix, hdx_site, db_url, db_params, gsheet_auth):
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
        gauth = GoogleAuth()
        gauth.credentials = ServiceAccountCredentials._from_parsed_json_keyfile(info, scopes)
        gauth.credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        drive = GoogleDrive(gauth)
        credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        gc = pygsheets.authorize(custom_credentials=credentials)

        configuration = load_yaml('project_configuration.yml')
        parent_id = configuration['folder_id']
        child_folder = drive.CreateFile({'title': 'datasets', 'parents': [{'id': parent_id}], 'mimeType': 'application/vnd.google-apps.folder'})
        try:
            child_folder.Delete()
        except FileNotUploadedError:
            pass
        child_folder.Upload()

        spreadsheet = gc.open_by_url(configuration['spreadsheet_url'])
        sheet = spreadsheet.worksheet_by_title('datasets')
        sheet.clear()
        row = ['update freq', 'fresh', 'no days', 'title', 'run date', 'last modified', 'dataset date', 'dataset end date', 'org title', 'URL', 'id', 'org id', 'maintainer', 'what updated', 'resources']
        sheet.update_row(1, row)
        run_number, run_date = session.query(DBRun.run_number, DBRun.run_date).order_by(DBRun.run_number.desc()).first()
        logger.info(run_number)

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
        rowno = 1

        with Download() as downloader:
            with temp_dir() as tempdir:
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
                                dataset_folder = drive.CreateFile({'title': dataset_name, 'parents': [{'id': child_folder['id']}],
                                                                   'mimeType': 'application/vnd.google-apps.folder'})
                                try:
                                    dataset_folder.Delete()
                                except FileNotUploadedError:
                                    pass
                                dataset_folder.Upload()
                                for url in urls:
                                    urlpath = urlsplit(url).path
                                    filename = basename(urlpath)
                                    path = downloader.download_file(url, tempdir, filename)
                                    file = drive.CreateFile({'title': filename, 'parents': [{'id': dataset_folder['id']}]})
                                    file.SetContentFile(path)
                                    file.Upload()
                                dataset[14] = dataset_folder['alternateLink']
                            else:
                                dataset[14] = ''
                            rowno += 1
                            sheet.update_row(rowno, dataset)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DatasetSample')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters. Overrides --db_url.')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    args = parser.parse_args()
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
    main(hdx_key, user_agent, preprefix, hdx_site, db_url, args.db_params, gsheet_auth)
