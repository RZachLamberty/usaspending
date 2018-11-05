#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module: data.py
Author: zlamberty
Created: 2018-11-04

Description:


Usage:
    <usage>

"""

import glob
import logging
import logging.config
import os
import re
import shutil
import subprocess
import zipfile

import requests
import yaml

from google.cloud import storage


# ----------------------------- #
#   Module Constants            #
# ----------------------------- #

HERE = os.path.dirname(os.path.realpath(__file__))
LOGGER = logging.getLogger('usaspending-data')
LOGCONF = os.path.join(HERE, 'logging.yaml')
with open(LOGCONF, 'rb') as f:
    logging.config.dictConfig(yaml.load(f))

logging.getLogger('urllib3').setLevel(logging.WARN)

CREDENTIALS_JSON = os.path.join(
    os.path.expanduser('~'), '.secrets', 'Zach Sandbox-e5337c4f4799.json'
)


# ----------------------------- #
#   Main routine                #
# ----------------------------- #

def _zipurls(year=2019):
    """generator of urls of zip archives"""
    while True:
        try:
            resp = requests.post(
                'https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/',
                data={
                    'agency': 'all',
                    'fiscal_year': year,
                    'type': 'contracts'
                }
            )
            zipurl = resp.json()['monthly_files'][0]['url']
            assert 'Delta' not in zipurl
            yield zipurl
            year -= 1
        except AssertionError:
            break
        except Exception as e:
            LOGGER.error(e)
            raise e


def storage_client(credentials_json=CREDENTIALS_JSON):
    return storage.Client.from_service_account_json(credentials_json)


def get(year=2019):
    """download all data"""
    for zipurl in _zipurls(year):
        LOGGER.info('downloading {}'.format(zipurl))
        zipname = 'data/{}'.format(os.path.basename(zipurl))
        subprocess.run('wget --quiet {} -O {}'.format(zipurl, zipname).split(' '))

        LOGGER.info('unzipping {}'.format(zipname))
        year = int(re.match('data/(\d{4})', zipname).groups()[0])
        with zipfile.ZipFile(zipname) as zfp:
            zfp.extractall(path='data/')

        LOGGER.info('renaming unzipped csvs')
        for fname in glob.glob('data/all_contracts_prime_transactions_*.csv'):
            shutil.move(fname, fname.replace('all_', '{}_all_'.format(year)))

        LOGGER.info('uploading csvs')
        quick_upload()

        LOGGER.info('cleaning up')
        os.remove(zipname)
        for fname in glob.glob('data/*.csv'):
            os.remove(fname)


def upload(file_glob='data/*.csv', bucket_name='eri-rzl-usaspending',
           credentials_json=CREDENTIALS_JSON):
    """upload to google storage"""
    sc = storage_client()
    bucket = sc.get_bucket(bucket_name)

    for src in glob.glob(file_glob):
        dst = os.path.basename(src)
        LOGGER.info('uploading {} to {}:{}'.format(src, bucket_name, dst))
        blob = bucket.get_blob(dst) or bucket.blob(dst)
        blob.upload_from_filename(src)
        LOGGER.info('upload complete')


def quick_upload(file_glob='data/*.csv', bucket_name='eri-rzl-usaspending'):
    """upload using the cli, much faster. maybe only because it is
    parallelized, but stil, much faster

    """
    cmd = ('gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M'
           'cp ./data/20*_all_contracts_prime_transactions_*.csv'
           'gs://eri-rzl-usaspending/').split()
    subprocess.run([
        'gsutil', '-m', '-o', 'Util:parallel_composite_upload_threshold=150M',
        'cp', file_glob, 'gs://{}'.format(bucket_name)
    ])


def main(year=2019, file_glob='data/*.csv', bucket_name='eri-rzl-spending',
         credentials_json=CREDENTIALS_JSON):
    """get and upload"""
    get(year)
    quick_upload(file_glob, bucket_name)


# ----------------------------- #
#   Command line                #
# ----------------------------- #

if __name__ == '__main__':
    main()
