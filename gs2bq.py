#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module: gs2bq.py
Author: zlamberty
Created: 2018-11-04

Description:
    push data loaded into a bucket on google storage into bigquery table

Usage:
    >>> python gs2bq.py

"""

import collections
import csv
import datetime
import json
import logging
import logging.config
import os

import apache_beam as beam
import yaml

from apache_beam.options.pipeline_options import PipelineOptions


# ----------------------------- #
#   Module Constants            #
# ----------------------------- #

HERE = os.path.dirname(os.path.realpath(__file__))
LOGGER = logging.getLogger(__name__)
LOGCONF = os.path.join(HERE, 'logging.yaml')
with open(LOGCONF, 'rb') as f:
    logging.config.dictConfig(yaml.load(f))

# load the schema from a `json` file
#
# the file itself was created by using `dataprep` to import one file (2019 1).
# this inferred most data types and then I tweaked the rest by hand, and
# renamed one column (not renamed in the edited `json`). the `json` object was
# then acquired using the `bq` tool
with open('schema.json') as fp:
    _SCHEMA = json.load(fp)['schema']['fields']
# drop the log1p elements
_SCHEMA = [_ for _ in _SCHEMA if 'log1p' not in _['name']]
# replace the 1*** keys
for d in _SCHEMA:
    if d['name'].startswith('1'):
        d['name'] = 'x' + d['name']
_SCHEMA_TXT = ','.join(['{name}:{type}'.format(**d) for d in _SCHEMA])
_HEADERS = [_['name'] for _ in _SCHEMA]
_NON_STR_TYPES = {_['name']: _['type'] for _ in _SCHEMA if _['type'] != 'STRING'}
TRUES = ['t', 'Y', 'YES']
FALSES = ['f', 'N', 'NO']


# ----------------------------- #
#   arg parser                  #
# ----------------------------- #

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('-i', '--input', help="input file glob")
        parser.add_argument('-o', '--output', help="output file glob")


# ----------------------------- #
#   functions                   #
# ----------------------------- #

class ParseRecordFn(beam.DoFn):
    def process(self, element):
        rec = collections.OrderedDict()
        for (k, v) in zip(_HEADERS, csv.reader([element]).next()):
            v_type = _NON_STR_TYPES.get(k)
            # conversions!
            if v == '':
                v = None
            elif v_type == 'BOOLEAN':
                if v in TRUES:
                    v = True
                elif v in FALSES:
                    v = False
                else:
                    # don't do anything until we figure out nans
                    pass
            elif v_type == 'DATETIME':
                # leave these as-is; they are already in a json-ready string
                # format and we'd have to parse them back that way either way
                #try:
                #    v = datetime.datetime.strptime(v, '%Y-%m-%d %X')
                #except ValueError:
                #    v = datetime.datetime.strptime(v, '%Y-%m-%d')
                pass
            elif v_type == 'FLOAT':
                v = float(v)
            elif v_type == 'INTEGER':
                v = int(v)
            else:
                pass
            rec[k] = v
        return [rec]


def build_record(element):
    rec = collections.OrderedDict()
    for (k, v) in zip(_HEADERS, csv.reader([element]).next()):
        v_type = _NON_STR_TYPES.get(k)
        # conversions!
        if v == '':
            v = None
        elif v_type == 'BOOLEAN':
            if v in TRUES:
                v = True
            elif v in FALSES:
                v = False
            else:
                # don't do anything until we figure out nans
                pass
        elif v_type == 'DATETIME':
            # leave these as-is; they are already in a json-ready string
            # format and we'd have to parse them back that way either way
            #try:
            #    v = datetime.datetime.strptime(v, '%Y-%m-%d %X')
            #except ValueError:
            #    v = datetime.datetime.strptime(v, '%Y-%m-%d')
            pass
        elif v_type == 'FLOAT':
            v = float(v)
        elif v_type == 'INTEGER':
            v = int(v)
        else:
            pass
        rec[k] = v
    return rec


# ----------------------------- #
#   Main routine                #
# ----------------------------- #

def main():
    """do it to it lars

    args:

    returns:

    raises:

    """

    p = beam.Pipeline(options=MyOptions())

    transactions = (
        p
        | "read_files" >> beam.io.ReadFromText(
            file_pattern='data/test.csv',
            skip_header_lines=1
        )
        #| "to_records" >> beam.ParDo(ParseRecordFn())
        | "to_records" >> beam.Map(build_record)
        | "to_bq" >> beam.io.WriteToBigQuery(
            table='zach-sandbox-221015:usaspending.rzltest',
            schema=_SCHEMA_TXT,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
    )

    results = p.run().wait_until_finish()

    return results, p


# ----------------------------- #
#   Command line                #
# ----------------------------- #

if __name__ == '__main__':
    main()
