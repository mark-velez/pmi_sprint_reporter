#!/usr/bin/env python
import logging
import cloudstorage
from cloudstorage import cloudstorage_api
from flask import Flask

import api_util
import common
import gcs_utils


PREFIX = '/data_steward/v1/'
app = Flask(__name__)


class DataError(RuntimeError):
    """Bad sample data during import.

  Args:
    msg: Passed through to superclass.
    external: If True, this error should be reported to external partners (HPO). Externally
        reported DataErrors are only reported if HPO recipients are in the config.
  """

    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external


@api_util.auth_required_cron
def validate_hpo_files(hpo_id):
    logging.info('Validating hpo_id %s' % hpo_id)
    gcs_path = gcs_utils.hpo_gcs_path(hpo_id)
    found_cdm_files = _find_cdm_files(gcs_path)
    result = [
        (cdm_file, 1 if cdm_file in map(lambda f: f.filename, found_cdm_files) else 0) for cdm_file in common.CDM_FILES
    ]
    _save_result_in_gcs('%s/result.csv' % gcs_path, result)
    return '{"report-generator-status": "started"}'


def _find_cdm_files(gcs_path):
    """
    Returns list of GCSFileStat of CDM files found in the bucket
    :param gcs_path: full path of directory to look for CDM files
    :return:
    """
    bucket_stat_list = list(cloudstorage_api.listbucket(gcs_path))
    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [
        s for s in bucket_stat_list
        if s.filename.lower() in map(lambda t: gcs_path + '%s' % t, common.CDM_FILES)]
    return bucket_stat_list


def _save_result_in_gcs(gcs_path, cdm_file_results):
    """
    Save the validation results in specified path
    :param gcs_path: full GCS path
    :param cdm_file_results: list of tuples (<cdm_file_name>, <found>)
    :return:
    """
    with cloudstorage.open(gcs_path, 'w', content_type='text/plain') as f:
        f.write('"cdm_file_name","found"\n')
        for (cdm_file_name, found) in cdm_file_results:
            line = '"%(cdm_file_name)s","%(found)s"\n' % locals()
            f.write(line)


app.add_url_rule(
    PREFIX + 'ValidateHpoFiles/<string:hpo_id>',
    endpoint='validate_hpo_files',
    view_func=validate_hpo_files,
    methods=['GET'])
