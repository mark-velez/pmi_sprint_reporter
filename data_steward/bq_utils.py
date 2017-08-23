import os
from googleapiclient.discovery import build
from google.appengine.api import app_identity
import json

import common
import gcs_utils


def get_dataset_id():
    return os.environ.get('BIGQUERY_DATASET_ID')


def create_service():
    return build('bigquery', 'v2')


def load_table_from_bucket(hpo_id, cdm_table_name):
    """
    Load csv file from a bucket into a table in bigquery
    :param hpo_id: ID for the HPO site
    :param cdm_table_name: name of the CDM table
    :return: an object describing the associated bigquery job
    """
    assert (cdm_table_name in common.CDM_TABLES)
    app_id = app_identity.get_application_id()
    dataset_id = get_dataset_id()
    bq_service = build('bigquery', 'v2')

    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    fields_filename = '%s.json' % cdm_table_name
    gcs_object_path = 'gs://%s/%s.csv' % (bucket, cdm_table_name)

    # Prefix table names with hpo_id
    # TODO revisit this
    table_id = hpo_id + '_' + cdm_table_name

    fields = json.load(open(fields_filename, 'r'))
    job_body = {
        'configuration':
            {
                'load':
                    {
                        'sourceUris': [gcs_object_path],
                        'schema': {'fields': fields},
                        'destinationTable': {
                            'projectId': app_id,
                            'datasetId': dataset_id,
                            'tableId': table_id
                        },
                        'skipLeadingRows': 1
                    }
            }
    }
    insert_result = bq_service.jobs().insert(projectId=app_id, body=job_body).execute()
    return insert_result
