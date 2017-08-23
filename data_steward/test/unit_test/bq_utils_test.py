import unittest

import os

import bq_utils
import gcs_utils
import cloudstorage
from google.appengine.ext import testbed


class BqUtilsTest(unittest.TestCase):
    def setUp(self):
        super(BqUtilsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_id = 'foo'
        self.gcs_path = gcs_utils.hpo_gcs_path(self.hpo_id)

    def _write_cloud_csv(self, gcs_path, contents_str):
        with cloudstorage.open(gcs_path, mode='w') as cloud_file:
            cloud_file.write(contents_str.encode('utf-8'))

    def _read_cloud_file(self, gcs_path):
        with cloudstorage.open(gcs_path) as cloudstorage_file:
            return cloudstorage_file.read()

    def test_load_table_from_bucket(self):
        person_csv = \
"""person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,birth_datetime,race_concept_id,ethnicity_concept_id,location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id
2285,8532,1956,10,1,,8516,38003564,60,,,04C307F9CE34D633,2,,2,,2,
3783,8532,1934,12,1,,8527,38003564,485,,,0816918B092DCD82,2,,1,,1,
21450,8532,1943,4,1,,8527,38003564,2093,,,2EA6BB4A86E0634F,2,,1,,1,
85908,8507,1943,11,1,,8527,38003564,78,,,BD248CFD7FB756FD,1,,1,,1,
53639,8507,1962,9,1,,8527,38003564,977,,,7554915866207BB6,1,,1,,1,
"""
        self._write_cloud_csv('%s/person.csv' % self.gcs_path, person_csv)
        bq_utils.load_table_from_bucket(self.hpo_id, 'person')

    def tearDown(self):
        pass
