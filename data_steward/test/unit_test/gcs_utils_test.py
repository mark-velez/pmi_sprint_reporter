import unittest

import os

import bq_utils
import gcs_utils
import cloudstorage
from google.appengine.ext import testbed


class GcsUtilsTest(unittest.TestCase):
    def setUp(self):
        super(GcsUtilsTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()

    def _write_cloud_csv(self, gcs_path, contents_str):
        with cloudstorage.open(gcs_path, mode='w') as cloud_file:
            cloud_file.write(contents_str.encode('utf-8'))

    def _read_cloud_file(self, gcs_path):
        with cloudstorage.open(gcs_path) as cloudstorage_file:
            return cloudstorage_file.read()

    def test_list_bucket(self):
        hpo_bucket = gcs_utils.get_hpo_bucket('foo')
        gcs_path = '/'.join([hpo_bucket, 'dummy'])
        result = gcs_utils.list_bucket(gcs_path)
        print result

    def tearDown(self):
        pass
