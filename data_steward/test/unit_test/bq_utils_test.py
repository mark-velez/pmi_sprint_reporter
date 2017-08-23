import unittest

import bq_utils
import gcs_utils
from google.appengine.ext import testbed


_FAKE_HPO_ID = 'foo'


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
        self.hpo_bucket = gcs_utils.get_hpo_bucket(_FAKE_HPO_ID)
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_load_table_from_bucket(self):
        bq_utils.load_table_from_bucket(self.hpo_bucket, 'person')

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
