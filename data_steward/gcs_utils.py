"""
Wrappers around Google Cloud Storage API (adapted from https://goo.gl/dRKiYz)
"""

from google.appengine.api import app_identity
import googleapiclient.discovery
import os
import StringIO

from oauth2client.client import GoogleCredentials


def get_hpo_bucket(hpo_id):
    # TODO determine how to map bucket
    return os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())


def hpo_gcs_path(hpo_id):
    """
    Get the fully qualified GCS path where HPO files will be located
    :param hpo_id: the id for an HPO
    :return: fully qualified GCS path
    """
    bucket_name = get_hpo_bucket(hpo_id)
    return '/%s/' % bucket_name


def create_service():
    credentials = GoogleCredentials.get_application_default()
    return googleapiclient.discovery.build('storage', 'v1', credentials=credentials)


def list_bucket_dir(gcs_path):
    """
    Get metadata for each object within the given GCS path
    :param gcs_path: full GCS path (e.g. `/<bucket_name>/path/to/person.csv`)
    :return: list of metadata objects
    """
    service = create_service()
    gcs_path_parts = gcs_path.split('/')
    if len(gcs_path_parts) < 2:
        raise ValueError('%s is not a valid GCS path' % gcs_path)
    bucket = gcs_path_parts[0]
    prefix = '/'.join(gcs_path_parts[1:]) + '/'
    req = service.objects().list(bucket=bucket, prefix=prefix, delimiter='/')

    all_objects = []
    while req:
        resp = req.execute()
        items = [item for item in resp.get('items', []) if item['name'] != prefix]
        all_objects.extend(items or [])
        req = service.objects().list_next(req, resp)
    return all_objects


def get_object(bucket, filename):
    # TODO accept gcs path
    service = create_service()
    req = service.objects().get_media(bucket=bucket, object=filename)
    with StringIO.StringIO() as out_file:
        downloader = googleapiclient.http.MediaIoBaseDownload(out_file, req)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        return out_file.getvalue()


def upload_object(bucket, filename):
    # TODO accept gcs path
    service = create_service()
    body = {'name': filename}
    # http://g.co/dv/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#insert
    with open(filename, 'rb') as f:
        media_body = googleapiclient.http.MediaIoBaseUpload(f, 'application/octet-stream')
        req = service.objects().insert(bucket=bucket, body=body, media_body=media_body)
        return req.execute()
