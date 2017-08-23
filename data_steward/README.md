# Data Steward

Specification document and data curation processes for data submitted to the DRC.

## Development Requirements

 * Python 2.7.* (download from [here](https://www.python.org/downloads/) and install)
 * pip (download [get-pip.py](https://bootstrap.pypa.io/get-pip.py) and run `python get-pip.py`)
 * Google [Cloud SDK](https://cloud.google.com/sdk/downloads#interactive)
    * `google-cloud-sdk-app-engine-python` (follow instructions in Cloud SDK)
 * _Recommended: [virtualenv](https://pypi.python.org/pypi/virtualenv)_

### Local Environment

In general we want to keep development and testing local (see 
[Local Unit Testing for Python](https://cloud.google.com/appengine/docs/standard/python/tools/localunittesting)), 
but some services such as bigquery do not support local emulation and thus require access to cloud resources. The 
following environment variables are needed to configure access to these services. 

| name | description |
| ---- | ----------- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Location of service account credentials in JSON format (see [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork)) |
| `APPLICATION_ID` | Google cloud project ID. For development, we use `all-of-us-ehr-dev`. |
| `BIGQUERY_DATASET_ID` |  |

## Installation / Configuration

 * Install requirements by running

        pip install -t lib -r requirements.txt
