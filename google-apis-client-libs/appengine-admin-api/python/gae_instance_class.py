#!/usr/bin/env python

"""Simple command-line sample that demonstrates the use of App Engine Admin API. It allows you to dynamically change the instance class of a given application version.

Requirements:
    * You need to set GOOGLE_APPLICATION_CREDENTIALS="/path/to/json-credentials" or;
    * Authenticate with "gcloud beta auth application-default login"


Usage:
  $ gae_instance_class.py project_id [service] version new_ins_class
"""

import sys
import json
import argparse

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials


def create_service():
    """Executes the authentication flow and build the service"""
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the App Engine Admin API.
    return discovery.build('appengine', 'v1', credentials=credentials)


def change_class(admin_api, project_id, service, version, ins_class):
    """change the instance class of a given application version"""
    req = admin_api.apps().services().versions().patch(
        appsId=project_id, servicesId=service, versionsId=version,
        updateMask="instanceClass", body={"instanceClass":ins_class})
    return req.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud Project ID.')
    parser.add_argument('service', nargs='?', default='default',
        help='The App Engine service (default: %(default)s)')
    parser.add_argument('version', 
        help='The App Engine app version to be modified.')
    parser.add_argument('ins_class', 
        choices= ['B1', 'B2', 'B4', 'B4_1G', 'B8', 'F1', 'F2', 'F4', 'F4_1G'],
        help='The instance class to bet set.')

    args = parser.parse_args()

    admin_api = create_service()
    try:

        response = change_class(admin_api, args.project_id, args.service, 
            args.version, args.ins_class)
    except HttpError as err:
        print "Error: {}".format(json.loads(err.content)['error']['message'])
        sys.exit(0)
    print "Instance class of application '{}' successfully changed to {}".format(args.version, args.ins_class)