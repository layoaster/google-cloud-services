#!/usr/bin/env python

"""Simple command-line sample that demonstrates the use of Cloud Pub/Sub with a pull subscription. It sends and message and gets it in return.

Requirements:
    * You need to set GOOGLE_APPLICATION_CREDENTIALS="/path/to/json-credentials" or;
    * Authenticate with "gcloud beta auth application-default login"
    * Set the PROJECT_ID to your Google Cloud project ID


Usage:
  $ python echo.py message
"""

import sys
import time
import base64

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build


PROJECT_ID = "<YOUR-PROJECT-ID>"
TOPIC_NAME = "echo-topic"
SUBSCRIPTION_NAME= "echo-subscription"
NUM_RETRIES = 3
BATCH_SIZE = 3


def create_service():
    """Authenticates and creates the Cloud Pub/Sub client"""
    credentials = GoogleCredentials.get_application_default()
    return build('pubsub', 'v1', credentials=credentials)


def fq_name(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)

def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return fq_name('topics', project, topic)


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fq_name('subscriptions', project, subscription)

def create_topic(client):
    """Creates a new topic"""
    topic = client.projects().topics().create(
        name=get_full_topic_name(PROJECT_ID, TOPIC_NAME),
        body={}).execute(num_retries=NUM_RETRIES)

def delete_topic(client, topic):
    """Deletes a given topic"""
    client.projects().topics().delete(
        topic=get_full_topic_name(PROJECT_ID, topic)
        ).execute(num_retries=NUM_RETRIES)    

def create_subscription(client, topic):
    """Creates a new pull subscrition to a given topic"""
    name = get_full_subscription_name(PROJECT_ID, SUBSCRIPTION_NAME)
    body = {'topic': get_full_topic_name(PROJECT_ID, topic)}
    subscription = client.projects().subscriptions().create(
        name=name, body=body).execute(num_retries=NUM_RETRIES)

def delete_subscription(client, subscription):
    """Deletes  a given subscrition"""
    client.projects().subscriptions().delete(
        subscription=get_full_subscription_name(PROJECT_ID, subscription)
        ).execute(num_retries=NUM_RETRIES)

def publish_message(client, topic, message):
    name = get_full_topic_name(PROJECT_ID, topic)
    msg = base64.b64encode(message)
    body = {'messages': [{'data': msg}]}
    client.projects().topics().publish(topic=name, body=body
        ).execute(num_retries=NUM_RETRIES)


def pull_messages(client, subscription):
    """Pull messages from a given subscription."""
    name = get_full_subscription_name(PROJECT_ID,subscription)
    body = {'returnImmediately': False,
        'maxMessages': BATCH_SIZE }

    return client.projects().subscriptions().pull(
        subscription=name, body=body).execute(num_retries=NUM_RETRIES)

def ack_messages(client, ack_ids, subscription):
    """Acknowledges a list of given messages"""
    name = get_full_subscription_name(PROJECT_ID,subscription)
    ack_body = {'ackIds': ack_ids}
    pubsub.projects().subscriptions().acknowledge(
        subscription=name, body=ack_body
        ).execute(num_retries=NUM_RETRIES)
    



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "ERROR: no message to send"
        exit()
    
    message = ' '.join(sys.argv[1:])  # skip name of file

    pubsub = create_service()
    
    create_topic(pubsub)
    print "Topic '{}' created".format(TOPIC_NAME)
    create_subscription(pubsub, TOPIC_NAME)
    print "Subscription '{}' created\n".format(SUBSCRIPTION_NAME)

    publish_message(pubsub, TOPIC_NAME, message)
    resp = pull_messages(pubsub, SUBSCRIPTION_NAME)
    print "Message sent"

    receivedMessages = resp.get('receivedMessages')
    if receivedMessages:
        ack_ids = []
        for receivedMessage in receivedMessages:
            message = receivedMessage.get('message')
            if message:
                print "echo: {}\n".format(base64.b64decode(message.get('data')))
                ack_ids.append(receivedMessage.get('ackId'))
        ack_messages(pubsub, ack_ids, SUBSCRIPTION_NAME)

    delete_subscription(pubsub, SUBSCRIPTION_NAME)
    print "Subscription removed"
    delete_topic(pubsub, TOPIC_NAME)
    print "Topic removed"
