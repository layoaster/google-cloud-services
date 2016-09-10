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

from gcloud import pubsub

PROJECT_ID = "<YOUR-PROJECT-ID>"
TOPIC_NAME = "echo-topic"
SUBSCRIPTION_NAME= "echo-subscription"
NUM_RETRIES = 3
BATCH_SIZE = 3


def create_topic(client, topicName):
    """Creates a new topic"""
    topic = client.topic(topicName)
    if not topic.exists():
        topic.create()
    return topic

def delete_topic(topic):
    """Deletes a given topic"""
    if topic.exists():
        topic.delete()  

def create_subscription(topic, subscriptionName):
    """Creates a new pull subscription to a given topic"""
    subscription = topic.subscription(subscriptionName)
    if not subscription.exists():
        subscription.create()
    return subscription

def delete_subscription(subscription):
    """Deletes a given subscription"""
    if subscription.exists():
        subscription.delete()

def publish_message(topic, message):
    """Publish a message to a given topic"""
    topic.publish(bytes(message))

def pull_messages(subscription):
    """Pull messages from a given subscription."""
    return subscription.pull(return_immediately=False, 
        max_messages=BATCH_SIZE)

def ack_messages(subscription, ack_ids):
    """Acknowledges a list of given messages"""
    subscription.acknowledge(ack_ids)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "ERROR: no message to send"
        exit()
    
    message = ' '.join(sys.argv[1:])  # skip name of file

    client = pubsub.Client(project=PROJECT_ID)
    
    topic = create_topic(client, TOPIC_NAME)
    print "Topic '{}' created".format(TOPIC_NAME)

    subscription = create_subscription(topic, SUBSCRIPTION_NAME)
    print "Subscription '{}' created\n".format(SUBSCRIPTION_NAME)

    publish_message(topic, message)
    print "Message sent"

    receivedMessages = pull_messages(subscription)
    if receivedMessages:
        ack_ids = []
        for ack_id, message in receivedMessages:
            print "echo: {}\n".format(message.data)
            ack_ids.append(ack_id)
        ack_messages(subscription, ack_ids)

    delete_subscription(subscription)
    print "Subscription removed"

    delete_topic(topic)
    print "Topic removed"
