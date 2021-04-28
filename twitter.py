import tweepy
from tweepy import OAuthHandler
import csv
import json
import pandas as pd
from google.cloud import pubsub_v1


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('xxxxxxxxxxxxxxxxxxx', 'xxxxxxxxxxxxxxxxxxx')

USER_KEY = 'xxxxxxxxxxxxxxxxxxx'
USER_SECRET = 'xxxxxxxxxxxxxxxxxxx'
ACCESS_TOKEN = 'xxxxxxxxxxxxxxxxxxx'
ACCESS_SECRET = 'xxxxxxxxxxxxxxxxxxx'

auth = OAuthHandler(USER_KEY, USER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
tweets=[]
keyword = input('Enter the desired keyword: ')

def write_to_pubsub(data):
    data = data.encode("utf-8")
    future = publisher.publish(
        topic_path, data
    )

for tweet in tweepy.Cursor(api.search, q=keyword, lang="en").items(10000):
    try:
      x= tweet.text
      write_to_pubsub(x)

    except tweepy.TweepError as e:
      print(e.reason)
      continue

    except StopIteration:
      break
  
print('The data has been succesfully retrieved from twitter')