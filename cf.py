from twitterscraper import query_tweets
from langdetect import detect
from google.cloud import storage
import pandas as pd
import datetime as dt
import csv


def scrapeByTheHour(request):
    print("Begin scanning for tweets related to "+ "vodafone"+ ": "+ str(dt.datetime.now()))
    list_of_tweets = query_tweets("vodafone", 1, begindate=dt.date(2020,2,4), enddate=dt.date(2020,2,5))
    print("Scanning complete: "+ str(dt.datetime.now()))
    df = pd.DataFrame(columns=['id', 'user_id', 'body', 'timestamp'])

    storage_client = storage.Client()
    bucket_name = "g09-datasets"
    bucket = storage.Bucket(bucket_name)

    for tweet in list_of_tweets:
        lang = detect(tweet.text)
        if lang == 'en':
            df = df.append(
                {
                'id':       tweet.tweet_id,
                'user_id':  tweet.user_id,
                'body':     tweet.text,
                'timestamp':tweet.timestamp,
                },
                ignore_index=True
            )

    df.to_csv('/tmp/blob.csv')
    blob = bucket.blob('twitter/crawl/2020_02_05.csv')
    blob.upload_from_filename('/tmp/blob.csv')