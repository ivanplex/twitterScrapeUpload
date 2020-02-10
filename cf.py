from twitterscraper import query_tweets
from langdetect import detect
from google.cloud import storage
import pandas as pd
import datetime as dt
import csv


def uploadToGCS(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the bucket.
    
    Taken from example:
    https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-code-sample
    """
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    print("Preparing to upload.")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def scrapeByTheHour(request):
    print("Begin scanning for tweets related to "+ "vodafone"+ ": "+ str(dt.datetime.now()))
    list_of_tweets = query_tweets("vodafone", 10, begindate=dt.date(2020,2,4), enddate=dt.date(2020,2,5))
    print("Scanning complete: "+ str(dt.datetime.now()))
    df = pd.DataFrame(columns=['id', 'user_id', 'body', 'timestamp'])

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
    uploadToGCS('g09-datasets', '/tmp/blob.csv', 'twitter/crawl/2020_02_04.csv')