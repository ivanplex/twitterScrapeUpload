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

def scrapeByTheHour(searchTerm, start, end, filename, limit=None):
    print("Begin scanning for tweets related to "+ "vodafone"+ ": "+ str(dt.datetime.now()))
    list_of_tweets = query_tweets(searchTerm, limit=limit, begindate=start, enddate=end)
    print("Scanning complete: "+ str(dt.datetime.now()))
    df = pd.DataFrame(columns=['id', 'tweet_url', 'timestamp', 'timestamp_epochs', 'search_term', 'user_id', 'username', 'user_screen_name', 'body', 'body_html' ,'body_links', 'body_hashtags', 'body_lang', 'action_likes', 'action_retweets', 'action_replies', 'action_is_replied', 'action_is_reply_to', 'action_parent_tweet_id', 'action_reply_to_users'])

    for tweet in list_of_tweets:
        try:
            lang = detect(tweet.text)
            df = df.append(
                {
                'id':                       tweet.tweet_id,
                'tweet_url':                tweet.tweet_url,
                'timestamp':                tweet.timestamp,
                'timestamp_epochs':         tweet.timestamp_epochs,

                'search_term':              searchTerm,

                'user_id':                  tweet.user_id,
                'username':                 tweet.username,
                'user_screen_name':         tweet.screen_name,

                'body':                     tweet.text,
                'body_html':                tweet.text_html,
                'body_links':               tweet.links,
                'body_hashtags':            tweet.hashtags,

                'body_lang':                lang,

                'action_likes':             tweet.likes,
                'action_retweets':          tweet.retweets,
                'action_replies':           tweet.replies,
                'action_is_replied':        tweet.is_replied,
                'action_is_reply_to':       tweet.is_reply_to,
                'action_parent_tweet_id':   tweet.parent_tweet_id,
                'action_reply_to_users':    tweet.reply_to_users,
                },
                ignore_index=True
            )
        except:
            print("Error detecting lang: "+tweet.text)

    df.to_csv('/tmp/blob.csv')
    uploadToGCS('g09-datasets', '/tmp/blob.csv', filename)
    return len(df.index)