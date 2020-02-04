from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv() # Load Service Account Key location

client = bigquery.Client()

table_id = "cogniflare-rd.g09_social_media.twitter"

schema = [
    bigquery.SchemaField("tweet_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("body", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("datetime", "DATETIME")
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.