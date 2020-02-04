from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv() # Load Service Account Key location

class BigQueryDatabase:

    def __init__(self, project_id, dataset_id, table_id):
        self.client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def doesTableExist(self, project_id, dataset_id, table_id):
        # If table exist
        try:
            bigquery.tables().get(
                projectId=project_id, 
                datasetId=dataset_id,
                tableId=table_id).execute()
            return True
        except HttpError, err
            if err.resp.status <> 404:
            raise
            return False

    def createTable(self):
        # table_path = "cogniflare-rd.g09_social_media.twitter"
        table_path = self.project_id + self.dataset_id + self.table_id

        schema = [
            bigquery.SchemaField("tweet_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("body", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("datetime", "DATETIME")
        ]

        table = bigquery.Table(table_path, schema=schema)
        table = self.client.create_table(table)  # Make an API request.

    


