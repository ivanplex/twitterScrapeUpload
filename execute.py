from main import scrapeByTheHour
import os

# Used by Google Cloud storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "crawler-service-account.key.json"

scrapeByTheHour(limit=10)