from main import scrapeByTheHour
from datetime import date, timedelta
import os

# Used by Google Cloud storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "crawler-service-account.key.json"

today = date.today()
yesterday = today - timedelta(days = 1)
filename='twitter/crawl/'+str(yesterday)+'.csv'
scrapeByTheHour(searchTerm='vodafone', limit=10, start=yesterday, end=today, filename=filename)