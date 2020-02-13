from main import scrapeByTheHour
from db import TwitterLogDB
from datetime import date, timedelta
import os

# Used by Google Cloud storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "crawler-service-account.key.json"

##
# Logging DB
# Crawl task data goes here
##
logDB = TwitterLogDB(host='104.196.70.209', user='k8s', password='', db='twitter', tablename='twitter_log')

# Evaluate crawl period
endDate = logDB.earliestQuery()
if not endDate:
    # if there is no record of previous crawls
    endDate = date.today()
startDate = endDate - timedelta(days = 1)

context = 'vodafone'

# Mark starting time on logging DB
success, logID = logDB.startCrawl(startDate, endDate, context)

filename='twitter/crawl/'+str(startDate)+'.csv'
scrapeByTheHour(searchTerm=context, limit=None, start=startDate, end=endDate, filename=filename)

# Mark end time on logging DB
logDB.endCrawl(logID)