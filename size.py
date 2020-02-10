from twitterscraper import query_tweets
from sys import getsizeof
import datetime as dt

list_of_tweets = query_tweets("vodafone", limit=None, begindate=dt.date(2020,2,4), enddate=dt.date(2020,2,5))
print(getsizeof(list_of_tweets))