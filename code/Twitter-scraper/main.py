# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 22:34:34 2021

@author: brend
"""

from twitter_scraper import TwitterWebdriver, TweetsScraper
import pymongo
import warnings
from tqdm import tqdm

# MongoDb Settings
mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col = "Tweets2"

# Webdriver Settings
## Path of the chromedriver.exe file
webdriver_path = r"chromedriver_win32\chromedriver.exe"
## Headless mode means webdriver will not be visible
is_headless = False
## In no GUI mode (Linux), will disable extensions and some GPU components
no_gui = False

# Scraper Settings
## Maximum number of pages/views/scrolls on the initial results page
max_pages = 50
## Maximum number of attempts to reload page in case of error (eg limit exceeded)
max_retry_page = 3
## Maximum number of attempts to scroll down without receiving new Tweets before considering done
max_retry_scroll = 3
## Only search Tweets not replies
no_replies = True
## Search the keywords as hashtags (True) or text (False)
hashtag_mode = True


id_keywords = ["refining", "refinery", "refinery plant", "refineries"]

# For hashtag mode
events_keywords = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "runreduction", "run_reduction", 
                   "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacityreduction", 
                   "capacity_reduction", "expansion", "capacityexpansion", "capacity_expansion", "newrefinery", "new", 
                   "inauguration", "commissioning", "down", "runcuts", "run_cuts", "shutdown", "attack", "blaze", "smoke"]
# For text search mode
events_keywords = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "run reduction", 
                   "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacity reduction", 
                   "expansion", "capacity expansion", "new", "inauguration", "commissioning", "down", "run cuts", 
                   "shutdown", "attack", "blaze", "smoke"]


# Create network MongoDB connection
def createNetworkMongo(mongoDB_Host = "localhost", mongoDB_Db = "admin", mongoDB_Col = "", mongoDB_Port = 27017, mongoDB_Auth = False, mongoDB_Username = None, mongoDB_Password = None, noTest = False, retry_count = 10):
    from pymongo import MongoClient
    if mongoDB_Auth:
        while True:
            try:
                myclient = MongoClient(host = mongoDB_Host, port = mongoDB_Port, username = mongoDB_Username, 
                                       password = mongoDB_Password, authSource = mongoDB_Db, authMechanism = 'SCRAM-SHA-256')
                break
            except pymongo.errors.AutoReconnect:
                retry_count -= 1
            if retry_count <= 0: break
    else: 
        myclient = MongoClient(host = mongoDB_Host, port = mongoDB_Port)
    mydb = myclient[mongoDB_Db]
    mycol = mydb[mongoDB_Col]
    if not noTest:
        testConn = mycol.find({})
        try:
            testConn = testConn[0]
        except IndexError:
            warnings.warn("MongoDb connection failed - %s.%s @ %s" % (mongoDB_Db, mongoDB_Col, mongoDB_Host))
    return mycol


# Scrape tweets for single list of hashtags
def scrape_tweets():
    # Connect to MongoDb
    mycol = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col)
    # If MongoDb collection is empty then create relevant indexes
    if mycol.count_documents({}) == 0:
        mycol.create_index([("id", pymongo.ASCENDING)], name = "id", unique = True)
        mycol.create_index([("user_id", pymongo.ASCENDING)], name = "user_id", unique = False)
    # Get webdriver
    webdriver = TwitterWebdriver(is_headless, no_gui, webdriver_path)
    # Initiate Tweets scraper with selected keywords
    tweetsScraper = TweetsScraper(mycol, webdriver, ["refineryplant", "fire"], no_replies, hashtag_mode)
    # Scrape all tweets for those keywords
    tweetsScraper.scrape_many(max_pages, max_retry_page, max_retry_scroll)
    
    
# Scrape tweets for all combinations of id_hashtags and events_keywords
def scrape_tweets_multiple(id_keywords, events_keywords):
    # Connect to MongoDb
    print("Connecting to MongoDb ...")
    mycol = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col, noTest = True)
    # If MongoDb collection is empty then create relevant indexes
    if mycol.count_documents({}) == 0:
        mycol.create_index([("id", pymongo.ASCENDING)], name = "id", unique = True)
        mycol.create_index([("user_id", pymongo.ASCENDING)], name = "user_id", unique = False)
    # Get webdriver
    print("Starting webdriver ...")
    webdriver = TwitterWebdriver(is_headless, no_gui, webdriver_path)
    print("Scraping ...")
    # Process all combinations of events keywords and id keywords
    with tqdm(total = len(events_keywords), miniters = 1) as pbar:
        for event_keyword in events_keywords:
            for id_keyword in id_keywords:
                 # Initiate Tweets scraper for current id and event keywords
                tweetsScraper = TweetsScraper(mycol, webdriver, [id_keyword, event_keyword], 
                                              no_replies, hashtag_mode)
                # Scrape all tweets for those keywords
                tweetsScraper.scrape_many(max_pages, max_retry_page, max_retry_scroll)
            pbar.update(1)
    print("Done")
    
    
if __name__ == '__main__':
    scrape_tweets_multiple(id_keywords, events_keywords)
