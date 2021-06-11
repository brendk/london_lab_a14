# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 01:05:20 2021

@author: brend
"""

from iLox.twitter_scraper import TwitterWebdriver, TweetsScraper
import pymongo
from tqdm import tqdm


class TweetsScraping():
    
    def __init__(self, parent):
        attributes = [attr for attr in dir(parent) if not attr.startswith("__")]
        for attribute in attributes:
            setattr(self, attribute, getattr(parent, attribute))
        self._parent = parent
        
    # Scrape tweets for all combinations of id and events keywords - hashtags
    def _scrape_tweets_hashtags(self):
        # Process all combinations of events keywords and id keywords
        for event_keyword in tqdm(self.tweets_scraping_params["events_keywords_hashtag"], 
                                  disable = self.ilox_logger.display_pb(), 
                                  desc = "Scraping Twitter hashtags", 
                                  leave = True):
            for id_keyword in self.tweets_scraping_params["id_keywords"]:
                # Initiate Tweets scraper for current id and event keywords
                tweetsScraper = TweetsScraper(self.mycol_tweets, 
                                              self.webdriver, 
                                              [id_keyword, event_keyword], 
                                              self.tweets_scraping_params["no_replies"], 
                                              True)
                # Scrape all tweets for those keywords
                tweetsScraper.scrape_many(self.tweets_scraping_params["max_pages"], 
                                          self.tweets_scraping_params["max_retry_page"], 
                                          self.tweets_scraping_params["max_retry_scroll"])
        
    # Scrape tweets for all combinations of id and events keywords - keywords
    def _scrape_tweets_text(self):
        # Process all combinations of events keywords and id keywords
        for event_keyword in tqdm(self.tweets_scraping_params["events_keywords_text"], 
                                  disable = self.ilox_logger.display_pb(), 
                                  desc = "Scraping Twitter keywords", 
                                  leave = True):
            for id_keyword in self.tweets_scraping_params["id_keywords"]:
                # Initiate Tweets scraper for current id and event keywords
                tweetsScraper = TweetsScraper(self.mycol_tweets, 
                                              self.webdriver, 
                                              [id_keyword, event_keyword], 
                                              self.tweets_scraping_params["no_replies"], 
                                              False)
                # Scrape all tweets for those keywords
                tweetsScraper.scrape_many(self.tweets_scraping_params["max_pages"], 
                                          self.tweets_scraping_params["max_retry_page"], 
                                          self.tweets_scraping_params["max_retry_scroll"])
        
    def _run(self):
        # If MongoDb collection is empty then create relevant indexes
        if self.mycol_tweets.count_documents({}) == 0:
            self.mycol_tweets.create_index(
                [("id", pymongo.ASCENDING)], name = "id", unique = True)
            self.mycol_tweets.create_index(
                [("user_id", pymongo.ASCENDING)], name = "user_id", unique = False)
        # Get webdriver
        print("Starting webdriver ...")
        self.webdriver = TwitterWebdriver(self.tweets_scraping_params["is_headless"], 
                                          self.tweets_scraping_params["no_gui"], 
                                          self.tweets_scraping_params["webdriver_path"])
        if self.tweets_scraping_params["hashtag_mode"]:
            self._scrape_tweets_hashtags()
        if self.tweets_scraping_params["keywords_mode"]:
            self._scrape_tweets_hashtags()
        # Close webdriver
        self.webdriver.quit()
                
                
                
                
                
                
                
                
                
                
                
                
                