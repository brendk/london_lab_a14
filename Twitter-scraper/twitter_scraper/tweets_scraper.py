# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 23:34:41 2021

@author: brend
"""

import urllib
import json
import pymongo
import time
import random


class TweetsScraper():
    
    def __init__(self, mycol, webdriver, keywords, no_replies = True, hashtag_mode = True):
        
        """
        Scrape Tweets by scrolling down pages
        Args:
            * mycol (pymongo.Collection): MongoDb connector to save the Tweets
            * webdriver (TwitterWebdriver): To browse Twitter and scroll
            * keywords (list): List of keywords to search
            * max_pages (int): Maximum number of pages/views/scrolls on the initial results page
            * max_retry_page (int): Maximum number of attempts to reload page in case of error (eg limit exceeded)
            * max_retry_scroll (int): Maximum number of attempts to scroll down without receiving new Tweets before considering done
            * no_replies (bool): Only search Tweets not replies
            * hashtag_mode (bool): Search for hashtags of the supplied keywords
        """
        
        base_url = "https://twitter.com/search?{}"
        if hashtag_mode:
            params = {"q": "(" + " AND ".join(["#" + str(i) for i in keywords]) + ")",
                      "src": "typeahead_click"}
        else:
            params = {"q": "(" + " AND ".join([str(i) for i in keywords]) + ")",
                      "src": "typeahead_click"}
        if no_replies:
            params["q"] += " -filter:replies"
            
        self.url = base_url.format(urllib.parse.urlencode(params))
        self.webdriver = webdriver
        self.mycol = mycol
        
    # Send multiple MongoDb operations and execute in bulk, skip errors
    def _bulk_write(self, data):
        if len(data) == 0:
            return {}
        bulk = pymongo.bulk.BulkOperationBuilder(self.mycol, ordered = False)
        for item in data.copy():
            bulk.insert(item)
        try:
            response = bulk.execute()
        except pymongo.errors.BulkWriteError:
            response = {}
        return response
        
    # Extract Tweets from response and save to MongoDb
    def _process_requests(self):
        self.status = "ok"
        # Extract requests
        requests = [r for r in self.webdriver.requests if "adaptive" in r.url]
        # Clear requests cache
        del self.webdriver.requests
        self.len_requests = len(requests)
        # If no new request made by Twitter, means no more tweets pages
        if self.len_requests == 0:
            self.status = "no_requests"
            return
        self.requests_tweets = []
        for r in requests:
            # If fails to convert body to JSON, retry
            try:
                these_tweets = json.loads(r.response.body.decode("utf-8"))
            except:
                self.status = "json_convert_failed"
                return
            # Means triggered error (eg rate limit exceeded), clean cookies etc and retry
            if "globalObjects" not in these_tweets.keys():
                self.status = "retry_clean"
                return
            these_tweets = list(these_tweets["globalObjects"]["tweets"].values())
            self.requests_tweets.append(these_tweets)
            status = self._bulk_write(these_tweets)
            del status
        self.status = "ok"
        
    # Scroll page max_pages times, from webdriver currently top of page
    # max_pages = number of pages/views to scrape
    # max_retry_scroll = max number of attempted scrolls yielding 0 tweets
    def _scrape_pages(self, max_pages, max_retry_scroll):
        # True when complete, no more Tweet pages (or too many scroll errors)
        self.complete = False
        # To reload the whole page
        self.reload_page = False
        # To clean cookies etc and reload the whole page
        self.reload_clean_page = False
        # Loops through tweets pages until max_pages or max_retry_scroll with 0 tweets
        for pages_count in range(max_pages):
            self._process_requests()
            # If no requests made then retry scrolling at next iteration and deduct 1 from max_retry_scroll
            if self.status == "no_requests":
                max_retry_scroll -= 1
                # If max_retry_scroll exceeded then exit
                if max_retry_scroll == 0:
                    self.complete = True
                    return
            # If fails to convert body to JSON, reload page
            elif self.status == "json_convert_failed":
                self.reload_page = True
                return
            # If triggered error, clean cookies etc and retry
            elif self.status == "retry_clean":
                self.reload_clean_page = True
                return
            # Otherwise wait random time and scroll down
            time.sleep(random.randint(2, 4))
            self.webdriver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    
    # Load initial page and scroll max_pages pages of Tweets for specified criteria
    # max_pages = number of pages/views to scrape
    # max_retry_scroll = max number of attempted scrolls yielding 0 tweets
    # max_retry_page = max number of retry to load the initial page
    def scrape_many(self, max_pages, max_retry_page, max_retry_scroll):
        # +1 to include initial page load
        for iter_count in range(max_retry_page + 1):
            self.webdriver.get(self.url)
            self._scrape_pages(max_pages, max_retry_scroll)
            # If complete then break
            if self.complete:
                break
            # Reload page
            if self.reload_page:
                self.webdriver.refresh()
            # Delete all cookies, wait additional time then refresh
            if self.reload_clean_page:
                self.webdriver.delete_all_cookies()
                time.sleep(random.randint(5, 10))
                self.webdriver.refresh()
            # Otherwise wait random time and retry
            time.sleep(random.randint(2, 6))