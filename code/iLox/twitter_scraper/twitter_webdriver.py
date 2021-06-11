# -*- coding: utf-8 -*-
"""
Created on Mon Apr 12 23:34:41 2021

@author: brend
"""

from seleniumwire.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


class TwitterWebdriver(Chrome):
    
    def __init__(self, is_headless, no_gui, webdriver_path):
        
        seleniumwire_options = options = {
            "disable_encoding": True  # Tell Twitter not to compress the responses
            }
        
        options = Options()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        if is_headless:
            options.add_argument('--headless')
        if no_gui:
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
        
        super().__init__(executable_path = webdriver_path, 
                         seleniumwire_options = seleniumwire_options, 
                         chrome_options = options)