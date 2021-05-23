# -*- coding: utf-8 -*-
"""
Created on Tue May 18 20:31:43 2021

@author: brend
"""

import pandas as pd
import pymongo
import warnings
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
import time
from tqdm import tqdm

# MongoDb Settings
mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col_Refineries = "Refineries"
mongoDB_Col_Tweets = "Tweets_clean"
mongoDB_Col_Headlines = "Headlines_filtered"
mongoDB_Col_GPEs = "GPEs"

refineries_file = "data/GeoAssets_Table.csv"
owners_file = "data/GeoAsset_Owner.csv"



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


# Prepare refineries data - Get geo data from Nominatim
def prep_ref_data(geolocator, refineries_file):
    ref_data = pd.read_csv(refineries_file)
    # Dataframe with only distinct coordinates
    coord_df = ref_data[["GeoAssetID", "Latitude", "Longitude"]].drop_duplicates()
    coord_items = []
    for item in tqdm(coord_df.to_dict(orient = "index").values()):
        location = geolocator.reverse("%r, %r" % (item["Latitude"], item["Longitude"]), language = "en")
        item = {**item, **location.raw["address"]}
        item["bounding_box"] = [float(i) for i in location.raw["boundingbox"]]
        coord_items.append(item)
    # Set fields missing for some items to NaN
    coord_items = pd.DataFrame(coord_items).to_dict(orient = "records")
    return coord_items
        
        
# Fill MongoDb refineries collection
def refineries_to_mongo(mycol_ref, coord_items):
    for coord_item in coord_items:
        coord_item["point"] = {"coordinates": [coord_item.pop("Longitude"), coord_item.pop("Latitude")], 
                               "type": "Point"}
        coord_item["bounding_box"] = {"coordinates": [[[coord_item["bounding_box"][2], coord_item["bounding_box"][1]], 
                                                       [coord_item["bounding_box"][3], coord_item["bounding_box"][1]], 
                                                       [coord_item["bounding_box"][3], coord_item["bounding_box"][0]],
                                                       [coord_item["bounding_box"][2], coord_item["bounding_box"][0]],
                                                       [coord_item["bounding_box"][2], coord_item["bounding_box"][1]]]], 
                                      "type": "Polygon"}
        mycol_ref.insert_one(coord_item)
    # Create indexes if not already exists
    existing_idx = mycol_ref.index_information()
    if "point_2dsphere" not in existing_idx.keys():
        mycol_ref.create_index([("point", pymongo.GEOSPHERE)], name = "point_2dsphere", unique = False)
    if "bounding_box_2dsphere" not in existing_idx.keys():
        mycol_ref.create_index([("bounding_box", pymongo.GEOSPHERE)], name = "bounding_box_2dsphere", unique = False)
        
        
# Find location data of all GPEs, cities, countries etc
def find_all_locations(geolocator, mycol_gpes, mycol_ref, mycol_tweets, mycol_head, refineries_file, coord_items):
    
    # Query location on Nominatim
    def query_nominatim(geolocator, gpe, max_attempts = 10, wait_error = 5, wait = 0.5):
        # Try max_attempts times
        for i in range(max_attempts):
            try:
                geo_data = geolocator.geocode(gpe, geometry = "geojson", language = "en")
                # Wait normal wait time per request
                time.sleep(wait)
                # If successful then return output
                return geo_data
            # Otherwise wait wait_error seconds
            except GeocoderUnavailable:
                time.sleep(wait_error)
        # If exit loop after trying max_attempts times then try request for Paris
        try:
            geo_data = geolocator.geocode("Paris", geometry = "geojson", language = "en")
            # If request is successful return None, will continue with next gpe
            return None
        # Otherwise raise error, means Nominatim API is down, break execution
        except GeocoderUnavailable:
            raise
    
    # Get all GPEs from headlines
    gpes = [x for x1 in [[i2["initial"] for i2 in i["geo_tags"] if i2["type"] == "GPE"] for i in mycol_head.find({"geo_tags.type":"GPE"})] for x in x1]
    # Add all GPEs from Tweets
    gpes.extend([x for x1 in [[i2["initial"] for i2 in i["geo_tags"] if i2["type"] == "GPE"] for i in mycol_tweets.find({"geo_tags.type":"GPE"})] for x in x1])
    gpes = [gpe for gpe in list(set(gpes)) if str(gpe) != "nan"]
    gpes_data = []
    for gpe in tqdm(gpes):
        try:
            geo_data = query_nominatim(geolocator, gpe)
        except GeocoderUnavailable:
            warnings.warn("Nominatim API is down")
            break
        if geo_data is not None:
            geo_data.raw["GPE"] = gpe
            gpes_data.append(geo_data.raw)
    # Create indexes if not already exists
    existing_idx = mycol_gpes.index_information()
    if "place_id" not in existing_idx.keys():
        mycol_gpes.create_index([("place_id", pymongo.ASCENDING)], name = "place_id", unique = True)
    # Group same places returned from different gpes (spelling)
    place_ids = list(set([i["place_id"] for i in gpes_data]))
    for place_id in place_ids:
        # All GPEs that returned this same place (different spellings)
        these_gpes = [i for i in gpes_data if i["place_id"] == place_id]
        # Aggregate different spellings under one location returned by Nominatim
        keep_keys = ["class", "display_name", "geojson", "importance", "lat", "lon", "osm_id", "osm_type", "place_id", "type"]
        # Drop results with missing location info
        these_gpes = [i for i in these_gpes if "geojson" in i.keys() and i["geojson"]["type"] not in ["Point", "LineString", "MultiLineString"]]
        if len(these_gpes) == 0:
            continue
        main_result = {k: these_gpes[0][k] for k in keep_keys}
        main_result["GPEs"] = list(set([i["GPE"] for i in these_gpes] + [main_result["display_name"].split(",")[0].strip()]))
        main_result["point"] = {"type": "Point", "coordinates": [float(main_result.pop("lon")), float(main_result.pop("lat"))]}
        main_result["boundaries"] = main_result.pop("geojson")
        # Insert in MongoDb
        try:
            mycol_gpes.insert_one(main_result)
        except pymongo.errors.DuplicateKeyError:
            continue
    # Create indexes if not already exists
    existing_idx = mycol_gpes.index_information()
    if "point_2dsphere" not in existing_idx.keys():
        mycol_gpes.create_index([("point", pymongo.GEOSPHERE)], name = "point_2dsphere", unique = False)
    if "boundaries_2dsphere" not in existing_idx.keys():
        mycol_gpes.create_index([("boundaries", pymongo.GEOSPHERE)], name = "boundaries_2dsphere", unique = False)
    if "GPEs_text" not in existing_idx.keys():
        mycol_gpes.create_index([("GPEs", pymongo.TEXT)], name = "GPEs_text", unique = False)


def main():
    # Connect to MongoDb collections
    mycol_ref = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Refineries)
    mycol_tweets = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Tweets)
    mycol_head = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Headlines)
    mycol_gpes = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_GPEs)
    # Connect with Nominatim
    geolocator = Nominatim(user_agent = "z#7xRKtSX86S$zRUG2h2")
    # Prepare refineries data - Get geo data from Nominatim
    coord_items = prep_ref_data(geolocator, refineries_file)
    # Fill MongoDb refineries collection
    refineries_to_mongo(mycol_ref, coord_items)
    # Find location data of all GPEs, cities, countries etc
    find_all_locations(geolocator, mycol_gpes, mycol_ref, mycol_tweets, mycol_head, refineries_file, coord_items)