# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 19:40:09 2021

@author: brend
"""

import os
import pymongo
import pandas as pd
import numpy as np
import warnings
import re
from tqdm import tqdm
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
from area import area
import time
import spacy


refineries_file = "data/GeoAssets_Table.csv"
owners_file = "data/GeoAsset_Owner.csv"
norps_gpes_file = "data/NORPs_to_GPEs.xlsx"

mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col_Refineries = "Refineries"
mongoDB_Col_GPEs = "GPEs"
mongoDB_Col_Headlines = "Headlines_filtered"

events_keywords_h = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "runreduction", "run_reduction", 
                     "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacityreduction", 
                     "capacity_reduction", "expansion", "capacityexpansion", "capacity_expansion", "newrefinery", 
                     "inauguration", "commissioning", "down", "runcuts", "run_cuts", "shutdown", "attack", "blaze", "smoke"]

events_keywords_t = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "run reduction", 
                     "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacity reduction", 
                     "expansion", "capacity expansion", "inauguration", "commissioning", "down", "run cuts", 
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
    mycol_headlines = mydb[mongoDB_Col]
    if not noTest:
        testConn = mycol_headlines.find({})
        try:
            testConn = testConn[0]
        except IndexError:
            warnings.warn("MongoDb connection failed - %s.%s @ %s" % (mongoDB_Db, mongoDB_Col, mongoDB_Host))
    return mycol_headlines


# Get list of potential refineries names
def get_geo_names_refineries(refineries_file, mycol_refineries, force_rerun = False):
    if not force_rerun and "geo_names_refineries.csv" in os.listdir("data"):
        geo_names_r = pd.read_csv("data/geo_names_refineries.csv").to_dict(orient = "records")
        return geo_names_r
    names = pd.read_csv(refineries_file)[["GeoAssetID", "GeoAssetName"]].drop_duplicates(subset = ["GeoAssetName"])
    geo_names_r_original = names.to_dict(orient = "records")
    geo_names_r = []
    for geo_name in tqdm(geo_names_r_original):
        # Store original name and id
        geo_name_orig = geo_name["GeoAssetName"]
        name_id = geo_name["GeoAssetID"]
        # Remove text between parentheses (and parentheses)
        geo_name = re.sub("[\(].*?[\)]", "", geo_name["GeoAssetName"]).strip()
        # Convert to list of words (lowercase)
        words = geo_name.lower().split(" ")
        # Remove "refin*" and "|" + "(" and ")"
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in words if not w.startswith("refin")]
        # Recreate sentence
        geo_name = " ".join(words).strip()
        geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": geo_name, "type": "refname"})
        # If 3 words on less, also add 2 first words merged + all 3 merged (e.g. CarsonLA)
        if len(words) <= 3:
            geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": "".join(words[:3]), "type": "refname"})
            geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": "".join(words[:2]), "type": "refname"})
    # Convert geo_names_r to dataframe to drop duplicates
    geo_names_r_df = pd.DataFrame(geo_names_r).drop_duplicates(subset = ["initial", "match"])
    # Keep only not empty names
    geo_names_r_df = geo_names_r_df[geo_names_r_df["match"] != ""]
    # Save as CSV file to avoid rerun
    geo_names_r_df.to_csv("data/geo_names_refineries.csv", index = False)
    # Return as list
    geo_names_r = geo_names_r_df.to_dict(orient = "records")
    # Update MongoDb collection
    mycol_refineries.update_many({}, {"$set": {"refnames": []}})
    for geo_name in geo_names_r:
        mycol_refineries.update_one({"GeoAssetID": geo_name["id"]}, 
                                    {"$push": {"refnames": geo_name["match"]}})
    return geo_names_r


# Get list of potential owners names
def get_owners_names(owners_file, force_rerun = False):
    if not force_rerun and "owners_names.csv" in os.listdir("data"):
        owners_names = pd.read_csv("data/owners_names.csv").to_dict(orient = "records")
        return owners_names
    names = pd.read_csv(owners_file)[["GeoAssetID", "CompanyName"]].drop_duplicates()
    names = names.replace("Unknown", np.NaN).dropna().to_dict(orient = "records")
    owners_names = []
    for owner_name in tqdm(names):
        # Store original name and id
        name_orig = owner_name["CompanyName"]
        name_id = owner_name["GeoAssetID"]
        # Remove text between parentheses (and parentheses)
        owner_name_2 = re.sub("[\(].*?[\)]", "", owner_name["CompanyName"]).strip()
        # Convert to list of words (lowercase)
        words = owner_name_2.lower().split(" ")
        # Remove "|" + "(" and ")"
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in words]
        # Recreate sentence
        owner_name_2 = " ".join(words).strip()
        owners_names.append({"id": name_id, "initial": name_orig, "match": owner_name_2, "type": "ownername"})
        # If 3 words on less, also add 2 first words merged + all 3 merged (e.g. CarsonLA)
        if len(words) <= 3:
            owners_names.append({"id": name_id, "initial": name_orig, "match": "".join(words[:3]), "type": "ownername"})
            owners_names.append({"id": name_id, "initial": name_orig, "match": "".join(words[:2]), "type": "ownername"})
    # Convert geo_names_r to dataframe to drop duplicates
    owners_names_df = pd.DataFrame(owners_names).drop_duplicates(subset = ["id", "match"])
    # Keep only not empty names
    owners_names_df = owners_names_df[owners_names_df["match"] != ""]
    # Save as CSV file to avoid rerun
    owners_names_df.to_csv("data/owners_names.csv", index = False)
    # Return as list
    owners_names = owners_names_df.to_dict(orient = "records")
    return owners_names


# Get list of potential refineries names
def get_geo_names_cities(refineries_file, force_rerun = False):
    if not force_rerun and "geo_names_cities.csv" in os.listdir("data"):
        cities_names = pd.read_csv("data/geo_names_cities.csv").to_dict(orient = "records")
        return cities_names
    cities = pd.read_csv(refineries_file)[["GeoAssetID", "City"]].drop_duplicates().dropna()
    cities_names_original = cities.to_dict(orient = "records")
    cities_names = []
    for city_name in tqdm(cities_names_original):
        # Store original name and id
        city_name_orig = city_name["City"]
        name_id = city_name["GeoAssetID"]
        # If name contains "-", create new one (keep existing) without "-"
        if "-" in city_name["City"]:
            city_name_2 = " ".join(city_name["City"].replace("-", " ").strip().split())
            words = [w.strip().replace("|", "").replace("(", "").replace(")", "").replace(",", "") for w in city_name_2.split(" ")]
            if len(words) <= 3:
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": city_name_2, "type": "cityname"})
        # If name contains ",", create new ones (keep existing) by splitting on ","
        if "," in city_name["City"]:
            for sub_city_name in city_name["City"].split(","):
                sub_city_name_2 = sub_city_name.strip()
                words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in sub_city_name_2.split(" ")]
                if len(words) <= 3:
                    cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
                    cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": sub_city_name_2, "type": "cityname"})
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "").replace(",", "") for w in city_name["City"].split(" ")]
        if len(words) <= 3:
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
        cities_names.append({"id": name_id, "initial": city_name_orig, "match": city_name["City"], "type": "cityname"})
    # Convert geo_names_r to dataframe to drop duplicates
    cities_names_df = pd.DataFrame(cities_names).drop_duplicates(subset = ["id", "match"])
    # Keep only names with positive length (not empty) and drop duplicates
    cities_names_df = cities_names_df[-cities_names_df["match"].isin(["nan", ""])]
    # Save as CSV file to avoid rerun
    cities_names_df.to_csv("data/geo_names_cities.csv", index = False)
    # Return as list
    cities_names = cities_names_df.to_dict(orient = "records")
    return cities_names


# Extract GPEs and NORPs using Spacy
def extract_spacy(text, spacy_models, norps_gpes, snippet = None):
    spacy_results = []
    for spacy_model in spacy_models:
        [spacy_results.append((str(i), i.label_)) for i in spacy_model(text.replace("#", "")).ents if i.label_ in ["GPE", "NORP"]]
        if snippet is not None:
            [spacy_results.append((str(i), i.label_)) for i in spacy_model(snippet.replace("#", "")).ents if i.label_ in ["GPE", "NORP"]]
    spacy_results = pd.DataFrame(spacy_results, columns = ["text", "label"])
    # List of texts flagged both as GPEs and NORPs, keep only GPEs
    flag_both = [i for i in spacy_results["text"].unique() if len(spacy_results[spacy_results["text"] == i].drop_duplicates()) > 1]
    spacy_results.drop(spacy_results[(spacy_results["text"].isin(flag_both)) & (spacy_results["label"] == "NORP")].index, inplace = True)
    spacy_results["text"] = spacy_results.apply(lambda x: norps_gpes[x["text"]] if x["text"] in norps_gpes.keys() and x["label"] == "NORP" else x["text"], axis = 1)
    return list(set(spacy_results["text"].tolist()))


# Look for location names in headlines
def geotag_headlines(mycol_headlines, norps_gpes_file, spacy_models):
    # Read NORPs to GPEs conversion file
    norps_gpes = pd.read_excel(norps_gpes_file).set_index("NORP")["GPE"].to_dict()
    all_headlines = [i for i in mycol_headlines.find({})]
    for this_headline in tqdm(all_headlines):
        # Use spacy on headline's text and snippet (replacing hashtags)
        these_locations = extract_spacy(this_headline["text"], spacy_models, norps_gpes, 
                                        snippet = this_headline["snippet"])
        these_locations = [{"id": None, "initial": i, "match": i, "type": "GPE"} for i in these_locations]
        for this_location in these_locations:
            mycol_headlines.update_one({"_id": this_headline["_id"]}, {"$push": {"geo_tags": this_location}})
    return


# Geotag headlines from refineries names
def geotag_headlines_refineries(mycol_headlines, geo_names_r):
    for geo_name in tqdm(geo_names_r):
        # Get ids of headlines containing current geoname in text
        these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        # Also get ids of headlines containing current geoname in snippet
        these_ids = [i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        # Also perform text match using index (accents insensitive, include text + snippet)
        these_ids.extend([i["_id"] for i in mycol_headlines.find({"$text": {"$search": "\"" + geo_name["match"] + "\""}}, {"_id": 1})])
        # Add current geo_name to matching headline's geo_tags field
        if len(these_ids) > 0:
            mycol_headlines.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
    return
        

# Geotag headlines from cities names
def geotag_headlines_cities(mycol_headlines, cities_names):
    for geo_name in tqdm(cities_names):
        # Get ids of headlines containing current geoname
        # Only look for whole word if len of geo_name is less than 8 (e.g. to get RichmondCA)
        if len(geo_name) < 8:
            these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
            these_ids.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})])
        else:
            these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": geo_name["match"], "$options": "i"}}, {"_id": 1})]
            these_ids.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": geo_name["match"], "$options": "i"}}, {"_id": 1})])
        # Also perform text match using index (accents insensitive)
        these_ids.extend([i["_id"] for i in mycol_headlines.find({"$text": {"$search": "\"" + geo_name["match"] + "\""}}, {"_id": 1})])
        # Add current geo_name to matching headline's geo_tags field
        if len(these_ids) > 0:
            mycol_headlines.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
    return


# Match headlines with owners names
def match_headlines_owners(mycol_headlines, owners_names):
    for owner_name in tqdm(owners_names):
        # Get ids of headlines containing current owner_name
        # Only look for whole word if len of owner_name is less than 8 (e.g. to get RichmondCA)
        if len(owner_name["match"]) < 8:
            # For total, search case sensitive for Total and TOTAL
            if owner_name["initial"] == "Total":
                these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": "\\b" + owner_name["initial"] + "\\b"}}, {"_id": 1})]
                these_ids.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": "\\b" + owner_name["initial"].upper() + "\\b"}}, {"_id": 1})])
            else:
                these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
                these_ids.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})])
        else:
            these_ids = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": owner_name["match"], "$options": "i"}}, {"_id": 1})]
            these_ids.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": owner_name["match"], "$options": "i"}}, {"_id": 1})])
        # Also perform text match using index (accents insensitive) only if owner_name sufficiently long as cannot do whole word for index search
        if len(owner_name["match"]) > 10:
            these_ids.extend([i["_id"] for i in mycol_headlines.find({"$text": {"$search": "\"" + owner_name["match"] + "\""}}, {"_id": 1})])
        # Add current owner_name to matching headline's owner_tags field
        if len(these_ids) > 0:
            mycol_headlines.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"owner_tags": owner_name}})
    # Clean matches to keep only long name if short name overlap, e.g. Total and Saudi Aramco Total Refining and Petrochemical Company
    # And also drop duplicates on id
    headlines_match = [i for i in list(mycol_headlines.find({"owner_tags":{"$not": {"$size": 0}}}, {"owner_tags": 1})) if len(i["owner_tags"]) > 1]
    for headline in headlines_match:
        this_df = pd.DataFrame(headline["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        if len(this_df["initial"].unique()) > 1:
            overlaps = [i for i in this_df["match"] if len([i2 for i2 in this_df["match"] if len(i2) > len(i) and i.lower() in i2.lower()]) > 0]
            this_df.drop(this_df[this_df["match"].isin(overlaps)].index, inplace = True)
        mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"owner_tags": this_df.to_dict(orient = "records")}})
    return


# Prepare MongoDb collection (add geo_tags key)
def prep_col(mycol_headlines):
    # Create indexes if not already exist
    existing_idx = mycol_headlines.index_information()
    if "text" not in existing_idx.keys():
        mycol_headlines.create_index([("text", pymongo.ASCENDING)], name = "text", unique = False)
    if "snippet" not in existing_idx.keys():
        mycol_headlines.create_index([("snippet", pymongo.ASCENDING)], name = "snippet", unique = False)
    if "text_snippet_text" not in existing_idx.keys():
        mycol_headlines.create_index([("text", pymongo.TEXT), ("snippet", pymongo.TEXT)], name = "text_snippet_text", 
                           unique = False, default_language = "en", language_override = "en")
    mycol_headlines.update_many({}, {"$set": {"geo_tags": []}})
    mycol_headlines.update_many({}, {"$set": {"owner_tags": []}})
    mycol_headlines.update_many({}, {"$set": {"ref_match": []}})
    mycol_headlines.update_many({}, {"$set": {"country_match": []}})
    mycol_headlines.update_many({}, {"$set": {"events_tags": []}})
    
    
# Get probabilities of match for refineries
def get_match_proba(mycol_headlines, mycol_refineries, mycol_gpes, geolocator, refineries_file):
    
    # Query location on Nominatim
    def query_nominatim(geolocator, gpe, max_attempts = 10, wait_error = 5, wait = 0.5, exactly_one = True):
        # Try max_attempts times
        for i in range(max_attempts):
            try:
                geo_data = geolocator.geocode(gpe, geometry = "geojson", language = "en", exactly_one = exactly_one)
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
    
    # Get polygons for GPEs
    def get_polygons(mycol_gpes, geolocator, gpes):
        polygons = []
        for gpe in gpes:
            # If GPE already exists in GPEs database then load from there and save on Nominatim queries
            if mycol_gpes.count_documents({"GPEs": gpe}) > 0:
                polygons.extend([i["boundaries"] for i in list(mycol_gpes.find({"GPEs": gpe}, {"boundaries": 1, "_id": 0}).sort([("importance", -1)]))])
            # Otherwise query from Nominatim then save in GPEs database
            else:
                try:
                    geo_data = query_nominatim(geolocator, gpe, exactly_one = False)
                except GeocoderUnavailable:
                    warnings.warn("Nominatim API is down")
                    break
                if geo_data is not None:
                    geo_data = [i for i in geo_data if "geojson" in i.raw.keys() and i.raw["geojson"]["type"] not in ["Point", "LineString", "MultiLineString"]]
                    for this_geo_data in geo_data:
                        this_geo_data.raw["GPEs"] = list(set([gpe, this_geo_data.raw["display_name"].split(",")[0].strip()]))
                        # If this location (from boundaries) already in database, add GPE to GPEs of that location
                        if mycol_gpes.count_documents({"boundaries": this_geo_data.raw["geojson"]}) > 0:
                            mycol_gpes.update_one({"boundaries": this_geo_data.raw["geojson"]}, {"$push": {"GPEs": gpe}})
                            polygons.append(this_geo_data.raw["geojson"])
                        # Else format to save in database
                        else:
                            keep_keys = ["class", "display_name", "geojson", "importance", "lat", "lon", "osm_id", "osm_type", 
                                         "place_id", "type", "GPEs"]
                            this_geo_data = {k: this_geo_data.raw[k] for k in keep_keys if k in this_geo_data.raw.keys()}
                            this_geo_data["point"] = {"type": "Point", "coordinates": [float(this_geo_data.pop("lon")), float(this_geo_data.pop("lat"))]}
                            this_geo_data["boundaries"] = this_geo_data.pop("geojson")
                            # Insert in MongoDb
                            try:
                                mycol_gpes.insert_one(this_geo_data)
                            except pymongo.errors.DuplicateKeyError:
                                pass
                            polygons.append(this_geo_data["boundaries"])
                else:
                    # If no result then continue
                    continue
        return polygons
    
    # Use GPEs to match refineries, potentially with cityname, refname or ownername
    def match_gpes(mycol_gpes, mycol_refineries, refineries_df, geolocator, headline_date, gpes, geo_tags, owner_tags):
        global polygons
        # Get polygons
        polygons = get_polygons(mycol_gpes, geolocator, gpes["match"].tolist())
        # Compute area in sqm to start with largest
        areas = pd.DataFrame({"area": {index: area(polygons[index]) for index in range(len(polygons))}}).sort_values("area", ascending = False)
        polygons = {index: polygons[index] for index in range(len(polygons))}
        all_matchs = []
        
        # If only GPE
        if len(geo_tags[geo_tags["type"] == "refname"]) == 0 and len(owner_tags) == 0:
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}}, {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                                 (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If refnames then first priority
        if len(geo_tags[geo_tags["type"] == "refname"]) > 0:
            ref_ids_initial = geo_tags[geo_tags["type"] == "refname"]["id"].astype(int).tolist()
            refineries_match = refineries_df[
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & (refineries_df["FromDate"] <= headline_date) & (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, "GeoAssetID": {"$in": ref_ids_initial}}, {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                                 (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If ownernames then second priority
        if len(owner_tags) > 0:
            ref_ids_initial = owner_tags["id"].astype(int).tolist()
            refineries_match = refineries_df[
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & (refineries_df["FromDate"] <= headline_date) & (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, "GeoAssetID": {"$in": ref_ids_initial}}, {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                                 (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If refnames and ownernames then use both and take intersection
        if len(geo_tags[geo_tags["type"] == "refname"]) > 0 and len(owner_tags) > 0:
            ref_ids_initial_refnames = geo_tags[geo_tags["type"] == "refname"]["id"].astype(int).tolist()
            ref_ids_initial_ownames = owner_tags["id"].astype(int).tolist()
            ref_ids_initial = set(ref_ids_initial_ownames).intersection(ref_ids_initial_refnames)
            refineries_match = refineries_df[
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & (refineries_df["FromDate"] <= headline_date) & (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, "GeoAssetID": {"$in": ref_ids_initial}}, {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                                 (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If no exact match found then return match with less refineries
        best_match = [i for i in all_matchs if len(i) == min([len(i2) for i2 in all_matchs])]
        if len(best_match) > 0:
            best_match = best_match[0]
        return best_match
    
    # Test
    headlines_multi = [h for h in mycol_headlines.find(
        {"geo_tags": {"$not": {"$size": 0}}}, {"geo_tags": 1, "owner_tags": 1, "firstCreated": 1}) 
        if len([i for i in h["geo_tags"] if i["type"]=="refname"])>1 and len(
                [i for i in h["geo_tags"] if i["type"] == "GPE"]) > 0 and len(h["owner_tags"]) > 0]
    
    headlines_multi = [h for h in mycol_headlines.find(
        {"ref_match": {"$not": {"$size": 0}}}, {"geo_tags": 1, "owner_tags": 1, "ref_match": 1, "text": 1}) 
        if len(h["ref_match"]) > 1]
        
    
    # Add priority to after " - "
    # Consider only headline first then snippet if not found
    
    # Load both files
    refineries_df = pd.read_csv(refineries_file)[["GeoAssetID", "GeoAssetName", "City", "FromDate", "ToDate"]]
    refineries_df["FromDate"] = pd.to_datetime(refineries_df["FromDate"])
    # When FromDate is NaT means from infinite
    refineries_df["FromDate"] = refineries_df["FromDate"].fillna(pd.to_datetime("1800-01-01"))
    # When ToDate is NaT means to today
    refineries_df["ToDate"] = pd.to_datetime(refineries_df["ToDate"])
    refineries_df["ToDate"] = refineries_df["ToDate"].fillna(pd.to_datetime(datetime.utcnow().date()))
    # Get all headlines with at least 1 geo_tag
    match_headlines = [i for i in mycol_headlines.find({"geo_tags": {"$not": {"$size": 0}}, "ref_match": {"$size": 0}}, 
                                             {"geo_tags": 1, "owner_tags": 1, "firstCreated": 1})]
    # Loop through each headline
    for headline in tqdm(match_headlines):
        headline_date = datetime.strptime(headline["firstCreated"], "%Y-%m-%dT%H:%M:%S.%fZ")
        geo_tags = pd.DataFrame(headline["geo_tags"])
        gpes = geo_tags[geo_tags["type"].isin(["GPE"])]
        geo_tags = geo_tags[geo_tags["type"].isin(["refname", "cityname"])].drop_duplicates(subset = ["id", "type"])
        refnames = geo_tags[geo_tags["type"] == "refname"]
        citynames = geo_tags[geo_tags["type"] == "cityname"]
        owner_tags = pd.DataFrame(headline["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        all_matchs = []
        
        # First look at refineries names refname
        ref_ids = refnames["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the headline
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                         (refineries_df["ToDate"] >= headline_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        elif len(refineries_match) > 1:
            all_matchs.append(refineries_match)
        # Otherwise look at city - keep only ids that appear twice (must match for refname + city)
        ref_ids.extend(citynames["id"].astype(int).tolist())
        ref_ids = list(set([r for r in ref_ids if ref_ids.count(r) == len(geo_tags["type"].unique())]))
        # Get list of refineries with selected ids and active at the time of the headline
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                         (refineries_df["ToDate"] >= headline_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Else if > 1 match and no owner information or GPE then save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1 and len(owner_tags) == 0 and len(gpes) == 0:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Else if > 1 match and owner information or GPE then add matchs found to all_matchs list
        elif len(refineries_match) > 1:
            all_matchs.append(refineries_match)
        # If owner then look at owner
        if len(owner_tags) > 0:
            # If found some ref_ids so far, take intersection to have match both existing ref_ids + those from ownername
            if len(ref_ids) > 0:
                ref_ids = set(ref_ids).intersection(owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist())
            # Otherwise only take ref_ids from ownername
            else:
                ref_ids = owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist()
            # Get list of refineries with selected ids and active at the time of the headline
            refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                             (refineries_df["ToDate"] >= headline_date)]
            # If only 1 match then save to collection with proba = 100% and continue
            if len(refineries_match) == 1:
                match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
                match_dict["confidence"] = 1.0
                mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
                continue
            # Else if > 1 match then add matchs found to all_matchs list
            elif len(refineries_match) > 1:
                all_matchs.append(refineries_match)
        # Else if GPE information then use it
        if len(gpes) > 0:
            refineries_match = match_gpes(mycol_gpes, mycol_refineries, refineries_df, geolocator, headline_date, gpes, geo_tags, owner_tags)
            # If only 1 match then save to collection with proba = 100% and continue
            if len(refineries_match) == 1:
                match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
                match_dict["confidence"] = 1.0
                mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
                continue
            # Else if > 1 match then add matchs found to all_matchs list
            elif len(refineries_match) > 1:
                all_matchs.append(refineries_match)
        # If found some match then add best (lowest number of refineries) in collection with proba = 1/n matchs
        if len(all_matchs) > 0:
            best_match = [i for i in all_matchs if len(i) == min([len(i2) for i2 in all_matchs])][0]
            match_dict = best_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise continue
        else:
            continue
        
    # Get all headlines with owner_tag but no geo_tag
    match_headlines = [i for i in mycol_headlines.find({"geo_tags": {"$size": 0}, "owner_tags": {"$not": {"$size": 0}}}, 
                                             {"geo_tags": 1, "owner_tags": 1, "firstCreated": 1})]
    # Loop through each headline
    for headline in tqdm(match_headlines):
        headline_date = datetime.strptime(headline["firstCreated"], "%Y-%m-%dT%H:%M:%S.%fZ")
        owner_tags = pd.DataFrame(headline["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        ref_ids = owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the headline
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= headline_date) & 
                                         (refineries_df["ToDate"] >= headline_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_headlines.update_one({"_id": headline["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        
        
# Match to country/countries based on ref_match
def country_match(mycol_headlines, mycol_refineries):
    headlines_match = list(mycol_headlines.find({"ref_match": {"$not": {"$size": 0}}}, {"_id": 1, "ref_match": 1}))
    for this_headline in tqdm(headlines_match):
        countries = pd.DataFrame(list(
            mycol_refineries.find({"GeoAssetID": {"$in": [i["GeoAssetID"] for i in this_headline["ref_match"]]}}, 
                                  {"country": 1, "_id": 0})))
        this_match = (countries.country.value_counts()/len(countries)).round(2).to_dict()
        this_match = pd.DataFrame({"p": this_match}).reset_index(drop = False).rename(columns = {"index": "country"})
        mycol_headlines.update_one({"_id": this_headline["_id"]}, {"$set": {"country_match": this_match.to_dict(orient = "records")}})
        
        
# Extract event types from headlines
def match_headlines_event_type(mycol_headlines, events_keywords_h, events_keywords_t):
    # Get ids of headlines containing current event keyword in text or snippet
    for event_name in tqdm(events_keywords_t):
        these_headlines = [i["_id"] for i in mycol_headlines.find({"text": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, {"_id": 1})]
        these_headlines.extend([i["_id"] for i in mycol_headlines.find({"snippet": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, {"_id": 1})])
        # Add current geo_name to matching Tweet's events_tags field       
        if len(these_headlines) > 0:
            mycol_headlines.update_many({"_id": {"$in": list(set(these_headlines))}}, {"$push": {"events_tags": event_name}})
    # Drop duplicate match in events_tags field
    these_headlines = [i for i in mycol_headlines.find({"events_tags": {"$not": {"$size": 0}}}, {"events_tags": 1})]
    for this_headline in tqdm(these_headlines):
        mycol_headlines.update_one({"_id": this_headline["_id"]}, {"$set": {"events_tags": list(set(this_headline["events_tags"]))}})
    return


def main():
    # Load Spacy models
    nlp_sm = spacy.load("en_core_web_sm")
    nlp_md = spacy.load("en_core_web_md")
    nlp_lg = spacy.load("en_core_web_lg")
    spacy_models = [nlp_sm, nlp_md, nlp_lg]
    # Connect to Nominatim for Geomatching
    geolocator = Nominatim(user_agent = "z#7xRKtSX86S$zRUG2h2")
    # Connect to MongoDb collection
    mycol_headlines = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Headlines)
    mycol_refineries = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Refineries)
    mycol_gpes = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_GPEs)
    # Prepare MongoDb collection (add geo_tags key)
    prep_col(mycol_headlines)
    # Get refineries names from merged file 
    geo_names_r = get_geo_names_refineries(mycol_refineries, refineries_file)
    # Match headlines with refineries names
    geotag_headlines_refineries(mycol_headlines, geo_names_r)
    # Get cities names from merged file 
    cities_names = get_geo_names_cities(refineries_file)
    # Match headlines with cities names
    geotag_headlines_cities(mycol_headlines, cities_names)
    # Use Spacy to extract other locations (GPEs)
    geotag_headlines(mycol_headlines, norps_gpes_file, spacy_models)
    # Get owners names
    owners_names = get_owners_names(owners_file)
    # Match headlines with owners names
    match_headlines_owners(mycol_headlines, owners_names)
    # Get probabilities of match for refineries
    get_match_proba(mycol_headlines, mycol_refineries, mycol_gpes, geolocator, refineries_file)
    # Match to country/countries based on ref_match
    country_match(mycol_headlines, mycol_refineries)
    # Extract event types from headlines
    match_headlines_event_type(mycol_headlines, events_keywords_h, events_keywords_t)
