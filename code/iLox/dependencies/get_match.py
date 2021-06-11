# -*- coding: utf-8 -*-
"""
Created on Sat Jun  5 00:43:02 2021

@author: brend
"""

import time
from tqdm import tqdm
from datetime import datetime
from geopy.exc import GeocoderUnavailable, GeocoderServiceError
import pymongo
import pandas as pd
from area import area
import warnings



# Get probabilities of match for refineries
def get_match_proba(refineries_df, mycol_items, mycol_refineries, mycol_gpes, geolocator, 
                    display_pb, match_start, date_key, nominatim_max_attempts = 10, 
                    nominatim_wait_error = 5, nominatim_wait = 0.5):
    
    # Query location on Nominatim
    def query_nominatim(geolocator, gpe, max_attempts = nominatim_max_attempts, 
                        wait_error = nominatim_wait_error, wait = nominatim_wait, 
                        exactly_one = True):
        # Try max_attempts times
        for i in range(max_attempts):
            try:
                geo_data = geolocator.geocode(
                    gpe, geometry = "geojson", language = "en", exactly_one = exactly_one)
                # Wait normal wait time per request
                time.sleep(wait)
                # If successful then return output
                return geo_data
            # Otherwise wait wait_error seconds
            except GeocoderUnavailable:
                time.sleep(wait_error)
            # Sometimes exactly_one = False creates GeocoderServiceError, retry with True
            except GeocoderServiceError:
                time.sleep(wait)
                try:
                    geo_data = geolocator.geocode(
                        gpe, geometry = "geojson", language = "en", exactly_one = True)
                    # Wait normal wait time per request
                    time.sleep(wait)
                    # If successful then return output
                    return [geo_data]
                # Otherwise wait wait_error seconds
                except (GeocoderUnavailable, GeocoderServiceError) as e:
                    time.sleep(wait_error)
        # If exit loop after trying max_attempts times then try request for Paris
        try:
            geo_data = geolocator.geocode(
                "Paris", geometry = "geojson", language = "en")
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
                polygons.extend([i["boundaries"] for i in list(mycol_gpes.find(
                    {"GPEs": gpe}, {"boundaries": 1, "_id": 0}).sort([("importance", -1)]))])
            # Otherwise query from Nominatim then save in GPEs database
            else:
                try:
                    geo_data = query_nominatim(geolocator, gpe, exactly_one = False)
                except GeocoderUnavailable:
                    warnings.warn("Nominatim API is down")
                    break
                if geo_data is not None:
                    geo_data = [i for i in geo_data if "geojson" in i.raw.keys() and 
                                i.raw["geojson"]["type"] not in ["Point", "LineString", "MultiLineString"]]
                    for this_geo_data in geo_data:
                        this_geo_data.raw["GPEs"] = list(set(
                            [gpe, this_geo_data.raw["display_name"].split(",")[0].strip()]))
                        # If this location (from boundaries) already in database, add GPE to GPEs of that location
                        if mycol_gpes.count_documents(
                                {"boundaries": this_geo_data.raw["geojson"]}) > 0:
                            mycol_gpes.update_one(
                                {"boundaries": this_geo_data.raw["geojson"]}, {"$push": {"GPEs": gpe}})
                            polygons.append(this_geo_data.raw["geojson"])
                        # Else format to save in database
                        else:
                            keep_keys = ["class", "display_name", "geojson", "importance", "lat", "lon", 
                                         "osm_id", "osm_type", "place_id", "type", "GPEs"]
                            this_geo_data = {k: this_geo_data.raw[k] for k in keep_keys if 
                                             k in this_geo_data.raw.keys()}
                            this_geo_data["point"] = {
                                "type": "Point", "coordinates": [float(this_geo_data.pop("lon")), 
                                                                 float(this_geo_data.pop("lat"))]}
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
    def match_gpes(mycol_gpes, mycol_refineries, refineries_df, geolocator, headline_date, 
                   gpes, geo_tags, owner_tags):
        global polygons
        # Get polygons
        polygons = get_polygons(mycol_gpes, geolocator, gpes["match"].tolist())
        # Compute area in sqm to start with largest
        areas = pd.DataFrame(
            {"area": {index: area(polygons[index]) for index in range(len(polygons))}}
            ).sort_values("area", ascending = False)
        polygons = {index: polygons[index] for index in range(len(polygons))}
        all_matchs = []
        
        # If only GPE
        if len(geo_tags[geo_tags["type"] == "refname"]) == 0 and len(owner_tags) == 0:
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}}, 
                    {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[
                    refineries_df["GeoAssetID"].isin(ref_ids) & 
                    (refineries_df["FromDate"] <= headline_date) & 
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
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & 
                (refineries_df["FromDate"] <= headline_date) & 
                (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, 
                     "GeoAssetID": {"$in": ref_ids_initial}}, {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[
                    refineries_df["GeoAssetID"].isin(ref_ids) & 
                    (refineries_df["FromDate"] <= headline_date) & 
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
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & 
                (refineries_df["FromDate"] <= headline_date) & 
                (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, 
                     "GeoAssetID": {"$in": ref_ids_initial}}, 
                    {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[
                    refineries_df["GeoAssetID"].isin(ref_ids) & 
                    (refineries_df["FromDate"] <= headline_date) & 
                    (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If refnames and ownernames then use both and take intersection
        if len(geo_tags[geo_tags["type"] == "refname"]) > 0 and len(owner_tags) > 0:
            ref_ids_initial_refnames = geo_tags[
                geo_tags["type"] == "refname"]["id"].astype(int).tolist()
            ref_ids_initial_ownames = owner_tags["id"].astype(int).tolist()
            ref_ids_initial = set(ref_ids_initial_ownames).intersection(ref_ids_initial_refnames)
            refineries_match = refineries_df[
                refineries_df["GeoAssetID"].isin(ref_ids_initial) & 
                (refineries_df["FromDate"] <= headline_date) & 
                (refineries_df["ToDate"] >= headline_date)]
            ref_ids_initial = refineries_match["GeoAssetID"].tolist()
            # Loop through polygons starting from largest
            for polygon_index in areas.index:
                ref_ids = [i["GeoAssetID"] for i in mycol_refineries.find(
                    {"point": {"$geoWithin": {"$geometry": polygons[polygon_index]}}, 
                     "GeoAssetID": {"$in": ref_ids_initial}}, 
                    {"GeoAssetID": 1, "point": 1, "_id": 0})]
                refineries_match = refineries_df[
                    refineries_df["GeoAssetID"].isin(ref_ids) & 
                    (refineries_df["FromDate"] <= headline_date) & 
                    (refineries_df["ToDate"] >= headline_date)]
                # If perfect match then stop
                if len(refineries_match) == 1:
                    return refineries_match
                elif len(refineries_match) > 0:
                    all_matchs.append(refineries_match)
        # If no exact match found then return match with less refineries
        best_match = [i for i in all_matchs if len(i) == 
                      min([len(i2) for i2 in all_matchs])]
        if len(best_match) > 0:
            best_match = best_match[0]
        return best_match
    
    # Get all items with at least 1 geo_tag, unmatched yet and according to timeframe
    match_items = [i for i in mycol_items.find(
        {date_key: {"$gte": match_start}, "geo_tags": {"$not": {"$size": 0}}, 
         "ref_match": {"$size": 0}}, {"geo_tags": 1, "owner_tags": 1, date_key: 1})]
   
    # Loop through each item
    for item in tqdm(match_items, disable = display_pb, 
                     desc = "Matching to refineries", leave = True):
        item_date = datetime.strptime(item[date_key], 
                                      "%Y-%m-%dT%H:%M:%S.%fZ")
        geo_tags = pd.DataFrame(item["geo_tags"])
        gpes = geo_tags[geo_tags["type"].isin(["GPE"])]
        geo_tags = geo_tags[
            geo_tags["type"].isin(["refname", "cityname"])
            ].drop_duplicates(subset = ["id", "type"])
        refnames = geo_tags[geo_tags["type"] == "refname"]
        citynames = geo_tags[geo_tags["type"] == "cityname"]
        owner_tags = pd.DataFrame(
            item["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        all_matchs = []
        
        # First look at refineries names refname
        ref_ids = refnames["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the item
        refineries_match = refineries_df[
            refineries_df["GeoAssetID"].isin(ref_ids) & 
            (refineries_df["FromDate"] <= item_date) & 
            (refineries_df["ToDate"] >= item_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        elif len(refineries_match) > 1:
            all_matchs.append(refineries_match)
        # Otherwise look at city - keep only ids that appear twice (must match for refname + city)
        ref_ids.extend(citynames["id"].astype(int).tolist())
        ref_ids = list(set([r for r in ref_ids if 
                            ref_ids.count(r) == len(geo_tags["type"].unique())]))
        # Get list of refineries with selected ids and active at the time of the item
        refineries_match = refineries_df[
            refineries_df["GeoAssetID"].isin(ref_ids) & 
            (refineries_df["FromDate"] <= item_date) & 
            (refineries_df["ToDate"] >= item_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Else if > 1 match and no owner information or GPE then save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1 and len(owner_tags) == 0 and len(gpes) == 0:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Else if > 1 match and owner information or GPE then add matchs found to all_matchs list
        elif len(refineries_match) > 1:
            all_matchs.append(refineries_match)
        # If owner then look at owner
        if len(owner_tags) > 0:
            # If found some ref_ids so far, take intersection to have match both existing ref_ids + those from ownername
            if len(ref_ids) > 0:
                ref_ids = set(ref_ids).intersection(
                    owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist())
            # Otherwise only take ref_ids from ownername
            else:
                ref_ids = owner_tags[
                    owner_tags["type"] == "ownername"]["id"].astype(int).tolist()
            # Get list of refineries with selected ids and active at the time of the item
            refineries_match = refineries_df[
                refineries_df["GeoAssetID"].isin(ref_ids) & 
                (refineries_df["FromDate"] <= item_date) & 
                (refineries_df["ToDate"] >= item_date)]
            # If only 1 match then save to collection with proba = 100% and continue
            if len(refineries_match) == 1:
                match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
                match_dict["confidence"] = 1.0
                mycol_items.update_one(
                    {"_id": item["_id"]}, 
                    {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
                continue
            # Else if > 1 match then add matchs found to all_matchs list
            elif len(refineries_match) > 1:
                all_matchs.append(refineries_match)
        # Else if GPE information then use it
        if len(gpes) > 0:
            refineries_match = match_gpes(mycol_gpes, mycol_refineries, refineries_df, 
                                          geolocator, item_date, gpes, geo_tags, owner_tags)
            # If only 1 match then save to collection with proba = 100% and continue
            if len(refineries_match) == 1:
                match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
                match_dict["confidence"] = 1.0
                mycol_items.update_one(
                    {"_id": item["_id"]}, 
                    {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
                continue
            # Else if > 1 match then add matchs found to all_matchs list
            elif len(refineries_match) > 1:
                all_matchs.append(refineries_match)
        # If found some match then add best (lowest number of refineries) in collection with proba = 1/n matchs
        if len(all_matchs) > 0:
            best_match = [i for i in all_matchs if len(i) == 
                          min([len(i2) for i2 in all_matchs])][0]
            match_dict = best_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise continue
        else:
            continue
        
    # Get all items with owner_tag but no geo_tag
    match_items = [i for i in mycol_items.find(
        {date_key: {"$gte": match_start}, 
         "geo_tags": {"$size": 0}, "owner_tags": {"$not": {"$size": 0}}}, 
        {"geo_tags": 1, "owner_tags": 1, date_key: 1})]
    # Loop through each item
    for item in tqdm(match_items, disable = display_pb, 
                     desc = "Matching to refineries (only owner_tags)", leave = True):
        item_date = datetime.strptime(item[date_key], "%Y-%m-%dT%H:%M:%S.%fZ")
        owner_tags = pd.DataFrame(
            item["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        ref_ids = owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the item
        refineries_match = refineries_df[
            refineries_df["GeoAssetID"].isin(ref_ids) & 
            (refineries_df["FromDate"] <= item_date) & 
            (refineries_df["ToDate"] >= item_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = round(1.0 / len(match_dict), 2)
            mycol_items.update_one(
                {"_id": item["_id"]}, 
                {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue