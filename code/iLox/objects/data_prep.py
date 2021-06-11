# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 01:02:53 2021

@author: brend
"""

import os
import pymongo
import pandas as pd
import numpy as np
import re
from tqdm import tqdm


class DataPrep():
    
    def __init__(self, parent):
        attributes = [attr for attr in dir(parent) if not attr.startswith("__")]
        for attribute in attributes:
            setattr(self, attribute, getattr(parent, attribute))
        self._parent = parent
        self.refineries_file = self.data_prep_params["refineries_file"]
        self.owners_file = self.data_prep_params["owners_file"]
        self.geo_names_refineries = self.data_prep_params["geo_names_refineries"]
        self.geo_names_cities = self.data_prep_params["geo_names_cities"]
        self.ref_owners_names = self.data_prep_params["ref_owners_names"]
        
    def _set_parent_attrs(self):
        attributes = [attr for attr in dir(self) if not attr.startswith("_")]
        for attribute in attributes:
            setattr(self._parent, attribute, getattr(self, attribute))
        
    # Perform data preparation process from scratch
    def _run(self):
        # Update refineries collection
        self._prep_ref_data()
        # Create refineries names
        self._get_geo_names_refineries()
        # Get cities names from merged file 
        self._get_geo_names_cities()
        # Get list of potential owners names
        self._get_owners_names()
        # Set parent attributes
        self._set_parent_attrs()
        
    # Load previously generated output of data preparation process
    def _read(self):
        if os.path.exists(self.geo_names_refineries):
            self.geo_names_r = pd.read_csv(self.geo_names_refineries).to_dict(orient = "records")
        else:
            print("geo_names_refineries file not found, running corresponding DataPrep level")
            self._get_geo_names_refineries()
        if os.path.exists(self.geo_names_cities):
            self.cities_names = pd.read_csv(self.geo_names_cities).to_dict(orient = "records")
        else:
            print("geo_names_cities file not found, running corresponding DataPrep level")
            self._get_geo_names_cities()
        if os.path.exists(self.ref_owners_names):
            self.owners_names = pd.read_csv(self.ref_owners_names).to_dict(orient = "records")
        else:
            print("ref_owners_names file not found, running corresponding DataPrep level")
            self._get_geo_names_cities()
        # Set parent attributes
        self._set_parent_attrs()
            
    # Prepare refineries data - Get geo data from Nominatim
    def _prep_ref_data(self):
        ref_data = pd.read_csv(self.refineries_file)
        # Dataframe with only distinct coordinates
        coord_df = ref_data[["GeoAssetID", "Latitude", "Longitude"]].drop_duplicates()
        # Only process new refineries or those specified in update_refs
        process_ids = list(set(ref_data["GeoAssetID"].tolist()).difference(self.mycol_refineries.distinct("GeoAssetID")))
        process_ids.extend(self.data_prep_params["update_refs"])
        process_ids = list(set(process_ids))
        coord_items = []
        for item in tqdm(coord_df[coord_df["GeoAssetID"].isin(process_ids)].to_dict(orient = "index").values(), 
                         disable = self.ilox_logger.display_pb(), 
                         desc = "Updating Refineries collection", 
                         leave = True):
            location = self.geolocator.reverse("%r, %r" % (item["Latitude"], item["Longitude"]), language = "en")
            item = {**item, **location.raw["address"]}
            item["bounding_box"] = [float(i) for i in location.raw["boundingbox"]]
            coord_items.append(item)
        # Set fields missing for some items to NaN
        coord_items = pd.DataFrame(coord_items).to_dict(orient = "records")
        self._refineries_to_mongo(coord_items)
    
    # Fill MongoDb refineries collection
    def _refineries_to_mongo(self, coord_items):
        for coord_item in coord_items:
            coord_item["point"] = {"coordinates": [coord_item.pop("Longitude"), coord_item.pop("Latitude")], 
                                   "type": "Point"}
            coord_item["bounding_box"] = {"coordinates": [[[coord_item["bounding_box"][2], coord_item["bounding_box"][1]], 
                                                           [coord_item["bounding_box"][3], coord_item["bounding_box"][1]], 
                                                           [coord_item["bounding_box"][3], coord_item["bounding_box"][0]],
                                                           [coord_item["bounding_box"][2], coord_item["bounding_box"][0]],
                                                           [coord_item["bounding_box"][2], coord_item["bounding_box"][1]]]], 
                                          "type": "Polygon"}
            self.mycol_refineries.insert_one(coord_item)
        # Create indexes if not already exists
        existing_idx = self.mycol_refineries.index_information()
        if "point_2dsphere" not in existing_idx.keys():
            self.mycol_refineries.create_index(
                [("point", pymongo.GEOSPHERE)], name = "point_2dsphere", unique = False)
        if "bounding_box_2dsphere" not in existing_idx.keys():
            self.mycol_refineries.create_index(
                [("bounding_box", pymongo.GEOSPHERE)], name = "bounding_box_2dsphere", unique = False)
        
    # Get list of potential refineries names
    def _get_geo_names_refineries(self):
        names = pd.read_csv(
            self.refineries_file)[["GeoAssetID", "GeoAssetName"]].drop_duplicates(subset = ["GeoAssetName"])
        geo_names_r_original = names.to_dict(orient = "records")
        geo_names_r = []
        for geo_name in tqdm(geo_names_r_original, disable = self.ilox_logger.display_pb(), 
                             desc = "Creating refineries names", 
                             leave = True):
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
            geo_names_r_df.to_csv(self.geo_names_refineries, index = False)
            # Return as list
            self.geo_names_r = geo_names_r_df.to_dict(orient = "records")
            # Update MongoDb collection
            self.mycol_refineries.update_many({}, {"$set": {"refnames": []}})
            for geo_name in self.geo_names_r:
                self.mycol_refineries.update_one(
                    {"GeoAssetID": geo_name["id"]}, 
                    {"$push": {"refnames": geo_name["match"]}})
                
    # Get list of potential cities names
    def _get_geo_names_cities(self):
        cities = pd.read_csv(
            self.refineries_file)[["GeoAssetID", "City"]].drop_duplicates().dropna()
        cities_names_original = cities.to_dict(orient = "records")
        cities_names = []
        for city_name in tqdm(cities_names_original, disable = self.ilox_logger.display_pb(), 
                              desc = "Creating cities names", 
                              leave = True):
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
        cities_names_df.to_csv(self.geo_names_cities, index = False)
        # Return as list
        self.cities_names = cities_names_df.to_dict(orient = "records")
        
    # Get list of potential owners names
    def _get_owners_names(self):
        names = pd.read_csv(self.owners_file)[["GeoAssetID", "CompanyName"]].drop_duplicates()
        names = names.replace("Unknown", np.NaN).dropna().to_dict(orient = "records")
        owners_names = []
        for owner_name in tqdm(names, disable = self.ilox_logger.display_pb(), 
                               desc = "Creating owners names", 
                               leave = True):
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
        owners_names_df.to_csv(self.ref_owners_names, index = False)
        # Return as list
        self.owners_names = owners_names_df.to_dict(orient = "records")