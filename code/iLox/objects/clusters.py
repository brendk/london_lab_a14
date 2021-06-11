# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 00:22:32 2021

@author: brend
"""

import os
import pandas as pd
import numpy as np
from functools import reduce
from tqdm import tqdm
from datetime import datetime


class RefEventsClusters():
    
    def __init__(self, parent):
        attributes = [attr for attr in dir(parent) if not attr.startswith("__")]
        for attribute in attributes:
            setattr(self, attribute, getattr(parent, attribute))
        self._parent = parent
        self.time_threshold = self.clustering_params["time_threshold_days"]
        self.unsplit_threshold = self.clustering_params["no_split_days"]
        self.corr_threshold_events = self.clustering_params["events_corr_threshold"]
        self.events_subsplit = self.clustering_params["events_subsplit"]
        self.events_subsplit_min_size = self.clustering_params["events_subsplit_min_size"]
        
    # Get events correlation matrix
    def _get_events_correlation_matrix(self):
        cor_mats = []
        all_events = self.mycol_merged.distinct("events_tags")
        # Loop through all countries
        for country in self.mycol_merged.distinct("country_match.0.country"):
            # Get all items with this country as first match and 100% match
            items = list(self.mycol_merged.find(
                {"country_match.0.country": country, 
                 "country_match.0.p": 1}, 
                {"created_at": 1, "events_tags": 1, "_id": 0}))
            # Count of events types
            items = [{k: item[k] if k != "events_tags" else 
                      {event: item[k].count(event) for event in all_events} 
                      for k in item.keys()} for item in items]
            items = [{**item, **item["events_tags"]} for item in items]
            # Convert to dataframe and datetime format + sort based on date
            items = pd.DataFrame(items).sort_values(
                by = "created_at").drop(columns = ["events_tags"])
            items["created_at"] = pd.to_datetime(items["created_at"])
            # Sum of events types by date
            items = items.groupby([items["created_at"].dt.date]).sum()
            # Create correlation matrix, replace NaN values (no data) by 0
            cor_mat = items.corr().replace(np.NaN, 0)
            # Append to list, replace negative values by 0 (we're interested in similar events only)
            cor_mats.append(cor_mat.where(cor_mat > 0, 0))
        # Main correlation matrix is sum of all countries' cor_mats (0 for NaN)
        main_corr = reduce(lambda x, y: x.add(y, fill_value = 0), cor_mats)
        # Divide by number of times events appear together to get average
        n_together = [i.where(i <= 0, 1) for i in cor_mats]
        n_together = reduce(lambda x, y: x.add(y, fill_value = 0), n_together)
        self.events_corr_mat = main_corr / n_together
        self.events_corr_mat = self.events_corr_mat.replace(np.NaN, 0)
        # Save as CSV file to avoid rerun
        self.events_corr_mat.to_csv(self.clustering_params["events_corr_mat"], index = True)
        
    # Get GeoAssetIDs of matchs in a cluster
    def _get_asset_ids_cluster(self, this_df, cluster):
        asset_ids = list(self.mycol_merged.find(
            {"_id": {"$in": this_df.iloc[cluster]["_id"].tolist()}}, 
            {"ref_match.GeoAssetID":1, "_id": 0}))
        asset_ids = [[i2["GeoAssetID"] for i2 in i["ref_match"]] for i in asset_ids]
        return asset_ids
    
    # Get ObjectIds of cluster items
    def _get_items_ids_cluster(self, this_df, cluster):
        return this_df.iloc[cluster]["_id"].tolist()
    
    # Prepare data for time and events clustering for all countries
    def _prepare_clustering_data(self):
        # Time-based
        # Import headline information from mongodb. Change it into a list.
        self.data_headline_t = list(self.mycol_merged.find(
            {"country_match.p": {"$gt": 0.5}}, 
            {"country_match": 1, "created_at": 1, "text": 1}))
        for headline in tqdm(self.data_headline_t, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Preparing time clustering data", 
                             leave = True):
            # Only keep country_match with highest probability
            headline["country"] = pd.DataFrame(
                headline.pop("country_match")).sort_values("p").iloc[-1]["country"]
            # Change time to timestamp
            headline["time"] = datetime.strptime(
                headline.pop("created_at"), "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
        self.data_headline_t = pd.DataFrame(self.data_headline_t)
        # Events-based
        self.events_types = self.mycol_merged.distinct("events_tags")
        # Get events and country for each headline
        self.data_headline_e = list(self.mycol_merged.find(
            {"country_match.p": {"$gt": 0.5}}, 
            {"country_match": 1, "events_tags": 1}))
        for headline in tqdm(self.data_headline_e, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Preparing events clustering data", 
                             leave = True):
            # Only keep country_match with highest probability
            headline["country"] = pd.DataFrame(
                headline.pop("country_match")).sort_values("p").iloc[-1]["country"]
            # Change events_tags to 1 or 0 for each event type
            events_tags = headline.pop("events_tags")
            for event_type in self.events_types:
                headline[event_type] = min(events_tags.count(event_type), 1)
        # Group by country
        countries_list = pd.DataFrame(
            self.data_headline_e)["country"].unique().tolist()
        self.data_headline_e = {
            country: {i.pop("_id"): dict((k, v) for k,v in i.items() if k != "country") 
                      for i in self.data_headline_e if i["country"] == country} 
            for country in countries_list}
        
    # Perform clustering on time/country
    def _time_based_clustering(self, country):
        this_df = self.data_headline_t[
            self.data_headline_t["country"] == country][["_id", "time"]]
        this_df = this_df.sort_values("time")
        country_net = np.array(this_df["time"])
        adjacency_matrix = np.abs((country_net[:, None] - country_net))
        # Create adjacency matrix for each country. Value = 1 if time < chosen timeframe)
        adjacency_matrix = adjacency_matrix < 86400 * self.time_threshold
        adjacency_matrix = adjacency_matrix.astype(int)
        clusters_list = []
        for col_index in range(adjacency_matrix.shape[1]):
            if len([l for l in clusters_list if col_index in l]) > 0:
                continue
            these_rows = np.where(adjacency_matrix[:,col_index] > 0)
            this_cluster = [l for l in clusters_list if len(
                [i for i in these_rows[0] if i in l]) > 0]
            if len(this_cluster) > 0:
                this_cluster[0].extend(these_rows[0].tolist())
            else:
                clusters_list.append(these_rows[0].tolist())
        return clusters_list, this_df
    
    # Create adjacency matrix for unsplittable events in each country. 
    # Value = 1 if time < chosen timeframe
    def _get_unsplit_matrix(self, this_data_t):
        this_df = this_data_t[["_id", "time"]]
        country_net = np.array(this_df["time"])
        adjacency_matrix_unsplit = np.abs((country_net[:, None] - country_net))
        adjacency_matrix_unsplit = (
            adjacency_matrix_unsplit < 86400 * self.unsplit_threshold).astype(int)
        return adjacency_matrix_unsplit
    
    # Perform clustering on events
    def _events_based_clustering(self, this_data_e):
        events_types = list(set(
            [x for x1 in [
                [k for k in i.keys()] for i in this_data_e.values()] 
                for x in x1]))
        n_events = len(events_types)
        country_items = np.ones((
            len(this_data_e), len(this_data_e), 
            n_events, n_events), dtype = np.bool)
        country_items_T = np.ones((
            len(this_data_e), len(this_data_e), 
            n_events, n_events), dtype = np.bool)
        for index, item in enumerate(this_data_e.values()):
            country_items[index] = np.multiply(
                np.array(list(item.values())), country_items[index])
            country_items_T[index] = np.swapaxes(
                country_items[index], 0, 2).T
        this_matrix = np.multiply(
            country_items, np.swapaxes(np.swapaxes(country_items_T, 0, 2), 1, 3).T)
        # Matrix with number of matchs between all tweets
        n_match_matrix = np.sum(this_matrix, (2,3))
        # Replace 0s by 1s to avoid division by zero error (sum of corr will be 
        # 0 anyway if no matching events)
        n_match_matrix = np.where(n_match_matrix == 0, 1, n_match_matrix)
        # Inverse-distance matrix (sum of correlations, high means close)
        this_distance_matrix = np.sum(
            np.multiply(this_matrix, self.events_corr_mat.to_numpy()), (2,3))
        # Divide by number of matching events to get average
        this_avg_distance_matrix = np.divide(
            this_distance_matrix, n_match_matrix)
        # Set diagonal to 1 (is not 1 because if headline has fire + attack, 
        # will match fire-fire = 1 + attack-fire * 2 < 2 etc)
        np.fill_diagonal(this_avg_distance_matrix, 1)
        # Apply threshold
        events_clusters_matrix = (
            this_avg_distance_matrix > self.corr_threshold_events).astype(int)
        return events_clusters_matrix
    
    # Split cluster using events clustering
    def _split_cluster_events(self, these_items, country):
        this_data_t = self.data_headline_t[
            self.data_headline_t["_id"].isin(these_items)]
        # Sort value by time and save order
        this_data_t = this_data_t.sort_values("time")
        data_ids = this_data_t["_id"].tolist()
        this_data_e =  {k: self.data_headline_e[country][k] for k in data_ids}
        events_clusters_matrix = self._events_based_clustering(this_data_e)
        adjacency_matrix_unsplit = self._get_unsplit_matrix(this_data_t)
        clusters_mix = np.minimum(
            events_clusters_matrix + adjacency_matrix_unsplit, 1)
        clusters_mix_list = []
        for col_index in range(clusters_mix.shape[1]):
            if len([l for l in clusters_mix_list if col_index in l]) > 0:
                continue
            these_rows = np.where(clusters_mix[:,col_index] > 0)
            this_cluster = [l for l in clusters_mix_list if len(
                [i for i in these_rows[0] if i in l]) > 0]
            if len(this_cluster) > 0:
                this_cluster[0].extend(these_rows[0].tolist())
            else:
                clusters_mix_list.append(these_rows[0].tolist())
        clusters_mix_list = [list(set(i)) for i in clusters_mix_list]
        return clusters_mix_list, data_ids
        
    # Update ref_match for clustered_items
    def _update_ref_match_cluster(self, these_items, ref_match, 
                                  country, cluster_index):
        cluster_name = "%s-%r" % (country, cluster_index)
        # Update with new ref_match if ref_match is not None
        if ref_match is not None:
            self.mycol_merged.update_many(
                {"_id": {"$in": these_items}}, 
                {"$set": {"ref_match_cl": ref_match, "cluster_id": cluster_name}})
        # Otherwise update cluster_id and set ref_match_cl to original ref_match
        else:
            self.mycol_merged.update_many(
                {"_id": {"$in": these_items}}, 
                {"$set": {"cluster_id": cluster_name}})
            for item in these_items:
                self.mycol_merged.update_one(
                    {"_id": item}, 
                    {"$set": {"ref_match_cl": self.mycol_merged.find_one(
                        {"_id": item})["ref_match"]}})
        
    # Perform clustering on single country
    def _single_country_clustering(self, country):
        # Get clusters based on time
        clusters_list, this_df = self._time_based_clustering(country)
        # Clear previous clusters in database for this country
        self.mycol_merged.update_many(
            {"cluster_id": {"$regex": country + "-"}}, 
            {"$set": {"cluster_id": None}})
        cluster_index = 0
        # Loop through each cluster
        for cluster in tqdm(clusters_list, 
                            disable = self.ilox_logger.display_pb(), 
                            desc = "Clustering for %s" % country, 
                            leave = False,
                            miniters = len(clusters_list) / 20):
            ids = self._get_items_ids_cluster(this_df, cluster)
            # If only 1 item in cluster then save to MongoDb
            if len(cluster) == 1:
                self._update_ref_match_cluster(
                    ids, None, country, cluster_index)
                cluster_index += 1
                continue
            # If all items have same ref_match(s) then save to MongoDb
            ref_matchs = self._get_asset_ids_cluster(this_df, cluster)
            if len(list(set([tuple(i) for i in ref_matchs]))) == 1:
                self._update_ref_match_cluster(
                    ids, None, country, cluster_index)
                cluster_index += 1
                continue
            # If multiple matchs match items with p > 1 to p == 1 using most frequent p
            if len(list(set([tuple(i) for i in ref_matchs]))) > 1:
                # Matchs with p == 1
                single_matchs = [i[0] for i in ref_matchs if len(i) == 1]
                # If no single match, perform events clustering
                if len(single_matchs) == 0:
                    subclusters_list, data_ids = self._split_cluster_events(
                        ids, country)
                    # Loop through subclusters
                    for subcluster in subclusters_list:
                        # Get corresponding ids
                        these_items = [data_ids[i] for i in subcluster]
                        # Update cluster_id, not ref_match
                        self._update_ref_match_cluster(
                            these_items, None, country, cluster_index)
                        cluster_index += 1
                    continue
                # Count of occurence of single_matchs, sorted by most frequent
                single_matchs = pd.DataFrame(
                    [{"i": i, "count": single_matchs.count(i)} 
                     for i in list(set(single_matchs))]).sort_values(
                             "count", ascending = False)["i"].tolist()
                # Loop through single matchs
                for single_match in single_matchs:
                    # Take items with this single match, match them to single 
                    # match and remove from clusters_list
                    these_matchs = [m for m in ref_matchs if single_match in m]
                    # Get corresponding ids
                    these_items = [
                        self._get_items_ids_cluster(this_df, [i])[0] for i in cluster if 
                        self._get_asset_ids_cluster(this_df, [i])[0] in these_matchs]
                    # Get ref_match of perfect match
                    ref_match = self.mycol_merged.find_one(
                        {"_id": {"$in": these_items}, "ref_match": {"$size": 1}}, 
                        {"ref_match": 1})["ref_match"]
                    # If events_subsplit, split based on events distance
                    if self.events_subsplit and len(these_items) >= self.events_subsplit_min_size:
                        subclusters_list, data_ids = self._split_cluster_events(
                            these_items, country)
                        # Loop through subclusters
                        for subcluster in subclusters_list:
                            # Get corresponding ids
                            these_items = [data_ids[i] for i in subcluster]
                            # Update ref_match for clustered_items
                            self._update_ref_match_cluster(
                                these_items, ref_match, country, cluster_index)
                            cluster_index += 1
                    else:
                        # Update ref_match for clustered_items
                        self._update_ref_match_cluster(
                            these_items, ref_match, country, cluster_index)
                        cluster_index += 1
                    # Remove already clustered items from ref_matchs
                    ref_matchs = [r for r in ref_matchs if r not in these_matchs]
                continue
        
    # Run
    def _run(self):
        # Load events_corr_mat or create
        if os.path.exists(self.clustering_params["events_corr_mat"]):
            self.events_corr_mat = pd.read_csv(self.clustering_params["events_corr_mat"], index_col = 0)
        else:
            print("events_corr_mat file not found, creating")
            self._get_events_correlation_matrix()
        # Prepare data needed for clustering
        self._prepare_clustering_data()
        # Run clustering for each country
        all_countries = self.mycol_merged.distinct("country_match.country")
        for country in tqdm(all_countries, desc = "Clustering per country", leave = True):
            self._single_country_clustering(country)