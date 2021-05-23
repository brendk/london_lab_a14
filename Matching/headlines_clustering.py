# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 19:40:09 2021

@author: brend
"""

import pymongo
import pandas as pd
import numpy as np
import networkx as nx
import warnings
from tqdm import tqdm
from datetime import datetime


mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col_Headlines = "Headlines_filtered"
mongoDB_Col_Refineries = "Refineries"


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


# Prepare data needed for time clustering
def prepare_time_clustering_data(mycol_headlines):
    #Import headline information from mongodb. Change it into a list.
    data_headline = list(mycol_headlines.find({"country_match.p": {"$gt": 0.5}}, 
                                              {"country_match": 1, "firstCreated": 1, "text": 1}))
    for headline in data_headline:
        # Only keep country_match with highest probability
        headline["country"] = pd.DataFrame(headline.pop("country_match")).sort_values("p").iloc[-1]["country"]
        # Change time to timestamp
        headline["time"] = datetime.strptime(headline.pop("firstCreated"), "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
    data_headline = pd.DataFrame(data_headline)
    return data_headline


# Time-based clustering, time_threshold in days
def time_based_clustering(mycol_headlines, data_headline, time_threshold = 7):
    for country in tqdm(data_headline["country"].unique()):
        this_df = data_headline[data_headline["country"] == country][["_id", "time"]]
        country_net = np.array(this_df["time"])
        adjacency_matrix = np.abs((country_net[:, None] - country_net))
        ##Create adjacency matrix for each country. Value = 1 if time < chosen timeframe)
        adjacency_matrix = adjacency_matrix < 86400 * time_threshold
        adjacency_matrix = adjacency_matrix.astype(int)
        clusters_list = []
        for col_index in range(adjacency_matrix.shape[1]):
            if len([l for l in clusters_list if col_index in l])>0:
                continue
            these_rows = np.where(adjacency_matrix[:,col_index] > 0)
            this_cluster = [l for l in clusters_list if len([i for i in these_rows[0] if i in l]) > 0]
            if len(this_cluster) > 0:
                this_cluster[0].extend(these_rows[0].tolist())
            else:
                clusters_list.append(these_rows[0].tolist())
            
        clusters_list = [[this_df.iloc[i]["_id"] for i in list(set(l))] for l in clusters_list]
        for cluster_index, cluster in enumerate(clusters_list):
            cluster_name = "%s-%r" % (country, cluster_index)
            mycol_headlines.update_many({"_id": {"$in": cluster}}, {"$set":{"cluster_id": cluster_name}})
            
            
# Prepare data needed for events clustering
def prepare_events_clustering_data(mycol_headlines):
    clustering_accidents = list(mycol_headlines.find({"cluster_id": {"$not": {"$size": 0}}}, {"events_tags": 1, "cluster_id": 1}))
    events_types = mycol_headlines.distinct("events_tags")       
    clusters = {cluster_id: [x for x1 in [i["events_tags"] for i in clustering_accidents if i["cluster_id"] == cluster_id] for x in x1]
                for cluster_id in mycol_headlines.distinct("cluster_id") if cluster_id != None}
    events_counts = {cluster_id: {event: clusters[cluster_id].count(event) for event in events_types} for cluster_id in clusters.keys()}
    #Create correlation matrix and change values into absolute values
    corr_matrix = np.abs(pd.DataFrame(events_counts).transpose().corr()).replace(np.nan, 0)
    #Create a matrix of the same dimension, only with ones
    matrix_one = np.ones((corr_matrix.shape))
    #Set distance between accidents = (1-corr)^2
    #The square permits to separate accidents better
    distance_matrix = np.square(matrix_one - corr_matrix)
    return distance_matrix
    
    
# Create dataframe with length (in seconds) of clusters
# CHECK United States-218
def get_clusters_duration(mycol_headlines):
    all_clusters = {cluster_id: [datetime.strptime(i["firstCreated"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() 
                                 for i in mycol_headlines.find({'cluster_id':cluster_id}, {"firstCreated": 1})] 
                    for cluster_id in mycol_headlines.distinct("cluster_id", {"cluster_id": {"$exists": True}})}
    all_clusters = {cluster_id: max(all_clusters[cluster_id]) - min(all_clusters[cluster_id]) for cluster_id in all_clusters.keys()}
    clusters_duration = pd.DataFrame({"duration": all_clusters}).sort_values("duration", ascending = False)
    return clusters_duration


def main():
    # Connect to MongoDb collections
    mycol_headlines = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Headlines)
    mycol_refineries = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col_Refineries)
    mycol_headlines.update_many({}, {"$set": {"cluster_id": []}})
    # Prepare data needed for time clustering
    data_headline = prepare_time_clustering_data(mycol_headlines, mycol_refineries)
    # Perform time-based clustering
    time_based_clustering(mycol_headlines, data_headline, time_threshold = 7)
    # Prepare data needed for events clustering
    prepare_events_clustering_data(mycol_headlines)



#Dataframe with headlines and accidents in each
#Matrix that calculates  (sum of distances between each pair of accidents)/(number of pairs)
                                     

#Average between pairs of distances if 2 or more accidents in the same tweet


#To make it visual, create a network for each country with networkx
#A = nx.from_numpy_matrix(adjacency_matrix)
#A = nx.Graph(A)

nx.draw_networkx(A, with_labels=True)

#Extract clusters

###ASSUMPTION: THERE CAN'T BE 2 ACCIDENTS THE SAME DAY (OR TIMEFRAME CHOSEN) IN THE SAME COUNTRY

#Times series pour chaque évènement
# Axe x : clusters
# axe y: nombre d'occurrences de l'évènement
# Calculer corrélation entre évènement