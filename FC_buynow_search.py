## Jun Fang
## Supplyframe 07/08/2017
## Get buynow and search counts on FindChips from start_date to end_date

import numpy as np
import pandas as pd
import pickle
import pymongo
from datetime import date, timedelta

from pyspark.conf import SparkConf
from pyspark.context import SparkContext

## Start main function
if __name__ == "__main__":

    # Set up start date and end date
    date_start = date(2014, 1, 1)
    date_end = date(2017, 8, 25)
    date_delta = date_end - date_start    

    # Get buyNow data on FindChips
    date_audit = date(2014,3,19)  # audit has different formats before and after this date 
    target_audit_prev, target_audit_new = "0", "0000000000"    
    target_buynow = set(["buynow", "buynow_mpn", "submit rfq", "submit rfq_mpn"])   # cost of x in set: O(1)
    target_search = set(["searchResultsImp_new".lower()])
    target_action = set(["buynow", "buynow_mpn", "submit rfq", "submit rfq_mpn", "searchResultsImp_new".lower()])
    
    # Spark configuration
    nodes = ["local[*]", "spark://us-lax-1w-science-00:7077"]  # local mode or cluster mode
    master_node = nodes[1]

    my_conf = SparkConf() ; 
    my_conf.setMaster(master_node) ; 
    my_conf.setAppName("FC buynow and search");
    my_conf.set("spark.executor.memory", "32g")
    my_conf.set('spark.driver.maxResultSize', '10g')
    my_conf.set('spark.cores.max', 18)
    sc = SparkContext(conf=my_conf) 
    
    # Define the HDFS path
    HDFS_PATH = "hdfs://us-lax-1w-nn-00.vpc.supplyframe.com:8020"
    ORTHO_HDFS_PATH = HDFS_PATH + "/prod/etl/orthogonal_lite/"
    
    # MongoDB configuration
    client = pymongo.MongoClient('us-lax-1w-science-00.vpc.supplyframe.com')
    db = client['jfang_FC_buynow_search']
    dates_name = date_start.strftime("%Y_%m_%d") + '_' + date_end.strftime("%Y_%m_%d")
    coll_name_buynow = "buynow_" + dates_name  # set MongoDB collection names 
    coll_name_search = "search_" + dates_name
    collection_buynow = db[coll_name_buynow]
    collection_search = db[coll_name_search]
    if coll_name_buynow in db.collection_names():
        collection_buynow.drop()
    if coll_name_search in db.collection_names():
        collection_search.drop()
    
    # Record buynow and search counts
    buynow_dic = {}
    search_dic = {}
    
    def parseLine(line):
        ip = line[3]
        agent = line[10]
        action = line[2].lower()
        mpn = line[7].upper()
        if action in target_buynow:
            act = "B"
        elif action in target_search:
            act = "S"
        else:
            act = "O"
        return ((ip, agent), act, mpn) 
    
    # For loop to process daily data 
    for i in range(date_delta.days + 1):
        date_current = date_start + timedelta(days=i)
        if date_current < date_audit:
            target_audit = target_audit_prev
        else:
            target_audit = target_audit_new
        dt = date_current.strftime("%Y/%m/%d")
        if dt == '2015/11/18':  # no data on this date
            continue
        path = ORTHO_HDFS_PATH + dt + "/part-*"

        # filter out FindChips data
        FindChips_rdd = sc.textFile(path).map(lambda x: x.split('\t')) \
            .filter(lambda x: target_audit == x[0] and 'fc' in x[5].lower()[:2] and 'srp' in x[5].lower() \
                    and x[5] != 'FC_sep' and 'syndication' not in x[5].lower() and 'webservice' not in x[5].lower())

        # cache FindChips RDD
        cache = FindChips_rdd.map( parseLine ).collect()
        cache_rdd = sc.parallelize(cache)
        
        # find out bot traffic
        bot_rdd = cache_rdd.mapValues(lambda x: int(x[0] == 'B')).mapValues(lambda x: (x, 1-x)) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .mapValues(lambda x: ( x[0], x[0] / (x[1] + x[0])) ) \
            .filter(lambda x: x[1][0] > 5 and x[1][1] >= 0.95) \
            .map(lambda x: x[0])
        bot_traffic = set(bot_rdd.collect())  # attention: import to use set

        # target events
        targets = cache_rdd.filter(lambda x: x[0] not in bot_traffic) \
            .map(lambda x: (x[2], x[1])) \
            .mapValues( lambda x: (int(x[0] == 'B'), int(x[0] == 'S')) ) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
            .collect()    

        # buynow and search count in target events
        for mpn, action_count in targets:
            buynow_count = action_count[0]
            search_count = action_count[1]
            if buynow_count > 0:
                buynow_dic[mpn] = buynow_dic.get(mpn, []) + [(dt, buynow_count)]
            if search_count > 0:
                search_dic[mpn] = search_dic.get(mpn, []) + [(dt, search_count)]
    
    # Insert buynow counts into MongoDB
    collection_buynow.insert_many( 
            [{
                "MPN": mpn,
                "dates": [b[0] for b in dt_num],
                "buynow_counts": [b[1] for b in dt_num]
            } for mpn, dt_num in buynow_dic.items()]
        )

    # Insert search counts into MongoDB
    collection_search.insert_many( 
            [{
                "search_term": term,
                "dates": [b[0] for b in dt_num],
                "search_counts": [b[1] for b in dt_num]
            } for term, dt_num in search_dic.items()]
        )
