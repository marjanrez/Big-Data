#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 23:53:30 2019

@author: marjanrezvani
"""

from pyspark import SparkContext
def get_abuse_words():
    abuse_words = set()
    Dil = open('drug_illegal.txt').readlines()
    for sublist in Dil:
            abuse_words.add(sublist.rstrip()) 
    Ds = open('drug_sched2.txt').readlines()
    for sublist in Ds:
            abuse_words.add(sublist.rstrip()) 
    return abuse_words
        


def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)


def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None


def processTweets(records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')    

    abuse_words = get_abuse_words()
    
    for fields in records:
        try:
            tweet = fields.split("|")
            content = tweet[-1]
            
            if any(word in content for word in abuse_words):
                
                p = geom.Point(proj(float(tweet[2]), float(tweet[1])))
                idx = findZone(p,index,zones)
                if idx and zones['plctrpop10'][idx] != 0:
                    yield ((zones['plctract10'][idx], zones['plctrpop10'][idx]), 1)
        except:
            pass      
        
if __name__ == "__main__":
    sc = SparkContext()
    rdd = sc.textFile('/data/share/bdm/tweets-100m.csv')
    output = rdd.mapPartitions(processTweets).reduceByKey(lambda x,y: x+y).map\
(lambda x: (x[0][0], float(x[1])/x[0][1])).sortByKey(ascending=True).saveAsTextFile('Final_challenge_output')

