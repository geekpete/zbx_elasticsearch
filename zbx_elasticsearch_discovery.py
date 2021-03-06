#!/usr/bin/env python
# coding: utf-8
__author__ = 'Peter Dyson <pete@geekpete.com>'
__version__ = '0.1.2'
__license__ = 'GPLv3'
__source__ = 'https://github.com/geekpete/zbx_elasticsearch/zbx_elasticsearch_discovery.py'

"""
zbx_elasticsearch - A python zabbix plugin for monitoring elasticsearch.

Copyright (C) 2014 Peter Dyson <pete@geekpete.com>
Valuable contributions by Norfolkislander.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.


"""


import requests
import json
import sys, os, time
import argparse

# some global settings
cache_location = "/tmp"

def cluster_discovery(cluster_stats, endpoint, port):
    cluster_discovery = []
    element = {'{#CLUSTERNAME}': cluster_stats['cluster_name'],
               '{#ENDPOINT}': endpoint,
               '{#PORT}': port}
    cluster_discovery.append(element)
    print json.dumps({"data": cluster_discovery})

def index_discovery(index_stats, endpoint, port):
    index_discovery = []
    for index in index_stats:
        element = {'{#INDEXNAME}': index,
                   '{#ENDPOINT}': endpoint,
                   '{#PORT}': port}
        index_discovery.append(element)
    print json.dumps({"data": index_discovery})


def node_hosts_discovery(node_stats, endpoint, port):
    node_hosts_discovery = []
    for node in node_stats:
        element = {'{#NODEHOST}': node_stats[node]['host'],
                   '{#ENDPOINT}': endpoint,
                   '{#PORT}': port}
        node_hosts_discovery.append(element)
    print json.dumps({'data': node_hosts_discovery})

def node_names_discovery(node_stats, endpoint, port):
    node_names_discovery = []
    for node in node_stats:
        element = {'{#NODENAME}': node_stats[node]['name'],
                   '{#ENDPOINT}': endpoint,
                   '{#PORT}': port}
        node_names_discovery.append(element)
    print json.dumps({'data': node_names_discovery})

def zabbix_fail():
    print "ZBX_NOTSUPPORTED"
    sys.exit(2)


def fetch_stats(api_uri, cache_file, endpoint, port, metric=None):
    try:
        body = ""
        if os.path.isfile(cache_file) and (os.path.getmtime(cache_file) + 30) > time.time():
            f = file(cache_file,'r')
            body = json.load(f)
            f.close()
        else:
            es_target = 'http://%s:%s' % (endpoint, port)
            stats_req = requests.get(es_target + api_uri, stream=True)
            stats = stats_req.text
            f = file(cache_file,'w')
            f.write(stats)
            f.close()
            body = json.loads(stats)
        stats = body
        if metric:
            metric_parts=metric.split('.')
            while len(metric_parts):
                stats=stats[metric_parts.pop(0)]
        return stats
    except Exception, e:
        zabbix_fail()
        print str(e)

def main(argv):

    # fetch parameters
    parser = argparse.ArgumentParser(description='Zabbix Elasticsearch Discovery Plugin')
    parser.add_argument('-e', '--endpoint', type=str, required=True, help='Elasticsearch endpoint to query')
    parser.add_argument('-p', '--port', type=str, required=False, default=9200, help='Optional HTTP port if not 9200')
    parser.add_argument('-d', '--discovery', type=str, required=True, choices=['cluster','index','node_names','node_hosts'], help='Perform Elasticsearch Discovery of a specific type')
    args = parser.parse_args()

    if args.discovery=="cluster":
        # Do cluster discovery
   
        # set the cluster_stats URI path
        api_uri = "/_cluster/stats"

        # set the indices_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch." + args.endpoint + "_" + str(args.port) + ".cluster_stats_cache"

        # fetch indices stats page
        cluster_stats = fetch_stats(api_uri, cache_file, args.endpoint, args.port)

        # do node host discovery
        cluster_discovery(cluster_stats, args.endpoint, args.port)

    elif args.discovery=="index":
        # Do index discovery

        # set the indices_stats URI path
        api_uri = "/_stats"

        # set the indices_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch." + args.endpoint + "_" + str(args.port) + ".indices_stats_cache"

        # fetch indices stats page
        index_stats = fetch_stats(api_uri, cache_file, args.endpoint, args.port, 'indices')

        # do node host discovery
        index_discovery(index_stats, args.endpoint, args.port)

    else:
        # Do node discovery

        # set the nodes_stats URI path
        api_uri = "/_nodes/stats"

        # set the nodes_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch."  + args.endpoint + "_" + str(args.port) + ".nodes_stats_cache"

        try:
            # Fetch nodes stats page
            nodes = fetch_stats(api_uri, cache_file, args.endpoint, args.port, 'nodes')
        except Exception as e:
            #print "Error: %s" % e.args
            zabbix_fail()

        if args.discovery=="node_names":
            # Do node name discovery
            node_names_discovery(nodes, args.endpoint, args.port)
        elif args.discovery=="node_hosts":
            # do node host discovery
            node_hosts_discovery(nodes, args.endpoint, args.port)
        else:
            print "not yet implemented"

if __name__ == "__main__":
    main(sys.argv)

