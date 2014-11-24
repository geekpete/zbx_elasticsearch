#!/usr/bin/env python
# coding: utf-8
__author__ = 'Peter Dyson <pete@geekpete.com>'
__version__ = '0.1.1'
__license__ = 'GPLv3'
__source__ = 'http://github.com/geekpete/zbx_elasticsearch/zbx_elasticsearch.py'

"""
zbx_elasticsearch - A python zabbix plugin for monitoring elasticsearch.

Copyright (C) 2014 Peter Dyson <pete@geekpete.com>

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

def index_discovery(index_stats):
    index_discovery = []
    for index in index_stats['indices']:
        element = {'{#INDEXNAME}': index}
        index_discovery.append(element)
    print json.dumps({'data': index_discovery})

def zabbix_fail():
    print "ZBX_NOTSUPPORTED"
    sys.exit(2)

def fetch_stats(api_uri, cache_file, endpoint, port, metric):
    try:
        body = ""
        if os.path.isfile(cache_file) and (os.path.getmtime(cache_file) + 30) > time.time():
            f = file(cache_file,'r')
            body = json.load(f)
            f.close()
        else:
            es_target = 'http://%s:%s' % (endpoint, port)
            stats_req = requests.get(es_target + api_uri)
            stats = stats_req.text
            f = file(cache_file,'w')
            f.write(stats)
            f.close()
            body = json.loads(stats)
        metric_parts=metric.split('.')
        stats = body
        while len(metric_parts):
            stats=stats[metric_parts.pop(0)]
        return stats
    except Exception, e:
        zabbix_fail()
        print str(e)

def main(argv):

    # fetch parameters
    parser = argparse.ArgumentParser(description='Zabbix Elasticsearch Plugin')
    parser.add_argument('-e', '--endpoint', type=str, required=True, help='Elasticsearch endpoint to query')
    parser.add_argument('-p', '--port', type=str, required=False, default=9200, help='Optional HTTP port if not 9200')
    parser.add_argument('-a', '--api', type=str, required=True, default="", help='Stats/Info API to query', choices=['cluster_state','cluster_stats','nodes_info','nodes_stats','indices_stats'])
    parser.add_argument('-i', '--index', type=str, required=False, default="", help='Index to fetch metrics against')
    parser.add_argument('-m', '--metric', type=str, required=True, help='metric to fetch, eg. indices.docs.count')
    parser.add_argument('-n', '--node', type=str, required=False, help='node name to fetch metric for if using nodes_stats API, value of nodes.x.name, use either node or host not both')
    parser.add_argument('--host', type=str, required=False, help='node host to fetch metric for if using nodes_stats API, value of nodes.x.host, use either node or host not both')
    args = parser.parse_args()

    # set uri/file cache based on api used and fetch metric
    if args.api == "indices_stats":
        # set the indices_stats URI path
        api_uri = "/_stats"

        # set the indices_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch." + args.endpoint + "_" + str(args.port) + ".indices_stats_cache"

        # if an index was specified, then construct the metric with the given index
        if args.index:
            target_metric = "indices." + str(args.index) +  "." + str(args.metric)
        else:
            target_metric = args.metric

        try:
            print fetch_stats(api_uri, cache_file, args.endpoint, args.port, target_metric)
        except Exception, e:
            zabbix_fail()
    elif args.api == "nodes_stats":
        stats = ""
        # set the nodes_stats URI path
        api_uri = "/_nodes/stats"

        # set the nodes_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch."  + args.endpoint + "_" + str(args.port) + ".nodes_stats_cache"

        # check if node name or node host is specified and fail if neither are
        if (not args.node and not args.host):
            print "nodes_stats API requires either  --node parameter or --host parameter"
            sys.exit(1)


        try:
            # fetch nodes_stats page
            nodes = fetch_stats(api_uri, cache_file, args.endpoint, args.port, 'nodes')
        except Exception as e:
            #print "Error: %s" % e.args
            zabbix_fail()
        if args.node:
            for node_id in nodes.keys():
                if nodes[node_id]['name'] == args.node:
                    stats = nodes[node_id]
            if stats == "":
                # node name not found
                print "ERROR: node name not found in cluster"
                sys.exit(1)

        elif args.host:
            for node_id in nodes.keys():
                if nodes[node_id]['host'] == args.host:
                    stats = nodes[node_id]
            if stats == "":
                # node name not found
                print "ERROR: node host not found in cluster"
                sys.exit(1)
        else:
            zabbix_fail()
            #print "error: unhandled exception"
            #sys.exit(1)

        # fetch the required metric
        metric_parts=args.metric.split('.')
        try:
            while len(metric_parts):
                stats=stats[metric_parts.pop(0)]
        except Exception as e:
            print "Error: %s" % e.args
        print stats
    elif args.api == "cluster_stats":
        # set the indices_stats URI path
        api_uri = "/_cluster/stats"

        # set the indices_stats cache file location
        cache_file = cache_location + "/zbx_elasticsearch." + args.endpoint + "_" + str(args.port) + ".cluster_stats_cache"

        # set the target metric as the one specified
        target_metric = args.metric

        try:
            print fetch_stats(api_uri, cache_file, args.endpoint, args.port, target_metric)
        except Exception, e:
            zabbix_fail()
    else:
        print "not yet implemented"

if __name__ == "__main__":
    main(sys.argv)