#!/usr/bin/env python3
# -*- coding: utf-8 -*-

## Copyright (C) 2016 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

description = """
Manage Kibana dashboards (download, upload, create from template...)

Example:

dashboards.py --list --es_url http://localhost:9200

"""

import argparse
import elasticsearch
import logging

# Description of the documents of interest
# Keu is the type of document in ElasticSearch, value is the name to show
documents = {
    'index-pattern': 'Index patterns',
    'search': 'Searches',
    'visualization': 'Visualizations',
    'dashboard': 'Dashboards'
}

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-l", "--list",
                        type=str, nargs="*",
                        choices=documents.keys(),
                        help="List available elements of given type"
                            + " (default: all)")
    parser.add_argument("-e", "--es_url",
                        help = "ElasticSearch url where Kibana indexes are stored (http://user:secret@host:port/res)")
    parser.add_argument("-i", "--kibana_index", default=".kibana",
                        help = "Kibana index in ElasticSearch (default: '.kibana')")
    parser.add_argument("-log", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    parser.add_argument("--logfile", type=str,
                        help = "Log file")
    args = parser.parse_args()
    return args

def list_elements(es, index, document):
    """List all elements of type document from index.

    :param       es: ElasticSearch objects
    :param    index: Kibana index in Elasticsearch (usually ".kibana")
    :param document: Type of document to list
    :returns Dictionary, keys are document ids, values are title and description

    """

    documents = es.search(index=index, doc_type=document,
                            body={"query": {"match_all": {}}},
                        filter_path=['hits.hits._id',
                                    'hits.hits._source.title',
                                    'hits.hits._source.description'])
    logging.debug ("Documents: " + str(documents))
    doc_dict = {}
    for document in documents['hits']['hits']:
        element = {
            'title': document['_source']['title']
        }
        if 'description' in document['_source']:
            element['description'] = document['_source']['description']
        else:
            element['description'] = ''
        doc_dict[document['_id']] = element
    return doc_dict

def print_elements(elements):

    for id in elements:
        element = elements[id]
        print(' ', id+': ', element['title'], '(', element['description'], ')')

def list(es, index, kinds):
    """List all elements of kind, in index.

    :param       es: ElasticSearch objects
    :param    index: Kibana index in Elasticsearch (usually ".kibana")
    :param     kind: Types of document to list (list)

    """

    if len(kinds) == 0:
        # If no kinds, list all kinds of elements
        kinds = documents.keys()
    for kind in kinds:
        elements = list_elements(es=es, index=args.kibana_index,
                                document=kind)
        print(documents[kind]+':')
        print_elements(elements)

if __name__ == "__main__":
    args = parse_args()
    if args.logging:
        log_format = '%(levelname)s:%(message)s'
        if args.logging == "info":
            level = logging.INFO
        elif args.logging == "debug":
            level = logging.DEBUG
        if args.logfile:
            logging.basicConfig(format=log_format, level=level,
                                filename = args.logfile, filemode = "w")
        else:
            logging.basicConfig(format=log_format, level=level)

    es = elasticsearch.Elasticsearch([args.es_url])
    if args.list is not None:
        list(es=es, index=args.kibana_index,kinds=args.list)
