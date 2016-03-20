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
import json
import sys
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
    parser.add_argument("-g", "--get",
                        type=str, nargs="+",
                        help="Get specified dashboards")
    parser.add_argument("-f", "--file",
                        type=str,
                        help="File to save / retrieve dashboards information")
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

def get_dashboard(es, index, id):

    document = es.get(index=index, doc_type='dashboard', id=id)
    logging.debug('Retrieved dashboard document: ' + str(document))
    visualization_ids = []
    if 'panelsJSON' in document['_source']:
        visualizations = json.loads(document['_source']['panelsJSON'])
        for visualization in visualizations:
            logging.debug('Visualization description: ' + str(visualization))
            visualization_ids.append(visualization['id'])
    return document, visualization_ids

def get_visualization(es, index, id):

    document = es.get(index=index, doc_type='visualization', id=id)
    logging.debug('Retrieved visualization document: ' + str(document))
    if 'savedSearchId' in document['_source']:
        # Visualization based in a search, let's find its id
        search = document['_source']['savedSearchId']
    else:
        search = None
    return document, search

def get_search(es, index, id):

    document = es.get(index=index, doc_type='search', id=id)
    logging.debug('Retrieved search document: ' + str(document))
    return document

def get(es, index, dashboards):
    """Get a list of dashboards, with all their elements, from ElasticSearch.

    """

    elements = {
        'dashboards': {},
        'visualizations': {},
        'searches': {}
        }
    for dashboard in dashboards:
        logging.info('Getting dashboard: ' + dashboard)
        (document, visualizations) = get_dashboard(es=es, index=index,
                                                    id=dashboard)
        elements['dashboards'][dashboard] = document
        logging.debug('Found visualizations: ' + str(visualizations))
        for visualization in visualizations:
            if visualization not in elements['visualizations']:
                logging.info('Getting visualization: ' + visualization)
                (document, search) = get_visualization(es=es, index=index,
                                                        id=visualization)
                elements['visualizations'][visualization] = document
                if search and (search not in elements['searches']):
                    logging.info('Getting search: ' + search)
                    document = get_search(es=es, index=index, id=search)
                    elements['searches'][search] = document
    logging.debug('Dashboards:')
    logging.debug(elements['dashboards'])
    logging.debug('Visualizations:')
    logging.debug(elements['visualizations'])
    logging.debug('Searches:')
    logging.debug(elements['searches'])
    return elements

def save(file, elements):

    if file is None:
        json.dump(elements, sys.stdout, sort_keys=True, indent=4)
    else:
        with open(file, 'w') as fp:
            json.dump(elements, fp, sort_keys=True, indent=4)

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

    if args.file:
        file = args.file
    else:
        file = None
    es = elasticsearch.Elasticsearch([args.es_url])
    if args.list is not None:
        list(es=es, index=args.kibana_index,kinds=args.list)
    if args.get is not None:
        elements = get(es=es, index=args.kibana_index,dashboards=args.get)
        save(args.file, elements)
