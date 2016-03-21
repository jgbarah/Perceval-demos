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

When getting dasbhoards, they are saved to a file, if specified, or
to stdout, if not.

Example:

dashboards.py --get Git --es_url http://localhost:9200 --file dashboards.json

"""

import argparse
import elasticsearch
import json
import sys
import logging

# Description of the documents of interest
# Key is the type of document in ElasticSearch, value is the name to show
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
                        help="Get (and save locally) specified dashboards")
    parser.add_argument("--list_indices", action="store_true",
                        help="List indices for gotten elements")
    parser.add_argument("--new_index",
                        type=str,
                        help="New index to use for visualizations and searches")
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

class Kibana (object):
    """Kibana instance.

    :param elasticsearch: elasticsearch.Elasticsearch object
    :param         index: Kibana index (default: '.kibana')

    """

    def __init__(self, elasticsearch, index = '.kibana'):
        self.es = elasticsearch
        self.index = index

    def list_elements(self, document):
        """List all elements of type document in the Kibana instance.

        :param document: Type of document to list
        :returns Dictionary, keys are document ids, values are title and description

        """

        documents = self.es.search(index=self.index, doc_type=document,
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

    def list(self, kinds):
        """List all elements of kind.

        :param     kind: Types of document to list (list)

        """

        if len(kinds) == 0:
            # If no kinds, list all kinds of elements
            kinds = documents.keys()
        for kind in kinds:
            elements = self.list_elements(document=kind)
            print(documents[kind]+':')
            for id in elements:
                element = elements[id]
                print(' ', id+': ', element['title'], '(',
                        element['description'], ')')

    def get_dashboard(self, id):

        document = self.es.get(index=self.index, doc_type='dashboard', id=id)
        logging.debug('Retrieved dashboard document: ' + str(document))
        visualization_ids = []
        if 'panelsJSON' in document['_source']:
            visualizations = json.loads(document['_source']['panelsJSON'])
            for visualization in visualizations:
                logging.debug('Visualization description: ' + str(visualization))
                visualization_ids.append(visualization['id'])
        return document, visualization_ids

    def get_visualization(self, id):

        document = self.es.get(index=self.index, doc_type='visualization', id=id)
        logging.debug('Retrieved visualization document: ' + str(document))
        if 'savedSearchId' in document['_source']:
            # Visualization based in a search, let's find its id
            search = document['_source']['savedSearchId']
        else:
            search = None
        return document, search

    def get_search(self, id):

        document = self.es.get(index=self.index, doc_type='search', id=id)
        logging.debug('Retrieved search document: ' + str(document))
        return document

    def get(self, dashboards):
        """Get a list of dashboards, with all their elements, from ElasticSearch.

        """

        elements = Elements()
        for dashboard in dashboards:
            logging.info('Getting dashboard: ' + dashboard)
            (document, visualizations) = self.get_dashboard(id=dashboard)
            elements.dashboards[dashboard] = document
            logging.debug('Found visualizations: ' + str(visualizations))
            for visualization in visualizations:
                if visualization not in elements.visualizations:
                    logging.info('Getting visualization: ' + visualization)
                    (document, search) = self.get_visualization(id=visualization)
                    elements.visualizations[visualization] = document
                    if search and (search not in elements.searches):
                        logging.info('Getting search: ' + search)
                        document = self.get_search(id=search)
                        elements.searches[search] = document
        logging.debug('Dashboards:')
        logging.debug(elements.dashboards)
        logging.debug('Visualizations:')
        logging.debug(elements.visualizations)
        logging.debug('Searches:')
        logging.debug(elements.searches)
        return elements

class Elements (object):
    """Description of the elements composing a Kibana dashboard.

    Includes information usually obtained from Kibana.

    """

    def __init__(self):
        self.dashboards = {}
        self.visualizations = {}
        self.searches = {}

    def save(self, file):
        """Save contents to a file, in JSON format.
        """

        to_save = {
            'dashboards': self.dashboards,
            'visualizations': self.visualizations,
            'searches': self.searches,
        }
        if file is None:
            json.dump(to_save, sys.stdout, sort_keys=True, indent=4)
        else:
            with open(file, 'w') as fp:
                json.dump(to_save, fp, sort_keys=True, indent=4)

    def find_index (self, kind, id, new_index=None):
        """Find the index in a visualization, and maybe change it.

        Don't change the index if not found, or if new_index is None

        :param      kind: Kind of element, {'visualization'|'search'}
        :param        id: Id of the element to inspect
        :param new_index: Name of the new index (default: None)
        :returns Name of the index (or new index, if changed)

        """

        if kind == 'visualization':
            data = self.visualizations[id]
        elif kind == 'search':
            data = self.searches[id]
        meta = data['_source']['kibanaSavedObjectMeta']
        search = json.loads(meta['searchSourceJSON'])
        if 'index' in search:
            index = search['index']
            logging.info('Index for ' + kind + ' ' + id + ': ' + index)
            if new_index is not None:
                search['index'] = new_index
                data['_source'] ['kibanaSavedObjectMeta']['searchSourceJSON'] \
                    = json.dumps(search)
                logging.info('New index for ' + kind + ' ' + id +
                    ': ' + new_index)
                index = new_index
        else:
            index = None
        return index

    def find_indices(self, new_index=None):
        """Find all indices in elements.
        """

        indices = {}
        for visualization in self.visualizations:
            index = self.find_index("visualization", visualization, new_index)
            indices[index] = True
        for search in self.searches:
            index = self.find_index("search", search, new_index)
            indices[index] = True

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

    # Dictionary with all elements (got or put)
    elements = {}
    if args.file:
        file = args.file
    else:
        file = None
    es = elasticsearch.Elasticsearch([args.es_url])
    kibana = Kibana(elasticsearch=es, index=args.kibana_index)
    if args.list is not None:
        kibana.list(kinds=args.list)
    if args.get is not None:
        elements = kibana.get(dashboards=args.get)
        if args.new_index:
            # Change indexes before saving
            elements.find_indices(args.new_index)
        elements.save(args.file)
    if args.list_indices:
        elements.find_indices()
