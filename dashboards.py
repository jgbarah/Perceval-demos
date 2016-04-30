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

Posssible source/destination for dashboards (and all the related elements):
* ElasticSearch: es <url> [<index>]
    <url>: http://user:secret@host:port/res
    <index>: Kibana index (default: '.kibana')
    Example: --src kibana http://localhost:9200
* File: file [<filename>]
    <filename> file name (default: stdin/stdout)
    Example: --dst file dashboards.json
    Example: --src file

Example:

dashboards.py --dashboards Git --src es http://localhost:9200 --dst file dashboards.json

dashboards.py --src es http://localhost:5601/elastcsearch --dashboards Git --dst file

"""

import argparse
import elasticsearch
import json
import sys
import logging
import collections

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
    parser.add_argument("-s", "--src",
                        type=str, nargs="*",
                        help="Source of dashboard(s) (file|elasticsearch)")
    parser.add_argument("-d", "--dst",
                        type=str, nargs="*",
                        help = "Destination of dashboard(s) (file|elasticsearch)")
    parser.add_argument("--dashboards",
                        type=str, nargs="+",
                        help="Dashboards to retrieve / save")

    parser.add_argument("-l", "--list",
                        type=str, nargs="*",
                        choices=documents.keys(),
                        help="List available elements of given type"
                            + " (default: all)")
    parser.add_argument("-g", "--get",
                        type=str, nargs="+",
                        help="Get (and save locally) specified dashboards")
    parser.add_argument("-p", "--put",
                        type=str, nargs="+",
                        help="Put (to ElasticSearch) specified dashboards")
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

def visualizations_in_dashboard (document):
    """Return all visualizations in dashboard.

    :param document: Dictionary describing a dashboard
    :returns: List of the ids of all visualizations found

    """

    ids = []
    if 'panelsJSON' in document:
        visualizations = json.loads(document['panelsJSON'])
        for visualization in visualizations:
            logging.debug('Visualization description: ' + str(visualization))
            ids.append(visualization['id'])
    logging.debug('Found visualizations: ' + str(ids))
    return ids

def search_in_visualization (document):
    """Return the search in a visualization, if any.

    :param document: Dictionary describing a dashboard
    :returns: Search id, or None, if none found.

    """

    if 'savedSearchId' in document:
        # Visualization based in a search, let's find its id
        search = document['savedSearchId']
    else:
        search = None
    return search

class Instance (object):
    """Instance with elements composing dashbards.

    Usually, they can be a Elastic instance, or a file.

    """

    def list(self, kinds):
        """List all elements of kind in instance.

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

class Elastic (Instance):
    """ElasticSearch instance.

    :param elasticsearch: elasticsearch.Elasticsearch object
    :param         index: Kibana index (default: '.kibana')

    """

    def __init__(self, url, index = '.kibana'):
        self.es = elasticsearch.Elasticsearch(url)
        self.index = index

    def list_elements(self, document):
        """List all elements of type document in the Elastic instance.

        :param document: Type of document to list
        :returns: Dictionary, keys are document ids, values are title and description

        """

        documents = self.es.search(index=self.index, doc_type=document,
                                    body={"query": {"match_all": {}}},
                                    filter_path=['hits.hits._id',
                                        'hits.hits._source.title',
                                        'hits.hits._source.description'])
        logging.debug ("Documents: " + str(documents))
        doc_dict = {}
        for document in documents['hits']['hits']['_source']:
            element = {
                'title': document['title']
            }
            if 'description' in document:
                element['description'] = document['description']
            else:
                element['description'] = ''
            doc_dict[document['_id']] = element
        return doc_dict

    def retrieve_document(self, kind, id):

        document = self.es.get(index=self.index, doc_type=kind, id=id)
        logging.debug('Retrieved ' + kind + ' document: ' + str(document))
        return document['_source']

    def save_document(self, kind, id, document):

        self.es.index(index=self.index, doc_type=kind, id=id, body=document)
        logging.debug('Retrieved ' + kind + ' document: ' + str(document))

    def retrieve(self, dashboards):
        """Get dashboards, with all their elements, from ElasticSearch.

        :param dashboards: dashboards to retrieve, with all their elements
        :returns: Elements object with retrieved elements

        """

        elements = Elements()
        for dashboard in dashboards:
            logging.info('Getting dashboard: ' + dashboard)
            document = self.retrieve_document(kind='dashboard', id=dashboard)
            elements.add_element(
                kind='dashboard', name=dashboard, element=document
            )
            visualizations = visualizations_in_dashboard(document=document)
            for visualization in visualizations:
                if visualization not in elements.get_ids_kind('visualization'):
                    logging.info('Getting visualization: ' + visualization)
                    document = self.retrieve_document(kind='visualization',
                                                    id=visualization)
                    elements.add_element(
                        kind='visualization', name=visualization, element=document
                    )
                    search = search_in_visualization(document=document)
                    if search and (search not in elements.get_ids_kind('search')):
                        logging.info('Getting search: ' + search)
                        document = self.retrieve_document(kind='search',
                                                        id=search)
                        elements.add_element(
                            kind='search', name=search, element=document
                        )
        logging.debug(elements)
        return elements

    def save(self, elements, dashboards = None):
        """Save a list of dashboards, with all their elements, to ElasticSearch.

        Save the specified dashboards, with all the elements (visualizations,
        searches) they include. Find the elements to save in elements.
        If no dashboard is specified, save all the elements for all the
        dashboards in elements.

        :param   elements: Elements object, to get elements to save
        :param dashboards: dashboards to save (default: None)

        """

        es_kinds = {'dashboards': 'dashboard',
                'searches': 'search',
                'visualizations' : 'visualization'}
        to_save = elements.get_elements(dashboards).get_dict()
        for kind in to_save:
            es_kind = es_kinds[kind]
            for element in to_save[kind]:
                document = to_save[kind][element]
                self.save_document(kind=es_kind, id=element, document=document)
        return

class File (Instance):
    """JSON file for storing / retrieving dashboard descriptions.

    :param name: File name, None if stdout/stdin is to be used (default: None)

    """

    def __init__(self, name=None):

        logging.debug('New file: ' + str(name))
        self.name = name

    def list_elements(self, document):
        """List all elements of type document in the file.

        :param document: Type of document to list
        :returns: Dictionary, keys are document ids, values are title and description

        """

        elements = self.retrieve()
        logging.debug ("Documents: " + str(elements))
        for document in documents['hits']['hits']['_source']:
            element = {
                'title': document['title']
            }
            if 'description' in document:
                element['description'] = document['description']
            else:
                element['description'] = ''
            doc_dict[document['_id']] = element
        return doc_dict

    def save(self, elements, dashboards=None):
        """Save elements (object of class Elements) to a file, in JSON format.

        elements is a description of one or more dashboards.
        It should include descriptions of all visualizations and searches
        needed, too.

        :param   elements: elements to save (Elements class)
        :param dashboards: dashboards to save

        """

        to_save = elements.get_elements(dashboards)
        if self.name is None:
            json.dump(to_save.get_dict(), sys.stdout,sort_keys=True, indent=4)
        else:
            with open(self.name, 'w') as fp:
                json.dump(to_save.get_dict(), fp, sort_keys=True, indent=4)

    def retrieve(self, dashboards=None):
        """Retrieve elements (object of class Elements) form a file, in JSON format.

        The file should include a JSON description of one or more dashboards.
        It should include descriptions of all visualizations and searches
        needed, too. If dashboards are specified, only the elements needed
        to produce those dashhboards are returned.

        :param dashboards: dashboards to retrieve
        :returns Elements object with retrieved elements

        """

        if self.name is None:
            retrieved = json.loads(sys.stdin)
        else:
            with open(self.name, 'r') as fp:
                retrieved = json.load(fp)
        elements = Elements()
        try:
            elements._set_store('dashboard', retrieved['dashboards'])
            elements._set_store('visualization', retrieved['visualizations'])
            elements._set_store('search', retrieved['searches'])
        except KeyError as key:
            print('Bad format in JSON data:', key)
        to_return = elements.get_elements(dashboards)
        return elements

class Elements (object):
    """Description of the elements composing a Kibana dashboard.

    Includes information usually obtained from Kibana.

    """

    # kinds is a mapping from kind name to keys in self.data
    kinds = collections.OrderedDict ([
        ('dashboard', 'dashboards'),
        ('visualization', 'visualizations'),
        ('search', 'searches')
    ])

    def __init__(self):
        # Dictionary with a key per kind, which will store a dictionary
        # with all elements of that kind
        self.data = {}
        for kind, tag in self.kinds.items():
            self.data[tag] = {}

    def _get_store (self, kind):
        """Get the dictionary where elements of kind are stored.

        """

        return self.data[self.kinds[kind]]

    def _set_store (self, kind, elements):
        """Set the coontents of the dictionary where elements of kind are stored.

        """

        self.data[self.kinds[kind]] = elements

    def __str__(self):

        strs = ['  ' + self.kinds[kind] + ': ' + str(self._get_store(kind)) + '\n'
            for kind in self.kinds]
        return ''.join(strs)

    def get_dict(self):

        return self.data

    def find_index (self, kind, id, new_index=None):
        """Find the index in a visualization, and maybe change it.

        Don't change the index if not found, or if new_index is None

        :param      kind: Kind of element, {'visualization'|'search'}
        :param        id: Id of the element to inspect
        :param new_index: Name of the new index (default: None)
        :returns: Name of the index (or new index, if changed)

        """

        assert kind in ['visualization', 'search'], \
            'Not a valid kind "%s".' % kind
        data = self._get_store(kind)[id]
        meta = data['kibanaSavedObjectMeta']
        search = json.loads(meta['searchSourceJSON'])
        if 'index' in search:
            index = search['index']
            logging.info('Index for ' + kind + ' ' + id + ': ' + index)
            if new_index is not None:
                search['index'] = new_index
                data['kibanaSavedObjectMeta']['searchSourceJSON'] \
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
        for kind in ['visualization', 'search']:
            for element in self._get_store(kind):
                index = self.find_index(kind, element, new_index)
                if index is not None:
                    indices[index] = True
        for index in indices:
            print('Index:', index)

    def add_element (self, kind, name, element):
        """Add an element of a certain kind.

        :param    kind: kind of element
        :param    name: name of element
        :param element: element to add

        """

        assert kind in self.kinds, 'Not a valid kind "%s".' % kind
        elements = self._get_store(kind)
        elements[name] = element

    def get_element (self, kind, name):
        """Get an element of a certain kind.

        :param    kind: kind of element
        :param    name: name of element
        :returns: element

        """

        assert kind in self.kinds, 'Not a valid kind "%s".' % kind
        elements = self._get_store(kind)
        return elements[name]

    def get_ids_kind (self, kind):
        """Get the ids of all elements of a certain kind.

        :param    kind: kind of element
        :returns: list of ids

        """

        assert kind in self.kinds, 'Not a valid kind "%s".' % kind
        elements = self._get_store(kind)
        return elements.keys()

    def get_elements (self, dashboards):
        """Return an Elements object, with elements defined in dashboards.

        Acts as a filter, retuurning an Elements object with only the
        elements needed for dashboards.

        :param dashboards: list of dashboards whose elements to find
        :returns: Elements object

        """

        elements = Elements()
        dash_store = self._get_store('dashboard')
        if dashboards is None:
            dashboards = dash_store.keys()
        for dashboard in dashboards:
            logging.info('Preparing dashboard to return: ' + dashboard)
            element = dash_store[dashboard]
            elements.add_element('dashboard', dashboard, element)
            visualizations = visualizations_in_dashboard(document=element)
            for visualization in visualizations:
                vis_store = self._get_store('visualization')
                if visualization not in elements._get_store('visualization'):
                    logging.info('Preparing visualization to return: ' + visualization)
                    element = vis_store[visualization]
                    elements.add_element('visualization', visualization, element)
                    search = search_in_visualization(document=element)
                    if search and (search not in elements._get_store('search')):
                        search_store = self._get_store('search')
                        logging.info('Preparing search to return: ' + search)
                        element = search_store[search]
                        elements.add_element('search', search, element)
        return elements

def get_target(address):
    """Get the target object corresponding to address.

    The target object will be a ElasticSearch or File instance. Addreeses can
    be of the following kinds:

    Elasticsearch: "es elasticsearch_url [index]", such as "es http://localhost:9200"
    Kibana: "kb kibana_url [index]", such as "kb http://localhost:5601"
    File: "file file_name", such as "file dashboards.json"

    :param address: Address (list of str)
    :returns:       ElasticSearch or File object

    """

    logging.debug("Address: " + str(address))
    assert len(address) > 0, \
        'Address "%s" not valid, too few elements' % ' '.join(address)
    if address[0] in ('es', 'kb') :
        assert len(address) > 1, \
            'Address "%s" not valid for "es", too few elements' \
            % ' '.join(address)
        if address[0] == 'es':
            url = address[1]
        else:
            url = address[1] + '/elasticsearch'
        if len(address) > 2:
            index = address[2]
        else:
            index = '.kibana'
        target = Elastic(url=url, index=index)
    elif address[0] == 'file':
        if len(address) > 1:
            filename = address[1]
            target = File(filename)
        else:
            target = File()
    else:
        raise Exception('Address "%s" not valid, unrecognized target "%s"' \
            % (' '.join(address), address[0]))
    return target

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

    if args.dashboards:
        dashboards = args.dashboards
    else:
        dashboards = None
    if args.src:
        source = get_target(args.src)
        elements = source.retrieve(dashboards)
    else:
        elements = Elements()
    if args.dst:
        destination = get_target(args.dst)
        destination.save(elements, dashboards)

    exit()

    # If no args.file, use None, which will be interpreted as stdout or stdin
    if args.file:
        file = File(args.file)
    else:
        file = File()
    es = elasticsearch.Elasticsearch([args.es_url])
    elastic = Elastic(elasticsearch=es, index=args.kibana_index)
    if args.list is not None:
        elastic.list(kinds=args.list)
    if args.get is not None:
        elements = elastic.get(dashboards=args.get)
        if args.new_index:
            # Change indexes before saving
            elements.find_indices(args.new_index)
        file.save(elements=elements)
    if args.put is not None:
        elements = file.retrieve()
        elastic.put(dashboards=args.put, elements=elements)
    if args.list_indices:
        elements.find_indices()
