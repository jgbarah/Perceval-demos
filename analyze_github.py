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
Analyze a GitHub repository (git, issues, pull requests)

Example:

analyze-github.py --repo MetricsGrimoire/Bicho

"""

import argparse
import perceval.backends
import logging
import tempfile
import os
import elasticsearch
from datetime import datetime, timezone
import email.utils

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-r", "--repo",
                        help = "Git repo to analyze ('owner/project')")
    parser.add_argument("-e", "--es_url",
                        help = "ElasticSearch url (http://user:secret@host:port/res)")
    parser.add_argument("-i", "--es_index",
                        help = "ElasticSearch index prefix")
    parser.add_argument("-l", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    parser.add_argument("--logfile", type=str,
                        help = "Log file")
    args = parser.parse_args()
    return args

mapping_commit = {
    "properties" : {
        "metadata": {
            "properties": {
                "updated_on": {"type":"date"},
                "author":{"type":"string",
                          "index":"not_analyzed"}
                }
        }
    }
}

class Chains (object):
    """Chains (directed graph) of enrichers.

    """

    def __init__ (self):

        # All data structures are based on nodes, which are integers,
        # starting at 0 (next node is the next integer to use as node id)
        self.last_node = 0
        # Dictorionary for graph, each key is a node holding as data the
        # list of nodes that follow it.
        self.graph = {}
        # Dictionary for nodes, each key is a node holding as data the object
        # for that node
        self.nodes = {}
        # List of starting nodes
        self.start = []

    def attach (self, object, point = None):
        """Attach a new object to the graph, returns attach point for next).

        If point is None (or no point), attach at the beginning.

        :param object: Object to attach
        :param  point: Attach point

        """

        node = self.last_node
        self.last_node = self.last_node + 1
        self.nodes[node] = object
        if point is None:
            self.start.append(node)
        else:
            self.graph[point].append(node)
        self.graph[node] = []
        return node

    def run_nodes (self, nodes, input):
        """Run all nodes from these nodes onwards.

        """

        for next in nodes:
            logging.debug ("Running node: " + str(next) + str(self.nodes[next]))
            output = self.nodes[next].enrich (input)
            self.run_nodes (self.graph[next], output)

    def run (self, input):

        self.run_nodes (self.start, input)

class Enricher (object):
    """Root of enricher classes.

    Each enricher will perrom some transformation on the input, producing
    some output.

    """

    def enrich (self, item):

        raise Exception ("Should be overriden by child class")

class Metadata(Enricher):
    """Class to add metadata to some item.
    """

    def __init__ (self, metadata):
        """Use metadata to init class.

        :param metadata: Dictionary with common metadata for all items.

        """

        self.metadata = metadata

    def enrich (self, item):
        """Enrich item with metadata.

        :param item: Input item
        :returns:    Output item

        """

        metadata = self.metadata
        metadata["updated_on"] = datetime.now(timezone.utc)
        document = {"raw": item,
                    "metadata": metadata}
        return document

class Elastic_Sink (Enricher):
    """Sink for uploading data to ElasticSearch.
    """

    def __init__ (self, es, index, type):
        """Create a ElasticSearch sink for uploading items.

        """

        self.es = es
        self.index = index
        self.type = type
        try:
            es.indices.delete(self.index)
        except elasticsearch.exceptions.NotFoundError:
            # Index could not be deleted because it was not found. Ignore.
            logging.info("Could not delete index, it was not found: " \
                        + self.index)
        self.es.indices.create(self.index,
                                {"mappings": {"commit": mapping_commit}})

    def _id (self, item):

        raise Exception ("Should be overriden by child class")

    def enrich (self, item):
        """Upload item to ElasticSearch, using id.

        """

        # Check surrogate escaping, and remove it if needed
        try:
            res = self.es.index(index = self.index, doc_type = self.type,
                                id = self._id(item), body = item)
        except UnicodeEncodeError as e:
            if e.reason == 'surrogates not allowed':
                logging.debug ("Surrogate found in: " + body)
                body = body.encode('utf-8', "backslashreplace").decode('utf-8')
                res = self.es.index(index = self.index, doc_type = self.type,
                                    id = self._id(item), body = item)
            else:
                raise
        logging.debug("Result: " + str(res))

class Elastic_Sink_Commit_Raw (Elastic_Sink):
    """Elastic sink for raw commits.
    """

    def _id (self, item):
        """Commit id (hash) is a good unique id.
        """

        return item["raw"]["commit"]

class Elastic_Sink_Commit_Rich (Elastic_Sink):
    """Elastic sink for rich commits.
    """

    def _id (self, item):
        """Commit id (hash) is a good unique id.
        """

        return item["commit"]

class Filter(Enricher):
    """Class to filter some fields from an item.
    """

    def __init__ (self, filter, default = {}):
        """Init class.

        Both filter and default are dictionaries, keyed by input fields to
        filter. In the case of filter, values are names to use as output
        fields. In case of default, values are default values to use when
        the corresponding field does not exist.

        :param filter: Values to filter
        :param default: Default values

        """

        self.filter = filter
        self.default = default

    def enrich (self, item):
        """Enrich item with metadata.

        If any field to filter is not in item, the default value will be
        used, if defined (None otherwise).

        :param item: Input item
        :returns:    Output item

        """

        output = {}
        for old, new in self.filter.items():
            try:
                output[new] = item[old]
            except KeyError:
                if old in self.default:
                    output[new] = self.default[old]
                else:
                    output[new] = None
        return output

class Fix_Dates(Enricher):
    """Class to convert RFC 2822 dates to aware datetime.
    """

    def __init__ (self, fields):
        """Init class.

        """

        self.fields = fields
        self.next = []

    def enrich (self, item):
        """Convert specified fields to datetime.

        :param item: Input item
        :returns:    Output item

        """

        output = item
        for field in self.fields:
            output[field] = email.utils.parsedate_to_datetime(item[field])
        return output


def git_analysis (repo, dir, es, es_index):
    """Analyze the git repository.

    :param     repo: Name of the GitHub repository ('owner/repository')
    :param      dir: Directory for cloning the git repository
    :param       es: ElasticSearch object, ready to push data to it
    :param es_index: Prefix for ElasticSearch index to use

    """

    git_repo = "https://github.com/" + repo + ".git"
    logging.debug("Using temporary directory: " + dir)
    git_parser = perceval.backends.git.Git(uri=git_repo,
                                            gitpath=os.path.join(dir, repo))
    metadata = Metadata ({"retriever": "Perceval",
                        "backend_name": "git",
                        "backend_version": "0.1.0",
                        "origin": git_repo})
    logging.info("Parsing git log output...")
    # Define enrichers
    raw_sink = Elastic_Sink_Commit_Raw(es = es, index = es_index + "-git-raw",
                                            type = "commit")
    commit_filter = Filter (filter = {"commit": "commit",
                                        "Author": "author",
                                        "Commit": "committer",
                                        "AuthorDate": "author_date",
                                        "CommitDate": "committer_date",
                                        "message": "message"})
    fix_dates = Fix_Dates (["author_date", "committer_date"])
    rich_sink = Elastic_Sink_Commit_Rich(es = es, index = es_index + "-git-rich",
                                                type = "commit")
    chains = Chains()
    # Compose chain for raw commits
    point = chains.attach (object = metadata)
    chains.attach (object = raw_sink, point = point)
    # Compose chain for rich commits
    point = chains.attach (object = commit_filter)
    point = chains.attach (object = fix_dates, point = point)
    chains.attach (object = rich_sink, point = point)
    # Run chains for each commit parsed
    for item in git_parser.fetch():
        logging.debug(item)
        chains.run(item)

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

    repo = args.repo
    es = elasticsearch.Elasticsearch([args.es_url])
    with tempfile.TemporaryDirectory() as tmpdir:
        items = git_analysis(repo = repo, dir = tmpdir,
                            es = es, es_index = args.es_index)
