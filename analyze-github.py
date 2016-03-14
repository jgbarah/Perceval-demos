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
#import subprocess
import logging
import tempfile
import os
#import io
import elasticsearch
from datetime import datetime, timezone

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-r", "--repo",
                        help = "Git repo to analyze ('owner/project')")
    parser.add_argument("-e", "--es_url",
                        help = "ElasticSearch url (http://user:secret@host:port/res)")
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
                "updated_on": {"type":"date"}
                }
        }
    }
}

class Metadata(object):
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

class ElasticSearch_Sink (object):
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

    def enrich (self, item, id):
        """Upload item to ElasticSearch, using id.

        """

        res = self.es.index(index = self.index, doc_type = self.type,
                            id=id, body=item)
        logging.debug("Result: " + str(res))

def git_analysis (repo, dir, es, es_index):
    """Analyze the git repository.

    :param     repo: Name of the GitHub repository ('owner/repository')
    :param      dir: Directory for cloning the git repository
    :param       es: ElasticSearch object, ready to push data to it
    :param es_index: ElasticSearch index to use

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
    es_sink = ElasticSearch_Sink(es = es, index = "git-raw", type = "commit")

    for item in git_parser.fetch():
        logging.debug(item)
        item = metadata.enrich (item)
        es_sink.enrich (item, item["raw"]["commit"])


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
                            es = es, es_index = "git-raw")
