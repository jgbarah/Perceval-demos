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
Analyze a git repository (url or git log file)

Example:

analyze-github.py --repo git_log_file

"""

import argparse
import analyze_github
import perceval.backends
import elasticsearch
import logging
import os.path

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-r", "--repo",
                        help = "Url of git repo to analyze")
    parser.add_argument("-p", "--gitpath",
                        help = "Git path to git repo or log file to analyze")
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

def git_analysis (repo, gitpath, es, es_index):
    """Analyze a git repository.

    :param     repo: url of the git repo
    :param  gitpath: Directory for cloning the git repository, or git log file
    :param       es: ElasticSearch object, ready to push data to it
    :param es_index: Prefix for ElasticSearch index to use

    """

    git_parser = perceval.backends.git.Git(uri=repo, gitpath=gitpath)
    metadata = analyze_github.Metadata ({"retriever": "Perceval",
                        "backend_name": "git",
                        "backend_version": "0.1.0",
                        "origin": repo})
    logging.info("Parsing git log output...")
    # Define enrichers
    raw_sink = analyze_github.Elastic_Sink_Commit_Raw(es = es, index = es_index + "-git-raw",
                                            type = "commit")
    commit_filter = analyze_github.Filter (filter = {"commit": "commit",
                                        "Author": "author",
                                        "Commit": "committer",
                                        "AuthorDate": "author_date",
                                        "CommitDate": "committer_date",
                                        "message": "message"},
                                        default = {"message": ""})
    fix_dates = analyze_github.Fix_Dates (["author_date", "committer_date"])
    rich_sink = analyze_github.Elastic_Sink_Commit_Rich(es = es, index = es_index + "-git-rich",
                                                type = "commit")
    chains = analyze_github.Chains()
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

    es = elasticsearch.Elasticsearch([args.es_url])
    items = git_analysis(repo = args.repo, gitpath = args.gitpath,
                        es = es, es_index = args.es_index)
