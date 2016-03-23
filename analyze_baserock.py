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

description = """Find the description of a Baserock package

Example:

analyze_baserock.py --strata core --pkg git-minimal --logging info

Before running, clone the definitions git repository:

git clone git://git.baserock.org/baserock/baserock/definitions.git baserock-definitions

"""

import argparse
import logging
import yaml
import subprocess

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-s", "--strata", type=str,
                        help="Baserock strata")
    parser.add_argument("-p", "--pkg", type=str,
                        help="Baserock package (within strata)")
    parser.add_argument("-log", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    parser.add_argument("--logfile", type=str,
                        help = "Log file")
    args = parser.parse_args()
    return args


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
    strata_template = 'baserock-definitions/strata/%s.morph'
    strata_file = strata_template % args.strata
    logging.debug('Reading strata file: ' + strata_file)
    with open(strata_file, 'r') as strata_fp:
        strata = yaml.safe_load(strata_fp)
    for chunk in strata['chunks']:
        if args.pkg == chunk['name']:
            pkg_data = chunk
    logging.debug('Info about pkg: ' + str(pkg_data))
    repo_name = pkg_data['repo']
    if repo_name == 'upstream:git':
        repo_url = 'http://git.baserock.org/cgit/delta/' \
            + repo_name.replace('upstream:', '', 1) + '.git'
        repo_ref = pkg_data['ref']
        logging.info('Repo: ' + repo_name + ', ' + repo_url + ', ' + repo_ref)
        repo_dir = 'baserock:' + repo_name
        git_clone_cmd = ['git', 'clone', repo_url, repo_dir]
        logging.debug('Running ' + ' '.join(git_clone_cmd))
        subprocess.call(git_clone_cmd)
        git_ckeckout_cmd = ['git', '-C', repo_dir, 'checkout', repo_ref]
        logging.debug('Running ' + ' '.join(git_ckeckout_cmd))
        subprocess.call(git_ckeckout_cmd)
    else:
        logging.info('Repo: ' + repo_name)
