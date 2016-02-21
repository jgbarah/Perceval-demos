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
Compare directories

"""

import argparse
import filecmp
import difflib
import os
import perceval.backends
import subprocess
import logging

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-r", "--repo",
                        help = "Git repo to compare")
    parser.add_argument("-p", "--pkg",
                        help = "Source package to compare")
    parser.add_argument("-l", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    args = parser.parse_args()
    return args

def count_unique(dir, files):
    """Count unique files.

    Unique files are those that are only in one of the directories
    that are compared (left or right).

    :params dir: directory to count
    :params files: files in that directory
    :returns: tuple with number of files and total lines in those files

    """

    num_files = len(files)
    num_lines = 0
    for file in files:
        name = os.path.join(dir, file)
        if os.path.isfile(name):
            num_lines += sum(1 for line in open(name))
            logging.debug("Unique file: %s (lines: %d)" % (name, num_lines))
    logging.debug ("Unique files in dir %s: files: %d, lines: %d"
        % (dir, num_files, num_lines))
    return (num_files, num_lines)

def compare_files(file_left, file_right):
    """Compare two files.

    :params file_left: left file to compare
    :params file_right: left file to compare
    :returns: tuple with 1 (if different), 0 (if equal), lines added, removed

    """

    added = 0
    removed = 0
    with open(file_left,'r', encoding="ascii", errors="surrogateescape") as left, \
        open(file_right,'r', encoding="ascii", errors="surrogateescape") as right:
        diff = difflib.ndiff(left.readlines(), right.readlines())
        for line in diff:
            if line.startswith('+'):
                added += 1
            elif line.startswith('-'):
                removed += 1
    if (added + removed) > 0:
        diff = 1
    else:
        diff = 0
    return (diff, added, removed)

def count_common(dir_left, dir_right, files):
    """Count common files.

    Common files are those that are in both directories being compared
    (left or right).

    :params dir_left: left directory to count
    :params dir_right: right directory to count
    :params files: files in both directories
    :returns: tuple with number of diff files, and total lines added,
        removed in those files
    """

    added = 0
    removed = 0
    diff_files = 0
    for file in files:
        name_left = os.path.join(dir_left, file)
        name_right = os.path.join(dir_right, file)
        (diff, added_l, removed_l) = compare_files(name_left, name_right)
        diff_files += diff
        added += added_l
        removed += removed_l
    return (diff_files, added, removed)

def compare_dirs(dcmp):
    """Comparte two directories given their filecmp.dircmp object.

    :params dcmp: filecmp.dircmp object for directories to compare
    :returns: dictionary with differences

    """

    (left_files, left_lines) = count_unique(dir = dcmp.left,
                                            files = dcmp.left_only)
    (right_files, right_lines) = count_unique(dir = dcmp.right,
                                            files = dcmp.right_only)
    (diff_files, added_lines, removed_lines) \
        = count_common(dcmp.left, dcmp.right, dcmp.common_files)
    for sub_dcmp in dcmp.subdirs.values():
        (left_f, left_l, right_f, right_l, diff_f, added_l, removed_l) \
            = compare_dirs(sub_dcmp)
        left_files += left_f
        left_lines += left_l
        right_files += right_f
        right_lines += right_l
        diff_files += diff_f
        added_lines += added_l
        removed_lines += removed_l
    return (left_files, left_lines, right_files, right_lines,
        diff_files, added_lines, removed_lines)


if __name__ == "__main__":
    args = parse_args()
    if args.logging:
        log_format = '%(levelname)s:%(message)s'
        if args.logging == "info":
            logging.basicConfig(format=log_format, level=logging.INFO)
        elif args.logging == "debug":
            logging.basicConfig(format=log_format, level=logging.DEBUG)
    dcmp = filecmp.dircmp(args.repo, args.pkg)
    (left_files, right_files, diff_files,
        left_lines, right_lines, added_lines, removed_lines) \
        = compare_dirs(dcmp)
    print ("Files: %d, %d, %d, lines: %d, %d, %d, %d)"
        % (left_files, right_files, diff_files,
            left_lines, right_lines, added_lines, removed_lines))
    exit()
    git_class = perceval.backends.git.Git
    git_parser = git_class("git.log")
    for item in git_parser.fetch():
        print(item["commit"])
        print()
    exit()
