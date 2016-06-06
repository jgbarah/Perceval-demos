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

Example:

diff_test.py --repo git.repo -p git-2.7.0 --after 2016-02-01 --step 10 -l info

"""

import argparse
import filecmp
import difflib
import os
import perceval.backends
import subprocess
import logging
import io
import datetime

def parse_args ():
    """
    Parse command line arguments

    """
    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("-r", "--repo",
                        help = "Git repo to compare")
    parser.add_argument("-p", "--pkg",
                        help = "Source package to compare")
    parser.add_argument("-d", "--dpkg",
                        help = "Debian source package to compare (dsc file)")
    parser.add_argument("--after", type=str,
                        help = "Consider only commits after date (eg: 2016-01-31)")
    parser.add_argument("--before", type=str,
                        help = "Consider only commits before date (eg: 2016-01-31)")
    parser.add_argument("-l", "--logging", type=str, choices=["info", "debug"],
                        help = "Logging level for output")
    parser.add_argument("--logfile", type=str,
                        help = "Log file")
    parser.add_argument("--step", type=int, default=1,
                        help = "Step (compare every step commits, instead of all)")
    args = parser.parse_args()
    return args

def extract_dpkg(dpkg):
    """Extract Debian package.

    Extracts a Debian package, give its dsc file. The other components (the
    original file and the diff file should be in the same directory). This
    function assumes that dpkg-source is already installed and ready to run.

    :params   dpkg: dsc file for a Debian package
    :retursn: name of directory where the package was extracted.

    """

    dir = os.path.splitext(dpkg)[0]
    logging.info("Extracting Debian pkg in dir: " + dir)
    subprocess.call(["dpkg-source", "--extract", dpkg, dir],
                    stdout = subprocess.DEVNULL, stderr = subprocess.DEVNULL)
    return dir

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
            num_lines += sum(1 for line in open(name, encoding="ascii",
                                                errors="surrogateescape"))
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
    """Compare two directories given their filecmp.dircmp object.

    Produces as a result a dictionary with metrcis about the comparison:
     * left_files: number of files unique in left directory
     * right_files: number of files unique in right directory
     * diff_files: number of files present in both directories, but different
     * left_lines: number of lines for files unique in left directory
     * right_lines: number of lines for files unique in left directory
     * added_lines: number of lines added in files present in both directories
     * removed_lines: number of lines removed in files present in both directories

    added_lines, removed_lines refer only to files counted as diff_files

    :params dcmp: filecmp.dircmp object for directories to compare
    :returns: dictionary with differences

    """

    m = {}
    (m["left_files"], m["left_lines"]) \
        = count_unique(dir = dcmp.left, files = dcmp.left_only)
    (m["right_files"], m["right_lines"]) \
        = count_unique(dir = dcmp.right, files = dcmp.right_only)
    (m["diff_files"], m["added_lines"], m["removed_lines"]) \
        = count_common(dcmp.left, dcmp.right, dcmp.common_files)
    for sub_dcmp in dcmp.subdirs.values():
        m_subdir = compare_dirs(sub_dcmp)
        for metric, value in m_subdir.items():
            m[metric] += value
    return m


class Metrics:
    """Data structure for dealing with metrics related to commits.

    """

    def __init__(self, repo, dir):

        # List of commit hashes, ordered as returned by git
        self.commits = []
        # Dictionary with metrics, key is the commit number (order in commits)
        self.metrics = {}
        # Repository and directory to compare
        self.repo = repo
        self.dir = dir

    def add_commit(self, commit, date):
        """Add commit info to data structure.

        :params commit: hash of the commit
        :params date: commit date

        """

        self.commits.append([commit, date])

    def get_commit(self, seq_no):
        """Get a commit tuple (hash, date) for a given commit sequence.

        """

        return self.commits[seq_no]

    def num_commits(self):
        """Return the number of commits stored.

        """

        return len(self.commits)

    def compute_metrics(self, commit_no):
        """Compute metrics for commmit number (ordered as from git log).

        Checks out the corresponding commit in the git repository, and
        compute the metrics for its difference with the given package.
        The returned metrics are those produced by compare_dirs plus:
         * commit: hash for the commit
         * date: commit date for the commit (as a string)

        :params commits: list of all commits
        :params commmit_no: commit number (starting in 0)
        :returns: dictionary with metrics
        """

        commit = self.commits[commit_no]
        subprocess.call(["git", "-C", self.repo, "checkout", commit[0]],
                        stdout = subprocess.DEVNULL, stderr = subprocess.DEVNULL)
        dcmp = filecmp.dircmp(self.repo, self.dir)
        m = compare_dirs(dcmp)
        logging.debug ("Commit %s. Files: %d, %d, %d, lines: %d, %d, %d, %d)"
            % (commit[0], m["left_files"], m["right_files"], m["diff_files"],
            m["left_lines"], m["right_lines"],
            m["added_lines"], m["removed_lines"]))
        m["total_files"] = m["left_files"] + m["right_files"] + m["diff_files"]
        m["total_lines"] = m["left_lines"] + m["right_lines"] \
            + m["added_lines"] + m["removed_lines"]
        m["commit_seq"] = commit_no
        m["commit"] = commit[0]
        m["date"] = commit[1]
        return m

    def compute_range (self, first, last, step):
        """Compute metrics for a range of commits.

        Compute metrics for a range of commits, but only for those in the
        appropriate step.

        :params first: first commit to consider
        :params last: last commit to consider
        :params step: only consider commits coincident with step
        :returns: dictionary with (updated) metrics

        """

        for seq_no in list(range(first, last, step)) + [last]:
            logging.info("Computing metrics for %d." % seq_no)
            if seq_no not in self.metrics:
                m = self.compute_metrics(seq_no)
                logging.info(m)
                self.metrics[seq_no] = m

    def min_range (self, length, metric):
        """Find range of minimum values.

        Returns a range of minimum values. The range will have at least
        length values. In fact, a tuple with the lowest sequence number, and
        the maximum sequence number for the range, the sequence number for
        the minimum value, and the minimum value.

        :params length: length (number of values) of the range
        :params metric: name of the metric to consider for comparison
        :returns: tuple (min, max, min_index, min_value)

        """

        values = []
        indexes = []
        seq_commits = sorted(self.metrics)
        for seq_no in seq_commits:
            value = self.metrics[seq_no][metric]
            if len(values) <= length:
                # Still room, just add to lists
                values.append(value)
                indexes.append(seq_no)
            else:
                # Only worry if largest in lists is larger than value
                largest = max(values)
                if value < largest:
                    # Largest is larger. Remove it from lists
                    largest_index = values.index(largest)
                    values.pop(largest_index)
                    indexes.pop(largest_index)
                    # And now add value to the right
                    values.append(value)
                    indexes.append(seq_no)
            logging.info("values: " + str(values))
            logging.info("indexes " + str(indexes))
        min_value = min(values)
        min_index = indexes[values.index(min_value)]
        # Add next computed checkout on the left and on the right, just in case we're
        # on the edge of the checkouts we have computed
        if indexes[0] > seq_commits[0]:
            left_seq = seq_commits[seq_commits.index(indexes[0])-1]
            indexes.insert(0, left_seq)
            values.insert(0, self.metrics[left_seq][metric])
        if indexes[-1] < seq_commits[-1]:
            right_seq = seq_commits[seq_commits.index(indexes[-1])+1]
            indexes.append(right_seq)
            values.append(self.metrics[right_seq][metric])
        logging.info("values: " + str(values))
        logging.info("indexes " + str(indexes))
        return (indexes[0], indexes[-1], min_index, min_value)

    def metrics_items (self):
        """Iterator returning metrics for all computed commits.

        """

        return [self.metrics[seq_no] for seq_no in sorted(self.metrics)]
        #return self.metrics.values()

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

    if args.dpkg:
        dir = extract_dpkg(args.dpkg)
    else:
        dir = args.pkg
    metrics = Metrics(repo=args.repo, dir=dir)
    git_parser = perceval.backends.git.Git(uri=args.repo, gitpath=args.repo)
    from_date = datetime.datetime.strptime(args.after, '%Y-%m-%d')
    for item in git_parser.fetch(from_date = from_date):
        metrics.add_commit(item['data']['commit'], item['data']['CommitDate'])
    logging.info("%d commits parsed." % metrics.num_commits())

    left = 0
    right = metrics.num_commits()-1
    step = args.step
    while step >= 1:
        metrics.compute_range (left, right, step)
        (left, right, min_seq, min_value) = metrics.min_range(3, "total_lines")
        logging.info("Step: %d, left: %d, right: %d, min. seq: %d, min. value: %d."
                    % (step, left, right, min_seq, min_value))
        step = step // 2
    min_commit = metrics.get_commit(min_seq)
    print ("Most similar checkout: %d (diff: %d), date: %s, hash: %s." %
            (min_seq, min_value, min_commit[1], min_commit[0]))
    print ("commit_seq", "commit", "date", "total_files", "total_lines",
        "left_files", "right_files", "diff_files",
        "left_lines", "right_lines", "added_lines", "removed_lines",
        sep=",", flush=True)
    for m in metrics.metrics_items():
        print(m["commit_seq"], m["commit"], m["date"],
            m["total_files"], m["total_lines"],
            m["left_files"], m["right_files"], m["diff_files"],
            m["left_lines"], m["right_lines"], m["added_lines"], m["removed_lines"],
            sep=",", flush=True)
