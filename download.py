#!/usr/bin/env python3
"""
ABCD-BIDS Downloader

"""

__doc__ = """
This python script takes in a list of data subsets and a list of 
subjects/sessions and downloads the corresponding files from NDA 
using the NDA's provided AWS S3 links.
"""

import os
import sys
import argparse
import logging

from src.Downloader import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

HOME = os.path.expanduser("~")
HERE = os.path.dirname(os.path.abspath(sys.argv[0]))

def generate_parser():

    parser = argparse.ArgumentParser(
        prog='download.py',
        description=__doc__
    )
    parser.add_argument(
        '-dp', '--package', metavar='<package-id>', type=str, action='store',
        help='Flags to download all S3 files in package. Required.')
    parser.add_argument(
        "-m", "--manifest", dest="manifest_file", type=str, required=True,
        help=("Path to the .csv file downloaded from the NDA containing s3 links "
              "for all subjects and their derivatives.")
    )
    parser.add_argument(
       "-o", "--output", dest="output", type=str, required=True,
        help=("Path to root folder which NDA data will be downloaded into.  "
              "A folder will be created at the given path if one does not "
              "already exist.")
    )
    parser.add_argument(
        "-s", "--subject-list", dest="subject_list_file", type=str, required=False,
        help=("Path to a .txt file containing a list of subjects for which derivatives and "
              "inputs will be downloaded. By default without providing input to this "
              "argument all available subjects are selected.")
    )
    parser.add_argument(
        "-l", "--logs", dest="log_folder", type=str, required=False,
        default=HOME,
        help=("Path to existent folder to contain your download success and failure logs.  "
              "By default, the logs are output to your home directory: ~/")
    )
    parser.add_argument(
        "-b", "--basenames-file", dest='basenames_file', type=str, required=False,
        default = os.path.join(HERE, 'data_subsets.txt'),
        help=("Path to a .txt file containing a list of all the data basenames names to download "
              "for each subject.  By default all the possible derivatives and inputs will be will "
              "be used.  This is the data_subsets.txt file included in this repository.  "
              "To select a subset it is recomended that you simply copy this file and remove all "
              "the basenames that you do not want.")
    )
    parser.add_argument(
        "-r", "--resume", dest="resume", action="store_true", required=False,
        default = False,
        help=("Flag to resume a previous download.  If this flag is set, the script will read "
              "the {log_folder}/.progress_report.csv file to determine which files have "
              "already been downloaded and will skip those files.")
    )
    parser.add_argument(
        '-wt', '--workerThreads', metavar='<thread-count>', type=int, action='store',
        help='''Specifies the number of downloads to attempt in parallel. For example, running 'downloadcmd -dp 12345 -wt 10' will 
    cause the program to download a maximum of 10 files simultaneously until all of the files from package 12345 have been downloaded. 
    A default value is calculated based on the number of cpus found on the machine, however a higher value can be chosen to decrease download times. 
    If this value is set too high the download will slow. With 32 GB of RAM, a value of '10' is probably close to the maximum number of 
    parallel downloads that the computer can handle''')

    return parser

def main():
    parser = generate_parser()
    args = parser.parse_args()

    ABCC_Downloader = Downloader(args)

if __name__ == "__main__":  

    main()