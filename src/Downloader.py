#!/usr/bin/env python3

__doc__ = """
This python script takes in a list of data subsets and a list of 
subjects/sessions and downloads the corresponding files from NDA 
using the NDA's provided AWS S3 links.
"""

import getpass
import keyring
import requests
import argparse
import pandas as pd
from queue import Queue
from threading import Thread
import multiprocessing

from utils import *

logger = logging.getLogger(__name__)

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
        '-wt', '--workerThreads', metavar='<thread-count>', type=int, action='store',
        help='''Specifies the number of downloads to attempt in parallel. For example, running 'downloadcmd -dp 12345 -wt 10' will 
    cause the program to download a maximum of 10 files simultaneously until all of the files from package 12345 have been downloaded. 
    A default value is calculated based on the number of cpus found on the machine, however a higher value can be chosen to decrease download times. 
    If this value is set too high the download will slow. With 32 GB of RAM, a value of '10' is probably close to the maximum number of 
    parallel downloads that the computer can handle''')

    return parser

class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as e:
                # An exception happened in this thread
                logger.info(str(e))
                logger.info(get_traceback())

            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads, queue_size=None):
        queue_size = queue_size or num_threads * 100
        self.tasks = Queue(queue_size)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()

class Downloader:

    def __init__(self, args):

        user_auth = Authenticator()
        self.auth = user_auth.auth
        # ID of data package that was created by user on the NDA
        self.package_id = args.package
        self.package_url = 'https://nda.nih.gov/api/package'

        # Datastructure manifest that is automatically included in the data package (TODO: Download instead of input)
        self.manifest = pd.read_csv(args.manifest_file,'\t')

        # List of data subsets that the user intends to download
        self.data_basenames = args.basenames_file

        # List of subjects
        self.subject_list_file = args.subject_list_file
        self.subject_list = self.get_subject_list()

        # Create a list of the manifest names
        self.manifest_names = self.generate_manifest_list()

        self.s3_links_arr = self.manifest[self.manifest['manifest_name'].isin(self.manifest_names)]['associated_file'].values

        self.thread_num = args.workerThreads if args.workerThreads else max([1, multiprocessing.cpu_count() - 1])
        self.download_queue = Queue()

    @staticmethod
    def request_header():
        return {'content-type': 'application/json'}
    
    def get_subject_list(self):
        """
        If a list of subject is provided then use that, else collect all unique
        subject ids from the s3 spreadsheet and use that instead
        :param manifest_df: pandas dataframe created from the s3 csv
        :param subject_list_file: cli path to file containing list of subjects
        :return: subject_list
        """
        subject_list = set()

        if self.subject_list_file:
            print('\tSubjects:\t%s' % self.subject_list_file)
            subject_list = [line.rstrip('\n') for line in open(self.subject_list_file)]

        # Otherwise get all subjects from the S3 spreadsheet
        else:
            print('\tSubjects:\tAll subjects')
            for manifest_name in self.manifest['manifest_name'].values:
                subject_id = manifest_name.split('.')[0]
                subject_list.add(subject_id)

        return list(subject_list)

    def generate_manifest_list(self):
        """
        Take the list of subjects and list of basenames and concatenate them to
        match the ${SUBJECT}.${BASENAME}.manifest.json to match the manifest name
        in the s3 file.
        :param args: argparse namespace containing all CLI arguments. The specific
        arguments used by this function are
        :return: manifest_names: list of manifest_name
        """

        # if a subject list is not provided
        basenames = [line.rstrip('\n') for line in open(self.data_basenames)]

        manifest_names = []
        for sub in self.subject_list:
            for base in basenames:
                manifest = sub + '.' + base + '.manifest.json'
                manifest_names += [manifest]
        return manifest_names

    def generate_download_file_ids(self):
        batch_size = self.thread_num
        while True:
            files = self.get_package_files_by_s3_url()

    def get_package_files_by_s3_url(self):
        url = self.package_url + '/{}/files'.format(self.package_id)



class Authenticator:

    def __init__(self):
        self.service_name = 'nda-tools'
        self.auth = self.get_auth()

    def get_auth(self):
        self.ndar_username = input('Enter your NIMH Data Archives username: ')
        try:
            self.ndar_password = keyring.get_password(self.service_name, self.ndar_username)
        except:
            self.ndar_passowrd = getpass.getpass('Enter your NIMH Data Archives password: ')
        return requests.auth.HTTPBasicAuth(self.ndar_username, self.ndar_password)

def main():
    parser = generate_parser()
    args = parser.parse_args()

    ABCC_Downloader = Downloader(args)

if __name__ == "__main__":  

    main()



