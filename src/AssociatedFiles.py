#! /usr/bin/env python


import json
import re
from timeit import default_timer
import logging

import getpass
import keyring
import requests

from queue import Queue
from threading import Thread

# Problem: Collection 3165 is too download all at once
# Solution: 
#   1) Use paginator to collect download urls in digestable chunks
#   2) Create a database (json) on the user side of all files associated with the collection
#   3) Make list of files to download and run the download

# Collect all files via paginator
#  With a size of 10 each request takes on average 42 seconds without any failures
#       there are 133415 pages so to collect the entire dataset it would take 64 days
#  With a size of 1000 each request takes on average 48 seconds without any failures
#       there are 13342 pages which would take 7.5 days
# size = 100
#   num_pages = 133415
#   avg_request_time = 42 seconds
#   total_download_time = 64 days
# size = 1000
#   num_pages = 13342
#   avg_request_time = 48 seconds
#   total_download_time = 7.5 days
# size = 10000 # starts to fail with timeout errors. Async might be necessary

def human_size(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(round(bytes, 2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])

def human_time(seconds):
    """ Returns time in seconds in a humand readable format """
    m, s = divmod(seconds, 60) # Equivalent to (seconds / 60, seconds % 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return str(f'{d:d}:{h:02d}:{m:02d}:{s:02d}')

def retry(func):
    MAX_TRIES = 3
    def _retry(*args, **kwargs):
        attempt = 1
        while attempt < MAX_TRIES + 1:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f'Exception thrown when attempting to request {args[0]}, attempt {attempt} of {MAX_TRIES}'
                       ' {e}')
                attempt += 1
        print(f'')
    return _retry

@retry
def get_data(url, auth):
    resp = requests.get(url, auth=auth)
    data = resp.json()
    return data

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
    
class QueryAssociatedFiles:
    
    def __init__(self):
        # User authentication info
        self.SERVICE_NAME = 'nda-tools'
        self.ndar_username = None # perronea
        self.ndar_password = None
        self.auth = self.get_auth()

        # Package metadata
        self.package_id = 1203969 # ABCC: 1203969 test: 1206127
        self.package_size = None # 168.57TB
        self.file_count = None # 12,573,784
        self.get_package_metadata()

        # Request parameters
        #self.thread_num = workerThreads if workerThreads else max([1, multiprocessing.cpu_count() - 1])
        self.num_workers = 5 # Number of parallel threads should be less than the number of NDA servers (maybe 9?)
        self.page_size = 1000 # 1000 seems to be the max

        # Get first page to setup parallel jobs and estimate run time
        print(f'Requesting first page from {self.package_id} to estimate run time.')
        start_time = default_timer()
        first_url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(self.package_id, 1, self.page_size)
        data = get_data(first_url, self.auth)
        end_time = default_timer()
        elapsed_time = "{:5.2f}s".format(end_time - start_time)

        last_url = data['_links']['last']['href']
        self.num_pages = self.extract_page_num(last_url)

        print(f'  Requesting 1 page from {self.package_id} took {elapsed_time}.')
        print(f'  There are {self.num_pages} pages of {self.page_size} associated files and {self.num_workers} parallel requests.')
        est_runtime = self.num_pages * (end_time - start_time) / self.num_workers
        print(f'  Estimated run time to collect a list of all associated files and metadata: {human_time(int(est_runtime))}')

        self.response_list = {'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(self.package_id, page, self.page_size): None for page in range(1, self.num_pages)}
        self.response_list[first_url] = data

        self.request_queue = Queue() # Work queue to track all urls that need to be requested
        self.associated_files = [] # Compile all results in a list once complete

    def get_auth(self):
        ndar_username = input('Enter your NIMH Data Archives username: ')
        try:
            ndar_password = keyring.get_password(self.SERVICE_NAME, ndar_username)
        except:
            ndar_passowrd = getpass.getpass('Enter your NIMH Data Archives password: ')
        return requests.auth.HTTPBasicAuth(ndar_username, ndar_password)


    def get_package_metadata(self):
        print(f'Collecting metadata on data package {self.package_id}')
        url = 'https://nda.nih.gov/api/package/{}'.format(self.package_id)
        data = get_data(url, self.auth)
        self.package_size = human_size(data['total_package_size']) # 168.57TB
        self.file_count = data['file_count'] # 12573784
        print(f'  Package ID {self.package_id} contains {self.file_count} files and is {self.package_size}')

    def threaded_request(self, q):
        while not q.empty():
            page, url = q.get()
            try:
                start_time = default_timer()
                self.response_list[url] = get_data(url, self.auth)
                elapsed_time = default_timer() - start_time
                completed_at = "{:5.2f}s".format(elapsed_time)
                print("{0:<30} {1:>20}".format(url, completed_at))
            except:
                self.response_list[url] = None
                print("{0:<30} {1:>20}".format(url, "FAILURE"))
            q.task_done()
        return True

    def extract_page_num(self, url):
            return int(re.findall('page=[0-9]+', url)[0].replace('page=', ''))

    def do_work(self):
        # Initialize queue for urls to request
        work_queue = Queue(maxsize=0)

        # Load queue with urls indexed by page
        for page in range(1, self.num_pages + 1):
            work_queue.put((page, 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(self.package_id, page, self.page_size)))

        START_TIME = default_timer()

        for i in range(self.num_workers):
            #logging.debug('Starting thread ', i)
            worker = Thread(target=self.threaded_request, args=(work_queue,))
            worker.setDaemon(True)
            worker.start()

        print("{0:<30} {1:>20}".format("Requested URL", "Completed at"))
        work_queue.join()
        print('All tasks completed in {}s'.format(default_timer() - START_TIME))

        return

    def save(self, var):
        with open('intermediate_results.pkl', 'wb') as f:
            pickle.dump(var, f)

    def join_db(self):
        # TODO: Join all requests into a single dataframe
        output = []
        for page in range(1, 10):
            url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, page, SIZE)
            output.extend(results[page]['results'])

        # TODO: Create hashmap of filename to download_url
        fn_download_map = {}
        for f in output:
            fn_download_map[f['download_alias']] = f['package_file_id']


        # TODO: Read in the datastructure manifest, create a list of all the associated files, extract out the filename and map to download_url
        ds_manifest_path = '/home/rando149/shared/code/internal/utilities/nda-abcd-s3-downloader/spreadsheets/datastructure_manifest.txt'
        ds_manifest = pd.read_csv(ds_manifest_path, sep='\t')


def main():
    #TODO
    return

if __name__ == '__main__':
    main()







