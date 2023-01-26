#! /usr/bin/env python


import requests
import json
import getpass
import keyring
from requests.auth import HTTPBasicAuth
from time import time
import timeit

# Problem: Collection 3165 is too download all at once
# Solution: 
#   1) Use paginator to collect download urls in digestable chunks
#   2) Create a db on the user side that can be queried to download
#       only the desired images
#   3) Make list of files to download and run the download

def get_auth():
    # Securely get NDA username and password
    ndar_username = input('Enter your NIMH Data Archives username: ')
    ndar_password = getpass.getpass('Enter your NIMH Data Archives password: ')
    return(ndar_username, ndar_password)

SERVICE_NAME = 'nda-tools'
ndar_username = input('Enter your NIMH Data Archives username: ')
try:
    ndar_password = keyring.get_password(SERVICE_NAME, ndar_username)
except:
    ndar_password = getpass.getpass('Enter your NIMH Data Archives password: ')

package_id = 1203969


def retry(func, retries=5):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(2)
                attempts += 1
        print('{} retries'.format(attempts))
    return retry_wrapper
                
@retry
def get_data(url, ndar_username, ndar_password):
    r = requests.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password))
    return r.json()


# Get metrics on data package
url = 'https://nda.nih.gov/api/package/{}'.format(package_id)
data = get_data(url, ndar_username, ndar_password)
#response = requests.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password))
#data = response.json()
package_size = data['total_package_size'] # 168.57TB
num_files = data['file_count'] # 12573784

def human_size(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(round(bytes, 2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])

print("Package ID {} contains {} files and is {}".format(package_id, num_files, human_size(package_size)))

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

page = 1
size = 1000
#url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(package_id, page, size)
#data = get_data(url, ndar_username, ndar_password)

#next_url = data['_links']['next']['href']
#curr_page = 
#last_url = data['_links']['last']['href']

def timer(func):
    def timer_wrapper(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print('Function {} executed in {}s'.format(func.__name__, t2-t1))
        return result
    return timer_wrapper

limit = 5
files = []
page = 1
url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(package_id, page, size)
while True:
    print('-----')
    print('Requesting {}'.format(url))
    t1 = time()
    data = get_data(url, ndar_username, ndar_password)
    t2 = time()
    print('Request took {}s'.format(t2 - t1))
    files.extend(data['results'])
    url = data['_links']['next']['href']
    last_url = data['_links']['last']['href']
    page += 1
    if page >= limit:
        break

#response = requests.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password))
#data = response.json()

import asyncio
from timeit import default_timer
from concurrent.futures import ThreadPoolExecutor
import re

START_TIME = default_timer()

PACKAGE_ID = 1203969
SIZE = 1000
first_url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, 1, SIZE)
data = get_data(first_url, ndar_username, ndar_password)
last_url = data['_links']['last']['href']
def extract_page_num(url):
    return int(re.findall('page=[0-9]+', url)[0].replace('page=', ''))
num_pages = extract_page_num(last_url)

output = [(first_url, data['results'])]

def sync_request(session, i):
    url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, i, SIZE)
    with session.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password)) as response:
        data = response.json()
        if response.status_code != 200:
            print("FAILURE::{0}".format(url))
        return data

def start_sync_process():
    with requests.session() as session:
        print("{0:<30} {1:>20}".format("No", "Completed at"))
        start_time = default_timer()
        for i in range(2,5):
            sync_request(session, i)
            elapsed_time = default_timer() - start_time
            completed_at = "{:5.2f}s".format(elapsed_time)
            print("{0:<30} {1:>20}".format(i, completed_at))
    

def async_request(session, i):
    start_time = default_timer()
    url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, i, SIZE)
    with session.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password)) as response:
        if response.status_code != 200:
            print("{0:<30} {1:>20}".format(i, "FAILURE"))
            return(url, None)
        data = response.json()
        elapsed_time = default_timer() - start_time
        completed_at = "{:5.2f}s".format(elapsed_time)
        print("{0:<30} {1:>20}".format(i, completed_at))
        return(url, data['results'])
    
async def start_async_process(num_workers, num_pages, output):
    print("{0:<30} {1:>20}".format("No", "Completed at"))
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    async_request,
                    *(session, i)
                )
                for i in range(1, num_pages)
            ]
            for response in await asyncio.gather(*tasks):
                output.append(response)
    return output

START_TIME = default_timer()
n_pages = 20
n_workers = 5
loop = asyncio.get_event_loop()
future = asyncio.ensure_future(start_async_process(n_workers, n_pages, output))
files = loop.run_until_complete(future)
elapsed_time = default_timer() - START_TIME
print('Total time for {0} pages: {1}'.format(n_pages, "{:5.2f}s".format(elapsed_time)))
#files = loop.run_until_complete(start_async_process(5, 10, output))


# Upper limit on API requests seems to be approximately 5 workers and size=1000

from asyncio import Queue

@asyncio.coroutine
def async_request(i, work_queue):
    task_start_time = default_timer()
    url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, i, SIZE)
    r = requests.get(url, auth=HTTPBasicAuth(ndar_username, ndar_password))
    if response.status_code != 200:
        print("{0:<30} {1:>20}".format(i, "FAILURE"))
        return(url, None)
    data = response.json()
    elapsed_time = default_timer() - task_start_time
    completed_at = "{:5.2f}s".format(elapsed_time)
    print("{0:<30} {1:>20}".format(i, completed_at))
    return(url, data['results'])

queue = Queue()
[queue.put_nowait(page) for page in range(2,10)]

import logging
from queue import Queue
from threading import Thread
from time import time

SERVICE_NAME = 'nda-tools'
NDAR_USERNAME = input('Enter your NIMH Data Archives username: ')
try:
    NDAR_PASSWORD = keyring.get_password(SERVICE_NAME, ndar_username)
except:
    NDAR_PASSWORD = getpass.getpass('Enter your NIMH Data Archives password: ')

PACKAGE_ID = 1203969
SIZE = 1250

def threaded_request(q, results):
    while not q.empty():
        page, url = q.get()
        try:
            start_time = default_timer()
            r = requests.get(url, auth=HTTPBasicAuth(NDAR_USERNAME, NDAR_PASSWORD))
            data = r.json()
            results[url] = data
            elapsed_time = default_timer() - start_time
            completed_at = "{:5.2f}s".format(elapsed_time)
            print("{0:<30} {1:>20}".format(url, completed_at))
        except:
            results[url] = None
            print("{0:<30} {1:>20}".format(url, "FAILURE"))
        q.task_done()
    return True

q = Queue(maxsize=0)
num_threads = 6

results = {'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, page, SIZE): None for page in range(1, num_pages)}

# Load queue with urls indexed by page
for page in range(1, 10):
    q.put((page, 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(PACKAGE_ID, page, SIZE)))

START_TIME = default_timer()

for i in range(num_threads):
    logging.debug('Starting thread ', i)
    worker = Thread(target=threaded_request, args=(q, results))
    worker.setDaemon(True)
    worker.start()

print("{0:<30} {1:>20}".format("Requested URL", "Completed at"))
q.join()
print('All tasks completed in {}s'.format(default_timer() - START_TIME))




class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            page = self.queue.get()
            try:
                threaded_request(page)
            finally:
                self.queue.task_done()

from threading import Thread

queue = Queue()
num_threads = 5
for _ in range(num_threads):
    #worker = DownloadWorker(queue)
    worker = Thread(target=threaded_request, args=(queue,))
    worker.daemon = True
    worker.start()

for i in range(1, num_pages):
    logger.info('Queueing {}'.format(i))
    queue.put(i)
queue.join()
logging.info('Completed in {')






