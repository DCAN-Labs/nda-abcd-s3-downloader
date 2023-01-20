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
# size = 10000

page = 1
size = 5000
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









