#!/usr/bin/env python3

import os
import sys
import json
import subprocess
import requests
from requests.adapters import HTTPAdapter, Retry
import configparser
from cryptography.fernet import Fernet
from NDA_submission_API import get_auth, get_submission_ids, get_associated_files

# To resume a stalled upload we must create a space delimited list of all directories containing the associated files for the submission

#ndar_username, ndar_password = get_auth()

#complete_uploads = get_submission_ids(3165, 'Upload Completed', ndar_username=ndar_username, ndar_password=ndar_password)

def get_dataset(submission_id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()
    # TODO implement retry. Sometimes requests fail due to [Errno 101] Network is unreachable
    s = requests.session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[101])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    r = s.get('https://nda.nih.gov/api/submission/{}'.format(submission_id), auth=(ndar_username, ndar_password))
    dataset = r.json()
    return dataset

def get_dataset_title(id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()
    dataset = get_dataset(id, ndar_username=ndar_username, ndar_password=ndar_password)
    dataset_title = dataset['dataset_title']
    return dataset_title

def get_all_datasets(collection_id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()

    complete_uploads = get_submission_ids(collection_id, 'Upload Completed', ndar_username=ndar_username, ndar_password=ndar_password)
    
    datasets = []
    for id in complete_uploads:
        dataset = get_dataset(id, ndar_username=ndar_username, ndar_password=ndar_password)
        datasets.append(dataset)
        print(dataset)

    return datasets

def get_s3_links(submission_id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()

    r = requests.get('https://nda.nih.gov/api/submission/{}/files'.format(submission_id), auth=(ndar_username, ndar_password))

    s3_links = []
    for x in r.json():
        if x['file_type'] == 'Submission Associated File':
            s3_links.append(x['file_remote_path'])
    
    return s3_links

def download_s3_link(s3_link, s3_config, output_dir):

    # To get the output path combine the relative path after submission_xxxxx with the output_dir
    rel_path = '/'.join(s3_link.split('/')[4:])
    output_path = os.path.join(output_dir, rel_path)

    s3_cmd = ['s3cmd', '--config', s3_config, '--no-progress', 'get', s3_link, output_path]

    download_cmd_exit_code = subprocess.call(s3_cmd)

    if download_cmd_exit_code != 0:
        print('ERROR: Failed to download {}'.format(s3_link))

    return

def create_submission_record(record_file, collection_id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()
    # Get a list of all submissions ids that have completed uploading
    submission_ids = get_submission_ids(collection_id, 'Upload Completed', ndar_username=ndar_username, ndar_password=ndar_password)
    with open(record_file, 'w+') as f:
        for submission_id in submission_ids:
            # Dump each dataset dict into the records file
            dataset = get_dataset(submission_id, ndar_username=ndar_username, ndar_password=ndar_password)
            json.dump(dataset, f)
            f.write('\n')

    return

def update_submission_record(record_file, collection_id, ndar_username=None, ndar_password=None):
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()
    # Read all datasets currently in the record file and format them as list of dicts
    with open(record_file, 'r+') as f:
        lines = f.readlines()
        datasets = [json.loads(line.rstrip()) for line in lines]
    print('There are {} submissions in the given record file'.format(len(datasets)))
    # Get a list of all submissions ids that have completed uploading
    submission_ids = get_submission_ids(collection_id, 'Upload Completed', ndar_username=ndar_username, ndar_password=ndar_password)
    print('There are {} new submissions that will be added to the given record file'.format(len(submission_ids) - len(datasets)))
    # Iterate through the list of submissions backward (as they are listed sequentially by date)
    new_datasets = []
    for submission_id in reversed(submission_ids):
        dataset = get_dataset(submission_id, ndar_username=ndar_username, ndar_password=ndar_password)
        # Prepend to new list if not already in the file (to maintain sequential order), else assume that all subsequent records have already been added and exit
        if dataset not in datasets:
            new_datasets.insert(0, dataset)
        else:
            # Double check that all submission ids have been added.
            if len(datasets) + len(new_datasets) == len(submission_ids):
                print('Record files has been updated successfully. Exiting.')
                break
            else:
                print('There are {} total submission IDs in this collection, but {} datasets. Please resolve this issue'.format(len(submission_ids), len(datasets) + len(new_datasets)))
                return
    with open(record_file, 'a+') as f:
        for dataset in new_datasets:
            json.dump(dataset, f)
            f.write('\n')
    return


def get_nda_credentials_from(config_file_path):
    """
    Given the path to a config file, returns user's NDA credentials.
    :param config_file_path: Path to file containing user's NDA username,
    encrypted form of user's NDA password, and key to that encryption.
    :return: Two variables: user's NDA username and password.
    """
    # Object to read/write config file containing NDA credentials
    config = configparser.ConfigParser()
    config.read(config_file_path)

    # Get encrypted password and encryption key from config file
    encryption_key = config["NDA"]["key"]
    encrypted_password = config["NDA"]["encrypted_password"]

    # Decrypt password to get user's NDA credentials
    username = config["NDA"]["username"]
    password = (
        Fernet(encryption_key.encode("UTF-8"))
        .decrypt(token=encrypted_password.encode("UTF-8"))
        .decode("UTF-8")
    )

    return username, password


def main():

    # Submission 33317 has 942 images
    # The first image took 16.5s to download and was 0.3337GB
    # Estimated total run time = 16.5s(942)(1min/60s)(1hr/60min) = 4.3hr
    # Estimate total disk usage = 0.3337GB(942) = 314GB

    # Get NDA username and password from config file created during abcd2bids
    CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".abcd2bids", "config.ini")
    un, pw = get_nda_credentials_from(CONFIG_FILE)

    # Get submission_id from command line
    submission_id = sys.argv[1]
    output_dir = sys.argv[2]

    # Get s3 links from submission
    s3_links = get_s3_links(submission_id, ndar_username=un, ndar_password=pw)
   
    print('There are {} s3 links'.format(len(s3_links)))

    # Hope s3 config is up to date and will stay valid throughout all the downloads
    s3_config = '/panfs/roc/groups/3/rando149/shared/code/internal/utilities/nda-abcd-s3-downloader/src/.s3cfg-ndar'

    for s3_link in s3_links:
        download_s3_link(s3_link, s3_config, output_dir)

    return

if __name__ == '__main__':
    main()


        

