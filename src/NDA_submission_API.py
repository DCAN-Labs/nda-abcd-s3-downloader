#!/use/bin/env python3

import getpass
import requests
import json

def get_auth():
    # Securely get NDA username and password
    ndar_username = input('Enter your NIMH Data Archives username: ')
    ndar_password = getpass.getpass('Enter your NIMH Data Archives password: ')
    return(ndar_username, ndar_password)

def get_submission_ids(collection_id, status, own_submissions='false', ndar_username=None, ndar_password=None):
    """
    Given a collection ID and submission status, return a list of all submission IDs with that status
    
    :param collection_id: The ID of the collection you want to query
           status: Submission status ('Uploading',
                                      'Submitted', 
                                      'Processing', 
                                      'Error on Download',
                                      'Upload Completed',
                                      'Investigator Approved',
                                      'DAC Approved',
                                      'Submitted Prototype')
            own_submissions: If 'true' returns only submissions that the user submitted
                             If 'false' returns all submissions regardless of user
    :return: A list of submission_ids.
    """
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()

    submission_ids = []
    r = requests.get('https://nda.nih.gov/api/submission/?status={}&collectionId={}&usersOwnSubmissions={}'
.format(status, collection_id, own_submissions), auth=(ndar_username, ndar_password))
    for x in r.json():
        submission_ids.append(x['submission_id'])
    return submission_ids

def get_associated_files(submission_id, ndar_username=None, ndar_password=None):
    """
    Given a submission ID, return a list of all associated files included in the submission
    
    :param submission_id: The ID of the submission you want to query
    :return: A list of relative local file paths that were included in the submission.
    """
    if not ndar_username or not ndar_password:
        ndar_username, ndar_password = get_auth()

    associated_files = []
    r = requests.get('https://nda.nih.gov/api/submission/{}/files'.format(submission_id), auth=(ndar_username, ndar_password))
    for x in r.json():
        if x['file_type'] == 'Submission Associated File':
            associated_files.append(x['file_user_path'])
    return associated_files

def main():

    # Example use case    
    #print(get_submission_ids(3165, 'Uploading'))
    un, pw = get_auth()
    print(get_associated_files(39961, ndar_username=un, ndar_password=pw))

    return

if __name__ == '__main__':
    main()
