#! /usr/bin/env python3

"""
Derivatives Downloader
Anders Perrone: perronea@ohsu.edu
Created 2019-11-05
Last Updated 2019-11-05
"""

__doc__ = """
This python script takes in a list of derivatives and a list of subjects/sessions
and downloads the corresponding derivatives using the s3 links 
"""

__version__ = "0.0.0"

import os
import sys
import subprocess
import argparse
from glob import glob
from datetime import datetime
import configparser
import pandas as pd
from cryptography.fernet import Fernet
from getpass import getpass

PWD = os.getcwd()
NDA_CREDENTIALS = os.path.join(os.path.expanduser("~"), ".abcd2bids", "config.ini")
NDA_AWS_TOKEN_MAKER = os.path.join(PWD, "src", "nda_aws_token_maker.py")

def generate_parser():

    parser = argparse.ArgumentParser(
        prog='derivatives_downloaders',
        description=__doc__
    )
    parser.add_argument(
        "--s3-spreadsheet", "-x", dest="s3_file", type=str, required=True,
        help=("Path to the .csv file downloaded from the NDA containing s3 links  "
              "for all subjects and their derivatives.")
    )
    parser.add_argument(
        "--subject-list", "-s", dest="subject_list_file", type=str, required=False,
        help=("Path to a .txt file containing a list of subjects for which        "
              "derivatives and inputs will be downloaded. By default if no")
    )
    parser.add_argument(
        "--derivatives-file", "-d", dest='basenames_file', type=str, required=False,
        default=os.path.join(os.path.dirname(os.path.realpath(__file__)),'derivative_basenames.txt'),
        help=("Path to a .txt file containing a list of all the derivative basenames to download"
              "for each subject. By default all the possible derivatives and inputs will be will"
              "be used. This is the derivative_basename.txt file included in this repository.   "
              "To select a subset it is recomended that you simply copy this file and remove all"
              "the basenames that you do not want.")
    )
    parser.add_argument(
       "--output", "-o", dest="output", type=str,required=True,
        help=("Path to folder which NDA data will be downloaded "
              "into. By default, data will be downloaded into the {} folder. "
              "A folder will be created at the given path if one does not "
              "already exist.".format(PWD))
    )
    parser.add_argument(
        "--credentials", "-c", dest='credentials', default=NDA_CREDENTIALS, required=False,
        help=("Path to config file with NDA credentials. If no "
              "config file exists at this path yet, then one will be created. "
              "Unless this option or --username and --password is added, the "
              "user will be prompted for their NDA username and password. "
              "By default, the config file will be located at {}".format(NDA_CREDENTIALS))
    )

    return parser

def _cli():

    #Command line interface
    parser = generate_parser()
    args = parser.parse_args()

    date_stamp = "{:%Y:m:d %H:%M}".format(datetime.now())

    make_nda_token(args)

    print('Derivatives downloader called at %s with:' % date_stamp)
    print('\ts3 Spreadsheet:    %s' % args.s3_file)

    manifest_df = pd.read_csv(args.s3_file)

    subject_list = get_subject_list(manifest_df, args.subject_list_file)
    print('\tDerivative basenames file:  %s' % args.basenames_file)
    manifest_names = generate_manifest_list(args.basenames_file, subject_list)

    s3_links_arr = manifest_df[manifest_df['MANIFEST_NAME'].isin(manifest_names)]['ASSOCIATED_FILES'].values 

    download_s3_files(s3_links_arr, args.output)



def get_subject_list(manifest_df, subject_list_file):
    """
    If a list of subject is provided then use that, else collect all uniq
    subject ids from the s3 spreadsheet and use that instead
    :param manifest_df: pandas dataframe created from the s3 csv
    :param subject_list_file: cli path to file containing list of subjects
    :return: subject_list
    """
    subject_list = []
    # If path to list of subjects provided use that
    if subject_list_file:
        print('\tSubject list:      %s' % subject_list_file)
        subject_list = [line.rstrip('\n') for line in open(subject_list_file)]
    # Otherwise get all subjects from the 23 spreadsheet
    else:
        print('\tSubject list:      All subjects')
        for manifest_name in manifest_df['MANIFEST_NAME'].values:
            subject_id = manifest_name.split('.')[0]
            if subject_id not in subject_list:
                subject_list.append(subject_id)
    return subject_list


def generate_manifest_list(basenames_file, subject_list):
    """
    Take the list of subjects and list of basenames and concatenate them to
    match the ${SUBJECT}.${BASENAME}.manifest.json to match the manifest name
    in the s3 file.
    :param args: argparse namespace containing all CLI arguments. The specific
    arguments used by this function are
    :return: manifest_names: list of MANIFEST_NAME
    """

    # if a subject list is not provided
    basenames = [line.rstrip('\n') for line in open(basenames_file)]

    manifest_names = []
    for sub in subject_list:
        for base in basenames:
            manifest = sub + '.' + base + '.manifest.json'
            manifest_names += [manifest]
    return manifest_names

def check_download_log(s3_fname, success_log):
    with open(success_log) as f:
        successs = f.readlines()
    for fname in successs:
        if s3_fname in fname:
            return True
    return False

def download_s3_files(s3_links_arr, output_dir):
    """
    
    """
    success_log = os.path.join(output_dir, 'succesful_downloads.txt')
    failed_log = os.path.join(output_dir, 'failed_downloads.txt') 
    bad_download = []
    for s3_path in s3_links_arr:
        if "dataset_description.json" in s3_path or "README" in s3_path or "CHANGES" in s3_path:
            continue
        else:
            # Check if the filename already in the success log
            if check_download_log('/'.join(s3_path.split('/')[4:]), success_log):
                print("{} Already downloaded".format(os.path.basename(s3_path)))
            else: 
                dest = os.path.join(output_dir, '/'.join(s3_path.split('/')[4:]))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                aws_cmd = ["aws", "s3", "cp", s3_path, dest, "--profile", "NDA"]
                try:
                    subprocess.call(aws_cmd)
                    with open(success_log, 'a+') as s:
                        s.write('/'.join(s3_path.split('/')[4:]) + '\n')
                except:
                    print("Error downloading: {}".format(s3_path))
                    bad_download.append(s3_path)
                    with open(failed_log, 'a+') as f:
                        f.write('/'.join(s3_path.split('/')[4:]) + '\n')

def make_nda_token(args):
    """
    Create NDA token by getting credentials from config file. If no config file
    exists yet, or user specified to make a new one by entering their NDA
    credentials as CLI args, then create one to store NDA credentials.
    :param args: argparse namespace containing all CLI arguments. The specific
    arguments used by this function are --username, --password, and --config.
    :return: N/A
    """
    # If config file with NDA credentials exists, then get credentials from it,
    # unless user entered other credentials to make a new config file
    if os.path.exists(args.credentials):
        username, password = get_nda_credentials_from(args.credentials)

    # Otherwise get NDA credentials from user & save them in a new config file,
    # overwriting the existing config file if user gave credentials as cli args
    else:

        # If NDA username was a CLI arg, use it; otherwise prompt user for it
        username = input("\nEnter your NIMH Data Archives username: ")

        # If NDA password was a CLI arg, use it; otherwise prompt user for it
        password = getpass("Enter your NIMH Data Archives password: ")

        make_config_file(args.credentials, username, password)

    # Try to make NDA token
    token_call_exit_code = subprocess.call((
        "python3",
        NDA_AWS_TOKEN_MAKER,
        username,
        password
    ))

    # If NDA credentials are invalid, tell user so without printing password.
    # Manually catch error instead of using try-except to avoid trying to
    # catch another file's exception.
    if token_call_exit_code is not 0:
        print("Failed to create NDA token using the username and decrypted "
              "password from {}.".format(os.path.abspath(args.credentials)))
        sys.exit(1)

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


def make_config_file(config_filepath, username, password):
    """
    Create a config file to save user's NDA credentials.
    :param config_filepath: Name and path of config file to create.
    :param username: User's NDA username to save in config file.
    :param password: User's NDA password to encrypt then save in config file.
    :return: N/A
    """
    # Object to read/write config file containing NDA credentials
    config = configparser.ConfigParser()

    # Encrypt user's NDA password by making an encryption key
    encryption_key = Fernet.generate_key()
    encrypted_password = (
        Fernet(encryption_key).encrypt(password.encode("UTF-8"))
    )

    # Save the encryption key and encrypted password to a new config file
    config["NDA"] = {
        "username": username,
        "encrypted_password": encrypted_password.decode("UTF-8"),
        "key": encryption_key.decode("UTF-8")
    }
    if not os.path.exists(os.path.dirname(config_filepath)):
        os.makedirs(os.path.dirname(config_filepath))
    with open(config_filepath, "w") as configfile:
        config.write(configfile)

    # Change permissions of the config file to prevent other users accessing it
    subprocess.check_call(("chmod", "700", config_filepath))

if __name__ == '__main__':

    _cli()

    print('\nAll done!')






