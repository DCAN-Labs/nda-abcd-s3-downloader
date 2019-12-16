#! /usr/bin/env python3

"""
ABCD-BIDS Downloader

Created   11/05/2019  Anders Perrone (perronea@ohsu.edu)
Modified  12/12/2019  Eric Earl (earl@ohsu.edu)
"""

__doc__ = """
This python script takes in a list of data subsets and a list of 
subjects/sessions and downloads the corresponding files from NDA 
using the NDA's provided AWS S3 links.
"""

import argparse
import configparser
import os
import re
import sys
import threading

from cryptography.fernet import Fernet
from datetime import datetime
from functools import partial
from getpass import getpass
from glob import glob
from multiprocessing.dummy import Pool
from pandas import read_csv
from subprocess import call, check_call


class RepeatTimer(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


HERE = os.path.dirname(os.path.abspath(sys.argv[0]))
HOME = os.path.expanduser("~")
NDA_CREDENTIALS = os.path.join(HOME, ".abcd2bids", "config.ini")
NDA_AWS_TOKEN_MAKER = os.path.join(HERE, "src", "nda_aws_token_maker.py")

def generate_parser():

    parser = argparse.ArgumentParser(
        prog='ABCD-BIDS Downloader',
        description=__doc__
    )
    parser.add_argument(
        "-i", "--input-s3", dest="s3_file", type=str, required=True,
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
        "-l", "--subject-list", dest="subject_list_file", type=str, required=False,
        help=("Path to a .txt file containing a list of subjects for which derivatives and "
              "inputs will be downloaded. By default all available subjects are selected.")
    )
    parser.add_argument(
        "-g", "--logs", dest="log_folder", type=str, required=False,
        default=HOME,
        help=("Path to existent folder to contain your download success and failure logs.  "
              "By default, the logs are output to: {}".format(HOME))
    )
    parser.add_argument(
        "-d", "--data-subsets-file", dest='basenames_file', type=str, required=False,
        default = os.path.join(HERE, 'data_subsets.txt'),
        help=("Path to a .txt file containing a list of all the data subset names to download "
              "for each subject.  By default all the possible derivatives and inputs will be will "
              "be used.  This is the data_subsets.txt file included in this repository.  "
              "To select a subset it is recomended that you simply copy this file and remove all "
              "the basenames that you do not want.")
    )
    parser.add_argument(
        "-c", "--credentials", dest='credentials', required=False,
        default = NDA_CREDENTIALS,
        help=("Path to config file with NDA credentials. If no "
              "config file exists at this path yet, one will be created.  "
              "Unless this option or --username and --password is added, the "
              "user will be prompted for their NDA username and password.  "
              "By default, the config file will be located at {}".format(NDA_CREDENTIALS))
    )
    parser.add_argument(
        "-p", "--parallel-downloads", dest="cores", type=int, required=False,
        default = 1,
        help=("Number of parallel downloads to do.  Defaults to 1 (serial downloading).")
    )

    return parser

def _cli():

    #Command line interface
    parser = generate_parser()
    args = parser.parse_args()

    date_stamp = "{:%Y:%m:%d %H:%M}".format(datetime.now())

    print('Derivatives downloader called at %s with:' % date_stamp)

    make_nda_token(args.credentials)

    # start an hourly thread ( 60 * 60 = 3600 seconds) to update the NDA download token
    t = RepeatTimer(3600, make_nda_token, [args.credentials])
    t.start()

    print('\tLog folder:\t%s' % args.log_folder)

    print('\tS3 Spreadsheet:\t%s' % args.s3_file)
    manifest_df = read_csv(args.s3_file)
    subject_list = get_subject_list(manifest_df, args.subject_list_file)

    print('\tData Subsets:\t%s' % args.basenames_file)
    manifest_names = generate_manifest_list(args.basenames_file, subject_list)

    print('\nReading in S3 links...')
    s3_links_arr = manifest_df[manifest_df['MANIFEST_NAME'].isin(manifest_names)]['ASSOCIATED_FILES'].values 

    bad = download_s3_files(s3_links_arr, args.output, args.log_folder, args.cores)

    print('\nProblematic commands:')
    for baddy in bad:
        print(baddy)


def get_subject_list(manifest_df, subject_list_file):
    """
    If a list of subject is provided then use that, else collect all unique
    subject ids from the s3 spreadsheet and use that instead
    :param manifest_df: pandas dataframe created from the s3 csv
    :param subject_list_file: cli path to file containing list of subjects
    :return: subject_list
    """
    subject_list = set()

    if subject_list_file:
        print('\tSubjects:\t%s' % subject_list_file)
        subject_list = [line.rstrip('\n') for line in open(subject_list_file)]

    # Otherwise get all subjects from the S3 spreadsheet
    else:
        print('\tSubjects:\tAll subjects')
        for manifest_name in manifest_df['MANIFEST_NAME'].values:
            subject_id = manifest_name.split('.')[0]
            subject_list.add(subject_id)

    return list(subject_list)


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


def download_s3_files(s3_links_arr, output_dir, log_dir, pool_size=1):
    """
    
    """

    bad_download = []
    commands = []

    # correct this typo, "succesful", not "successful", at a later date
    success_log = os.path.join(log_dir, 'succesful_downloads.txt')
    failed_log = os.path.join(log_dir, 'failed_downloads.txt')
    only_one_needed = [
        "CHANGES",
        "dataset_description.json",
        "README",
        "task-MID_bold.json",
        "task-nback_bold.json",
        "task-rest_bold.json",
        "task-SST_bold.json",
        "Gordon2014FreeSurferSubcortical_dparc.dlabel.nii",
        "HCP2016FreeSurferSubcortical_dparc.dlabel.nii",
        "Markov2012FreeSurferSubcortical_dparc.dlabel.nii",
        "Power2011FreeSurferSubcortical_dparc.dlabel.nii",
        "Yeo2011FreeSurferSubcortical_dparc.dlabel.nii"
    ]
    only_one_tuple = list(zip([0]*len(only_one_needed), only_one_needed))

    if os.path.isfile(success_log):
        with open(success_log) as f:
            success_set = set(f.readlines())
    else:
        success_set = set()

    download_set = set()

    print('Creating unique download list...')
    for s3_link in s3_links_arr:

        if s3_link[:4] != 's3:/':
            s3_path = 's3:/' + s3_link
        else:
            s3_path = s3_link

        dest = os.path.join(output_dir, '/'.join(s3_path.split('/')[4:]))

        skip = False
        for i, only_one_pair in enumerate(only_one_tuple):
            only_one_count = only_one_pair[0]
            only_one = only_one_pair[1]

            if only_one in s3_path:
                if only_one_count == 0:
                    only_one_tuple[i] = (1, only_one)
                else:
                    skip = True

                break

        if not skip and s3_path not in success_set:
            # Check if the filename already in the success log
            dest = os.path.join(output_dir, '/'.join(s3_path.split('/')[4:]))

            if not os.path.isfile(dest):
                download_set.add( (s3_path, dest) )

    # make unique s3 downloads
    print('Creating download commands...')
    for s3_path, dest in sorted(download_set, key=lambda x: x[1]):
        commands.append( ' ; '.join( [
                "mkdir -p " + os.path.dirname(dest),
                "aws s3 cp " + s3_path + " " + dest + " --profile NDA"
            ] )
        )

    if pool_size == 1:
        print('\nDownloading files serially...')
    elif pool_size > 1:
        print('\nParallel downloading with %d core(s)...' % pool_size)
    elif pool_size < 1:
        print('\nCannot download with less than 1 core.  Try changing your "-p" argument.  Quitting...')
        sys.exit()

    pool = Pool(pool_size) # pool_size concurrent commands at a time
    for i, returncode in enumerate(pool.imap(partial(call, shell=True), commands)):
        s3_path = re.search('.+aws\ s3\ cp\ (s3://.+)\ ' + output_dir + '.+', commands[i]).group(1)
        if returncode == 0:
            with open(success_log, 'a+') as s:
                s.write(s3_path + '\n')
        else:
            print( "Command failed: {}".format(commands[i]) )

            bad_download.append(s3_path)
            with open(failed_log, 'a+') as f:
                f.write(s3_path + '\n')

            bad_download.append(commands[i])

    pool.close()

    return bad_download

def make_nda_token(credentials):
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
    if os.path.exists(credentials):
        username, password = get_nda_credentials_from(credentials)

    # Otherwise get NDA credentials from user & save them in a new config file,
    # overwriting the existing config file if user gave credentials as cli args
    else:

        # If NDA username was a CLI arg, use it; otherwise prompt user for it
        username = input("\nEnter your NIMH Data Archives username: ")

        # If NDA password was a CLI arg, use it; otherwise prompt user for it
        password = getpass("Enter your NIMH Data Archives password: ")

        make_config_file(credentials, username, password)

    # Try to make NDA token
    token_call_exit_code = call((
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
              "password from {}.".format(os.path.abspath(credentials)))
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
    check_call( ("chmod", "700", config_filepath) )

if __name__ == '__main__':

    _cli()

    print('\nABCD-BIDS Downloader completed!')

