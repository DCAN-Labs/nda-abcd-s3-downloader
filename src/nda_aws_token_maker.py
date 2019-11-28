#! /usr/bin/env python3

import getpass
import os
import sys

try:
    from nda_aws_token_generator import *
except ImportError:
    from src.nda_aws_token_generator import *

if sys.version_info[0] < 3:
    # Python 2 specific imports
    input = raw_input
    from ConfigParser import ConfigParser
else:
    # Python 3 specific imports
    from configparser import ConfigParser


# Try to get NDA credentials from command line args passed in; if there are no
# args, then prompt user for credentials
if len(sys.argv) is 3:  # [0] is self, [1] is username, [2] is password, so 3
    username = sys.argv[1]
    password = sys.argv[2]
else:
    username = input('Enter your NIMH Data Archives username: ')
    password = getpass.getpass('Enter your NIMH Data Archives password: ')

web_service_url = 'https://nda.nih.gov/DataManager/dataManager'

generator = NDATokenGenerator(web_service_url)

try:
    token = generator.generate_token(username, password)
except Exception as e:
    print("Failed to create NDA token.")
    sys.exit(1)

# Read .aws/credentials from the user's HOME directory, add a NDA profile, and update with credentials
parser = ConfigParser()
parser.read(os.path.expanduser('~/.aws/credentials'))

if not parser.has_section('NDA'):
    parser.add_section('NDA')
parser.set('NDA', 'aws_access_key_id', token.access_key)
parser.set('NDA', 'aws_secret_access_key', token.secret_key)
parser.set('NDA', 'aws_session_token', token.session)

with open (os.path.expanduser('~/.aws/credentials'), 'w') as configfile:
    parser.write(configfile)

print('aws_access_key_id=%s\n'
      'aws_secret_access_key=%s\n'
      'security_token=%s\n'
      'expiration=%s\n'
      %(token.access_key,
        token.secret_key,
        token.session,
        token.expiration)
      )
