from __future__ import absolute_import, with_statement

import datetime
import functools
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
import traceback

import boto3
import requests
from requests.adapters import HTTPAdapter
import requests.packages.urllib3.util

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    from io import open
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

logger = logging.getLogger(__name__)


if sys.version_info[0] < 3:
    input = raw_input
else:
    pass


class Protocol(object):
    CSV = "csv"
    XML = "xml"
    JSON = "json"

    @staticmethod
    def get_protocol(cls):
        return cls.JSON

class DeserializeHandler():

    @staticmethod
    def none(r):
        return r

    @staticmethod
    def convert_json(r):
        return json.loads(r.text)

class HttpErrorHandlingStrategy():
    # error handling implementation methods. Each method takes a response object
    @staticmethod
    def ignore(r):
        pass

    @staticmethod
    def print_and_exit(r):
        # handle json and plain-text errors
        message = None
        try:
            if 'content-type' in r.headers and 'json' in r.headers['Content-Type']:
                message = r.json()['message']
        except (ValueError, json.JSONDecodeError):
            message = r.text

        if r.status_code == 401:
            # provide default message if one doesnt already exist
            message = message or 'The NDA username or password is not recognized.'
        else:
            message = '\nAn unexpected error was encountered and the program could not continue. Error message from service was: \n%s' % message
        logger.error(message)
        exit_client()

    @staticmethod
    def reraise_status(response):
        response.raise_for_status()

def exit_client(signal=signal.SIGTERM, frame=None, message=None):
    for t in threading.enumerate():
        try:
            t.shutdown_flag.set()
        except AttributeError as e:
            continue
    if message:
        logger.info('\n\n{}'.format(message))
    else:
        logger.info('\n\nExit signal received, shutting down...')
    os._exit(1)

def sanitize_file_path(file):
    """
    Replaces backslashes with forward slashes and removes leading / or drive:/.
    :param file: Relative or absolute filepath
    :return: Relative or absolute filepath with leading / or drive:/ removed
    """
    # Sanitize all backslashes (\) with forward slashes (/)
    file_key = file.replace('\\', '/').replace('//', '/')
    # If Mac/Linux full path
    if re.search(r'^/.+$', file):
        file_key = file.split('/', 1)[1]
    # If Windows full path
    elif re.search(r'^\D:/.+$', file_key):
        file_key = file_key.split(':/', 1)[1]
    return file_key

def check_read_permissions(file):
    try:
        open(file)
        return True
    except (OSError, IOError) as err:
        if err.errno == 13:
            logger.info('Permission Denied: {}'.format(file))
    return False

def get_error():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    ex = ''.join(tbe.format_exception_only())
    return 'Error: {}'.format(ex)

def get_traceback():
    exc_type, exc_value, exc_tb = sys.exc_info()
    tbe = traceback.TracebackException(
        exc_type, exc_value, exc_tb,
    )
    tb = ''.join(tbe.format())
    return tb

# return bucket and key for url (handles http and s3 protocol)
def deconstruct_s3_url(url):
    tmp = urlparse(url)
    if tmp.scheme == 's3':
        bucket = tmp.netloc
        path = tmp.path.lstrip('/')
    elif tmp.scheme == 'https':
        # presigned urls are either https://bucketname.s3.amazonaws.com/key... or
        # https://s3.amazonaws.com/bucket/path
        if tmp.hostname == 's3.amazonaws.com':
            bucket = tmp.path.split('/')[1]
            path = '/'.join(tmp.path.split('/')[2:])
            path = '/' + path
        else:
            bucket = tmp.hostname.replace('.s3.amazonaws.com', '')
            path = tmp.path
    else:
        raise Exception('Invalid URL passed to deconstruct_s3_url method: {}'.format(url))

    return bucket, path.lstrip('/')

# converts . and .. and ~ in file-paths. (as well as variable names like %HOME%
def convert_to_abs_path(file_name):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(file_name)))

def human_size(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(round(bytes, 2)) + units[0] if bytes < 1024 else human_size(bytes / 1024, units[1:])

def retry_connection_errors(func):
    @functools.wraps(func)
    def _retry(*args, **kwargs):
        tmp = None
        for i in range(10):
            try:
                tmp = func(*args, **kwargs)
                return tmp
            except requests.exceptions.ConnectionError as e:
                if i == 9:
                    raise e
                time.sleep(random.randint(10, 30))
    return _retry

def is_json(test):
    try:
        json.dumps(test)
        return True
    except:
        return False

@retry_connection_errors
def _send_prepared_request(prepped, timeout=150, deserialize_handler=DeserializeHandler.convert_json, error_handler=HttpErrorHandlingStrategy.print_and_exit):
    with requests.Session() as session:
        logger.debug('{} {} @ {}'.format(prepped.method , prepped.url, datetime.datetime.now()))
        session.mount(prepped.url, HTTPAdapter(max_retries=10))
        tmp = session.send(prepped, timeout=timeout)
        logger.debug('{} {} (elapsed = {})- STATUS {}'.format(prepped.method, prepped.url, tmp.elapsed, tmp.status_code))
        if not tmp.ok:
            error_handler(tmp)
    return deserialize_handler(tmp)

def get_request(url, headers=None, auth=None, _json=None, error_handler=HttpErrorHandlingStrategy.print_and_exit):
    tmp = None
    for i in range(10):
        try:
            with requests.Session() as session:
                session.mount(url, HTTPAdapter(max_retries=10))
                tmp = session.get(url, headers=headers, auth=auth, json=_json)
                if not tmp.ok:
                    error_handler(tmp)
            return tmp
        except requests.exceptions.ConnectionError as e:
            if i == 9:
                raise e
            time.sleep(random.randint(10, 30))

def post_request(url, _json, headers=None, auth=None, error_handler=HttpErrorHandlingStrategy.print_and_exit):
    tmp = None
    for i in range(10):
        try:
            with requests.Session() as session:
                session.mount(url, HTTPAdapter(max_retries=10))
                tmp = session.post(url, json=_json, headers=headers, auth=auth)
                if not tmp.ok:
                    error_handler(tmp)
            return tmp
        except requests.exceptions.ConnectionError as e:
            if i == 9:
                raise e
            time.sleep(random.randint(10, 30))

def get_data_and_header_params(payload, headers):
    data_param = {}
    if 'content-type' not in headers:
        if isinstance(payload, dict) or isinstance(payload, list):
            # setting the json arg will automatically add the content-header
            # https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
            data_param ={'json': payload}
        else:
            data_param = {'data': payload}
            if isinstance(payload, str) and is_json(payload) and 'content-type' not in headers:
                headers['content-type'] = 'application/json'
    else:
        data_param = {'data': payload}
    return data_param, headers

