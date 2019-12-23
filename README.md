# ABCD HCP BIDS App Derivatives Downloader

This tool can be used to download derivatives from the abcd-hcp-pipeline BIDS App. The derivatives are hosted by the NDA on an aws s3 bucket. 


## Usage

To use this downloader you must first have an NDA account and acquire `collection_3165_manifest_s3_links.csv` from the [NDA's website](https://ndar.nih.gov/)

You must also provide a list of the derivative types you wish to download. For simplicity a list of all possible derivatives is provided with this repository: `basename_derivatives.txt`. If you would only like a subset of derivatives it is recommended that you copy only the derivative types that you want into a new .txt file and point to that when calling `derivatives_downloader.py`.

```
usage: ABCD-BIDS Downloader [-h] -i S3_FILE -o OUTPUT [-l SUBJECT_LIST_FILE]
                            [-g LOG_FOLDER] [-d BASENAMES_FILE]
                            [-c CREDENTIALS] [-p CORES]

This python script takes in a list of data subsets and a list of
subjects/sessions and downloads the corresponding files from NDA using the
NDA's provided AWS S3 links.

optional arguments:
  -h, --help            show this help message and exit
  -i S3_FILE, --input-s3 S3_FILE
                        Path to the .csv file downloaded from the NDA
                        containing s3 links for all subjects and their
                        derivatives.
  -o OUTPUT, --output OUTPUT
                        Path to root folder which NDA data will be downloaded
                        into. A folder will be created at the given path if
                        one does not already exist.
  -l SUBJECT_LIST_FILE, --subject-list SUBJECT_LIST_FILE
                        Path to a .txt file containing a list of subjects for
                        which derivatives and inputs will be downloaded. By
                        default all available subjects are selected.
  -g LOG_FOLDER, --logs LOG_FOLDER
                        Path to existent folder to contain your download
                        success and failure logs. By default, the logs are
                        output to: {USER HOME}
  -d BASENAMES_FILE, --data-subsets-file BASENAMES_FILE
                        Path to a .txt file containing a list of all the data
                        subset names to download for each subject. By default
                        all the possible derivatives and inputs will be will
                        be used. This is the data_subsets.txt file included in
                        this repository. To select a subset it is recomended
                        that you simply copy this file and remove all the
                        basenames that you do not want.
  -c CREDENTIALS, --credentials CREDENTIALS
                        Path to config file with NDA credentials. If no config
                        file exists at this path yet, one will be created.
                        Unless this option or --username and --password is
                        added, the user will be prompted for their NDA
                        username and password. By default, the config file
                        will be located at
                        {USER HOME}/.abcd2bids/config.ini
  -p CORES, --parallel-downloads CORES
                        Number of parallel downloads to do. Defaults to 1
                        (serial downloading).

``` 

