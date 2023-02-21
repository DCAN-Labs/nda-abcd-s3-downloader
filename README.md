# NDA Collection 3165 ABCD-BIDS Downloader

This tool can be used to download data from the DCAN Labs ABCD-BIDS Collection 3165 that is hosted on the NIMH Data Archive (NDA). This dataset includes ABCD BIDS inputs and derivatives from the abcd-hcp-pipeline, fMRIPrep, and QSIPrep.  All files are stored in Amazon Web Services (AWS) Simple Storage Service (S3) buckets and can only be accessed via the NDA API.

## Overview

Collection 3165 is a massive dataset containing unprocessed BIDS formatted structural, functional, and difusion MRI data as well as processed data (derivatives) from the abcd-hcp-pipeline, fMRIPrep, and QSIPrep. This dataset is roughly 168TB with over 13 million files and is rapidly growing in size as more data is acquired. Due to the unusual suze if this collection the typical method of downloading data from the NDA via nda-tools has proven to be time consuming and prone to failure. As a result we are providing the community with this utility that was inspired by the nda-tools but designed specifically to download data from Collection 3165.

In the past users have been able to download data from any collection they have access to directly from S3 by using the token generator created by the NDA. This method has been deprecated and users are now required to first create a Data Package containing their desired data and then download data from their Packages using the APIs that the NDA created.

We recognize that users often want to download a subset of this collection, whether it be the BIDS input data, the functional data processed with abcd-hcp-pipeline, the diffusion processed with QSIPrep or a more specific subset of data from a particular population of subjects. To accommodate we recommend users provide a text file containing a list of data subsets and subjects

## Requirements

### Make Data Package on NDA website

To use this downloader you must first have an NDA account and create a package containing the "DCAN Labs ABCD-BIDS MRI pipeline inputs and derivatives" collection from the [NDA website](https://ndar.nih.gov/):

1. Navigate to the [NDA website](https://ndar.nih.gov/)and login to your account.
2. On the top navigation bar click "Get Data".
3. On the side bar click "Data from Labs".
4. Search for "DCAN Labs ABCD-BIDS Community Collection (ABCC)" in the Text Search.
5. After clicking on the collection title click "Add to Cart" at the bottom.
6. It will take a minute to update the "Filter Cart" in the upper right corner. When that is done select "Package/Add to Study".
7. By default all data submissions associated with this collections are selected. Alternatively you can select specific submissions to include in your data package.
8. Click "Create Package", name your package accordingly, and then click "Create Package".
    - IMPORTANT: Make sure "Include associated data files" is *selected*
9. Navigate to your NDA dashboard and from your NDA dashboard, click DataPackages. You should see the data package that you just created with a status of "Creating Package". This package may take a day or two to be created on the NDA side if the entire collection has been included (roughly 168 TB). Take note of the "Package ID" - this is a required input for the downloader.

### datastructure_manifest.txt

The datastructure_manifest.txt is a metadata file associated with the data package that contains a list of S3 URLs for all of the data in the package. There is an API endpoint for this file that is currently under construction. Unfortunately the only way to download this file is by following the above instructions a second time but deselect "Include associated data files" when creating the Data Package.

### data_subsets.txt

In addition to the "datastructure_manifest.txt" you must also provide a list of the data subset types you wish to download.  For ease of use a list of all possible data subsets is provided with this repository: `data_subsets.txt`.  If you would only like a subset of all possible data subsets you should copy only the data subset types that you want into a new `.txt` file and point to that when calling `download.py` with the `-d` option.

### subject_list.txt

By default all data subsets specified in the data_subsets.txt for ALL subjects will be downloaded. If data from only a subset of subjects should be downloaded a .txt file with each unique BIDS formated subject ID on a new line must be provided to `download.py` with the `-s` option. Here is an example of what this file might look like for 3 subjects.

```shell
sub-NDARINVXXXXXXX
sub-NDARINVYYYYYYY
sub-NDARINVZZZZZZZ
```

### Python dependencies

A list of all necessary pip installable dependencies can be found in requirements.txt. To install run the following command:

```shell
python3 -m pip install -r requirements.txt --user
```

## Usage

For full usage documentation, type the following while inside your folder containing this cloned repository.

```shell
python3 download.py -h

usage: download.py [-h] -i S3_FILE -o OUTPUT [-s SUBJECT_LIST_FILE]
                   [-l LOG_FOLDER] [-d BASENAMES_FILE] [-c CREDENTIALS]
                   [-p CORES]

This python script takes in a list of data subsets and a list of
subjects/sessions and downloads the corresponding files from NDA using the
NDA provided AWS S3 links.

optional arguments:
  -h, --help            Show this help message and exit.
  -dp, --package        Package ID of the NDA data package.
  -m, --manifest
                        Path to the datstructure_manifest.txt file 
                        downloaded from the NDA containing s3 links for all 
                        subjects and their derivatives.
  -o, --output
                        Path to root folder which NDA data will be downloaded
                        into. A folder will be created at the given path if
                        one does not already exist.
  -s, --subject-list
                        Path to a .txt file containing a list of subjects for
                        which derivatives and inputs will be downloaded. By
                        default without providing input to this argument all
                        available subjects are selected.
  -l, --logs
                        Path to existent folder to contain your download
                        success and failure logs. By default, the logs are
                        output to your home directory: ~/
  -d, --data-subsets-file 
                        Path to a .txt file containing a list of all the data
                        subset names to download for each subject. By default
                        all the possible derivatives and inputs will be will
                        be used. This is the data_subsets.txt file included in
                        this repository. To select a subset it is recomended
                        that you simply copy this file and remove all the
                        basenames that you do not want.
  -wt, --workerThreads
                        Specifies the number of downloads to attempt in
                        parallel. For example, running downloadcmd -dp 12345
                        -wt 10 will cause the program to download a maximum
                        of 10 files simultaneously until all of the files from
                        package 12345 have been downloaded. A default value is
                        calculated based on the number of cpus found on the
                        machine, however a higher value can be chosen to
                        decrease download times. If this value is set too high
                        the download will slow. With 32 GB of RAM, a value of
                        10 is probably close to the maximum number of
                        parallel downloads that the computer can handle
```
