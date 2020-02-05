# NDA Collection 3165 ABCD-BIDS Downloader

This tool can be used to download BIDS inputs and derivatives from the DCAN Labs ABCD-BIDS collection 3165 from the NIMH Data Archive (NDA).  All files are hosted by the NDA in Amazon Web Services (AWS) Simple Storage Service (S3) buckets.

## Requirements

### datastructure_manifest.txt 


To use this downloader you must first have an NDA account and download the "DCAN Labs ABCD-BIDS MRI pipeline inputs and derivatives" collection from the [NDA website](https://ndar.nih.gov/):
1.  Navigate to the [NDA website](https://ndar.nih.gov/)
1.  Under "Get Data" select "Get Data beta"
1.  On the side bar select "Data from Labs"
1.  Search for "DCAN Labs ABCD-BIDS MRI pipeline inputs and derivatives"
1.  After clicking on the Collection Title select "Shared Data"
1.  Click "Add to Cart" at the bottom
1.  It will take a minute to update the "Filter Cart" in the upper right corner, but when that is done select "Package/Add to Study"
1.  Select "Create Package", name your package accordingly, and click "Create Package"
    -   IMPORTANT: Make sure "Include associated data files" is deselected or it will automatically attempt to download all the data through the NDA's package manager which is unreliable on such a large dataset. That is why we've created this downloader for you.
1.  Now download the "Download Manager" to actually download the package or use the NDA's [nda-tools](https://github.com/NDAR/nda-tools) to download the package from the command line. This may take several minutes.
1.  After the download is complete find the "datastructure_manifest.txt" in the downloaded directory. This is the input S3 file that contains AWS S3 links to every input and derivative for all of the selected subjects and you will need to give the path to this file when calling download.py

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
  -s SUBJECT_LIST_FILE, --subject-list SUBJECT_LIST_FILE
                        Path to a .txt file containing a list of subjects for
                        which derivatives and inputs will be downloaded. By
                        default without providing input to this argument all
                        available subjects are selected.
  -l LOG_FOLDER, --logs LOG_FOLDER
                        Path to existent folder to contain your download
                        success and failure logs. By default, the logs are
                        output to your home directory: ~/
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
                        Unless this option is added, the user will be prompted
                        for their NDA username and password the first time
                        this script occurs. By default, the config file will
                        be located at ~/.abcd2bids/config.ini
  -p CORES, --parallel-downloads CORES
                        Number of parallel downloads to do. Defaults to 1
                        (serial downloading).
``` 
