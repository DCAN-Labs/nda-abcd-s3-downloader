# NDA Collection 3165 ABCD-BIDS Downloader

This tool can be used to download BIDS inputs and derivatives from the DCAN Labs ABCD-BIDS collection 3165 from the NIMH Data Archive (NDA).  All files are hosted by the NDA in Amazon Web Services (AWS) Simple Storage Service (S3) buckets.

## Usage

To use this downloader you must first have an NDA account and acquire the `collection_3165_manifest_s3_links.csv` from the [NDA website](https://ndar.nih.gov/).

You must also provide a list of the data subset types you wish to download.  For ease of use a list of all possible data subsets is provided with this repository: `data_subsets.txt`.  If you would only like a subset of all data subsets you should copy only the data subset types that you want into a new `.txt` file and point to that when calling `download.py` with the `-d` option.

For full usage documentation, type the following while inside your folder containing this cloned repository.

```shell
./download.py -h
``` 
