
import requests
import json

# Problem: Collection 3165 is too download all at once
# Solution: 
#   1) Use paginator to collect download urls in digestable chunks
#   2) Create a db on the user side that can be queried to download
#       only the desired images
#   3) Make list of files to download and run the download


package_id = 1203969

# Get metrics on data package
url = 'https://nda.nih.gov/api/package/{}'.format(package_id)
response = requests.get(url)
data = respone.json()
package_size = data['total_package_size']
num_files = data['file_count']

# 
page = 1
size = 30
url = 'https://nda.nih.gov/api/package/{}/files?page={}&size={}'.format(package_id, page, size)









