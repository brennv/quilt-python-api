# Convert files to live data sets on Quilt
1. Get a list of files you want to upload (see `files-to-download/`)
2. Download the files in the list (see `curl-all.py`)
3. Unzip downloaded files (if needed)
       cd downloads
       gunzip *.gz
4. `python create_data_set.py --help`


# File formats in this example
* [ENCDOE broadPeak format](https://genome.ucsc.edu/FAQ/FAQformat.html#format13)

# Resources
* [ENCODE Project](https://www.encodeproject.org/)
