# Convert files to live data sets on Quilt
1. Get a list of files you want to upload (see `files-to-download/`)
2. Download the files in the list (see `curl-all.py`)
3. Unzip downloaded files (if needed)
        cd downloads
        gunzip *.gz
4. Use `data_set.py` to create data sets (see `python data_set.py --help`)
```bash
python data_set.py -e http://quiltdata.com -u USERNAME -n "ENCODE data" -d "#script upload" -f downloads/wgEncodeBroadHistoneNhaH3k36me3StdPk.broadPeak
```


# File formats in this example
* [ENCDOE broadPeak format](https://genome.ucsc.edu/FAQ/FAQformat.html#format13)

# Resources
* [ENCODE Project](https://www.encodeproject.org/)
