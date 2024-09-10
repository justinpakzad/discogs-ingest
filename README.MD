# Discogs Ingest (WIP)
Discogs Ingest is a Python program that extracts the most recent Discogs data dump, parses the XML files, and allows for exporting to CSV and DuckDB. I developed a separate script that scraped features such as the number of people who want the record, have the record, and price stats. This enriched data was only scraped for electronic music. If you are interested in the enriched dataset, please just reach out.

## Installation
1. Clone the repository:
```
git clone https://github.com/justinpakzad/discogs-ingest
```
2. Create a virtual enviornment:
```
pyenv virtualenv 3.10.6 discogs_ingest
pyenv activate discogs_ingest
```
3. Install required dependencies:
```cd discogs_ingest
pip install -r requirements.txt
```

## CSV Export Usage
**Command Line Arguments:**
- `--dir`: Directory for saving output files. If the directory does not exist, it will be created (required).  
- `--raw_dir`: Directory where the raw data dumps live (not needed if using `--download`).  
- `--sample`: Extracts a sample of 50k to CSV (optional).
- `--download`: Downloads the latest Discogs data dumps (optional).  


**Csv Example:**
```
python main.py --dir <download_dir> --download  
``` 
## DuckDB Export Usage
The CSV files must be downloaded in order to make use of the DuckDB exporter.  

**Command Line Arguments:** 
- `--db`: Path where database will be saved.
- `--csvs`: Path where CSV files are located.

**DuckDB Example**
```
python duck_db.py --db <db_path> --csvs <csv_path>
```  



