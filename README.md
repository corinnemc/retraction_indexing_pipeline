# Retraction Indexing Pipeline
Created by Corinne McCumber
## Description
The repo shows a prototype workflow that partially automates the intake and cleaning of metadata for retracted 
publications that are indexed in two databases, [Retraction Watch](https://retractiondatabase.org/RetractionSearch.aspx?)
and [PubMed](https://pubmed.ncbi.nlm.nih.gov/). Retraction indexing makes retracted 
publications searchable through specific metadata or a specific category that is applied solely to retracted 
publications. This workflow builds on my own prior work manually assessing the retraction indexing of publications 
across 11 bibliographic databases [(Salami et al., 2024)](https://doi.org/10.31222/osf.io/gvfk5), and existing code from
[Salami & McCumber (2024)](https://doi.org/10.5281/zenodo.14183542) has been refactored from Jupyter Notebooks into Python scripts.

## Setup

1. OS information: Windows 10 Version 22H2 (OS Build 19045.5737)
2. Required packages: listed in requirements.txt
3. Complete package dependencies are stored in environment.yml. 
This project was developed using the virtual environment described in environment.yml, which was created using Conda. 
[See here for virtual environment installation instructions.](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
4. File structure: expected file structure is below. The ```/data/``` folder may need to be manually added 
when cloning this GitHub repo. Outputs will be saved in the ```/data/``` folder

```
├──retraction_indexing_pipeline
|  ├──data
|  ├──python_scripts
|  |  ├──a_retraction_watch_data_collection.py
|  |  ├──b_pubmed_data_collection.py
|  |  ├──c_profile_collected_data.py
|  |  ├──d_create_initial_unionlist.py
|  |  ├──e_pubmed_coverage_query.py
|  |  ├──f_update_unionlist_with_coverage.py
|  |  ├──g_data_analysis.py
|  |  └──master_script.py
|  ├──environment.yml
|  ├──README.md
└──└──requirements.txt
```
## Automated Use
1. In the ```/python_scripts/``` subfolder, open ```master_script.py```. 
2. Update line 13 with a valid email address (e.g. ```EMAIL = 'email@email.com'```) to be used in PubMed
queries. 
3. Update line 15 with the most recent Retraction Watch GitLab commit SHA from https://gitlab.com/crossref/retraction-watch-data
   (e.g. ```RETRACTION_WATCH_COMMIT_ID = '28e4670f328f4879ebfbbe2ae66e5f50b0e02184'```)
4. Run the master script. Complete checks listed in Manual Use section while automated processes 
are running. 

The whole process has taken about 35 minutes, with over 23 minutes devoted to initially
querying PubMed. CSV, HTML, and PNG output files are saved at each major step, so if there is an error, the process can
be resumed manually from the last successful step.

### All expected outputs in ```/data/```:
- ```{current date}_retraction_watch.csv```: all items indexed as retracted in Retraction Watch
- ```sha256_retraction_watch.csv```: SHA calculated for specific Retraction Watch download
- ```{current date}_pubmed.csv```: all items indexed as retracted in PubMed
- ```{current date}_retraction_watch_profiling_report.html```: ydata-profiling report of Retraction Watch indexed items
- ```{current date}_pubmed_profiling_report.html```: ydata-profiling report of PubMed indexed items
- ```retraction_watch_duplicatedrecords_{date}.csv```: Retraction Watch items not integrated into unionlist because 
their Digital Object Identifier (DOI) already appeared in the Retraction Watch data. For instance, if three items shared the same DOI in Retraction 
Watch, two items would appear in this file, and one item would appear in the unionlist. The first item in the data was
taken as ground truth.
- ```retraction_watch_recordsnodoi_{date}.csv```: Retraction Watch items not integrated into the unionlist because 
they did not have a DOI starting with '10'
- ```retraction_watch_recordswithdoi_{date}.csv```: Retraction Watch items integrated into the unionlist
- ```pubmed_duplicatedrecords_{date}.csv```: PubMed items not integrated into unionlist because 
their DOI already appeared in the PubMed data.
- ```pubmed_recordsnodoi_{date}.csv```: PubMed items not integrated into the unionlist because 
they did not have a DOI starting with '10'
- ```pubmed_recordswithdoi_{date}.csv```: PubMed items integrated into the unionlist
- ```datasources_overview.csv```: summary table showing aggregate counts of query results, records with DOI,
records without DOI, duplicate DOIs removed, and DOI records that also have a PubMed identifier
- ```{date}_unionlist.csv```: Combined items from Retraction Watch and Pubmed showing retraction indexing
- ```pubmed_coverednotindexed_{date}.csv```: Items with a PubMed identifier in the unionlist that are indexed as retracted
in Retraction Watch, covered in PubMed, but not indexed as retracted in PubMed
- ```{date}_unionlist_with_coverage.csv```: Combined items from Retraction Watch and Pubmed showing retraction indexing 
and coverage
- ```{date}_aggregate_results.csv```: total counts of all items indexed as retracted, all items covered, 
items covered but not indexed as retracted, and items not covered by each source
- ```indexing_status_upset_plot.png```: UpSet plot showing intersection of indexed items in both sources
- ```coverage_status_upset_plot.png```: UpSet plot showing intersection of covered items in both sources
- PLEASE NOTE: Pairwise agreement (a percentage) is shown via terminal output. Pairwise agreement can be calculated by 
taking (all items indexed in both sources) / (all items covered in both sources) * 100 

## Manual Use
### Step 1: Retraction Watch data collection
1. Open ```a_retraction_watch_data_collection.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change ```commit_id``` to the most recent commit SHA from https://gitlab.com/crossref/retraction-watch-data
      (e.g. ```commit_id = "6bea2b44d7521a4d9567148523756530eadfa194"```)
4. Run the script

This will output two files into ```/data/```:
- ```{current date}_retraction_watch.csv```: all items indexed as retracted in Retraction Watch
- ```sha256_retraction_watch.csv```: SHA calculated for this specific download

### Step 2: PubMed data collection
1. Open ```b_pubmed_data_collection.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - ```email="INSERT_EMAIL_HERE"``` should be updated to a valid email address string

#### Checks:
- Check that totals returned for each year interval do not exceed 10,000. If the count
shown with the print statement ```'{year} - {end_year}: {count} total number of retrieved PMIDs'```, exceeds 
10,000, lower the ```interval_year``` variable by 1 and rerun the script.
- Check that the total number of retracted publications matches the total shown on the PubMed interface when searching 
'https://pubmed.ncbi.nlm.nih.gov/?term="Retracted+Publication"[pt]'
- Once the script has successfully run, manually search PubMed for at least 5 items shown and verify that the metadata in 
the CSV file matches what is shown on the PubMed interface.

#### Troubleshooting
- If a ```<Response 414>``` error occurs, lower the ```no_records``` variable and rerun the script.
- If a ```<Response 500>``` error occurs, lower the ```no_records``` variable and rerun the script.

This will output one file into ```/data/```:
- ```{current date}_pubmed.csv```: all items indexed as retracted in PubMed

### Step 3: Profile collected data
1. Open ```c_profile_collected_data.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change ```file``` to match the file name created in step 1 (e.g. ```file = "2025-05-09_retraction_watch.csv"```)
4. Run the script.
5. Change ```file``` to match the file name created in step 2 (e.g. ```file = "2025-05-09_pubmed.csv"```)
6. Run the script again

This will output two files into ```/data/```:
- ```{current date}_retraction_watch_profiling_report.html```: ydata-profiling report of Retraction Watch indexed items
- ```{current date}_pubmed_profiling_report.html```: ydata-profiling report of PubMed indexed items

### Step 4: Create initial unionlist
1. Open ```d_create_initial_unionlist.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change the ```pubmed_date``` and ```retraction_watch_date``` to 
match the current date. This is used when reading in the saved CSV files. For example:
      - ```pubmed = clean_pubmed_data(pubmed_date='2025-05-09')```
      - ```retraction_watch = clean_retraction_watch_data(retraction_watch_date='2025-05-09')```

This script takes about 5 minutes to run, as the cleaning process requires iterating over thousands of rows.

This will output eight files into ```/data/```:
- ```retraction_watch_duplicatedrecords_{date}.csv```: Retraction Watch items not integrated into unionlist because 
their DOI already appeared in the Retraction Watch data. For instance, if three items shared the same DOI in Retraction 
Watch, two items would appear in this file, and one item would appear in the unionlist. The first item in the data was
taken as ground truth.
- ```retraction_watch_recordsnodoi_{date}.csv```: Retraction Watch items not integrated into the unionlist because 
they did not have a DOI starting with '10'
- ```retraction_watch_recordswithdoi_{date}.csv```: Retraction Watch items integrated into the unionlist
- ```pubmed_duplicatedrecords_{date}.csv```: PubMed items not integrated into unionlist because 
their DOI already appeared in the PubMed data.
- ```pubmed_recordsnodoi_{date}.csv```: PubMed items not integrated into the unionlist because 
they did not have a DOI starting with '10'
- ```pubmed_recordswithdoi_{date}.csv```: PubMed items integrated into the unionlist
- ```datasources_overview.csv```: summary table showing aggregate counts of query results, records with DOI,
records without DOI, duplicate DOIs removed, and DOI records that also have a PubMed identifier
- ```{date}_unionlist.csv```: Combined items from Retraction Watch and Pubmed showing retraction indexing

### Step 5: PubMed coverage query
1. Open ```e_pubmed_coverage_query.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change ```unionlist_date``` to the current date matching the unionlist file name generated in the 
previous step.
   - Change ```email='INSERT_EMAIL_HERE'``` to a valid email address (e.g. ```email='email@email.com'```)
4. Run the script

#### Troubleshooting
- If a ```<Response 500>``` error occurs, scroll up to ```def query_pubmed_with_pmid()```. Lower the ```cut``` kwarg 
and rerun the script (e.g. ```identifier_batches = batch_items(identifier_list, cut=200)```)

This will output one file into ```/data/```:
- ```pubmed_coverednotindexed_{date}.csv```: Items with a PubMed identifier in the unionlist that are indexed as retracted
in Retraction Watch, covered in PubMed, but not indexed as retracted in PubMed

### Step 6: Update unionlist with coverage
1. Open ```f_update_unionlist_with_coverage.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change ```unionlist_date``` to current date,
e.g. ```unionlist = read_in_unionlist_without_coverage(unionlist_date='2025-05-09')```.
This is used when reading in file names.
   - Change ```pubmed_date``` to current date,
e.g. ```pubmed_covered_not_indexed = read_in_covered_not_indexed(pubmed_date='2025-05-09')```.
This is used when reading in file names.
   - Change ```completed_unionlist_date``` to current date, e.g.
```merge_dataframes(unionlist=unionlist, pubmed_covered_not_indexed=pubmed_covered_not_indexed, completed_unionlist_date='2025-05-09')```
This is used when saving the output file.
4. Run the script

This script takes about 5 minutes to run, as the cleaning process requires iterating over thousands of rows.

This will output one file into ```/data/```:
- ```{date}_unionlist_with_coverage.csv```: Combined items from Retraction Watch and Pubmed showing retraction indexing 
and coverage

### Step 7: Data analysis
1. Open ```g_data_analysis.py```
2. Scroll to the bottom of the file.
3. Update information in ```main()```:
   - Change ```completed_unionlist_date``` to current date, e.g. 
```unionlist = read_in_unionlist_with_coverage(completed_unionlist_date='2025-05-09')```.
This is used when reading in files.
4. Run the script

This will output three files into ```/data/```:
- ```{date}_aggregate_results.csv```: total counts of all items indexed as retracted, all items covered, 
items covered but not indexed as retracted, and items not covered by each source
- ```indexing_status_upset_plot.png```: UpSet plot showing intersection of indexed items in both sources
- ```coverage_status_upset_plot.png```: UpSet plot showing intersection of covered items in both sources
- PLEASE NOTE: Pairwise agreement (a percentage) is shown via terminal output. Pairwise agreement can be calculated by 
taking (all items indexed in both sources) / (all items covered in both sources) * 100 

## Additional notes
1. Multiple API errors have been observed when querying PubMed. Pay special attention to the suggested Troubleshooting 
and to the checks listed under Manual Use
2. When combining information into the unionlist, specific items were screened out. 
   - If multiple items in a source have the same DOI, only the first item in the given dataset was used, as described for the file 
```retraction_watch_duplicatedrecords_{date}.csv```. A [DOI](https://www.doi.org/the-identifier/what-is-a-doi/) is a 
persistent identifier unique to a specific item, and two items should not be able to share a DOI. 
   - If items did not have a DOI, they were excluded from analysis and saved in a separate file, e.g. 
```retraction_watch_recordsnodoi_{date}.csv```
3. When fusing records for the unionlist, fusion occurred based on DOI, and PubMed metadata was 
preferred over Retraction Watch metadata. This is because
PubMed has been show to be more reliable for metadata aside from retraction indexing. See 
[Sebo and Sebo (2025)](https://www.doi.org/10.1080/08989621.2025.2465621), particularly
their supplemental materials. While both PubMed and Retraction Watch had PubMed Identifier (PMID) available, DOI was preferred
because in context of the larger 11-source pipeline, DOI is the standard identifier used across more databases.
4. When searching PubMed for coverage information, only items' PMIDs were used, which severely 
limits the accuracy of PubMed coverage information for items that are only indexed in Retraction Watch. Multiple attempts
were made to query the PubMed API via DOI, but results returned were inaccurate. As such, coverage information should be
regarded as an under-count for PubMed.

## References
Salami, M.O. & McCumber, C. (2024). Retraction indexing agreement: 2024 preprint 
final code (version 2.0.0) [Computer software]. Zenodo. 
https://doi.org/10.5281/zenodo.14183542

Salami, M. O., McCumber, C., & Schneider, J. (2024). Analyzing the Consistency of 
Retraction Indexing. MetaArXiv Preprints. https://doi.org/10.31222/osf.io/gvfk5

Sebo, P. & Sebo, M. (2025). Assessing database accuracy for article retractions:
A preliminary study comparing Retraction Watch Database, PubMed, and Web of Science.
Accountability in Research. https://www.doi.org/10.1080/08989621.2025.2465621
