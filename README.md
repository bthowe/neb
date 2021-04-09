# Overview
Python code for generating the csv files containing the Kauffman Foundation's New Employer Business (NEB) indicators 
data (see https://indicators.kauffman.org/series/newemployer). 

The repository has three main elements:
### 1. `data` subdirectory
This directory contains the source data (eight csv files and a `pkl` file of a string of a timestamp when the data was
pulled), output (six csv files available after running `neb_command.py`), and a `TEMP` directory and data files created 
when `neb_command.py` is run. 

### 2. `neb_command.py`
Running this file will generate the data using the function `neb_data_create_all`, which has three parameters:
 * `raw_data_fetch`, which allows the user to specify whether to fetch the raw data from source (see below) or use the data in 
 `data/raw_data`.
 * `raw_data_remove`, which allows the user to specify whether to remove the temporary data files.
 * `aws_filepath`, which allows the user to specify whether to stash the data in S3.   

The output consists of six csv files: one formatted the same as the file available for download on the webpage, and five
(one for each indicator) that are used to create the visualizations on the webpage. The data consists of annual values
for each indicator by state (including Washington DC) and U.S.

### 3. `constants.py`
A file with constant values used in `neb_command.py`


# Methodology and Data
For a detailed description of how each indicator is calculated and the data used see the working paper "New Employer 
Business Trends: A Methodological Note" available at https://www.kauffman.org/entrepreneurship/reports.


# Feedback
Questions or comments can be directed to indicators@kauffman.org.

# Disclaimer
The content presented in this repository is presented as a courtesy to be used only for informational purposes. The 
content is provided “As Is” and is not represented to be error free. The Kauffman Foundation makes no representation or 
warranty of any kind with respect to the content and any such representations or warranties are hereby expressly 
disclaimed.
