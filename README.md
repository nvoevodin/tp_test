## This is a documentation for the Trevor Project Engineering Task

[[_TOC_]]

# Task Overview

This project is a test to answer the below:

# Requirements

## Business Requirements

1. Requestor: Trevor Project
2. What problem the project aims to solve
    1. There are 2 tabular format csvs (1 with user info, 2nd with their login and logout info)
    2. Load the files into Google's BigQuery environment
    3. Access and extract and transform that data using Python
    4. The transformation should result in a user dataframe that answers the following:
        - Has user info (user_id, email, email_domain, account_created_date,account_created_year)
        - A field indicating the largest number of users that have been logged on at the same time as each user
        - A field indicating each user’s total logged in time
        - A field indicating each user’s most-active time of day (morning, afternoon, evening, night)
    5. The resulting data must be loaded back into BigQuery as a new view and made available to the requestor
    6. Code artifacts must be made available and shared via GitHub
3. Start Date: 09-23-2022 Deadline: 09-28-2022

## Technical Requirements

1. Technology used: BigQuery, Python, SQL, GitHub, VsCode
2. Permissions/access to folders required and where obtained: JSON File in the repo is needed to access the BigQuery project
3. Databases/servers used (internal and external data sources): BigQuery/Local UBUNTU server
4. Code location: https://github.com/nvoevodin/tp_test.git


## Personnel Requirements

| Name | Role | Email |
|--|--|--|
| John Davies | Product Owner | John.Davies@TheTrevorProject.org |
| Nikita Voevodin | Lead Developer | voevodin.nv@gmail.com |



# Development Workflow

This task requires building an ETL pipeline. Here are the steps:
1. Since the initial datasets were emailed, the BQ ingestion was manual.
2. I created a sandbox account and loaded the datasets as 2 separate tables.
3. To process the data, I needed a python environment. I used my local UBUNTU machine and VSCode editor
4. To access BQ Python API, I created a service account, gave myself owner and contributor's roles and generated an access token
5. The concurrency and shifting tasks are particularly complex -> logic to solve them had to be written in python
6. The extract and transform parts of the ETLprocess were mostrly done using sqlalchemy and pandas libraries
7. Once all transformations were complete, I used BQ python API to create and load the result table back into BQ
8. I also created a view that will reflect the main (final) table. 

# Use Workflow

The main usecase is the VIEW. Here is a simple sample:

```
   SELECT *
   FROM( 
   SELECT email_domain, sum(total_logged_hours) as total_logged_hours
   FROM tp-test-project-363423.test_data_repo.final_view
   GROUP BY email_domain) init_table
   ORDER BY total_logged_hours DESC
   LIMIT 10

```


There a 2 main files in this repo:

1. tp_book.ipynb a jupyter notebook - main file 
2. raw_code.py a .py script aimed for automation

Although, you can automate a jupyther notebook, it is more practical to automate a .py script. 

3. requirements.txt - a text file which contains a list of Python dependencies and packages used in this project

To reproduce the same results: 

To install the python dependencies please run the following command in terminal from the root of this project: 

pip install -r requirements.txt

Once all packages and dependencies are installed, the notebook should run as expected and output the same results.

Feel free to reach out if you are having any issues with running the notebook.

# Data Classification

Data Specs of the final view:

| Field Name | Type | 
|--|--|
| user_id | String |
| email | String |
| email_domain | String |
| account_created_date | Date |
| account_created_year | Int |
| max_concurrent_count | Int |
| total_logged_hours | Float |
| night_hours | Float |
| morning_hours | Float |
| afternoon_hours | Float |
| evening_hours | Float |
| night_ratio | Float |
| morning_ratio | Float |
| afternoon_ratio | Float |
| evening_ratio | Float |


# Authentication

John Davies was assigned Read-Access to the tp-test-project-363423.test_data_repo.final_view 

