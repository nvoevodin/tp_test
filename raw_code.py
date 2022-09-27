#####Libraries#####################################################
import pandas as pd
import numpy as np
from sqlalchemy import create_engine;
from sqlalchemy import text
import time
from datetime import datetime, timedelta
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import warnings
import seaborn as sns
import matplotlib.pyplot as plt
####################################################################


#####Inits############################################################
warnings.filterwarnings('ignore')
dirname = os.getcwd()

# Connect to BigQuery using google generated .json settings file
try: 
    db = create_engine(
    'bigquery://',
    credentials_path= dirname + '/tp-test-project-363423-00af74cfbd4d.json') # This will need to be changed to you auth file or request mine
    
except Exception as e: print(e)


# Simple function to query data
def callDB(query):
    """ 
    Pull data from the BigQuery connection.

    Args:
    query (str): the sql query to use.
 
    Returns:
    pandas dataframe
    """

    try: 
        df = pd.read_sql(query, con=db)
        return df
    except Exception as e: print(e)



# SQL queries to pull everything from both datasets
CONNECTIONS_QUERY = """
   SELECT *
   FROM tp-test-project-363423.test_data_repo.connection_events

   """

USERS_QUERY = """
   SELECT *
   FROM tp-test-project-363423.test_data_repo.users
   """ 


# Testing the connection and checking top rows of the activity dataset
connections_df = callDB(CONNECTIONS_QUERY)

# Pulling the user's table
users_df = callDB(USERS_QUERY)


###########################################################################3




######Master user table mods##################################################

# creating a copy of the master users table for modification
users_df_modified = users_df.copy()

# using string split function to separate by @ and select second part
users_df_modified['email_domain'] = users_df_modified['email'].str.split('@').str[1]
# extracting date from datetime stamp
users_df_modified['account_created_date'] = pd.to_datetime(users_df_modified['created_at_utc']).dt.date
# extracting year
users_df_modified['account_created_year'] = pd.to_datetime(users_df_modified['created_at_utc']).dt.year
# selecting and renaming
users_df_modified = users_df_modified[['id', 'email','email_domain','account_created_date','account_created_year']].reset_index(drop = True).rename(columns={'id':'user_id'})


###############################################################################




#####Reshaping connections table###############################################33

#1) I need to reshape the dataset so that each login and logout are side by side

# creating keys for each user to enable pivoting for duplicated users
connections_df['key']=connections_df.groupby(['user_id','event_type']).cumcount()

# pivoting by user id and key
connections = connections_df.pivot(['key','user_id'], columns='event_type', values='timestamp').reset_index()

#2) Sorting logins and logouts so each pair represent each session by user
connections = connections.sort_values(['user_id','login','logout'],ascending = [False,False,False])

#3) Some logins do not have corresponding logouts. I will consider this to be corrupt data and will eliminate it
connections = connections[connections['logout'].notna()]

##############################################################################






####Counting concurrency####################################################
# create a new column to count concurrent users
connections['concurrent_count'] = 0

# timing this
start_time = time.time()

connections.reset_index(drop=True) # just in case
for i, row in connections.iterrows():
    
    connections.loc[i, 'concurrent_count'] = int(np.sum(
        (connections.login <= row.logout) & (connections.logout >= row.login)
    )) -1  # note the subtraction by one eliminates current session from overall count


print("--- %s seconds ---" % (time.time() - start_time))


# calculating the max number of concurrent connections per each user_id
max_concurent_connections_by_user = connections.groupby(['user_id'], sort=False)['concurrent_count'].max().reset_index().rename(columns={'concurrent_count':'max_concurrent_count'})

# joining to the users master table
users_df_result = users_df_modified.merge(max_concurent_connections_by_user, left_on='user_id', right_on='user_id')

#########################################################################



#####Counting logged in times##############################################################

# making a copy of the connection dataset
connections_total_time = connections.copy()

# calculate duration in seconds of each session
connections_total_time['log_period'] = (connections_total_time['logout'] - connections_total_time['login']).astype('timedelta64[s]')

# group by user -> sum -> convert to hours -> round to 2 decimals
connections_total_time = (connections_total_time.groupby(['user_id'])['log_period'].agg('sum')/3600).round(2).reset_index().rename(columns={'log_period':'total_logged_hours'})

# joining to the user master table
users_df_result = users_df_result.merge(connections_total_time, left_on='user_id', right_on='user_id')

###########################################################################################




#####Assigning shifts##################################################################

# copying the connections data
shifting = connections.copy()

# extracting day from each datetime. We will use it to dynamically create shifts for each loop iteration
shifting['day'] = shifting['login'].dt.floor("D")

# adding 4 empty columns to the data, 1 for each shift
shifting['night'] = ''
shifting['morning'] = ''
shifting['afternoon'] = ''
shifting['evening'] = ''


# writing logic to properly split time between shifts if needed
def time_in_shift(start, end, shift_start, shift_end):

    """ 
    Properly splits time between shifts if needed.
    The logic is as follows: if the user logs in before the actual the shift start time -> shift's start time takes place of the login time.
    if the user logs out after the shifts end time -> shift's end time takes place of the logout time. This logic is not perfect as sessions can span over
    multiple days. This function accounts for that by equally splitting the time in 4 if a session is longer than 24h. Need a bit more time to figure out the rest.

    Args:
    start (datetime): login timestamp.
    end (datetime): logout timestamp.
    shift_start (datetime): start time of a shift.
    shift_end (datetime): end time of a shift.
 
    Returns:
    hours spent in each shift (numeric)
    """
# first condition: if the session is longer than 24h -> split evenly between 4 shifts
    if (end - start).total_seconds()/3600 > 24:
        return (end - start).total_seconds()/3600/4
# if not -> follow the logic outlined in the description of this function        
    else:

        if start < shift_start:
            start = shift_start 
        if end > shift_end:
            end = shift_end
# calculating time spent in the session here (in hours)
        time_spent = (end-start).total_seconds()/3600

# negative hours means that no time was spent in that shift -> turn to 0
        if time_spent < 0:
            time_spent = 0

        return time_spent


# applying the time_in_shift function to each row of the connections dataset (now shifting)
for i in shifting.index:
# dynamically creating shifts for each session. Must be done because dates are always different.
    shift_start=(shifting.loc[i,'day'],
             shifting.loc[i,'day'] + timedelta(hours = 6),
             shifting.loc[i,'day'] + timedelta(hours = 12),
             shifting.loc[i,'day'] + timedelta(hours = 18))
    shift_end=  (shift_start[1],    
             shift_start[2],
             shift_start[3],
             shift_start[0] + timedelta(days=1))

# range here corresponds to 4 shifts
    for shift in range(4):

# storing time in the shift_time variable
        shift_time = time_in_shift(shifting.loc[i,'login'], shifting.loc[i,'logout'], shift_start[shift], shift_end[shift])

        
        
# inserting the time into the shifting dataframe depending on the shift
        if shift == 0:
            shifting.loc[i,'night'] = shift_time
        elif shift == 1:
            shifting.loc[i,'morning'] = shift_time
        elif shift == 2:
            shifting.loc[i,'afternoon'] = shift_time
        elif shift == 3:
            shifting.loc[i,'evening'] = shift_time


# calculating how many hours each user spent in each shift overall
shift_hours_per_user = shifting.groupby(['user_id']).agg({'night':'sum','morning':'sum', 'afternoon':'sum','evening':'sum'}).round(2).reset_index()

# this function calculates shift ratios or percent of time spent in each shift by each user
def shift_ratio_function(colname,shift):

    """ 
    This function calculates shift ratios or percent of time spent in each shift by each user, and adds it as a new column. 

    Args:
    colname (string): name for a new column.
    shift (string): shift name.
 
    Returns:
    dataframe with additional columns
    """
#adding 4 new columns with ratios
    shift_hours_per_user[colname] = \
    ((shift_hours_per_user[shift]/(shift_hours_per_user['night'] + \
    shift_hours_per_user['morning'] + \
    shift_hours_per_user['afternoon'] + \
    shift_hours_per_user['evening'])) * 100).round(2)

# inputs for the function
shift_inputs = [{'night_ratio':'night'}, {'morning_ratio':'morning'}, {'afternoon_ratio':'afternoon'},{'evening_ratio':'evening'}]


# looping of the inputs to calculate ratios for all users and shifts
for index in range(len(shift_inputs)):
    for key in shift_inputs[index]:
        shift_ratio_function(key,shift_inputs[index][key])


# joining to the user master table
users_df_result = users_df_result.merge(shift_hours_per_user, left_on='user_id', right_on='user_id')

users_df_result = users_df_result.reset_index().rename(columns={'night': 'night_hours', 'morning':'morning_hours', 'afternoon':'afternoon_hours', 'evening' : 'evening_hours'})
# printing head
users_df_result.head(5)

#####################################################################################



#####Updating the table in BQ########################################################

# Connecting to the API
credentials = service_account.Credentials.from_service_account_file(
    dirname + '/tp-test-project-363423-00af74cfbd4d.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Initiating a session
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# Creating the view
table_id = 'test_data_repo.view_table'


# Configuring the schema base
job_config = bigquery.LoadJobConfig(schema=[
    bigquery.SchemaField("user_id", "STRING"),
])

# Adding truncate condition to overwrite the table everytime. 
# The table is small and will change all the time -> most of the data will be overwritten anyway. 
# Better solution is to UPSERT (would require more time to setup)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

# Creating the job
job = client.load_table_from_dataframe(
    users_df_result, table_id, job_config=job_config
)

# Executing
job.result()
##########################################################################


######Creating the View########################################################


# # naming the view
# view_id = "tp-test-project-363423.test_data_repo.final_view"
# # specifying the source table
# source_id = "tp-test-project-363423.test_data_repo.view_table"

# # Linking the view to the final table
# view = bigquery.Table(view_id)
# view.view_query = f"SELECT *  FROM `{source_id}`"

# # Make an API request to create the view.
# view = client.create_table(view)

# print(f"Created {view.table_type}: {str(view.reference)}")


#####################################################################################



#######Extra Analysis###############################################################

# SQL queries to pull everything from both datasets
VIEW_QUERY = """
   SELECT *
   FROM tp-test-project-363423.test_data_repo.final_view

   """

data_for_analysis = connections_df = callDB(VIEW_QUERY)
data_for_analysis.head(5)


######################################################################################

########Top 10 domains by log in time##################################################

top_3_domains = data_for_analysis.groupby(['email_domain'])['total_logged_hours'].sum().reset_index().sort_values('total_logged_hours',ascending = False).head(10)


# barplot
top_3_domains_barplot = sns.barplot(x = 'total_logged_hours',
            y = 'email_domain',
            hue = 'email_domain',
            data = top_3_domains)

         
# Show the plot
plt.legend([],[], frameon=False)

#####################################################################################


####Most popular shift#############################################################

total_time_by_shift = data_for_analysis[['night','morning','afternoon','evening']].sum()

total_time_by_shift_df = pd.DataFrame({'shift':total_time_by_shift.index, 'total_hours':total_time_by_shift.values})

# barplot
total_time_by_shift_df_barplot = sns.barplot(x = 'total_hours',
            y = 'shift',
            hue = 'shift',
            data = total_time_by_shift_df)

         
 
# Show the plot
plt.legend([],[], frameon=False)


###################################################################################





