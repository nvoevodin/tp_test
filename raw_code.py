import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy import text
from pybigquery.api import ApiClient
import time
from datetime import datetime, timedelta
dirname = os.getcwd()

db = create_engine(
    'bigquery://',
    credentials_path='/home/nkta/Desktop/py/tp-test-project-363423-00af74cfbd4d.json'
)



CONNECTIONS_COUNT = """
   SELECT count(*)
   FROM tp-test-project-363423.test_data_repo.connection_events
   
    """

CONNECTIONS_QUERY = """
   SELECT *
   FROM tp-test-project-363423.test_data_repo.connection_events


   """

USERS_QUERY = """
   SELECT *
   FROM tp-test-project-363423.test_data_repo.users
   """   




connections_df = pd.read_sql(CONNECTIONS_QUERY, con = db)

sorted_df = connections_df.sort_values('timestamp')


sorted_df = sorted_df[sorted_df['timestamp'].notna()]

#sorted_df['counter'] = sorted_df['event_type'].map({'login':1,'logout':-1}).cumsum()

users_df = pd.read_sql(USERS_QUERY, con = db)

connections_df['key']=connections_df.groupby(['user_id','event_type']).cumcount()

connections = connections_df.pivot(['key','user_id'], columns='event_type', values='timestamp').reset_index()
connections = connections.sort_values(['user_id','login','logout'],ascending = [False,False,False])


connections = connections[connections['logout'].notna()]

#logins = connections_df[connections_df['event_type'] == 'login'].sort_values(['user_id','timestamp'],ascending = [False,False])
#logouts = connections_df[connections_df['event_type'] == 'logout'].sort_values(['user_id','timestamp'],ascending = [False,False])

#logins = logins[['user_id','timestamp']].rename(columns={'timestamp':'login_timestamp'})
#logouts = logouts[['user_id','timestamp']].rename(columns={'timestamp':'logout_timestamp'})

#connections_pivot = logins.merge(logouts, left_on = 'user_id', right_on = 'user_id')

test_df = connections



# ##################################

# data = [['aa', '2020-05-31 00:00:01', '2020-05-31 00:00:31'],
#         ['bb','2020-05-31 00:01:01', '2020-05-31 00:02:01'],
#         ['aa','2020-05-31 00:02:01', '2020-05-31 00:06:03'],
#         ['cc','2020-05-31 00:03:01', '2020-05-31 00:04:01'],
#         ['dd','2020-05-31 00:04:01', '2020-05-31 00:34:01'],
#         ['aa', '2020-05-31 00:05:01', '2020-05-31 00:07:31'],
#         ['bb','2020-05-31 00:05:01', '2020-05-31 00:06:01'],
#         ['aa','2020-05-31 00:05:01', '2020-05-31 00:08:03'],
#         ['cc','2020-05-31 00:10:01', '2020-05-31 00:40:01'],
#         ['dd','2020-05-31 00:20:01', '2020-05-31 00:35:01']]


# df_test = pd.DataFrame(data,  columns=['user_id','login', 'logout'], dtype='datetime64[ns]')
# df_test = df_test.sort_values(['user_id','login','logout'],ascending = [False,False,False])


# # create a new column for simultaneous
# df_test['simultaneous'] = 0

# start_time = time.time()

# # loop through dataframe and check condition
# for i in df_test.index:
#     login, logout = df_test.loc[i,'login'], df_test.loc[i,'logout']
#     this_index = df_test.index.isin([i])
#     df_test.loc[i, 'simultaneous'] = int(sum(
#         (df_test[~this_index]['login'] <= logout) & (df_test[~this_index]['logout'] >= login)
#     ))
# print("--- %s seconds ---" % (time.time() - start_time))


# ########################################################

start_time = time.time()
# create a new column for simultaneous
test_df['simultaneous'] = 0


test_df.reset_index(drop=True) # just in case
for i, row in test_df.iterrows():
    
    test_df.loc[i, 'simultaneous'] = int(np.sum(
        (test_df.login <= row.logout) & (test_df.logout >= row.login)
    )) -1  # note the subtraction by one saves us from having to do df.loc[~this_index]

print("--- %s seconds ---" % (time.time() - start_time))

test_df_old = pd.read_csv(dirname + '/data.csv').rename(columns={'simultaneous' : 'concurrent_count'})


start_time = time.time()

# loop through dataframe and check condition
for i in test_df.index:
    login, logout = test_df.loc[i,'login'], test_df.loc[i,'logout']
    this_index = test_df.index.isin([i])
    test_df.loc[i, 'simultaneous'] = int(sum(
        (test_df[~this_index]['login'] <= logout) & (test_df[~this_index]['logout'] >= login)
    ))
print("--- %s seconds ---" % (time.time() - start_time))

df=test_df
df['simultaneous'] = 0

start_time = time.time()

df['simultaneous'] = df.apply(
    lambda x: int(
        sum(
            (df[df['user_id'] != x['user_id']]['login'] <= x['logout']) &
            (df[df['user_id'] != x['user_id']]['logout'] >= x['login'])
        )
    ),
    axis=1
)
print("--- %s seconds ---" % (time.time() - start_time))


#df.to_csv('data.csv')







max_concur = df.groupby(['user_id'], sort=False)['simultaneous'].max()


df_total_time = df.copy()

df_total_time['log_period'] = (df_total_time['logout'] - df_total_time['login']).astype('timedelta64[s]')

df_total_time = (df_total_time.groupby(['user_id'])['log_period'].agg('sum')/3600).round(2)



##########################################

shifting = connections.copy()
shifting['day'] = shifting['login'].dt.floor("D")

shifting['night'] = ''
shifting['morning'] = ''
shifting['afternoon'] = ''
shifting['evening'] = ''

def time_in_shift(start, end, shift_start, shift_end):

    if (end - start).total_seconds()/3600 > 24:
        return (end - start).total_seconds()/3600/4
    else:

        if start < shift_start:
            start = shift_start 
        if end > shift_end:
            end = shift_end

        time_spent = (end-start).total_seconds()/3600

        if time_spent < 0:
            time_spent = 0

        return time_spent


for i in shifting.index:
    shift_start=(shifting.loc[i,'day'],
             shifting.loc[i,'day'] + timedelta(hours = 6),
             shifting.loc[i,'day'] + timedelta(hours = 12),
             shifting.loc[i,'day'] + timedelta(hours = 18))
    shift_end=  (shift_start[1],    
             shift_start[2],
             shift_start[3],
             shift_start[0] + timedelta(days=1))

    for shift in range(4):

        shift_time = time_in_shift(shifting.loc[i,'login'], shifting.loc[i,'logout'], shift_start[shift], shift_end[shift])

        
        

        if shift == 0:
            shifting.loc[i,'night'] = shift_time
        elif shift == 1:
            shifting.loc[i,'morning'] = shift_time
        elif shift == 2:
            shifting.loc[i,'afternoon'] = shift_time
        elif shift == 3:
            shifting.loc[i,'evening'] = shift_time


shift_calc = shifting.groupby(['user_id']).agg({'night':'sum','morning':'sum', 'afternoon':'sum','evening':'sum'}).reset_index()


def shift_ratio_function(colname,shift):

    shift_calc[colname] = ((shift_calc[shift]/(shift_calc['night'] + shift_calc['morning'] + shift_calc['afternoon'] + shift_calc['evening'])) * 100).round(2)




shift_inputs = [{'night_ratio':'night'}, {'morning_ratio':'morning'}, {'afternoon_ratio':'afternoon'},{'evening_ratio':'evening'}]



for index in range(len(shift_inputs)):
    for key in shift_inputs[index]:
        shift_ratio_function(key,shift_inputs[index][key])
        #shift_calc[key] = ((shift_calc[shift_inputs[index][key]]/(shift_calc['night'] + shift_calc['morning'] + shift_calc['afternoon'] + shift_calc['evening'])) * 100).round(1)




shift_calc.to_sql('test_data_repo.view_table', db, if_exists='replace', index=False)


from google.oauth2 import service_account


