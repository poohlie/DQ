#Setting up packages

import sys
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

def recur_dictify(df):
    """This function takes in a dataframe and returns a nested dictionary.
    This function will be used to return a dictionary of the variance checks that need to be run, and their respective parameters."""
    if len(df.columns) == 1:
        if df.values.size == 1: return df.values[0][0]
        return df.values.squeeze()
    grouped = df.groupby(df.columns[0])
    d = {k: recur_dictify(g.iloc[:,1:]) for k,g in grouped}
    return d

def constructs(uid,pwd):
    """This function by default reads in a list of checks to be run for the category of completeness checks based on expected frequency of data.
    It assumes that all these are mapped to a specific exception_type_id in the table.
    Mapping in the table data_quality_check_config_v2 follows the format of
    data_frame = data_frame name
    table_name = name of table that the check is run on
    field_name = attribute by which completeness is checked against. typically data_date
    data_source = 3 letter data source code
    data_vendor = RDM data_vendor code
    exception_type_id = 126 (subject to change, this is based on what is in the config at this time)
    exception_type_name = Completeness Check (python)
    check_property = python
    check_property_value = sql by which the data_date, identifier, freq (if available e.g. eco), value is obtained
    """
    sql = """
    select * from rdm.poc_data_quality_check_config where exception_type_id = 11
    """
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=tcp:DB;PORT=1433;DATABASE=RDM_POC;UID='+uid+';PWD='+pwd)
    cursor = cnxn.cursor()
    df = pd.read_sql(sql,cnxn)
    cursor.close()
    del cursor
    cnxn.close()

    #setting up the necessary lists and dictionaries to feed through the script
    #note that the list of data frames and field names are expected to be unique here.
    df_list = df['dq_check_id'].unique().tolist()
    df2 = df[['dq_check_id','check_property','check_property_value']].copy()
    df_dict = recur_dictify(df2)
    return (df_list, df_dict, df)
 
#Data extraction functions
#Function for extraction of raw data
def rawdata(sql):
    """This function takes an SQL as an input, and extracts the information required into a dataframe.
    Connection is made to SQL server using Windows authentication, hence no need for credentials. """
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=tcp:DB;PORT=1433;DATABASE=RDM;Trusted_connect=yes')
    cursor = cnxn.cursor()
    df = pd.read_sql(sql,cnxn)
    cursor.close()
    del cursor
    cnxn.close()
    #removing rows where there are no values
    df=df[df.value.notnull()]
    #sorting the dataframe so that we can do the shift
    df = df.sort_values(by=['identifier','data_date'])
    #calculating a change column
    df['lagged'] = df.groupby(['identifier'])['value'].shift(1)
    df = df[df.lagged.notnull()]
    df['change'] = (df['value']-df['lagged'])/df['lagged']
    df['data_date'] = pd.to_datetime(df['data_date'], format='%Y-%m-%d')
    return df

#Function for extraction of dates to run check on
def checkdates(date_sql):
    """This function takes an SQL as an input, and extracts a list of dates for which checks need to be done.
    Connection is made to SQL server using Windows authentication, hence no need for credentials. """
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=tcp:DB;PORT=1433;DATABASE=RDM;Trusted_connect=yes')
    cursor = cnxn.cursor()
    cursor.execute(date_sql)
    date_list=list(cursor.fetchall())
    cursor.close()
    del cursor
    cnxn.close()
    #list comprehension to extract and convert to datetime format the first element of each tuple in the list into a new list.
    date_list = [datetime.strptime(i[0], '%Y-%m-%d') for i in date_list]
    return date_list

#Function for removing series without data for check date.
def clean_raw(df, input_date, window_size, small_window):
    """This function takes a dataframe and a date as an input, and extracts the data that is required to carry out the jump checks for the date.
    It makes an assumption that the frequency of the data is the same. """
    #pivot data to wide format for easy filtering.
    df = df.loc[df.data_date<=input_date]
    df = df.sort_values(by=['identifier','data_date'])
    df = df.groupby('identifier').tail(window_size)
    df2 = df.pivot(index='data_date', columns = 'identifier', values = 'change')
    #removing the columns (series) that are not required
    #Filter for the last 30 days. assuming only 30 days worth of change data will be required
    df2 = df2.tail(window_size)
    cols = ~df2.isna()[-1:].iloc[0]
    df2 = df2[cols[cols].index]
    df2 = df2.reset_index()
    #reshape data back into long format 
    df2 = df2.melt(id_vars='data_date')
    #merging to get back original columns
    df3 = pd.merge(df2, df, how='left', on=['data_date','identifier'], suffixes=('','_y'))
    df3 = df3[['data_date','identifier','value_y','lagged','change']]
    df3.columns = ['data_date','identifier','value','lagged','change']
    df3 = df3[df3.value.notnull()]
    return df3

#Function for calculating the Bollinger bands, and which side of the bands the point lies
def bands(df,sd_size, window_size, small_window, input_date):
    """
    This function calculates the sd_size SD bands around the mean, and also returns the side on which the value lies.
    The direction is indicated as -1 if the point lies below the bands, 1 if the point lies above the bands, and 0 otherwise."""
    df['mean'] = df.groupby('identifier')['change'].rolling(window_size, min_periods=small_window).mean().reset_index(0,drop=True)
    df['sd'] = df.groupby('identifier')['change'].rolling(window_size, min_periods=small_window).std().reset_index(0,drop=True)
    df['lower_band'] = df['mean'] - sd_size*df['sd']
    df['upper_band'] = df['mean'] + sd_size*df['sd']
    df2 = df[df['data_date']==input_date].reset_index()
    #setting up conditions for the direction
    conditions = [
        df2['change']<df2['lower_band'],
        df2['change']>df2['upper_band'],
        ((df2['change']<=df2['upper_band']) & (df2['change']>=df2['lower_band']))
    ]
    choices = [-1, 1, 0]
    df2['direction'] = np.select(conditions,choices,default=0)
    return df2

#Function for creating a correlation matrix
def corr(df, window_size, small_window, input_date):
    """
    This function creates a correlation matrix using the using the cleaned raw data.
    The window_size here remains the same as the size used for the calculation of the rolling SD."""
    df = df.pivot(index='data_date', columns='identifier', values='change')
    df = df.loc[df.index<=input_date]
    df = df.tail(window_size)
    #creating the correlation matrix
    df2 = df.corr(method='pearson',min_periods=small_window)
    df2['data_date'] = input_date
    df2 = df2.reset_index()
    return df2

#Function for a list of peers for each series
def peer(df, peer_size):
    """
    This function creates a list of peers for each series based on the correlation that is included in the input df.
    Peer_size takes into account how many peers we aim to return.
    What will be returned is a list of friends and enemies, each of size peer_size, subjection to the correlation being in the right direction. """
    df = pd.melt(df, id_vars=(['data_date','identifier']), var_name='peer_identifier', value_name='correlation')
    df = df.sort_values(by=['identifier','correlation'])
    df = df.dropna() #remove rows with no correlation
    df = df[df['identifier']!=df['peer_identifier']] #remove rows where it is correlation with self
    enemies = df.groupby(['data_date','identifier'])['data_date','identifier','peer_identifier','correlation'].head(peer_size)
    #enemies that have positive correlation are rmoved
    enemies = enemies[enemies['correlation']<0]
    enemies['sign'] = -1/(peer_size*2)
    friends = df.groupby(['data_date','identifier'])['data_date','identifier','peer_identifier','correlation'].tail(peer_size)
    #friends that have negative correlation are removed
    friends = friends[friends['correlation']>0]
    friends['sign'] = 1/(peer_size*2)
    peers = pd.concat([enemies,friends])
    return peers

#Function for combining the peers with their directions, to obtain a list of dates, series, and their respective peers and expected direction.
def peer_direction(peers, banded, match_rate):
    """
    This function takes maps a list of series to its peers and their respective directions."""
    df = pd.merge(peers, banded, how='left', left_on=['data_date','peer_identifier'], right_on=['data_date','identifier'], suffixes=('','_y'))
    #keeping only relevant columns
    df = df[['data_date','identifier','peer_identifier','correlation','sign','direction']]
    #grouping to get the expected direction for each identifier
    df2 = df[['data_date','identifier','sign','direction']].copy()
    df2['peer_direction'] = df2['sign']*df2['direction']
    df2 = df2.groupby(['data_date','identifier'])['peer_direction'].sum().reset_index()
    df2 = df2[['data_date','identifier','peer_direction']]
    df2['peer_direction'] = df2['peer_direction'].apply(lambda x: -1 if x<(-1*match_rate) else(0 if x<match_rate else 1))
    df3 = pd.merge(banded, df2, how='left', on=['data_date','identifier'], suffixes=('','_y'))
    df3 = df3[['data_date','identifier','direction','peer_direction']]
    return df3

#Decision tree for flag direction
def flag(peers_direction):
    """
    This function takes in a list of series, theirs and their peers' directions, and filters for a list of exceptions.
    """
    peers_direction['flag'] = np.where((((peers_direction['direction']==peers_direction['peer_direction']).astype(int)) + ((((peers_direction['direction']*peers_direction['direction']))*(-1))+1))>0, False, True)
    return peers_direction

#Function to filter for exception records
def exception_list(df):
    """
    This function takes in a list of securities and their conditions, and filters for where the exceptions are."""
    df = df[['data_date','identifier','flag']]
    df = df[df['flag']==True]
    return df
 
def merge_config(data_frame, exception_df, config_df, raw_df, df_dict):
    """This function merges the exceptions with the reference or configuration details to form the full exceptions table."""
    config_df = config_df[config_df['check_property'] == 'list']
    exception_df['dq_check_id'] = data_frame
    exception_merged = pd.merge(exception_df, config_df, how = 'left', on = 'dq_check_id', suffixes = ('_left','_right'))
    exception_merged = exception_merged[['data_date','identifier','dq_check_id','data_frame','table_name','field_name','data_source','data_vendor','exception_type_id','exception_type_name','criticality']]
    exception_merged = pd.merge(exception_merged, raw_df, how = 'left', on = ['identifier','data_date'], suffixes = ('_left','_right'))
    exception_merged['sd_size'] = df_dict[data_frame]['sd_size']
    exception_merged['peer_size'] = df_dict[data_frame]['peer_size']
    exception_merged['match_rate'] = df_dict[data_frame]['match_rate']
    exception_merged['message'] = 'Jump in series value exceeded threshold of '+exception_merged['sd_size'].astype(str)+'SD (jump should lie between '+round(exception_merged['lower_band']*100,2).astype(str)+'% and '+round(exception_merged['upper_band']*100,2).astype(str)+'%), compared against '+(exception_merged['peer_size'].astype(int)*2).astype(str)+' peers, out of which '+exception_merged['match_rate']+' of the peers did not see large jumps. Change in value was '+(round(exception_merged['change']*100,2)).astype(str)+'%, from a previous value of '+(exception_merged['lagged']).astype(str)+' to its current value of '+(exception_merged['value']).astype(str)+'.'
    exception_merged = exception_merged[['exception_type_id','exception_type_name','dq_check_id','data_frame','data_vendor','data_source','identifier','field_name','data_date','message','value','lagged','criticality']]
    exception_merged.columns = ['exception_type_id','exception_type_name','dq_check_id','data_frame','data_vendor','data_source','guid','field_name','data_date','exception_message','field_value','comparison_value','criticality']
    return exception_merged

#Main function
def main():
    """
    Main body of the function. This stitches together the codes to run the jump checks. """
    uid = sys.argv[1]
    pwd = sys.argv[2]
    
    df_list, df_dict, df_config = constructs(uid,pwd)
    exceptions = pd.DataFrame()

    window_size=30
    small_window=20

    for data_frame in df_list:
        sd_size = int(df_dict[data_frame]['sd_size'])
        peer_size = int(df_dict[data_frame]['peer_size'])
        match_rate = float(df_dict[data_frame]['match_rate'])
        df = rawdata(df_dict[data_frame]['list'])
        date_list = checkdates(df_dict[data_frame]['date'])

        #Loop through dates for the checks
        for input_date in date_list:
            cleaned_raw = clean_raw(df, input_date, window_size,small_window)
            banded = bands(cleaned_raw,sd_size, window_size,small_window, input_date)
            correlation_matrix = corr(cleaned_raw, window_size, small_window, input_date)
            peers = peer(correlation_matrix, peer_size)
            peers_direction = peer_direction(peers, banded, match_rate)
            flags = flag(peers_direction)
            exceptions_list = exception_list(flags)
            merged_config_df = merge_config(data_frame, exceptions_list, df_config, banded, df_dict)
            exceptions = exceptions.append(merged_config_df)

    exceptions.to_csv('variancecheck.csv',index=False)

#Code call
if __name__=="__main__":
    main()