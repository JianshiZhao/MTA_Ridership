# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 21:22:42 2016

@author: Jianshi
"""

import urllib
import pdb
import pandas
from datetime import datetime
from datetime import timedelta
import pandas


weather_csv_raw = 'C:\\DataScience\\test.txt'
weather_csv = 'C:\\DataScience\\weather.txt'
mta_weather_csv = 'C:\\DataScience\\mta_weather.csv'

mtadata_file_folder = 'C:\\DataScience\\nycmta\\'

st_date = '06-06-2015'
end_date = '06-11-2016'



def download_weather_data(st_date,end_date,savepath):
    '''
    Down load weather data provided date range. Date format 'mm-dd-yyyy'
    '''
    urlbase = "https://www.wunderground.com/"
    urllocation = "history/airport/KNYC/"
    urlstartdate = datetime.strptime(st_date,'%m-%d-%Y').strftime('%Y/%m/%d/')
    urlend_day = str(datetime.strptime(end_date,'%m-%d-%Y').day).zfill(2)
    urlend_mon = str(datetime.strptime(end_date,'%m-%d-%Y').month).zfill(2)
    urlend_year = str(datetime.strptime(end_date,'%m-%d-%Y').year)
    urlenddate = ''.join(['CustomHistory.html?dayend=',urlend_day,'&monthend=',urlend_mon,'&yearend=',urlend_year])
    urlextra = "&req_city=NA&req_state=NA&req_statename=NA&format=1"
    url = ''.join([urlbase,urllocation,urlstartdate,urlenddate,urlextra])
    textfile = urllib.URLopener()
    textfile.retrieve(url,savepath)

def clean_weather_data(inputcsv,newcsv):
    with open(inputcsv,'r') as f:
        text  = f.readlines()
    with open(newcsv,'w') as outfile:        
        for line in text:
            if len(line)>1:
                new_line = line.replace('<br />','')
                outfile.write(new_line)


def weather_match_mta(weather_csv, mta_weather_csv):
    '''
    Select the weather corresponding to the date of the mta data. 
    '''
    weather_df = pandas.read_csv(weather_csv)
    mta_weather = weather_df.iloc[0::7,:]
    mta_weather.to_csv(mta_weather_csv)

def download_mta_data(st_date,end_date,file_path):
    '''
    Provide the start and enddate to download the data file. The format of the
    date is a string 'mm-dd-yyyy'. The data on the public wesite 
    '''
    textfile = urllib.URLopener()
    url_base = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"
    #urldate = '160507'
    url_end = '.txt'
    # Change date to datetime object
    st_datetime = datetime.strptime(st_date,'%m-%d-%Y')
    end_datetime = datetime.strptime(end_date,'%m-%d-%Y')
    this_datetime = st_datetime
    while (end_datetime - this_datetime).days>0:
        url_date = this_datetime.strftime('%y%m%d')
        url = ''.join([url_base,url_date,url_end])
        textfile.retrieve(url,''.join([file_path,url_date,url_end]))
        this_datetime = this_datetime + timedelta(days = 7)        
        


def weekly_statistics(enddate):
    '''
    data argument format 'mm-dd-yyyy'
    Examine the statistics of a single week. Qestions to answer include:
    How many stations are included?
    What is the average hourly entry for each station?
    What is the total entries for that station?
    '''
    # create a dataframe for this day
        
    file_date = datetime.strptime(date,'%m-%d-%Y').strftime('%y%m%d')
    single_df = pandas.read_csv(''.join([mtadata_file_folder,file_date,'.txt']))
    # Number of UNITs
    print 'Number of UNITs: ', len(single_df['UNIT'].unique()) 
    print 'Number of Stations: ', len(single_df['STATION'].unique())
    
    
def unique_unit(dataframe):
    '''
    Find the unique unit and gather them together in time order
    '''
    unique_key = (dataframe['C/A'] + ',' + dataframe['UNIT'] + ',' + dataframe['SCP'] + ',' + dataframe['STATION']).unique()
    return unique_key
    

def combine_all_file(mtadata_file_folder, st_date, end_date):
    '''
    Combine all the data file in the last year
    '''
    st_dt = datetime.strptime(st_date,'%m-%d-%Y')
    end_dt = datetime.strptime(end_date,'%m-%d-%Y')
    this_dt = st_dt
    frame_num = 1
    while (end_dt - this_dt).days >=0:
        file_date = this_dt.strftime('%y%m%d')
        file_path = ''.join([mtadata_file_folder,file_date,'.txt'])
        this_frame = pandas.read_csv(file_path)
        try:
            all_frames = all_frames.append(this_frame,ignore_index = True)
            print "New frame appended. Number: " , frame_num 
        except NameError:
            all_frames = this_frame
            print "all_frames created!"
        this_dt = this_dt + timedelta(days = 7)   
        frame_num = frame_num +1
    
    return all_frames
    
    
def separate_unit(all_frames):
    '''
    This function is used to seperate all the different units and arange the record in 
    time order.
    '''
    unique_keys = unique_unit(all_frames)
    all_frame_key = all_frames['C/A'] + ',' + all_frames['UNIT'] + ',' + all_frames['SCP']+ ',' + all_frames['STATION']
    unit_dict = {}
    key_num = len(unique_keys)
    print "Total Keys Number: ", key_num
    for k in range(key_num):
        unit_dict[unique_keys[k]] = all_frames[all_frame_key == unique_keys[k]]
        print "Key:", k,'\t', unique_keys[k]
    return unit_dict
        
    


def add_hourly_entry(unit_frame):
    '''
    For each unit calculate the hourly entry and add to the dataframe for that unit
    '''
    unit_frame['4HR_ENTRY'] = unit_frame['ENTRIES'] - unit_frame['ENTRIES'].shift(1)
    unit_frame['DATE_TIME'] = pandas.to_datetime(unit_frame['DATE']+','+unit_frame['TIME'])
    unit_frame['4HR_ENTRY'] = unit_frame['4HR_ENTRY'].shift(-1)
    unit_frame['4HR_ENTRY'].fillna(0,inplace = True)
    unit_frame.loc[unit_frame['4HR_ENTRY']<0,'4HR_ENTRY'] = 0.0
    return unit_frame
    
    
def generate_daily_entry(unit_frame):
    '''
    This function returns a datafrme for separate dates
    '''
    unique_date = unit_frame['DATE'].unique()
    daily_entry={}
    daily_entry['DATE'] = []
    daily_entry['DAILY_TOTAL'] = []
    for date in unique_date:
        daily_entry['DATE'].append(date)
        daily_entry['DAILY_TOTAL'].append(unit_frame[unit_frame['DATE']==date]['4HR_ENTRY'].sum())
    
    df = pandas.DataFrame.from_dict(daily_entry)
    df['C/A'] = unit_frame['C/A'].iloc[0]
    df['UNIT'] = unit_frame['UNIT'].iloc[0]
    df['STATION'] = unit_frame['STATION'].iloc[0]
    return df
        


def generate_station_entry(daily_entry):
    '''
    This function is to create the daily entry record for each station. One 
    station may have many monitor units.
    '''
    unique_date = daily_entry['DATE'].unique()
    unique_station = daily_entry['STATION'].unique()
    station_entry = {}
    station_entry['STATION'] = []
    station_entry['DATE'] = []
    station_entry['STATION_TOTAL'] = []
    for station in unique_station:
        for date in unique_date:
            station_entry['DATE'].append(date)
            station_entry['STATION'].append(station)
            station_total = daily_entry[(daily_entry['STATION']==station) & (daily_entry['DATE']==date)]['DAILY_TOTAL'].sum()
            station_entry['STATION_TOTAL'].append(station_total)
            print "STATION, DATE:", station, date
    
    df = pandas.DataFrame.from_dict(station_entry)
    return df
    
    
    

        

if __name__ == '__main__':


'''
# The following code is to use the unit_dict to create daily entry for each unit    
key_num = 1    
for key in unit_dict.keys():
    unit_dict[key] = add_hourly_entry(unit_dict[key])
    try:
        daily_entry = daily_entry.append(generate_daily_entry(unit_dict[key]))
        print "Key number:", key_num
    except NameError:
        daily_entry = generate_daily_entry(unit_dict[key])
        print type(daily_entry)
        print "daily_entry created!"
    key_num = key_num +1    
'''    

# the following code is to create a full dataframe for all the units, and later 
# will used to generate the station entry dataframe

key_num = 1    
for key in unit_dict.keys():   
    try:
        daily_entry = daily_entry.append(generate_daily_entry(unit_dict[key]))
        print "Key number:", key_num
    except NameError:
        daily_entry = generate_daily_entry(unit_dict[key])
        print type(daily_entry)
        print "daily_entry created!"
    key_num = key_num +1    



    
    
    
    
    #all_frames = combine_all_file(mtadata_file_folder, st_date, end_date)
    #download_weather_data(url,weather_file_path)
    #clean_weather_data(weather_csv,weather_csv_new)
    


#--------------  check any 4HR_ENTRY has negative number ---------------
for key in unit_dict.keys():
    if len(unit_dict[key][unit_dict[key]['4HR_ENTRY']<0]) >0 :
        print key

key_number = 0
for key in unit_dict.keys():
    unit_dict[key].loc[unit_dict[key]['4HR_ENTRY']<0,'4HR_ENTRY'] = 0.0
    print 'Key number'    
    key_number = key_number + 1



# --------------------------- pandas great group by function ------------------

# to generate the statoin daily entry, just use the group by function, only takes for a single second 
# for 200000 records.

station_entry = daily_entry.groupby(['STATION','DATE'])['DAILY_TOTAL'].sum().reset_index(name='STATION_DAILY_TOTAL')




