# %%
import pandas as pd
import sys
import numpy as np

# %%
#accept path as cli argument 

# path = sys.argv[0]
# namef = sys.argv[1]
def main(path,namef):
    # df = pd.read_csv(r"D:\USER\Downloads\QualityCapture-30-120\Line_Stop_History_Report_original.csv")
    df = pd.read_csv(path)
    # df

    # %%
    stations_dict = {
    'VTU-30':'VTU-030',
    'VTU-40':'VTU-040',
    'VTU-60' :'VTU-060',
    'VTU-70' :'VTU-070',
    'VTU-80' :'VTU-080',
    'VTU-90':'VTU-090',
    }
    stations_list = [
    'VTU-030',
    'VTU-040',
    'VTU-060',
    'VTU-070',
    'VTU-080',
    'VTU-090',
    'VTU-100',
    'VTU-110',
    'VTU-120',
    ]

    # %%
    #replace Station_Names using stations_dict
    df['Station_Name'] = df['Station_Name'].replace(stations_dict)
    # df

    # %%
    #remove null value from df
    df = df[df['Stop_Reason'].notna()]

    # %%
    #check number of null values for each column
    df.isnull().sum()


    # %%
    df.drop(columns=['Primary_Owner','Secondary_Owner','Line_Stop_By'], inplace=True)

    # %%
    df.isnull().sum()

    # %%
    #remove null values from df['Serial_No']
    df = df[df['Serial_No'].notna()]

    # %%
    df = df[df['ActualLineLoss'].notna()]

    # %%
    # #len(df)

    # %%
    #select rows where Date in ['29 Sep 2022','30 Sep 2022']
    # df = df[df['Date'].isin(['29 Sep 2022','30 Sep 2022'])]
    # #len(df)

    # %%
    #for train and test set
    #dates before 15 Aug 2022 for train set
    #get row number for first occurence of date 16 Aug 2022
    #select rows  Stop_Time < 16/08/2022 00:00:00
    # df_train = df[df['Stop_Time'] < '16/08/2022 00:00:00']
    # len(df_train)
    # df = df_train.copy()
    # #dates after 15 Aug 2022 for test set
    # test_row_num = df[df['Date'] == '16 Aug 2022']
    # df_test = df[test_row_num.index[0]:]
    # dates = ['21 Sep 2022', '22 Sep 2022', '23 Sep 2022', '24 Sep 2022', '25 Sep 2022', '26 Sep 2022', '27 Sep 2022', '28 Sep 2022', '29 Sep 2022', '30 Sep 2022']
    # #select rows where Date is not in dates
    # df = df[~df['Date'].isin(dates)]
    ##len(df)

    # %%
    # #for testing purpose
    #all dates from 21 Sep 2022 - 30 Sep 2022
    dates = ['21 Sep 2022', '22 Sep 2022', '23 Sep 2022', '24 Sep 2022', '25 Sep 2022', '26 Sep 2022', '27 Sep 2022', '28 Sep 2022', '29 Sep 2022', '30 Sep 2022']
    #select rows where Date is not in dates
    df = df[df['Date'].isin(dates)]

    # %%
    # df = df[df['Date'].isin(['27 Sep 2022','28 Sep 2022'])]
    #len(df)

    # %%
    #convert ActualLineLoss to seconds
    # df['ActualLineLoss'] = pd.to_timedelta(df['ActualLineLoss'])

    df['ActualLineLoss_secs'] = (pd.to_timedelta(df['ActualLineLoss'])).dt.total_seconds()
    # df

    # %%
    df['ActualLineLoss_secs'].isnull().sum()

    # %%
    df[df['ActualLineLoss_secs'] != 0]

    # %%
    #create column Duration_secs from Duration
    # df['Duration_secs'] = (pd.to_timedelta(df['Duration'])).dt.total_seconds()
    # df[df['Duration_secs'] != df['ActualLineLoss_secs'] == 0]

    # %%
    # df['MCSPS'] = 0
    df = df[df['ActualLineLoss_secs'] != 0]
    # df

    # %%
    #create column index values as index
    df = df.reset_index(drop=True)
    df['index'] = df.index
    # df

    # %%
    df = df[df['Station_Name']!= 'VTU-Masking']

    # %%
    df = df[df['Station_Name'] != 'VTU-50']

    # %%
    df['Time'] = pd.to_datetime(df['Stop_Time'])

    # %%
    #group by Station_Name
    df_group = df.groupby('Station_Name')
    #create df for each group
    df_group_list = [df_group.get_group(x) for x in df_group.groups]
    new_df = pd.DataFrame()
    for name, group in df_group:
        ef = df_group.get_group(name) 
        ef['Stop_Time']  = pd.to_datetime(ef['Stop_Time'])
        ef['Stop_1'] = ef['Stop_Time'].shift(1)
        ef['Timesince_Previous_Stop'] = (ef['Stop_Time'] - ef['Stop_1']).dt.total_seconds()//60
        ef.dropna(inplace=True)
        ef['Timesince_Previous_Stop'] = ef['Timesince_Previous_Stop'].astype(int)
        new_df = pd.concat([new_df, ef], ignore_index=True)




    # %%
    # new_df['Timesince_Previous_Stop'] = (new_df['Stop_Time'] - new_df['Stop_1']).dt.total_seconds()//60
    # new_df['Timesince_Previous_Stop'] = new_df['Timesince_Previous_Stop'].astype(int)

    # %%
    new_df

    # %%
    #sort dataframe by Stop_Time
    new_df = new_df.sort_values(by=['Stop_Time'])
    new_df

    # %%
    new_df.info()

    # %%

    new_df.dropna(inplace=True)
    new_df.isna().sum()


    # %%
    new_df = new_df.reset_index(drop=True)


    # %%
    new_df = new_df[new_df['Timesince_Previous_Stop']>0]

    # %%
    morning_time = pd.to_datetime('06:00:00').time()
    morning_shift_time = pd.to_datetime('6:00:00').time()
    evening_shift_time = pd.to_datetime('15:30:00').time()

    # %%
    # def select_time(Stop_Time , m, e):
    #     print(type(Stop_Time),type(m),type(e))
    #     if Stop_Time >= m and Stop_Time <= e:
    #         return Stop_Time - pd.to_datetime(Stop_Time.year, Stop_Time.month, Stop_Time.day, 6, 0, 0)
    #     else:
    #         return Stop_Time - pd.to_datetime(Stop_Time.year, Stop_Time.month, Stop_Time.day, 15, 30, 0)    
    def select(Time_Since_Morning_Shift, Time_Since_Evening_Shift):
        if Time_Since_Morning_Shift < 0:
            return abs(Time_Since_Evening_Shift)
        elif Time_Since_Evening_Shift < 0:
            return Time_Since_Morning_Shift
        else:
            return min(Time_Since_Morning_Shift, Time_Since_Evening_Shift)

    # %%
    #create Columnd Date1 from Stop_Time
    new_df['Date1'] = pd.to_datetime(new_df['Stop_Time']).dt.date
    #create Column Morning_Time using Date1 and time 07:00:00
    new_df['Morning_Time'] = pd.to_datetime(new_df['Date1'].astype(str) + ' ' + morning_time.strftime('%H:%M:%S'))
    #create Column Morning_Shift_Time using Date1 and time 07:00:00
    new_df['Morning_Shift_Time'] = pd.to_datetime(new_df['Date1'].astype(str) + ' ' + morning_shift_time.strftime('%H:%M:%S'))
    #create Column Evening_Shift_Time using Date1 and time 15:30:00
    new_df['Evening_Shift_Time'] = pd.to_datetime(new_df['Date1'].astype(str) + ' ' + evening_shift_time.strftime('%H:%M:%S'))
    #create Column Time_Since_Morning using Stop_Time and Morning_Time in hours
    new_df['Time_Since_Morning'] = (new_df['Stop_Time'] - new_df['Morning_Time']).dt.total_seconds()//3600
    #create columne Time_Since_Shift_Start using select_time function 
    # new_df.info()
    # new_df['Time_Since_Shift_Start'] = new_df.apply(lambda x: select_time(x['Stop_Time'], x['Morning_Shift_Time'], x['Evening_Shift_Time']), axis=1)
    # #create Column Time_Since_Morning_Shift using Stop_Time and Morning_Shift_Time in hours
    new_df['Time_Since_Morning_Shift'] = (new_df['Stop_Time'] - new_df['Morning_Shift_Time']).dt.total_seconds()//3600
    # #create Column Time_Since_Evening_Shift using Stop_Time and Evening_Shift_Time in hours
    new_df['Time_Since_Evening_Shift'] = (new_df['Stop_Time'] - new_df['Evening_Shift_Time']).dt.total_seconds()//3600
    new_df['Time_Since_Shift_Start'] = new_df.apply(lambda x: select(x['Time_Since_Morning_Shift'], x['Time_Since_Evening_Shift']), axis=1)
    new_df.drop(columns=['Morning_Time', 'Morning_Shift_Time', 'Evening_Shift_Time', 'Time_Since_Morning_Shift', 'Time_Since_Evening_Shift','Date1'], inplace=True)
    new_df





    # %%
    #set Time_Since_Morning , Time_Since_Shift_Start to int
    # new_df['Time_Since_Morning'] = new_df['Time_Since_Morning'].astype(int)
    # new_df['Time_Since_Shift_Start'] = new_df['Time_Since_Shift_Start'].astype(int)
    # new_df

    # %%
    #Add 24 to Time_Since_Shift_Start if Time_Since_Shift_Start is negative
    # new_df.loc[new_df['Time_Since_Shift_Start'] < 0, 'Time_Since_Shift_Start'] = new_df['Time_Since_Shift_Start'] + 24
    # new_df

    # %%
    new_df

    # %%
    #if Time_Since Morning is negative, add 24 to Time_Since_Morning
    new_df.loc[new_df['Time_Since_Morning'] < 0, 'Time_Since_Morning'] = 12-abs(new_df['Time_Since_Morning']) + 12
    new_df

    # %%
    # new_df['Resume_Time'] = pd.to_datetime(new_df['Resume_Time'])


    # %%
    # generate normal entries
    #first calculate number of unique dates
    num_dates = new_df['Date'].nunique()
    dates = new_df['Date'].unique()
    #calculate min stop time max stop time for each date
    min_stop_time = new_df.groupby('Date')['Stop_Time'].min()
    max_stop_time = new_df.groupby('Date')['Stop_Time'].max()
    min_stop_time

    # %%
    # #get max from max_stop_time
    # max_stop_t = max_stop_time.max()
    # max_stop_t

    # %%
    #generate df with all dates with entries between min_stop_time - 10 mins and max_stop_time + 10 mins at a difference of 10 minute
    nf = pd.DataFrame()
    for i in range(num_dates):
        date = dates[i]
        min_time = min_stop_time[date] - pd.Timedelta(minutes=10)
        max_time = max_stop_time[date] + pd.Timedelta(minutes=10)
        jf = pd.DataFrame(pd.date_range(min_time, max_time, freq='10min',inclusive = 'both'), columns=['Time'])
        jf['Time_1'] = jf['Time'].shift(1)
        jf['Timesince_Previous_Stop'] = (jf['Time'] - jf['Time_1']).dt.total_seconds()//60
        jf.dropna(inplace=True)
        jf['Timesince_Previous_Stop'] = jf['Timesince_Previous_Stop'].astype(int)
        jf['Date1'] = pd.to_datetime(jf['Time']).dt.date
        #create Column Morning_Time using Date1 and time 07:00:00
        jf['Morning_Time'] = pd.to_datetime(jf['Date1'].astype(str) + ' ' + morning_time.strftime('%H:%M:%S'))
        #create Column Morning_Shift_Time using Date1 and time 07:00:00
        jf['Morning_Shift_Time'] = pd.to_datetime(jf['Date1'].astype(str) + ' ' + morning_shift_time.strftime('%H:%M:%S'))
        #create Column Evening_Shift_Time using Date1 and time 15:30:00
        jf['Evening_Shift_Time'] = pd.to_datetime(jf['Date1'].astype(str) + ' ' + evening_shift_time.strftime('%H:%M:%S'))
        #create Column Time_Since_Morning using Time and Morning_Time in hours
        jf['Time_Since_Morning'] = (jf['Time'] - jf['Morning_Time']).dt.total_seconds()//3600
        #create columne Time_Since_Shift_Start using select_time function 
        # #create Column Time_Since_Morning_Shift using Time and Morning_Shift_Time in hours
        jf['Time_Since_Morning_Shift'] = (jf['Time'] - jf['Morning_Shift_Time']).dt.total_seconds()//3600
        # #create Column Time_Since_Evening_Shift using Time and Evening_Shift_Time in hours
        jf['Time_Since_Evening_Shift'] = (jf['Time'] - jf['Evening_Shift_Time']).dt.total_seconds()//3600
        jf['Time_Since_Shift_Start'] = jf.apply(lambda x: select(x['Time_Since_Morning_Shift'], x['Time_Since_Evening_Shift']), axis=1)
        jf.drop(columns=['Morning_Time', 'Morning_Shift_Time', 'Evening_Shift_Time', 'Time_Since_Morning_Shift', 'Time_Since_Evening_Shift','Date1','Time_1'], inplace=True)
        
        nf = nf.append(jf)
        
    nf['Date'] = pd.to_datetime(nf['Time']).dt.date
    nf['Stop_Reason'] = 'Normal'
    nf['Remarks'] = 'Normal'
    nf['Station_Name'] = 'All'
    nf['Time_Since_Morning'] = nf['Time_Since_Morning'].astype(int)
    nf['Time_Since_Shift_Start'] = nf['Time_Since_Shift_Start'].astype(int)
    #rename Time column to Stop_Time
    nf.rename(columns={'Time':'Stop_Time'}, inplace=True)
    nf 



    # %%
    #add 12 + (12 -abs(Time_Since_Morning)) to Time_Since_Shift_Start if Time_Since_Shift_Start is negative
    nf.loc[nf['Time_Since_Shift_Start'] < 0, 'Time_Since_Shift_Start'] = 12 + (12 - abs(nf['Time_Since_Shift_Start']))


    # %%
    # #shift Resume_Time by 1 row
    # new_df['Resume_shift'] = new_df['Resume_Time'].shift(1)
    # #calculate Time_Since_Resume using Stop_Time and Resume_shift
    # new_df['Time_Between_Stops'] = (new_df['Stop_Time'] - new_df['Resume_shift']).dt.total_seconds()//60
    # new_df, new_df['Time_Between_Stops'].min(), new_df['Time_Between_Stops'].max(), new_df['Time_Between_Stops'].mean()


    # %%
    # new_df[new_df['Time_Between_Stops'] < 0]

    # %%
    useful_cols = ['Date','Stop','Stop_Time','Station_Name','Stop_Reason','Remarks','Timesince_Previous_Stop','Time_Since_Morning','Time_Since_Shift_Start']


    # %%
    nf['Stop'] = 0
    new_df['Stop'] = 1

    # %%
    # new_df

    # %%
    neg = nf[nf['Time_Since_Morning']<0]
    # neg


    # %%
    #reindex nf
    nf.reset_index(drop=True, inplace=True)

    # %%
    #add 12 + (12 -abs(Time_Since_Morning)) to Time_Since_Morning if Time_Since_Morning is negative
    nf.loc[nf['Time_Since_Morning'] < 0, 'Time_Since_Morning'] = 12 + (12 - abs(nf['Time_Since_Morning']))

    # %%
    nf = nf[useful_cols]
    new_df = new_df[useful_cols]
    new_df = new_df.append(nf)
    new_df


    # %%
    #sort new_df values by Stop_Time
    new_df.sort_values(by='Stop_Time', inplace=True)
    new_df.reset_index(drop=True, inplace=True)

    # %%
    new_df

    # %%
    #List of all Remarks
    Remarks = new_df['Remarks'].unique()
    num_Remarks = len(Remarks)
    #Create a Dictionary of Remarks and name them Remark_1, Remark_2, Remark_3, etc
    Remarks_Label = {}
    for i in range(num_Remarks):
        Remarks_Label[Remarks[i]] = 'Remark_' + str(i+1)
    print(Remarks_Label)
    #Replace Remarks with Remark_1, Remark_2, Remark_3, etc
    new_df.replace({'Remarks':Remarks_Label}, inplace=True)
    #perform same operation on Stop_Reason
    Stop_Reason = new_df['Stop_Reason'].unique()
    num_Stop_Reason = len(Stop_Reason)
    Stop_Reason_label = {}
    for i in range(num_Stop_Reason):
        Stop_Reason_label[Stop_Reason[i]] = 'Stop_Reason_' + str(i+1)
    print(Stop_Reason_label)
    new_df.replace({'Stop_Reason':Stop_Reason_label}, inplace=True)
    #perform same operation on Station_Name
    Station_Name = new_df['Station_Name'].unique()
    num_Station_Name = len(Station_Name)
    Station_Name_label = {}
    for i in range(num_Station_Name):
        Station_Name_label[Station_Name[i]] = 'Station_Name_' + str(i+1)
    print(Station_Name_label)
    new_df.replace({'Station_Name':Station_Name_label}, inplace=True)



    # %%
    gf = new_df.copy()
    #find number of occurences of each Remark
    gf['Remarks'].value_counts()
    #get min and max Stop_Time for each Remark
    gf.groupby('Remarks')['Stop_Time'].agg(['min','max'])
    #avg Time_Since_Previous_Stop for each Remark
    gf.groupby('Remarks')['Timesince_Previous_Stop'].mean()
    #create dictionary of Remarks and their mean Time_Since_Previous_Stop
    Remarks_dict = gf.groupby('Remarks')['Timesince_Previous_Stop'].mean().to_dict()
    #same for Stop_Reason
    Stop_Reason_dict = gf.groupby('Stop_Reason')['Timesince_Previous_Stop'].mean().to_dict()
    #create column Time_Since_Previous_Remark using Remarks_dict
    gf['Time_Since_Previous_Remark'] = gf['Remarks'].map(Remarks_dict)
    #create column Time_Since_Previous_Stop_Reason using Stop_Reason_dict
    gf['Time_Since_Previous_Stop_Reason'] = gf['Stop_Reason'].map(Stop_Reason_dict)

    # gf

    # %%
    new_df.isna().sum()

    # %%
    #add 12 + (12 -abs(Time_Since_Morning)) to Time_Since_Morning if Time_Since_Morning is negative
    gf.loc[gf['Time_Since_Morning'] < 0, 'Time_Since_Morning'] = 12 + (12 - abs(gf['Time_Since_Morning']))
    # neg = gf[gf['Time_Since_Shift_Start']<0]
    # neg


    # %%
    # gf
    #reg ex for only two letter word on single line
    # ^[a-zA-Z]{2}$

    # %%
    # gf.to_csv(r'C:\Users\priya\Projects\new_working\new_df.csv', index=False)

    # %%
    gf.describe()

    # %% [markdown]
    # PRINT LABEL ENCODINGS
    # 

    # %%
    #print Stop_Reason Dictionary
    print("Stop_Reason_label")
    for i in Stop_Reason_label:
        print(f" {i} : {Stop_Reason_label[i]}")
    #print Remarks Dictionary
    print("\n\nRemarks_label")
    for i in Remarks_Label:
        print(f" {i} : {Remarks_Label[i]}")

    # %%

    print("\n\nStation_Name_label")
    for i in Station_Name_label:
        print(f" {i} : {Station_Name_label[i]}")

    # %%
    ef = gf.copy()

    # %%
    #Create column Station_Name + Stop_Reason + Remarks
    # ef['Combination'] = gf['Station_Name'] + '-' + gf['Stop_Reason'] + '-' + gf['Remarks']
    # ef

    # %%
    #create dummy variables for Combination
    # ef = pd.get_dummies(ef, columns=['Combination'],prefix_sep = '-')
    # ef

    # %%
    #remove Combination- from column names
    # ef.columns = ef.columns.str.replace('Combination-','')

    # %%
    ef = gf.copy()

    # %%
    #Set Stop_Time to Hour:00:00
    ef['Stop_Time'] = ef['Stop_Time'].dt.floor('H')
    # ef


    # %%
    colsname = ['Stop','Date','Time_Since_Previous_Stop_Reason','Time_Since_Previous_Remark','Timesince_Previous_Stop']
    ef.drop(colsname, axis=1, inplace=True)


    # %%
    #create dummy variables for Station_Name, Stop_Reason, Remarks
    ef = pd.get_dummies(ef, columns=['Station_Name','Stop_Reason','Remarks'],prefix_sep = '-')
    ef.head()

    # %%
    ef.columns

    # %%
    l1 = ef.columns.to_list()
    l1 = l1[3:]
    # l1
    #create dictionary diss where keys l1 and values are 'sum'
    diss = {}
    for i in l1:
        diss[i] = 'sum'
    diss['Time_Since_Shift_Start'] =  'min'
    # diss

    # %%
    #groupby Stop_Time and sum all columns except min of Time_Since_Shift_Start
    tf = ef.groupby(['Stop_Time','Time_Since_Morning']).agg(diss).reset_index()


    # ef = ef.groupby(['Stop_Time','Time_Since_Morning','Time_Since_Shift_Start']).sum()
    # tf

    # %%
    #create a df from groupby result
    ff = pd.DataFrame(tf)
    # ff


    # %%
    fname = r'C:\Users\priya\Projects\new_working\\' + namef + r'.csv'
    ff.to_csv(fname, index=False)

    # %%
    #split data into train and test by 2022-10-08 00:00:00
    # train = ff[ff['Stop_Time'] < '16/08/2022 00:00:00']
    # test = ff[ff['Stop_Time'] >= '16/08/2022 00:00:00']
    # train.to_csv(r'C:\Users\priya\Projects\new_working\train_time_data_new.csv', index=False)
    # test.to_csv(r'C:\Users\priya\Projects\new_working\test_time_data_new.csv', index=False)


    # %%
    # l1


