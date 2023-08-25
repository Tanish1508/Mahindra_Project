"""script to prepare data for prediction
also predict using saved model
then predict repeat the process for next 6 rows
* next 6 rows are the last 5 rows of the input file + the predicted row
** predicted row is the predicted value of the last row of the input file

now after predicting the last row of the input file, we need to process the input file again
and predict the next 6 rows
using function called reprocess_input_file

"""
import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler

##imp data var
train = "train_time_data.csv"
train_data = pd.read_csv(train)
train_data.drop('Stop_Time',inplace=True,axis=1)
input_logfile_raw = "" #raw file from MES system
input_logfile = "" #processed raw file
model_path = "new_model_early_stop.h5"

start_time = "2021-03-01 00:00:00"
end_time = "2021-03-31 23:59:59"

scaler = MinMaxScaler()

df = pd.read_csv('21test.csv')
LOOK_BACK = 6
FORECAST_RANGE = 1

#function to calculate hours between two time stamps
def hours_between(start_time, end_time):
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    time_diff = end_time - start_time
    return time_diff.total_seconds()/3600

total_hours = hours_between(start_time, end_time)
print(total_hours)

####important functions start
def split_sequence(sequence, look_back, forecast_horizon):
 X, y = list(), list()
 for i in range(len(sequence)): 
   lag_end = i + look_back
   forecast_end = lag_end + forecast_horizon
   if forecast_end > len(sequence):
     break
   seq_x, seq_y = sequence[i:lag_end], sequence[lag_end:forecast_end]
   X.append(seq_x)
   y.append(seq_y)
 return np.array(X), np.array(y)


def inverse_transform(y_test, yhat):
 y_test_reshaped = y_test.reshape(-1, y_test.shape[-1])
 yhat_reshaped = yhat.reshape(-1, yhat.shape[-1])
 yhat_inverse = scaler.inverse_transform(yhat_reshaped) 
 y_test_inverse = scaler.inverse_transform(y_test_reshaped)
 return yhat_inverse, y_test_inverse


def evaluate_forecast(y_test_inverse, yhat_inverse):
 mse_ = tf.keras.losses.MeanSquaredError()
 mae_ = tf.keras.losses.MeanAbsoluteError()
 mape_ = tf.keras.losses.MeanAbsolutePercentageError()
 mae = mae_(y_test_inverse,yhat_inverse)
 print('mae:', mae)
 mse = mse_(y_test_inverse,yhat_inverse)
 print('mse:', mse)
 mape = mape_(y_test_inverse,yhat_inverse)
 print('mape:', mape)

####important functions end


#scaler function to scale data according to the training data
def scale_data(data,train):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler = scaler.fit(train)
    scaled_data = scaler.transform(data)
    return scaled_data, scaler
    

# def get_last_6_rows(path):
#     #validate whether the input file has 6 data rows

#     df = pd.read_csv(path)
#     if len(df) < 6:
#         print("Input file has less than 6 data rows")
#         return None
#     df.drop('Stop_Time',inplace=True,axis=1)
#     df = df[-6:]
#     return df

#create pd.get_last_6_rows to use with df
def get_last_6_rows_df(df):
    if len(df) < 6:
        print("Input file has less than 6 data rows")
        return None
    df = df[-6:]
    return df

def load_model_predict(model_path):
    model = tf.keras.models.load_model(model_path)
    # print(model.summary())
    return model


#prepare initial input data for prediction
def main(df):
    #remove stop time column
    
    model_path = "new_model_early_stop.h5"
    df.drop('Stop_Time',inplace=True,axis=1)
    df.apply(get_last_6_rows_df)
    model = load_model_predict(model_path)
    # evaluate_forecast(y_test_inverse, yhat_inverse)
    dat = []
    dat, scaler = scale_data(df,train_data)
    

    X, y = split_sequence(dat, LOOK_BACK, FORECAST_RANGE)
    yhat = model.predict(X, verbose=0)
    yhat_inverse, y_test_inverse = inverse_transform(y, yhat)
    print(yhat_inverse)


# list of columns of dat and train_data
dfcols = list(df.columns)
traincols = list(train_data.columns)

#function to show intersection of columns
def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

def diff(lst1, lst2):
    lst3 = [value for value in lst1 if value not in lst2]
    return lst3
print(*dfcols, sep = ", ")
# print(f"len of intersectio diff(dfcols,traincols))}")
# main(df)



#function to predict using model



