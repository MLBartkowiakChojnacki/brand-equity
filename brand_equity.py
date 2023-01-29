# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 19:38:23 2023
@author: krzys
"""

import os
import pandas as pd
import csv
import numpy as np

def set_working_directory(path: str) -> None:
    os.chdir(path)


def open_file(file_name: str) -> pd.DataFrame:
    with open(f'{file_name}.csv', 'r') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.readline())
        df = pd.read_csv(f'{file_name}.csv', sep = dialect.delimiter)
    return df
        

def get_unique_values(data_frame: pd.DataFrame
                      , selected_columns: list) -> pd.DataFrame:
    unique_values = data_frame[selected_columns]
    unique_values = unique_values.drop_duplicates()
    return unique_values


def combine(data_frame_1: pd.DataFrame
            , data_frame_2: pd.DataFrame
            , method: str = 'cross') -> pd.DataFrame:
    combined = data_frame_1.merge(data_frame_2, how='cross')
    return combined


def flatten_data_frame(data_frame: pd.DataFrame, del_nans: str = 'yes') -> pd.DataFrame:
    flatten = data_frame.to_numpy().flatten()
    flatten = pd.DataFrame(flatten, columns=['COMPANIES'])
    if del_nans == 'yes':
        flatten = flatten.dropna()
    return flatten


def get_points_info(data_frame: pd.DataFrame, columns: list, index: str) -> pd.DataFrame:
    points = df_source[index + columns].dropna(how='all')
    points = points.set_index(index[0])
    columns = points.columns
    df = pd.DataFrame()
    for column in columns:
        points = df_source[[index[0], column]].dropna()
        df = df.append(points)
    df = df.reset_index(drop = True)
    df["COMPANIES"] = df[columns].apply(lambda x: x[x.notna()][0], axis=1)
    return df[["RecordNo","COMPANIES"]]


if __name__ == "__main__":
    #set_working_directory(path = r'C:\Users\krzys\OneDrive\Dokumenty\repo_git\brand-equity\data\raw')
    df_source = open_file(r'data/raw/Grupa.1')
    companies = open_file(r'data/raw/COMPANY')

    #combine data to get every combination of company and recordno
    record_no = get_unique_values(data_frame = df_source, selected_columns = ['RecordNo'])
    df_combined = combine(data_frame_1 = companies, data_frame_2 = record_no)
    df_combined = df_combined.fillna(np.nan)
    df_combined.columns = ['COMPANIES', 'CLIENT_NAME', 'RECORD_NO']
    
    #get info about points for specified questions for specified columns
    points_5_q7q8 = df_source[['RecordNo','X3M1']][df_source['X3M1'] != 999]
    points_5_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_5_q7q8['AWARANESS'] = 5
    cols = ['X3M2','X3M3','X3M4','X3M5','X3M6','X3M7','X3M8','X3M9','X3M10']
    points_4_q7q8 = df_source[['RecordNo'] + cols].dropna(how='all')
    points_4_q7q8 = get_points_info(data_frame = df_source, columns = cols, index = ['RecordNo'])
    points_4_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_4_q7q8['AWARANESS'] = 4

    cols = ['X4M1','X4M2','X4M3','X4M4','X4M5','X4M6','X4M7','X4M8','X4M9','X4M10']   
    points_2_q7q8 = df_source[['RecordNo'] + cols].dropna(how='all')
    points_2_q7q8 = get_points_info(data_frame = df_source, columns = cols, index = ['RecordNo'])
    points_2_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_2_q7q8['AWARANESS'] = 2
  
    df_points = pd.concat([points_5_q7q8, points_4_q7q8, points_2_q7q8], ignore_index=True)
    df_points = df_points.drop_duplicates(subset=['RECORD_NO','COMPANIES'], keep="first")   
    df_points = df_points.query('COMPANIES not in [999, 998]')
    
    points_1_q7q8 = pd.concat([df_points[['RECORD_NO', 'COMPANIES']], df_combined[['RECORD_NO', 'COMPANIES']]]).drop_duplicates(keep=False)
    points_1_q7q8['AWARANESS'] = 1
    df_points = pd.concat([df_points, points_1_q7q8], ignore_index=True)
    df_points = pd.merge(df_points, companies,  how='left', left_on=['COMPANIES'], right_on = ['COMPANY_CODE'])
    
    cols_familiarity = ['X5M1',	'X5M2',	'X5M3',	'X5M4',	'X5M5',	'X5M6',	'X5M7',	'X5M8',	'X5M9',	'X5M10']
    df_familiarity = df_source[['RecordNo'] + cols_familiarity]
    df_familiarity.columns = ['RecordNo',101,102,103,104,105,106,107,108,109,110]
    
    list_familiarity = []
    for row in range(df_familiarity.shape[0]):
        for col in df_familiarity.columns[df_familiarity.columns != 'RecordNo']:
            list_familiarity.append([df_familiarity.iloc[row]['RecordNo'], col, df_familiarity.iloc[row][col]])
    
    df_familiarity = pd.DataFrame(list_familiarity, columns = ['RECORD_NO', 'COMPANIES', 'FAMILIARITY'])
    df_points = pd.merge(df_points, df_familiarity,  how='left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO', 'COMPANIES'])

    cols_ui_x6 = ['X6M1','X6M2','X6M3',	'X6M4',	'X6M5',	'X6M6',	'X6M7',	'X6M8','X6M9','X6M10']
    cols_ui_x7 = ['X7M1','X7M2','X7M3','X7M4','X7M5','X7M6','X7M7','X7M8','X7M9','X7M10']
    
    df_ui_x6 = df_source[['RecordNo'] + cols_ui_x6]
    df_ui_x7 = df_source[['RecordNo'] + cols_ui_x7]
    df_ui_x6 = get_points_info(data_frame = df_ui_x6, columns = cols_ui_x6, index = ['RecordNo'])
    df_ui_x7 = get_points_info(data_frame = df_ui_x7, columns = cols_ui_x7, index = ['RecordNo'])
    df_ui_x6.columns = ['RECORD_NO_X6', 'COMPANIES']
    df_ui_x7.columns = ['RECORD_NO_X7', 'COMPANIES']
    
    df_points['Used Last Season/This Season'] = 1
    df_points = df_points.merge(right = df_ui_x6, how = 'left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO_X6', 'COMPANIES'])
    df_points = df_points.merge(right = df_ui_x7, how = 'left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO_X7', 'COMPANIES'])
    df_points.loc[~np.isnan(df_points['RECORD_NO_X6']), 'Used Last Season/This Season'] = 4
    df_points.loc[~np.isnan(df_points['RECORD_NO_X7']), 'Used Last Season/This Season'] = 3
    df_points.loc[df_points['RECORD_NO_X6'] == df_points['RECORD_NO_X7'], 'Used Last Season/This Season'] = 5
    df_points = df_points.drop(columns = ['RECORD_NO_X6', 'RECORD_NO_X7'])

    columns_future_use = ['X8M1',	'X8M2',	'X8M3',	'X8M4',	'X8M5',	'X8M6'
                          ,	'X8M7',	'X8M8',	'X8M9',	'X8M10']
    df_future_use = df_source[['RecordNo'] + columns_future_use]
    df_future_use.columns = ['RecordNo',101,102,103,104,105,106,107,108,109,110] 
    list_future_use = []
    for row in range(df_future_use.shape[0]):
        for col in df_future_use.columns[df_future_use.columns != 'RecordNo']:
            list_future_use.append([df_future_use.iloc[row]['RecordNo'], col, df_future_use.iloc[row][col]])
    
    df_future_use = pd.DataFrame(list_future_use, columns = ['RECORD_NO', 'COMPANIES', 'FUTURE_USE'])
    df_points = pd.merge(df_points, df_future_use,  how='left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO', 'COMPANIES'])


    columns_satisfaction = ['X9M1',	'X9M2',	'X9M3',	'X9M4',	'X9M5',	'X9M6'
                         ,	'X9M7',	'X9M8',	'X9M9',	'X9M10']
    df_satisfaction = df_source[['RecordNo'] + columns_satisfaction]
    df_satisfaction.columns = ['RecordNo',101,102,103,104,105,106,107,108,109,110]
    list_satisfaction = []
    for row in range(df_satisfaction.shape[0]):
        for col in df_satisfaction.columns[df_satisfaction.columns != 'RecordNo']:
            list_satisfaction.append([df_satisfaction.iloc[row]['RecordNo'], col, df_satisfaction.iloc[row][col]])
    
    df_satisfaction = pd.DataFrame(list_satisfaction, columns = ['RECORD_NO', 'COMPANIES', 'SATISFACTION'])
    df_points = pd.merge(df_points, df_satisfaction,  how='left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO', 'COMPANIES'])

    #info about points for preference
    columns_preference = ['X10M1', 'X10M2', 'X10M3']
    df_preference_3 = df_source[['RecordNo', 'X10M1']]
    df_preference_2 = df_source[['RecordNo', 'X10M2']]
    df_preference_1 = df_source[['RecordNo', 'X10M3']]
    df_preference_3 = get_points_info(data_frame=df_preference_3, columns = ['X10M1'], index = ['RecordNo'])
    df_preference_2 = get_points_info(data_frame=df_preference_2, columns = ['X10M2'], index = ['RecordNo'])
    df_preference_1 = get_points_info(data_frame=df_preference_1, columns = ['X10M3'], index = ['RecordNo'])
    df_preference_3.columns = ['RECORD_NO', 'COMPANIES']
    df_preference_2.columns = ['RECORD_NO', 'COMPANIES']
    df_preference_1.columns = ['RECORD_NO', 'COMPANIES']
    df_preference_3['PREFERENCE'] = 5
    df_preference_2['PREFERENCE'] = 3
    df_preference_1['PREFERENCE'] = 1
    df_preference = pd.concat([df_preference_3, df_preference_2, df_preference_1])
    df_points = pd.merge(df_points, df_preference,  how='left', left_on=['RECORD_NO', 'COMPANIES'], right_on = ['RECORD_NO', 'COMPANIES'])

    columns_reordered = ['RECORD_NO','COMPANY_CODE', 'COMPANY_NAME', 'AWARANESS'
                         , 'FAMILIARITY', 'Used Last Season/This Season', 'FUTURE_USE'
                         , 'SATISFACTION','PREFERENCE']
    df_points = df_points[columns_reordered]
    df_points[df_points.columns[3:]] = df_points[df_points.columns[3:]].fillna(1)
    
    #save file to  data/processed
    df_points[~np.isnan(df_points['COMPANY_CODE'])].to_csv('data/processed/brand_equity.csv')
