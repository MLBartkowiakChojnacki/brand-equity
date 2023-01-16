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
    set_working_directory(path = r'C:\Users\krzys\OneDrive\Dokumenty\repo_git\brand-equity\data\raw')
    df_source = open_file('Grupa.1')
    companies = open_file('COMPANY')

    record_no = get_unique_values(data_frame = df_source, selected_columns = ['RecordNo'])
    df_combined = combine(data_frame_1 = companies, data_frame_2 = record_no)
    df_combined = df_combined.fillna(np.nan)
    df_combined.columns = ['COMPANIES', 'CLIENT_NAME', 'RECORD_NO']
    
    points_5_q7q8 = df_source[['RecordNo','X3M1']][df_source['X3M1'] != 999]
    points_5_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_5_q7q8['AWARANESS'] = 5
    
    #points_5_q7q8 = get_unique_values(data_frame = points_5_q7q8, selected_columns = ['RECORD_NO', 'COMPANIES'])
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

    