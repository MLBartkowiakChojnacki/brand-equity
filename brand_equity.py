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


def get_points_info(data_frame: pd.DataFrame, columns: list, index: str, points: int) -> pd.DataFrame:
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
    df_source = open_file('Source.Datafile.Brand.Equity')
    companies = open_file('COMPANY')
    record_no = get_unique_values(data_frame = df_source, selected_columns = ['RecordNo'])
    df_combined = combine(data_frame_1 = companies, data_frame_2 = record_no)
    df_combined = df_combined.fillna(np.nan)
    df_combined.columns = ['COMPANIES', 'CLIENT_NAME', 'RECORD_NO']
    points_5_q7q8 = df_source[['RecordNo','Q7M1']][df_source['Q7M1'] != 999]
    points_5_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_5_q7q8['POINTS'] = 5
    #points_5_q7q8 = get_unique_values(data_frame = points_5_q7q8, selected_columns = ['RECORD_NO', 'COMPANIES'])
    points_4_q7q8 = df_source[['RecordNo', 'Q7M2', 'Q7M3', 'Q7M4', 'Q7M5', 'Q7M6', 'Q7M7', 'Q7M8', 'Q7M9', 'Q7M10']].dropna(how='all')
    cols = ['Q7M2', 'Q7M3', 'Q7M4', 'Q7M5', 'Q7M6', 'Q7M7', 'Q7M8', 'Q7M9', 'Q7M10']
    points_4_q7q8 = get_points_info(data_frame = df_source, columns = cols, index = ['RecordNo'], points = 4)
    points_4_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_4_q7q8['POINTS'] = 4
    points_2_q7q8 = df_source[['RecordNo', 'Q8M1', 'Q8M2', 'Q8M3', 'Q8M4', 'Q8M5', 'Q8M6', 'Q8M7', 'Q8M8', 'Q8M9', 'Q8M10']].dropna(how='all')
    cols = ['Q8M1', 'Q8M2', 'Q8M3', 'Q8M4', 'Q8M5', 'Q8M6', 'Q8M7', 'Q8M8', 'Q8M9', 'Q8M10']
    points_2_q7q8 = get_points_info(data_frame = df_source, columns = cols, index = ['RecordNo'], points = 4)
    points_2_q7q8.columns = ['RECORD_NO', 'COMPANIES']
    points_2_q7q8['POINTS'] = 2
    df_points = pd.concat([points_5_q7q8, points_4_q7q8, points_2_q7q8], ignore_index=True)
    df_points = df_points.drop_duplicates(subset=['RECORD_NO','COMPANIES'], keep="first")   
    df_points = df_points.query('COMPANIES not in [999, 998]')
    points_1_q7q8 = pd.concat([df_points[['RECORD_NO', 'COMPANIES']], df_combined[['RECORD_NO', 'COMPANIES']]]).drop_duplicates(keep=False)
    points_1_q7q8['POINTS'] = 1
    df_points = pd.concat([df_points, points_1_q7q8], ignore_index=True)
    df_points = pd.merge(df_points, companies,  how='left', left_on=['COMPANIES'], right_on = ['COMPANY_CODE'])

