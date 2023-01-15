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
    unique_values.drop_duplicates()
    return unique_values


def combine(data_frame_1: pd.DataFrame
            , data_frame_2: pd.DataFrame
            , method: str = 'cross') -> pd.DataFrame:
    combined = data_frame_1.merge(data_frame_2, how='cross')
    return combined


if __name__ == "__main__":
    set_working_directory(path = r'C:\Users\krzys\OneDrive\Dokumenty\repo_git\brand-equity\data\raw')
    df_source = open_file('Source.Datafile.Brand.Equity')
    companies = open_file('COMPANY')
    record_no = get_unique_values(data_frame = df_source, selected_columns = ['RecordNo'])
    df_combined = combine(data_frame_1 = companies, data_frame_2 = record_no)
    df_combined = df_combined.fillna(np.nan)
    points_5_q7q8 = df_source['Q7M1'][df_source['Q7M1'] != 999].to_frame()
    points_4_q7q8 = df_source[['Q7M2', 'Q7M3', 'Q7M4', 'Q7M5', 'Q7M6', 'Q7M7', 'Q7M8', 'Q7M9', 'Q7M10']].dropna(how='all')
    points_2_q7q8 = df_source[['Q8M1', 'Q8M2', 'Q8M3', 'Q8M4', 'Q8M5', 'Q8M6', 'Q8M7', 'Q8M8', 'Q8M9', 'Q8M10']].dropna(how='all')
    