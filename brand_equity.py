# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 19:38:23 2023

@author: krzys
"""

import os
import pandas as pd
import csv
import numpy as np

os.chdir(r'C:\Users\krzys\Desktop\data science\5 semestr\Data science professional project\Brand equity\Brand.Equity\project_data')

with open('PROJECT_INFO.csv', 'r') as csvfile:
    dialect = csv.Sniffer().sniff(csvfile.readline())
    df_0 = pd.read_csv('Source.Datafile.Brand.Equity.csv', sep = dialect.delimiter)

with open('COMPANY.csv', 'r') as csvfile:
    dialect = csv.Sniffer().sniff(csvfile.readline())
    df_1 = pd.read_csv('COMPANY.csv', sep = dialect.delimiter)

with open('PROJECT_INFO.csv', 'r') as csvfile:
    dialect = csv.Sniffer().sniff(csvfile.readline())
    df_2 = pd.read_csv('PROJECT_INFO.csv', sep = dialect.delimiter)


comb = df_1.merge(df_2, how='cross')

#comb[(comb['RecordNo'] == 1135) & (comb['COMPANY_CODE'] == 106)]

comb.query('RecordNo == 1135 & COMPANY_CODE == 106')

#%%
result_dict = {5: {1135:106, 1136:104}
               , 4: {}
               , 2: {}
               , 1: {}}

#%%
df_0 = df_0.fillna(np.nan)
a_5 = df_0['Q7M1'][df_0['Q7M1'] != 999].to_frame()
a_4 = df_0[['Q7M2', 'Q7M3', 'Q7M4', 'Q7M5', 'Q7M6', 'Q7M7', 'Q7M8', 'Q7M9', 'Q7M10']]
a_5 = {5: a_5.to_dict()
       , 4: a_4.to_dict()}
