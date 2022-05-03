#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 22:26:47 2022

@author: Hans
"""

import pandas as pd
import numpy as np

# Daiwa electronic trading strategy with asia markets intraday volatility

# terminology
# th = trading hours, oa = open auction, ca = close auction

# hardcode exchange and index
exchange_dic = {'Australia':'AS51 Index', 
            "China":"SHSZ300 Index", 
            "Hong Kong":"HSI Index", 
            "India":"NIFTY Index", 
            "Indonesia":"MXID Index", 
            "Japan":"TPX Index", 
            "Korea":"MXKR Index", 
            "Malaysia":"MXMY Index", 
            "Philippines":"MXPH Index", 
            "Singapore":"MXSG Index",  
            "Taiwan":"TWSE Index", 
            "Thailand":"MXTH Index" 
            }

# Open auction start time
open_time_dic = {'AS51 Index': '10:00', 'SHSZ300 Index' : '09:15',
                 'HSI Index':"09:00",'NIFTY Index':'09:00',
                 'MXID Index':'08:45', 'TPX Index':'08:00',
                 'MXKR Index':'08:30', 'MXMY Index':'08:30',
                 'MXPH Index':'09:00', 'MXSG Index':'08:30',
                 'TWSE Index':'08:30', 'MXTH Index':'09:30'
                 }

# close auction start time
close_time_dic = {'AS51 Index': '16:00', 'SHSZ300 Index' : '14:57',
                 'HSI Index':"16:00",'NIFTY Index':'15:40',
                 'MXID Index':'14:50', 'TPX Index':'15:00',
                 'MXKR Index':'15:20', 'MXMY Index':'16:45',
                 'MXPH Index':'12:45', 'MXSG Index':'17:00',
                 'TWSE Index':'13:25', 'MXTH Index':'16:30'
                 }

# handling the trading hours volatility and transform to output format
# trading hours volatility add in both open and close auction price

# Construct three data sets, trading hour, open and close
def th_volatility_1(df, df_open, df_close): 

# For debug
# i = "China"

# df = pd.read_excel(xlsx, i, index_col=0, parse_dates=[0])
# df_open = pd.read_excel(xlsx, i + '_open', index_col=0, parse_dates=[0])
# df_close = pd.read_excel(xlsx, i + '_close', index_col=0, parse_dates=[0])    

    # Get the index name
    index_name = df.index.name
    
    df1 = df.iloc[1:] # remove first row
    df_open1 = df_open.iloc[1:]
    df_close1 = df_close.iloc[1:]
    
    # trading hour no need change time, but need to send index time to colume then can match with open and close
    time = df1.index.get_level_values(0)
    df1['year'] = [t.year for t in time]
    df1['month'] = [t.month for t in time]
    df1['hour'] = [t.hour for t in time]
    df1['minute'] = [t.minute for t in time]
    
    # open change time
    time = df_open1.index.get_level_values(0)
    df_open1['year'] = [t.year for t in time]
    df_open1['month'] = [t.month for t in time]
    df_open1['hour'] = [t.hour for t in time]
    df_open1['minute'] = [t.minute for t in time]
    
    open_hour = open_time_dic[index_name][0:2]
    open_minute = open_time_dic[index_name][3:5]
    df_open1['hour'] = df_open1['hour']+ int(open_hour)
    df_open1['minute'] = df_open1['minute']+ int(open_minute)
    
    # close change time
    time = df_close1.index.get_level_values(0)
    df_close1['year'] = [t.year for t in time]
    df_close1['month'] = [t.month for t in time]
    df_close1['hour'] = [t.hour for t in time]
    df_close1['minute'] = [t.minute for t in time]
    
    close_hour = close_time_dic[index_name][0:2]
    close_minute = close_time_dic[index_name][3:5]
    df_close1['hour'] = df_close1['hour']+ int(close_hour)
    df_close1['minute'] = df_close1['minute']+ int(close_minute)

    return df1, df_open1, df_close1, index_name

# Concatenate three into one with open and close need time transformation
def th_volatility_2(df1, df_open1, df_close1, index_name):
    # concat all three df with condition
    # except for India market, since it has no close auction data
    def empty_auction(dz1, dz_open, dz_close,index_name):
        if index_name == "NIFTY Index":
            df2 = pd.concat([dz1, dz_open])
            return df2
        else:        
            df2 = pd.concat([dz1, dz_open, dz_close])
            return df2
    
    df2 = empty_auction(df1, df_open1, df_close1, index_name)
    
    time1 = df2.index.get_level_values(0)
    df2['day'] = [t.day for t in time1]
    df2['second'] = [t.second for t in time1]
    
    df3 = df2
    cols1 = ['year','month','day']
    cols2 = ['hour','minute','second']
    df3['Time1'] = df3[cols1].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
    df3['Time2'] = df3[cols2].apply(lambda x: ':'.join(x.values.astype(str)), axis="columns")
    df3['Time'] = pd.to_datetime(df3['Time1'] +' ' + df3['Time2'])
    df4 = df3.set_index('Time')
    df5 = df4.iloc[:,:-8]    
    
    df6 = df5.stack(dropna=False).reset_index().rename(columns={'level_0':'Time','level_1':'Ticker',0:'Price'})    
    df7 = pd.DataFrame(df6).sort_values(by=['Ticker','Time'])    
    df8 = df7.set_index(['Time','Ticker'])    
    return df8

# Calculate volatility cost and export the excel
def th_volatility_3(df8, index_name):
    time = df8.index.get_level_values(0)
    
    stock_returns = df8.groupby(['Ticker']).pct_change(limit=0)*100
    stock_returns['year'] = [t.year for t in time]
    stock_returns['month'] = [t.month for t in time]
    stock_returns['hour'] = [t.hour for t in time]
    stock_returns['minute'] = [t.minute for t in time]
    stock_returns['Price'] = stock_returns['Price'].astype(float)
    
    stock_std = stock_returns.groupby(['Ticker', 'year', 'month', 'hour', 'minute']).std()    
    
    sym = stock_std.index.get_level_values(0)
    year = stock_std.index.get_level_values(1)
    month = stock_std.index.get_level_values(2)
    hour = stock_std.index.get_level_values(3)
    minute = stock_std.index.get_level_values(4)
    
    intra_vol = stock_std.groupby([year, month, hour, minute]).mean()*100
    intra_vol = intra_vol.reset_index()
    intra_vol = pd.pivot_table(intra_vol, index=['year','month', 'hour', 'minute'])
    
    # Export to excel
    country = [key for key, value in exchange_dic.items() if value == index_name]
    intra_vol.to_excel(country[0] + " Volatility" + '.xlsx', index = True)
        

#####
# Run function

#Input file
xlsx = pd.ExcelFile('/Users/Hans/Desktop/Daiwa/Chartbook/Intraday Volatility/Master_Intraday_Volatility_April22.xlsx')

# country = "Australia"
# country_open = "Australia_open"
# country_close= "Australia_close"


# wrape all functions togther
def volatility_export(country, country_open, country_close):
    ws_volatility = pd.read_excel(xlsx, country, index_col=0, parse_dates=[0])
    ws_open = pd.read_excel(xlsx, country_open, index_col=0, parse_dates=[0])
    ws_close = pd.read_excel(xlsx, country_close, index_col=0, parse_dates=[0])
    df1, df_open1, df_close1, index_name = th_volatility_1(ws_volatility, ws_open, ws_close)
    df8 = th_volatility_2(df1, df_open1, df_close1, index_name)
    th_volatility_3(df8, index_name)

# For debug
# country_list = ["India"]
      
# Export output country list
country_list = ['Australia', 'China', "Hong Kong",'India',
                'Indonesia', 'Japan', 'Korea', 'Malaysia', 
                'Philippines', 'Singapore',
                'Taiwan', 'Thailand']

for i in country_list:
    volatility_export(i, i + "_open", i + "_close")




