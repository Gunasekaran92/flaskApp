from flask import Flask,jsonify
import pandas as pd
import numpy as np
import json
import time
import datetime
from datetime import date

#Read and Write Database
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import update
from sqlalchemy import text

import pyodbc
import urllib3

from flask_cors import CORS


#Connection to SQL server
driver = "{ODBC Driver 17 for SQL Server}"
server = "expenses.database.windows.net"
database = "expenses"
user = "gunasekaran9600@gmail.com@expenses"
password = "Guna@1992"

conn = f"""Driver={driver};Server=tcp:{server},1433;Database={database};
Uid={user};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""

params = urllib.parse.quote_plus(conn)
conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
engine = create_engine(conn_str, echo=True)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


conn1 = engine.connect() 

@app.route('/donut')
def donut():
    #Donut Chart
    #Reads from the SQL server table:
    donut_raw = text(r"select * from dbo.v_donut_chart")
    result = conn1.execute(donut_raw)
    donut_df = pd.DataFrame(data=result,columns=['category','value'])
    #donut_df['value'] = donut_df['value'].astype(str) + "%"
    print(donut_df)
    donut_json= donut_df.assign(**donut_df.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records")
    return donut_json
    

conn2 = engine.connect()    

@app.route('/line')
def line():
    #Donut Chart
    #Reads from the SQL server table:
    line_raw = text(r"select * from dbo.v_line_chart")
    result = conn2.execute(line_raw)
    line_df = pd.DataFrame(data=result,columns=['Amount','month'])
    #line_df['Amount'] = line_df['Amount'].astype(str) + "$"
    line_json= line_df.assign(**line_df.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records")
    return line_json
    

conn3 = engine.connect()    

@app.route('/summary')    
def summary():
    #Donut Chart
    #Reads from the SQL server table:
    summary_raw = text(r"select * from dbo.v_grouped_expenses")
    result = conn3.execute(summary_raw)
    new = pd.DataFrame(data=result,columns=['Index','Id','Date', 'Grocery', 'Payer', 'Amount', 'Payer1', 'Payee',
       'Non-Payee', 'PaidFor', 'ExpensesFor', 'PaidForArun', 'PaidForGeo',
       'PaidForGuna', 'PaidForVinoth', 'PayerArun', 'PayerGeo', 'PayerGuna','GroceryGrouped'])
    #Create a summary dataframe
    date_df = new.copy()
    date_df['Date'] = pd.to_datetime(date_df['Date'])
    days = 100
    cutoffDate = date_df['Date'].max() - pd.DateOffset(days=days)
    summary_last10 = date_df[['Id','Date','Grocery','GroceryGrouped','Amount', 'Payer', 'PaidFor']][date_df['Date'] >= cutoffDate]
    summary_last10['Id'] = summary_last10['Id'].astype(int)
    summary_last10['Amount'] = summary_last10['Amount'].astype(str) + "$"
    summary_last10.rename(columns={"Payer":"Who Paid", "For Whom":"PaidFor"})
    summary_last10.sort_values(by=['Date'],ascending=False,inplace=True)
    summary_json= summary_last10.assign(**summary_last10.select_dtypes(['datetime']).astype(str).to_dict('list') ).to_json(orient="records")
    return summary_json


conn4 = engine.connect()    
    
@app.route('/metric')
def metric():
    #KPI
    #Reads from the SQL server table:
    summary_raw = text(r"select * from dbo.v_grouped_expenses")
    result = conn4.execute(summary_raw)
    new = pd.DataFrame(data=result,columns=['Index','Id','Date', 'Grocery', 'Payer', 'Amount', 'Payer1', 'Payee',
       'Non-Payee', 'PaidFor', 'ExpensesFor', 'PaidForArun', 'PaidForGeo',
       'PaidForGuna', 'PaidForVinoth', 'PayerArun', 'PayerGeo', 'PayerGuna','GroceryGrouped'])
    #Create a summary dataframe
    date_df = new.copy()
    date_df['Date'] = pd.to_datetime(date_df['Date'])
    #days = 30
    #cutoffDate = date_df['Date'].max() - pd.DateOffset(days=days)
    cutoffDate = date_df['Date'].max().normalize() - pd.offsets.MonthBegin(1)

    metrics = date_df[['Date','GroceryGrouped','Amount']][date_df['Date'] >= cutoffDate]
    metrics.sort_values(by=['Date'],ascending=False,inplace=True)

    metric = metrics.groupby('GroceryGrouped')['Amount'].agg(sum).astype('str') + "$" 
    metric = metric.reset_index()
    metrict = metric.T
    metrict = metrict.rename(columns=metrict.iloc[0])
    metrict = metrict.iloc[1:]
    metrict.reset_index(inplace=True)
    new_metrict = pd.DataFrame(columns=('index','Miscellaneous', 'Restaurant', 'Vegetables','Meat'))
    new_metrict = new_metrict.append(metrict)
    new_metrict.fillna(0,inplace=True)
    new_metrict = new_metrict.iloc[:,1:]
    metric_final = new_metrict.to_json(index=True,orient="records")   
    return metric_final     
    

conn5 = engine.connect()    

@app.route('/metricO')    
def metricO():
    #KPI
    #Reads from the SQL server table:
    summary_raw = text(r"select * from dbo.v_grouped_expenses")
    result = conn5.execute(summary_raw)
    new = pd.DataFrame(data=result,columns=['Index','Id','Date', 'Grocery', 'Payer', 'Amount', 'Payer1', 'Payee',
       'Non-Payee', 'PaidFor', 'ExpensesFor', 'PaidForArun', 'PaidForGeo',
       'PaidForGuna', 'PaidForVinoth', 'PayerArun', 'PayerGeo', 'PayerGuna','GroceryGrouped'])
    date_df = new.copy()
    date_df['Date'] = pd.to_datetime(date_df['Date'])
    cutoffDateStart = date_df['Date'].max().normalize() - pd.offsets.MonthBegin(2)
    cutoffDateEnd = date_df['Date'].max().normalize() - pd.offsets.MonthBegin(1)
    metrics = date_df[['Date','GroceryGrouped','Amount']][date_df['Date'].between(cutoffDateStart,cutoffDateEnd)]

    metrics.sort_values(by=['Date'],ascending=False,inplace=True)

    metric = metrics.groupby('GroceryGrouped')['Amount'].agg(sum).astype('str') + "$" 
    metric = metric.reset_index()
    metrict = metric.T
    metrict = metrict.rename(columns=metrict.iloc[0])
    metrict = metrict.iloc[1:]
    metrict.reset_index(inplace=True)
    new_metrict = pd.DataFrame(columns=('index','Miscellaneous', 'Restaurant', 'Vegetables','Meat'))
    new_metrict = new_metrict.append(metrict)
    new_metrict.fillna(0,inplace=True)
    new_metrict = new_metrict.iloc[:,1:]
    metric_final = new_metrict.to_json(index=True,orient="records") 
    return metric_final    

if __name__ =='__main__':
    app.run()