# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 11:01:55 2022

@author: kfll
"""

import pyodbc 
import pandas as pd
import numpy as np
import re
import subprocess
import os
import argparse, hashlib, json
import requests as r

# =============================================================================
#     USAGE
#     
#     python "O:/path/to/your/python_file.py" --token=APITOKEN --info="O:/path/to/your/csv_file.csv" -s SQLcode -outfile= "path to other CSV file"
# =============================================================================

def add_quote(text):
    return("\""+text+"\"")
#add quotes to python string



def _get_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--token')
    parser.add_argument('-s', '--sql') #SQL query
    parser.add_argument('-T', '--type', default = 'flat') # eav
    parser.add_argument('-o', '--overwrite', default = 'normal') #overwrite
    parser.add_argument('--auto', default = 'false')
    parser.add_argument('-i', '--info') # the filepath of the (csv) file you're importing
    parser.add_argument('-e', '--outfile') # the filepath of the (csv) file exported out
    parser.add_argument('-d', '--date', default = 'YMD') # MDY, DMY
    parser.add_argument('-rc', '--retcont', default = 'count') #ids, auto_ids, nothing
    parser.add_argument('--format', default = 'csv')
    parser.add_argument('--retform', default = 'csv')
    args = parser.parse_args()

    return args


def _main():
    #REDCap database token
    token = _get_arg().token
    
    
    export_file_py  = "O:\Teams\Research IT - Data Services\REDCap API Scripts\export_records.py" 
    import_file_py = "O:\Teams\Research IT - Data Services\REDCap API Scripts\import_records.py"
       

    ifile_hcv = _get_arg().info
    #"Q:\ETL\REDCap_ETL\HIV_HEpC_REDCap\import data\FOCUSHCV_DB_Import.csv"
    ofile_hcv = _get_arg().outfile
    #"Q:\ETL\REDCap_ETL\HIV_HEpC_REDCap\import data\FOCUSHCV_DB_ndat.csv"

    #Data export file path
    ecmd = "python "+add_quote(export_file_py)+" --token "+token+" --outFile "+add_quote(ifile_hcv)
    #print(ecmd)
    
    
    #Run and check if process run with out failing
    try:
        exportREDCap = subprocess.run(ecmd, check = True)
        exportREDCap.check_returncode
    except subprocess.CalledProcessError as e:
        print ( "Error:\nreturn code: ", e.returncode, "\nOutput: ", e.stderr.decode("utf-8") )
        raise
    
    #Read into python environment
    redcap = pd.read_csv(ifile_hcv)
    print("REDCap data includes "+str(redcap.shape[0])+" entries.")
    #print(redcap.head(2))
    
    
    #Extract data from SQL database
    server = 'am-dawg-sql-trt' 
    database = 'uwReports' 
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;')
    sql_query = "Select * from "+str(_get_arg().sql)
    sql_dat = pd.read_sql(sql_query, cnxn)
    print("DAWG data includes "+str(sql_dat.shape[0])+" entries.")
    cnxn.close()
    
    #Identify new data
    ndat = sql_dat[~sql_dat['record_id'].isin(redcap['record_id'].astype(str).tolist())]
    print("Expected to add "+str(ndat.shape[0])+ " new entries.")
    
    #Export data to csv
    ndat.to_csv(ofile_hcv, index = False)
    
    icmd = "python "+add_quote(import_file_py)+" --token "+token+" --info="+add_quote(ofile_hcv)
    try:
        importREDCap = subprocess.run(icmd, check = True)
        importREDCap.check_returncode
    except subprocess.CalledProcessError as e:
        print ( "Error:\nreturn code: ", e.returncode, "\nOutput: ", e.stderr.decode("utf-8") )
        raise

    #Update RNA if HCV (hierachical) --crate in separate script, export values with criteria and update those exported records only
# =============================================================================
#     if "hcv" in _get_arg().sql: 
#         existing_update = "Q:\ETL\REDCap_ETL\HIV_HEpC_REDCap\import data\FOCUSHCV_DB_rna_dat.csv"
#         exi_pend = redcap[(redcap['hcv_ab_result']==1) & (redcap['rna_order_date'].isnull())]
# 
#         exi_pendu = sql_dat[sql_dat['record_id'].isin(exi_pend['record_id'].astype(str).tolist())]
# 
# 
#         exi_pendu2 = exi_pendu[['record_id', 'rna_order_date', 'hcv_rna_test_result', 'hcv_test_status']]
#         exi_pendu2.to_csv(existing_update, index = False)
# 
#         #import RNA into REDCap
#         hcmd = "python "+add_quote(import_file_py)+" --token "+token+" --info="+add_quote(existing_update)
#         try:
#             importREDCap = subprocess.run(hcmd, check = True)
#             importREDCap.check_returncode
#         except subprocess.CalledProcessError as e:
#             print ( "Error:\nreturn code: ", e.returncode, "\nOutput: ", e.stderr.decode("utf-8") )
#             raise
# 
# =============================================================================


if __name__ == "__main__":
    _main()    

