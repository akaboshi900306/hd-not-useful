# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 12:19:59 2020

@author: YXG0RZH
"""


from datetime import timedelta, date
## reads BQ tables into Data Frames
from pandas_gbq import read_gbq 
import pandas as pd
import datetime
import os
#import win32com.client


d = datetime.datetime. today()
path= r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_allocation\error_files"
URL = os.path.join(path, d. strftime('%Y-%m-%d') + "_ERROR_FILE.xlsx")
df = pd.read_excel(URL)

sql = """
update  `analytics-supplychain-thd.DISTRO.FP_PLAY_DLY`
set EXECUTE = "Y"
where CAL_DT =current_date()
"""

sql = read_gbq(sql, project_id='analytics-supplychain-thd')

for index, row in df.iterrows():
    sql = """update  `analytics-supplychain-thd.DISTRO.FP_PLAY_DLY`
    set EXECUTE = "N"
    where CAL_DT =current_date()
    and RDC_NBR=""" + str(row["RDC"]) + """ and ASN_NUMBER= """ + "'"+str(row["ASN_NBR"])+"'"
    
    sql = read_gbq(sql, project_id='analytics-supplychain-thd')
    
    
    
    
sql = """
select distinct RDC_NBR, " " as MVNDR_NBR, TRLR_NBR as TRAILER_NBR , " " as PO_NBR, PRIORITY_NBR from `analytics-supplychain-thd.DISTRO.FP_PLAY_DLY`
where  cal_dt = current_date()
and EXECUTE = "Y"
"""

df = read_gbq(sql, project_id='analytics-supplychain-thd')
df["START_DATE"] = str(date.today().strftime('%m/%d/%Y'))
df["END_DATE"] = str((date.today() +timedelta(days=3)).strftime('%m/%d/%Y'))
path = r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_allocation\Upload_template"
df.to_csv(os.path.join(path, 'Upload_template' + '.csv'),index=False)

# sql = read_gbq(sql, project_id='analytics-supplychain-thd')


