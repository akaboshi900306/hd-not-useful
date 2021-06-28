# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 13:56:36 2020

@author: YXG0RZH
"""


#from oauth2client.client import GoogleCredentials
#from googleapiclient import discovery

import os
#import win32com.client

## reads BQ tables into Data Frames
from pandas_gbq import read_gbq 
from datetime import timedelta, date
import pandas as pd
import openpyxl
import datetime


'''

def create_service(svc_type='bigquery'):   
    credentials = GoogleCredentials.get_application_default()
    return discovery.build(svc_type, 'v2', credentials=credentials)




bq = create_service();
'''
query = '''WITH SKU AS (
SELECT DISTINCT
  SKU_NBR, 
  MAX(SKU_CRT_DT) as SKU_CRT_DT

FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD`

GROUP BY 1),

----------------------------------------------
CONF_ALLOCS AS(
SELECT DISTINCT
  R. MKT_DC_NBR AS RDC_NBR,
  A.LOC_NBR, 
  A.SKU_NBR,
  S.SKU_CRT_DT,
  SUM (A. CONF_ALLOC_QTY ) AS CONF_ALLOC_QTY

FROM `pr-edw-views-thd.SCHN_INV.STRSK_VIRT_ALLOC`  A
    
JOIN `pr-edw-views-thd.SCHN_ACTVY.AGG_RQST` R
  ON A. AGG_NBR= R. AGG_NBR 

JOIN SKU S
  ON S.SKU_NBR = A.SKU_NBR 

WHERE ORD_DT > '2020-02-01'
AND CAST(A.BQ_EFF_END_DTTM as DATE)= '9999-12-31'
GROUP BY 1,2,3,4),
----------------------------------------------
YARDA AS (
  SELECT DISTINCT
  T1.SKU_NBR, 
  T1.PO_NBR,
  S.SKU_CRT_DT,
  T1.RDC_NBR,
  UNITS_ORDERED AS UNITS_ORDERED,
  VENDOR_ID as MVEN


FROM  `analytics-supplychain-thd.WMS.WH_TRAILER_ON_YARD` AS T1

JOIN  `analytics-supplychain-thd.YMS.YMS_LATEST` AS T2
  ON  CAST(T1.RDC_NBR AS INT64) = CAST(T2.DC_NBR AS INT64) AND T1.TRLR_NBR = T2.TLR_NBR 
  AND CAST(T1.IN_YARD_DATE AS DATE) BETWEEN DATE_SUB(CAST(ARVL_TM AS DATE), INTERVAL 3 DAY) AND DATE_ADD(CAST(ARVL_TM AS DATE), INTERVAL 3 DAY)

JOIN `analytics-supplychain-thd.DISTRO.FP_PLAY_DLY`   T3
  ON CAST(T3.RDC_NBR AS INT64) = CAST(T1.RDC_NBR AS INT64) AND T1.TRLR_NBR = T3.TRLR_NBR 
 
LEFT JOIN `pr-edw-views-thd.SCHN_ACTVY.ALLOC_ASN_PO`E
ON  T1.PO_NBR =E.PO_NBR
AND CAST(T1. RDC_NBR as INT64) = CAST(E. RECV_LOC_NBR as INT64)
AND E.PO_CRT_DT= T1. PO_CRT_DT

JOIN SKU S
  ON S.SKU_NBR = T1.SKU_NBR

JOIN `pr-edw-views-thd.SCHN_ACTVY.AGG_RQST` R
  ON T1.PO_NBR = R.PO_NBR
  AND  CAST(T1.RDC_NBR AS INT64)= CAST(R.MKT_DC_NBR AS INT64)
  AND T1. PO_CRT_DT = R. ORD_DT

 JOIN  `pr-edw-views-thd.SCHN_ACTVY.SCHN_ORD_EVNT`   B
  ON R.PO_NBR =B. PO_NBR
  AND CAST(R.MKT_DC_NBR AS INT64)= CAST(B. RECV_LOC_NBR AS INT64)
  AND R.ORD_DT = B.ORD_CRT_DT
  
WHERE 1=1
AND T3.CAL_DT = CURRENT_DATE()
AND E.PO_NBR is null 
AND PUTWY_TYPE in ('OVR','PT3', 'PLT', 'FLU')
AND B.DSVC_TYP_CD <>3
AND B.RMETH_CD  in (1,2)
AND LOAD_TYP IN ('RDC', 'TF', 'LTL', 'COMBO', 'BOSS', 'COVR')
AND (CRT_PGM_ID in ('POP425I', 'PXP202') or AGG_TYP_CD = 5)
AND T2.ZONE Not LIKE 'DD%' and MOVE_2_ZONE IS NULL 
--    and t1.SKU_nbr = 285112
--   and CAST(T1.RDC_NBR AS INT64)= 5023
--AND R.ORD_DT > '2020-04-01'
),
-----------------------------------------------
YARD AS (
SELECT 
  SKU_NBR, 
  SKU_CRT_DT,
  RDC_NBR,
  MVEN,
  SUM(UNITS_ORDERED) AS UNITS_ORDERED
FROM YARDA
GROUP BY 1,2,3,4),
-----------------------------------

YARD1 AS (
  SELECT DISTINCT
  VENDOR_ID, 
  T1.PO_NBR,
  T1.RDC_NBR

FROM  `analytics-supplychain-thd.WMS.WH_TRAILER_ON_YARD` AS T1

JOIN  `analytics-supplychain-thd.YMS.YMS_LATEST` AS T2
  ON  CAST(T1.RDC_NBR AS INT64) = CAST(T2.DC_NBR AS INT64) AND T1.TRLR_NBR = T2.TLR_NBR 
  AND CAST(T1.IN_YARD_DATE AS DATE) BETWEEN DATE_SUB(CAST(ARVL_TM AS DATE), INTERVAL 3 DAY) AND DATE_ADD(CAST(ARVL_TM AS DATE), INTERVAL 3 DAY)

JOIN `pr-edw-views-thd.SCHN_ACTVY.AGG_RQST` R
  ON T1.PO_NBR = R.PO_NBR
  AND  CAST(T1.RDC_NBR AS INT64)= CAST(R.MKT_DC_NBR AS INT64)
  AND T1. PO_CRT_DT = R. ORD_DT

JOIN  `pr-edw-views-thd.SCHN_ACTVY.SCHN_ORD_EVNT`   B
  ON R.PO_NBR =B. PO_NBR
  AND CAST(R.MKT_DC_NBR AS INT64)= CAST(B. RECV_LOC_NBR AS INT64)
  AND R.ORD_DT = B.ORD_CRT_DT

WHERE PUTWY_TYPE in ('OVR','PT3', 'PLT','FLU')
AND B.DSVC_TYP_CD <> 3
AND B.RMETH_CD  in (1,2)
AND LOAD_TYP IN ('RDC', 'TF', 'LTL', 'COMBO', 'BOSS', 'COVR')
AND (CRT_PGM_ID in ('POP425I', 'PXP202') or AGG_TYP_CD = 5)
AND T2.ZONE Not LIKE 'DD%' and MOVE_2_ZONE IS NULL
--AND R.ORD_DT > '2020-04-01'
), 

----------------------------------------------
VIRT_ALLOCS AS(
  SELECT DISTINCT
  B.MKT_DC_NBR AS RDC_NBR,
  A.LOC_NBR, 
  A.SKU_NBR,
  S.SKU_CRT_DT,
  B.MVNDR_NBR ,
  SUM (A.EXPCTD_ALLOC_QTY) AS VIRT_EXPCTD_ALLOC_QTY

FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.STRSK_VIRT_ALLOC` A
  
JOIN SKU S
  ON S.SKU_NBR = A.SKU_NBR 
  
JOIN `pr-edw-views-thd.SCHN_ACTVY.AGG_RQST` B
  ON A. AGG_NBR= B. AGG_NBR 

JOIN YARD1 Y  --LIMIT TO VAS ON YARD
  ON Y.VENDOR_ID = B.MVNDR_NBR  
  AND Y.PO_NBR = B. PO_NBR
  AND CAST(Y.RDC_NBR AS INT64) = CAST(B. MKT_DC_NBR AS INT64)

WHERE ORD_DT > '2020-04-01'

GROUP BY 1,2,3,4,5),

----------------------------------------------
STRSKU AS(
SELECT DISTINCT
  A.RDC_NBR,
  A.LOC_NBR, 
  A.SKU_NBR,
  A.SKU_CRT_DT,
  VIRT_EXPCTD_ALLOC_QTY,
  CONF_ALLOC_QTY,
  UNITS_ORDERED,
  B.MVEN AS MVNDR_NBR

FROM VIRT_ALLOCS A

JOIN YARD B
  ON  CAST(B.RDC_NBR AS INT64) = CAST(A.RDC_NBR AS INT64)  
  AND B.SKU_NBR = A.SKU_NBR
  AND B.SKU_CRT_DT = A.SKU_CRT_DT
  
LEFT JOIN CONF_ALLOCS C
  ON C.SKU_NBR = A.SKU_NBR
  AND C.SKU_CRT_DT = A.SKU_CRT_DT
  AND CAST(C.LOC_NBR AS INT64) = CAST(A.LOC_NBR AS INT64)
),
----------------------------------------------
DETAIL AS(
SELECT DISTINCT
  E.MVNDR_NBR AS MVNDR, 
  E.RDC_NBR,
  E.SKU_NBR,
  E.LOC_NBR, 
  A. BUY_UOM_QTY,
  T.OH_QTY,
  T.OO_QTY,
  E.VIRT_EXPCTD_ALLOC_QTY,
  SUM(CASE WHEN(T.OH_QTY + T.OO_QTY + IFNULL(E.CONF_ALLOC_QTY,0)- IFNULL(T.CORD_ALLOC_QTY,0) )<0 THEN 0 
    ELSE (T.OH_QTY+T.OO_QTY+IFNULL(E.CONF_ALLOC_QTY,0)-IFNULL(T.CORD_ALLOC_QTY,0))/((LEAD_TM_QTY+ REV_TM_QTY)/(IFNULL(REV_TM_DAYS_CNT,0)+IFNULL(LEAD_TM_DAYS,0))*7)END) AS WOS,
  UNITS_ORDERED AS QTY_TO_ALLOC

FROM STRSKU E

LEFT JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY` A
  ON CAST(E.LOC_NBR AS INT64)= CAST(A.STR_NBR AS INT64) 
  AND A.SKU_NBR = E.SKU_NBR 
  AND A.SKU_CRT_DT = E.SKU_CRT_DT 
  AND A.CAL_DT =DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)

LEFT JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_INV_TRKL` T
  ON CAST(T.STR_NBR AS INT64)= CAST(E.LOC_NBR AS INT64) 
  AND T.SKU_NBR = E.SKU_NBR 
  AND T.SKU_CRT_DT = E.SKU_CRT_DT
  
WHERE 1=1

GROUP BY 1,2,3,4,5,6,7,8,10)

SELECT 
*,CASE WHEN WOS = 0 THEN 1 
  WHEN WOS <2 THEN 2
  ELSE 3 END AS STORE_RANK

FROM DETAIL

WHERE VIRT_EXPCTD_ALLOC_QTY>0 
ORDER BY RDC_NBR ASC, SKU_NBR ASC, STORE_RANK ASC, VIRT_EXPCTD_ALLOC_QTY DESC


'''

df= read_gbq(query, project_id='analytics-supplychain-thd') 



def build_truck(g):
    a = 0
    g.sort_values(['STORE_RANK','VIRT_EXPCTD_ALLOC_QTY',"WOS"], ascending=[True, False, True], inplace=True)
    for i, row in g.iterrows():
        a = a+ row["VIRT_EXPCTD_ALLOC_QTY"]
        if a  <= row["QTY_TO_ALLOC"] :
            g.loc[i,"included_qty"] = row["VIRT_EXPCTD_ALLOC_QTY"]
        if a >= row["QTY_TO_ALLOC"]:
            g.loc[i,"included_qty"] = row["QTY_TO_ALLOC"] - (a- row["VIRT_EXPCTD_ALLOC_QTY"])
    return g

new_po_df = df.groupby(['SKU_NBR',"RDC_NBR"], as_index=False).apply(build_truck)

new_po_df = new_po_df.reset_index(drop = True)
new_po_df = new_po_df[new_po_df.included_qty > 0]
new_po_df["included_qty"][new_po_df.WOS == 0] = new_po_df["included_qty"][new_po_df.WOS == 0] -new_po_df["BUY_UOM_QTY"][new_po_df.WOS == 0]
new_po_df["included_qty"][new_po_df.included_qty< 0] = 0
new_po_df = new_po_df[new_po_df.included_qty > 0]

df1 = new_po_df[["MVNDR","RDC_NBR",'SKU_NBR','LOC_NBR']]
df1["StartDate"] = str(date.today().strftime('%m/%d/%Y'))
df1["EndDate"] = str((date.today() +timedelta(days=1)).strftime('%m/%d/%Y'))
df1["Qty"] = new_po_df.included_qty
df1.columns.values[0] = 'Vendor'
df1.columns.values[1] = 'RDC'
df1.columns.values[2] = 'SKU'
df1.columns.values[3] = 'LOC'
df1.columns.values[6] = 'Qty'

book = openpyxl.load_workbook(r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_reference\OverrideUpload_Template (5).xlsx")

writer = pd.ExcelWriter(r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_reference\OverrideUpload_Template (5).xlsx", engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
df1.to_excel(writer, "Data",index=False)
writer.save()

d = datetime.datetime. today()
i=1
df2 = pd.read_excel(r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_reference\OverrideUpload_Template (5).xlsx", sheet_name= 'Data')
df2 = df2[:len(df1)]
count = len(df2)

while count >0 :
    num = min(count,1999)
    df = df2.iloc[0:num]
#    path= 'C:/Users/yxg0rzh/OneDrive - The Home Depot/Desktop/New folder (2)/RDC_OverrideUpload//'
    path= r"\\at2a5\vol4\DEPTS\TRAFFIC\PROJECTS\SCD\Supply Chain Analytics\Projects\Gary Gao\OverrideUpload_allocation"
    df.to_csv(os.path.join(path, 'RDC_OverrideUpload_' + d. strftime('%Y-%m-%d') + '_' + str(i) + '.csv'),index=False)
    count = count - num
    df2 = df2.iloc[-count:]
    i=i+1
    
print("Python Step 1 Complete!")

    
