#  History of changes to the JLQ code:
#  
#  2019_4_4 v2.1
#  - Change made by VXP0LMJ to table JLQ_EACH_TRANS_STG_2:
#  CAST(C.REVISEDDEMAND AS INT64) AS REVISEDDEMAND into CAST(FLOOR (C.REVISEDDEMAND) AS INT64) AS REVISEDDEMAND
#  `uat-supply-chain-thd.DCM.WEEKLY_PROD_DMND` `pr-edw-views-thd.SCHN_FCST.WEEKLY_PROD_DMND`

#  2019_5_23 v2.1
#  - Created historical table JLQ_COMPANY_JLQ_TRANS_HIST for BI team. The code will take data from JLQ_COMPANY_JLQ_TRANS and isert it in
#  JLQ_COMPANY_JLQ_TRANS two times with current and next week's FW_END_DATE timestamp. I.e. we are duplicating current week's record since the
#  process is ran biweekly.

#  2019_8_21 v2.1
#  - Removed the exceptions in the final portion of the query

#  2019_10_3 v2.2
#  - Fixed eroneous join on SUB_DEPT

#  2019_10_8 v2.3
#  - Predetermined Schema

#  2019_10_14 v2.4
#  - Added Pre-staging table to filter on SC List
#  - Removed BIWEEKLY table calculation
#  - Changed OUTL impact query

# 2020_04_29 v2.5
# -At step :JLQ_EACH_TRANS_STG_2
#  replace table `pr-edw-views-thd.SCHN_FCST.WEEKLY_PROD_DMND` (expired)
#  with `pr-edw-views-thd.SCHN_FCST_DMND.WKLY_AGG_SLS_HIST`(new)
# -change EFF_CORR_SLS_QTY as REVISEDDEMAND and GRSS_SLS_QTY AS REGULARDEMAND
#  Because in the old query mentions REVISEDDEMAND >0 so it already excludes the 
#  the condition REVISEDDEMAND = 0, so no change to make if we keep REVISEDDEMAND >0


# 2020_10_27 v2.6
# At step: JLQ_EACH_TRANS_STG_2, 
# `pr-edw-views-thd.TD_COPIES.ITEM_HIER_MAPPING` is expiring so exclude ITEM_ID
# At step: JLQ_ADJUSTED_FREQ
# `pr-edw-views-thd.TD_COPIES.ITEM_HIER_MAPPING` is expiring  
# `pr-edw-views-thd.SCHN_FCST.PRODUCT_LOCATION_OP` & `pr-edw-views-thd.SCHN_FCST.SESSION_RUN_MOD_SF` is stop updating
# so we use pr-inventory-planning-thd.SF.LOC_SKU_SF which has sku_nbr/loc_nbr with max(SF_CALC_DATE) to replace 
# these tables (also modelid and item id) to get SF value



{
  options(java.parameters = "- Xmx1024m")
  packages = list("bigrquery", "data.table", "bigQueryR", "stringr", "XML")
  for(i in 1:length(packages)) {
    if (is.element(packages[i],installed.packages()) == TRUE){
      library(packages[[i]],character.only=TRUE)
    } else {
      install.packages(packages[[i]])
      library(packages[[i]],character.only=TRUE)
    }
  }
}

setwd("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/JLQ Min Procedure  (DO NOT DELETE - Nirjhar Raina)/JLQ Inputs")

library(bigrquery)
library(data.table)
library(bigQueryR)
library(stringr)
library(XML)
project = "analytics-supplychain-thd"
dataset = "IXL0858_ANALYTICS"
bq_auth("yunzhong_gao@homedepot.com")



##############################################################################################################################################################
##############################################################################################################################################################
######################################################################  UPLOAD   ############################################################################# 


jlq_preferences <- fread( "JLQ Preferences.csv", colClasses = c( 
                                                                  SUB_DEPT = "character", # numeric = FLOAT
                                                                  CLASS_NBR = "integer",
                                                                  SC_NBR = "integer",
                                                                  JLQ_METHOD = "character",
                                                                  MIN_JLQ_FREQ = "integer",
                                                                  MIN_JLQ_SIZE = "integer",
                                                                  MAX_JLQ_SIZE = "integer",
                                                                  COMMENT = "character"
                                                                  
                                                                ))


query_exec( project = project,
            dataset = dataset,
            query = "DELETE FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_SC_LIST` WHERE 1=1",
            use_legacy_sql = FALSE,
            max_pages = Inf,
            billing = project
)


jlq_schema = schema_fields(jlq_preferences)

bq_job_wait( bq_perform_upload(  x = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_SC_LIST",
                                 values = jlq_preferences,
                                 fields = jlq_schema,
                                 create_disposition = "CREATE_IF_NEEDED",
                                 write_disposition = "WRITE_TRUNCATE",
                                 # source_format = "CSV",
                                 # nskip = 1,
                                 billing = project), 
             
             quiet = getOption( "bigrquery.quiet"), pause = 1
             
)





##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
######################################################################  CALCULATION QUERY SET   ##############################################################



#################     JLQ_PRE_STG    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_PRE_STG",
  write_disposition = "WRITE_TRUNCATE",
  query =
    "
  SELECT
  
  a. SKU_NBR,
  CAST( d. STR_NBR AS INT64) AS STR_NBR,
  a. SKU_CRT_DT,
  REPLACE ( a.SKU_DESC, '\"\', '' ) AS SKU_DESC,
  -- a. SKU_DESC,
  -- a. SKU_STAT_CD, 
  a. DEPT_NBR,
  -- a. SUB_DEPT_NBR,
  -- a. SUB_DEPT_DESC,
  a. CLASS_NBR,
  a. CLASS_DESC,
  a. SUB_CLASS_NBR,
  a. SUB_CLASS_DESC
  
  -------------------- get SKU info ---------------------
  FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` a
  
  JOIN (  SELECT
          SKU_NBR,
          MAX(SKU_CRT_DT) AS SKU_CRT_DT
          FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD`
          GROUP BY  1
        ) b
    ON a.SKU_NBR = b.SKU_NBR
    AND a.SKU_CRT_DT = b.SKU_CRT_DT
  
  
  ------------ get actively replenished SKUs ------------
  JOIN (  SELECT DISTINCT
          SKU_NBR,
          SKU_CRT_DT,
          STR_NBR
          FROM `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY`
          WHERE 1=1
          AND PARTITIONDATE = DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY)
          AND IPR_REPLE_IND = 1
          AND OK_TO_ORD_FLG = 'Y'
        ) d

    ON a.SKU_NBR = d.SKU_NBR
    AND a.SKU_CRT_DT = d.SKU_CRT_DT
  
  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_SC_LIST` c
    ON a.SUB_DEPT_NBR =c. SUB_DEPT
    AND a.CLASS_NBR = c.CLASS_NBR
    AND a.SUB_CLASS_NBR = c.SC_NBR
  
  WHERE 1=1
  
  --  AND ( CASE  WHEN a. SKU_NBR = 775940 AND CAST( d. STR_NBR AS INT64) = 2906 THEN 'Y'
  --              WHEN a. SKU_NBR = 510890 AND CAST( d. STR_NBR AS INT64) =  563 THEN 'Y'
  --              WHEN a. SKU_NBR = 775276 AND CAST( d. STR_NBR AS INT64) = 6505 THEN 'Y'
  --              WHEN a. SKU_NBR = 510890 AND CAST( d. STR_NBR AS INT64) = 8976 THEN 'Y'
  --              ELSE 'N'
  --              END ) = 'Y'
  
  --   AND a.DEPT_NBR = 21
  --   AND a.SUB_DEPT_NBR = '0021'
  --   AND a.CLASS_NBR = 1
  --   AND a.SUB_CLASS_NBR = 2
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project),
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 


#################     JLQ_EACH_TRANS_STG    #################

bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT 
  sls.STR_NBR,
  dtl.SKU_NBR,
  dtl.SKU_CRT_DT,
  b.FSCL_YR_WK_KEY_VAL,
  CAST(CEIL(SUM(dtl.UNT_SLS)) AS INT64) AS ITEM_QTY
  
  FROM `pr-edw-views-thd.SLS.POS_SLS_TRANS_DTL` AS sls,    UNNEST(sls.DTL) AS dtl
  
  JOIN `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` b
    ON sls.SLS_DT = b.CAL_DT
  
  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_PRE_STG` c
    ON  dtl.SKU_NBR = c.SKU_NBR
    AND dtl.SKU_CRT_DT = c.SKU_CRT_DT
    AND CAST( sls.STR_NBR AS INT64 ) = c.STR_NBR
  
  WHERE 1=1
  AND sls.POS_TRANS_TYP_CD = 'S' --SALE
  AND sls.POS_TRANS_STAT_IND ='NO' --NORMAL TRANSACTION
  AND sls.CSHR_TI_FLG = FALSE --NOT A TRAINING TRANSACTION
  AND sls.SLS_DT BETWEEN DATE_ADD(CURRENT_DATE, INTERVAL -364 DAY) AND DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY) --TRAILING YEAR     
  AND dtl.UNT_SLS > 0
  
  -- AND dtl.SKU_NBR = 386081
  -- AND CAST(sls.STR_NBR AS INT64) = 1130	
  -- AND sls.SLS_DT >= DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)

  GROUP BY   
  sls.STR_NBR,
  dtl.SKU_NBR,
  dtl.SKU_CRT_DT,
  sls.RGSTR_NBR,
  sls.POS_TRANS_ID,
  b.FSCL_YR_WK_KEY_VAL

    ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 




#################     JLQ_STR_SKU_WK_LIST    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_STR_SKU_WK_LIST",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT 
  SKU_NBR,
  SKU_CRT_DT,
  STR_NBR, 
  FSCL_YR_WK_KEY_VAL 
  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG`
  GROUP BY 1,2,3,4
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 




#################     JLQ_EACH_TRANS_STG_2    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG_2",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.*,
  --B.ITEM_ID,
  CAST(FLOOR (C.EFF_CORR_SLS_QTY) AS INT64) AS REVISEDDEMAND,
  C.GRSS_SLS_QTY as REGULARDEMAND
  
  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_STR_SKU_WK_LIST`  A
  --LEFT JOIN  `pr-edw-views-thd.TD_COPIES.ITEM_HIER_MAPPING` B
  --ON A.SKU_NBR = B.ITEM_SKU_NBR
  --AND A.SKU_CRT_DT = B.ITEM_CRT_DT
  
  --JOIN `pr-edw-views-thd.SCHN_FCST.WEEKLY_PROD_DMND` C 
  --ON B.ITEM_ID = C.PRODUCTNUMBER  ###notice the new data source has sku_number to join
  --AND CAST(A.STR_NBR AS INT64) = CAST(C.LOCATIONID AS INT64)
  --AND CAST(A.FSCL_YR_WK_KEY_VAL AS INT64) =  C.YEARPLUSWEEKNO 
  
  
  JOIN  `pr-edw-views-thd.SCHN_FCST_DMND.WKLY_AGG_SLS_HIST` C 
  ON A.SKU_NBR = C.SKU_NBR  ###notice the new data source has sku_number to join
  and A.SKU_CRT_DT = C.SKU_CRT_DT
  AND CAST(A.STR_NBR AS INT64) = CAST(C.LOC_NBR AS INT64)
  AND CAST(A.FSCL_YR_WK_KEY_VAL AS INT64) =  C.FSCL_YR_WK 
  
  
  
  WHERE 1=1
  AND C.EFF_CORR_SLS_QTY >= 0
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 




#################     UPDATE_JLQ_EACH_TRANS_STG    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.STR_NBR,
  A.SKU_NBR,
  A.SKU_CRT_DT,
  A.FSCL_YR_WK_KEY_VAL,
  CASE WHEN B.REVISEDDEMAND < A.ITEM_QTY THEN B.REVISEDDEMAND ELSE A.ITEM_QTY END AS ITEM_QTY
  
  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG` A
  LEFT JOIN  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG_2` B
  ON A.STR_NBR = B.STR_NBR
  AND A.SKU_NBR = B.SKU_NBR
  AND A.SKU_CRT_DT = B.SKU_CRT_DT
  AND A.FSCL_YR_WK_KEY_VAL = B.FSCL_YR_WK_KEY_VAL
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 






#################     JLQ_AGG_TRANS    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_AGG_TRANS",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  STR_NBR
  , SKU_NBR
  , SKU_CRT_DT
  , COUNT(*) OVER(PARTITION BY STR_NBR, SKU_NBR, SKU_CRT_DT) AS MAX_PKEY
  , ROW_NUMBER() OVER (PARTITION BY STR_NBR, SKU_NBR, SKU_CRT_DT ORDER BY ITEM_QTY DESC) AS PKEY
  , ITEM_QTY
  , COUNT(*) AS TRANS_CNT
  FROM  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_EACH_TRANS_STG` 
  
  
  --WHERE CAST(STR_NBR AS INT64) = 1904
  --AND SKU_NBR  = 488469
  GROUP BY
  STR_NBR
  , SKU_NBR
  , SKU_CRT_DT
  , ITEM_QTY
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 







#################     JLQ_CLUSTER_MEANS    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_CLUSTER_MEANS",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.STR_NBR
  , A.SKU_NBR
  , A.SKU_CRT_DT
  , B.MAX_PKEY
  , B.K1_START
  , B.K2_START
  , B.K3_START
  , SUM(CASE WHEN A.PKEY BETWEEN B.K1_START AND B.K2_START - 1 THEN A.ITEM_QTY * A.TRANS_CNT END)
  / CAST(SUM(CASE WHEN A.PKEY BETWEEN B.K1_START AND B.K2_START - 1 THEN A.TRANS_CNT END) AS FLOAT64) AS K1_MEAN
  , SUM(CASE WHEN A.PKEY BETWEEN B.K1_START AND B.K2_START - 1 THEN A.TRANS_CNT END) AS K1_CNT
  , SUM(CASE WHEN A.PKEY BETWEEN B.K2_START AND B.K3_START - 1 THEN A.ITEM_QTY * A.TRANS_CNT END)
  / CAST(SUM(CASE WHEN A.PKEY BETWEEN B.K2_START AND B.K3_START - 1 THEN A.TRANS_CNT END) AS FLOAT64) AS K2_MEAN
  , SUM(CASE WHEN A.PKEY BETWEEN B.K2_START AND B.K3_START - 1 THEN A.TRANS_CNT END) AS K2_CNT
  , SUM(CASE WHEN A.PKEY >= B.K3_START THEN A.ITEM_QTY * A.TRANS_CNT END)
  / CAST(SUM(CASE WHEN A.PKEY >= B.K3_START THEN A.TRANS_CNT END) AS FLOAT64) AS K3_MEAN
  , SUM(CASE WHEN A.PKEY >= B.K3_START THEN A.TRANS_CNT END) AS K3_CNT
  
  FROM  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_AGG_TRANS` A
  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_CLUSTER_GROUPS` B
  ON
  A.MAX_PKEY = B.MAX_PKEY
  
  
  -- WHERE  CAST(A.STR_NBR AS INT64) = 1904
  --AND A.SKU_NBR = 488469    
  
  GROUP BY
  A.STR_NBR
  , A.SKU_NBR
  , A.SKU_CRT_DT
  , B.MAX_PKEY
  , B.K1_START
  , B.K2_START
  , B.K3_START
  
  
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 





#################     JLQ_CLUSTER_SSE    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_CLUSTER_SSE",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.STR_NBR
  , A.SKU_NBR
  , A.SKU_CRT_DT
  , B.K1_MEAN
  , SQRT(SUM(CASE WHEN A.PKEY BETWEEN B.K1_START AND B.K2_START - 1 THEN POW((A.ITEM_QTY - K1_MEAN),2) * A.TRANS_CNT
  END) / B.K1_CNT) AS K1_STDDEV
  , B.K1_CNT
  , B.K2_MEAN
  , SQRT(SUM(CASE WHEN A.PKEY BETWEEN B.K2_START AND B.K3_START - 1 THEN POW((A.ITEM_QTY - K2_MEAN),2) * A.TRANS_CNT
  END) / B.K2_CNT) AS K2_STDDEV
  , B.K2_CNT
  , B.K3_MEAN
  , SQRT(SUM(CASE WHEN A.PKEY >= B.K3_START                         THEN POW((A.ITEM_QTY - K3_MEAN),2) * A.TRANS_CNT
  END) / B.K3_CNT) AS K3_STDDEV
  , B.K3_CNT
  , SUM(CASE WHEN A.PKEY BETWEEN B.K1_START AND B.K2_START - 1 THEN POW((A.ITEM_QTY - K1_MEAN),2) * A.TRANS_CNT
  WHEN A.PKEY BETWEEN B.K2_START AND B.K3_START - 1 THEN POW((A.ITEM_QTY - K2_MEAN),2) * A.TRANS_CNT
  WHEN A.PKEY >= B.K3_START                         THEN POW((A.ITEM_QTY - K3_MEAN),2) * A.TRANS_CNT
  END) AS SUM_SSE
  
  FROM  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_AGG_TRANS` A
  JOIN  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_CLUSTER_MEANS` B
  ON A.STR_NBR = B.STR_NBR
  AND A.SKU_NBR = B.SKU_NBR
  AND A.SKU_CRT_DT = B.SKU_CRT_DT
  AND A.MAX_PKEY = B.MAX_PKEY
  
  
  
  --  WHERE  CAST(A.STR_NBR AS INT64) = 1904
  --   AND A.SKU_NBR = 488469
  
  
  GROUP BY
  A.STR_NBR
  , A.SKU_NBR
  , A.SKU_CRT_DT
  , B.K1_START
  , B.K2_START
  , B.K3_START
  , B.K1_MEAN
  , B.K1_CNT
  , B.K2_MEAN
  , B.K2_CNT
  , B.K3_MEAN
  , B.K3_CNT
  
  
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 


#################     JLQ_COMPANY_JLQ_RAW    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_RAW",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  STR_NBR
  , SKU_NBR 
  , SKU_CRT_DT
  , LAST_UPD_DT
  , TOP_JLQ
  , TOP_STDDEV
  , TOP_FREQ
  , MID_JLQ
  , MID_STDDEV
  , MID_FREQ
  , BOT_JLQ
  , BOT_STDDEV
  , BOT_FREQ
  FROM (
  SELECT
  STR_NBR
  , SKU_NBR , SKU_CRT_DT
  , CURRENT_DATE() AS LAST_UPD_DT
  , K1_MEAN AS TOP_JLQ
  , K1_STDDEV AS TOP_STDDEV
  , K1_CNT AS TOP_FREQ
  , K2_MEAN AS MID_JLQ
  , K2_STDDEV AS MID_STDDEV
  , K2_CNT AS MID_FREQ
  , K3_MEAN AS BOT_JLQ
  , K3_STDDEV AS BOT_STDDEV
  , K3_CNT AS BOT_FREQ
  ,ROW_NUMBER() OVER (PARTITION BY STR_NBR, SKU_NBR, SKU_CRT_DT ORDER BY SUM_SSE ASC) AS ROW_RANK
  FROM  `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_CLUSTER_SSE`
  ) A
  WHERE ROW_RANK = 1
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 





#################     JLQ_SUBSET    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_SUBSET",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.STR_NBR
  , A.SKU_NBR
  , A.SKU_CRT_DT
  , CURRENT_DATE AS LAST_UPD_DT
  
  --, C.MIN_JLQ_SIZE ,C.MAX_JLQ_SIZE,COALESCE(CASE WHEN C.MAX_JLQ_SIZE = 0 THEN NULL ELSE C.MAX_JLQ_SIZE END, 9999) AS MAX_JLQ
  
  , CASE
  WHEN A.TOP_FREQ >= C.MIN_JLQ_FREQ THEN TOP_JLQ
  WHEN A.TOP_FREQ + A.MID_FREQ >= C.MIN_JLQ_FREQ THEN MID_JLQ
  WHEN A.TOP_FREQ + A.MID_FREQ + A.BOT_FREQ >= C.MIN_JLQ_FREQ THEN BOT_JLQ
  END AS JLQ
  , CASE
  WHEN A.TOP_FREQ >= C.MIN_JLQ_FREQ THEN TOP_STDDEV
  WHEN A.TOP_FREQ + A.MID_FREQ >= C.MIN_JLQ_FREQ THEN MID_STDDEV
  WHEN A.TOP_FREQ + A.MID_FREQ + A.BOT_FREQ >= C.MIN_JLQ_FREQ THEN BOT_STDDEV
  END AS STDDEV
  , CASE
  WHEN A.TOP_FREQ >= C.MIN_JLQ_FREQ THEN TOP_FREQ
  WHEN A.TOP_FREQ + A.MID_FREQ >= C.MIN_JLQ_FREQ THEN TOP_FREQ + MID_FREQ
  WHEN A.TOP_FREQ + A.MID_FREQ + A.BOT_FREQ >= C.MIN_JLQ_FREQ THEN TOP_FREQ + MID_FREQ + BOT_FREQ
  END AS FREQ
  
  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_RAW` A
  JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY` B
  ON
  A.STR_NBR = B.STR_NBR
  AND A.SKU_NBR = B.SKU_NBR
  AND A.SKU_CRT_DT = B.SKU_CRT_DT
  AND B.CAL_DT = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY) 
  AND B.IPR_REPLE_IND = 1
  
  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_SC_LIST` C
  ON B.CLASS_CTG_GRP_CD = C.SUB_DEPT
  AND B.ITEM_CLASS_CD = C.CLASS_NBR
  AND B.ITEM_SC_CD = C.SC_NBR
  
  WHERE
  CASE
  WHEN A.TOP_FREQ >= C.MIN_JLQ_FREQ THEN TOP_JLQ
  WHEN A.TOP_FREQ + A.MID_FREQ >= C.MIN_JLQ_FREQ THEN MID_JLQ
  WHEN A.TOP_FREQ + A.MID_FREQ + A.BOT_FREQ >= C.MIN_JLQ_FREQ THEN BOT_JLQ
  END
  BETWEEN C.MIN_JLQ_SIZE AND COALESCE(CASE WHEN C.MAX_JLQ_SIZE = 0 THEN NULL ELSE C.MAX_JLQ_SIZE END, 9999)
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 





#################     JLQ_ADJUSTED_FREQ    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_ADJUSTED_FREQ",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
 SELECT
  A.*
  , CAST(CASE
              WHEN ROUND(A.FREQ * F4_SF) > 500 THEN 500
              WHEN ROUND(A.FREQ * F4_SF) < 3 THEN 3
              ELSE ROUND(A.FREQ * F4_SF)
        END AS INT64) AS ADJ_FREQ
  
  FROM (
          SELECT
          A.*
          , CASE
          WHEN F.FSCL_WK_NBR = 1 THEN (D.SF03 + D.SF04 + D.SF05 + D.SF06) / 4
          WHEN F.FSCL_WK_NBR = 2 THEN (D.SF04 + D.SF05 + D.SF06 + D.SF07) / 4
          WHEN F.FSCL_WK_NBR = 3 THEN (D.SF05 + D.SF06 + D.SF07 + D.SF08) / 4
          WHEN F.FSCL_WK_NBR = 4 THEN (D.SF06 + D.SF07 + D.SF08 + D.SF09) / 4
          WHEN F.FSCL_WK_NBR = 5 THEN (D.SF07 + D.SF08 + D.SF09 + D.SF10) / 4
          WHEN F.FSCL_WK_NBR = 6 THEN (D.SF08 + D.SF09 + D.SF10 + D.SF11) / 4
          WHEN F.FSCL_WK_NBR = 7 THEN (D.SF09 + D.SF10 + D.SF11 + D.SF12) / 4
          WHEN F.FSCL_WK_NBR = 8 THEN (D.SF10 + D.SF11 + D.SF12 + D.SF13) / 4
          WHEN F.FSCL_WK_NBR = 9 THEN (D.SF11 + D.SF12 + D.SF13 + D.SF14) / 4
          WHEN F.FSCL_WK_NBR = 10 THEN (D.SF12 + D.SF13 + D.SF14 + D.SF15) / 4
          WHEN F.FSCL_WK_NBR = 11 THEN (D.SF13 + D.SF14 + D.SF15 + D.SF16) / 4
          WHEN F.FSCL_WK_NBR = 12 THEN (D.SF14 + D.SF15 + D.SF16 + D.SF17) / 4
          WHEN F.FSCL_WK_NBR = 13 THEN (D.SF15 + D.SF16 + D.SF17 + D.SF18) / 4
          WHEN F.FSCL_WK_NBR = 14 THEN (D.SF16 + D.SF17 + D.SF18 + D.SF19) / 4
          WHEN F.FSCL_WK_NBR = 15 THEN (D.SF17 + D.SF18 + D.SF19 + D.SF20) / 4
          WHEN F.FSCL_WK_NBR = 16 THEN (D.SF18 + D.SF19 + D.SF20 + D.SF21) / 4
          WHEN F.FSCL_WK_NBR = 17 THEN (D.SF19 + D.SF20 + D.SF21 + D.SF22) / 4
          WHEN F.FSCL_WK_NBR = 18 THEN (D.SF20 + D.SF21 + D.SF22 + D.SF23) / 4
          WHEN F.FSCL_WK_NBR = 19 THEN (D.SF21 + D.SF22 + D.SF23 + D.SF24) / 4
          WHEN F.FSCL_WK_NBR = 20 THEN (D.SF22 + D.SF23 + D.SF24 + D.SF25) / 4
          WHEN F.FSCL_WK_NBR = 21 THEN (D.SF23 + D.SF24 + D.SF25 + D.SF26) / 4
          WHEN F.FSCL_WK_NBR = 22 THEN (D.SF24 + D.SF25 + D.SF26 + D.SF27) / 4
          WHEN F.FSCL_WK_NBR = 23 THEN (D.SF25 + D.SF26 + D.SF27 + D.SF28) / 4
          WHEN F.FSCL_WK_NBR = 24 THEN (D.SF26 + D.SF27 + D.SF28 + D.SF29) / 4
          WHEN F.FSCL_WK_NBR = 25 THEN (D.SF27 + D.SF28 + D.SF29 + D.SF30) / 4
          WHEN F.FSCL_WK_NBR = 26 THEN (D.SF28 + D.SF29 + D.SF30 + D.SF31) / 4
          WHEN F.FSCL_WK_NBR = 27 THEN (D.SF29 + D.SF30 + D.SF31 + D.SF32) / 4
          WHEN F.FSCL_WK_NBR = 28 THEN (D.SF30 + D.SF31 + D.SF32 + D.SF33) / 4
          WHEN F.FSCL_WK_NBR = 29 THEN (D.SF31 + D.SF32 + D.SF33 + D.SF34) / 4
          WHEN F.FSCL_WK_NBR = 30 THEN (D.SF32 + D.SF33 + D.SF34 + D.SF35) / 4
          WHEN F.FSCL_WK_NBR = 31 THEN (D.SF33 + D.SF34 + D.SF35 + D.SF36) / 4
          WHEN F.FSCL_WK_NBR = 32 THEN (D.SF34 + D.SF35 + D.SF36 + D.SF37) / 4
          WHEN F.FSCL_WK_NBR = 33 THEN (D.SF35 + D.SF36 + D.SF37 + D.SF38) / 4
          WHEN F.FSCL_WK_NBR = 34 THEN (D.SF36 + D.SF37 + D.SF38 + D.SF39) / 4
          WHEN F.FSCL_WK_NBR = 35 THEN (D.SF37 + D.SF38 + D.SF39 + D.SF40) / 4
          WHEN F.FSCL_WK_NBR = 36 THEN (D.SF38 + D.SF39 + D.SF40 + D.SF41) / 4
          WHEN F.FSCL_WK_NBR = 37 THEN (D.SF39 + D.SF40 + D.SF41 + D.SF42) / 4
          WHEN F.FSCL_WK_NBR = 38 THEN (D.SF40 + D.SF41 + D.SF42 + D.SF43) / 4
          WHEN F.FSCL_WK_NBR = 39 THEN (D.SF41 + D.SF42 + D.SF43 + D.SF44) / 4
          WHEN F.FSCL_WK_NBR = 40 THEN (D.SF42 + D.SF43 + D.SF44 + D.SF45) / 4
          WHEN F.FSCL_WK_NBR = 41 THEN (D.SF43 + D.SF44 + D.SF45 + D.SF46) / 4
          WHEN F.FSCL_WK_NBR = 42 THEN (D.SF44 + D.SF45 + D.SF46 + D.SF47) / 4
          WHEN F.FSCL_WK_NBR = 43 THEN (D.SF45 + D.SF46 + D.SF47 + D.SF48) / 4
          WHEN F.FSCL_WK_NBR = 44 THEN (D.SF46 + D.SF47 + D.SF48 + D.SF49) / 4
          WHEN F.FSCL_WK_NBR = 45 THEN (D.SF47 + D.SF48 + D.SF49 + D.SF50) / 4
          WHEN F.FSCL_WK_NBR = 46 THEN (D.SF48 + D.SF49 + D.SF50 + D.SF51) / 4
          WHEN F.FSCL_WK_NBR = 47 THEN (D.SF49 + D.SF50 + D.SF51 + D.SF52) / 4
          WHEN F.FSCL_WK_NBR = 48 THEN (D.SF50 + D.SF51 + D.SF52 + D.SF01) / 4
          WHEN F.FSCL_WK_NBR = 49 THEN (D.SF51 + D.SF52 + D.SF01 + D.SF02) / 4
          WHEN F.FSCL_WK_NBR = 50 THEN (D.SF52 + D.SF01 + D.SF02 + D.SF03) / 4
          WHEN F.FSCL_WK_NBR = 51 THEN (D.SF01 + D.SF02 + D.SF03 + D.SF04) / 4
          ELSE (D.SF02 + D.SF03 + D.SF04 + D.SF05) / 4
          END AS F4_SF
          , LEAST(E.REV_TM_DAYS_CNT + E.LEAD_TM_DAYS, 50) AS RTLT
          
          FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_SUBSET` A
          
          join ( select * from `pr-inventory-planning-thd.SF.LOC_SKU_SF` 
          where SF_CALC_DATE = (select max(SF_CALC_DATE) from   `pr-inventory-planning-thd.SF.LOC_SKU_SF` )) D
          on A.SKU_NBR = D.SKU_NBR
          and CAST(A.STR_NBR AS INT64) = CAST(D.LOC_NBR AS INT64)
        
          # LEFT JOIN  `pr-edw-views-thd.TD_COPIES.ITEM_HIER_MAPPING` B
          #   ON A.SKU_NBR = B.ITEM_SKU_NBR
          #   AND A.SKU_CRT_DT = B.ITEM_CRT_DT
        
          # JOIN `pr-edw-views-thd.SCHN_FCST.PRODUCT_LOCATION_OP` C
          #   ON  CAST(A.STR_NBR AS INT64) = CAST(C.LOCATIONID  AS INT64)
          #   AND B.ITEM_ID = C.PRODUCTNUMBER
          # 
          # JOIN `pr-edw-views-thd.SCHN_FCST.SESSION_RUN_MOD_SF` D
          #   ON C.MODELID = D.MODELID
          
        
          JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY` E
            ON A.SKU_NBR = E.SKU_NBR
            AND A.SKU_CRT_DT = E.SKU_CRT_DT
            AND CAST(A.STR_NBR AS INT64) = CAST(E.STR_NBR AS INT64)
            AND E.CAL_DT = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)
            AND E.IPR_REPLE_IND = 1
            AND E.REV_TM_DAYS_CNT IS NOT NULL
            AND E.LEAD_TM_DAYS IS NOT NULL
        
          CROSS JOIN (
                        SELECT
                        FSCL_WK_NBR
                        FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD`
                        WHERE      CAL_DT = CURRENT_DATE()
                      ) F
          WHERE 1=1
          
          --AND CAST(A.STR_NBR AS INT64) = 4187
          --AND A.SKU_NBR = 656542

  ) A

  WHERE F4_SF > 0.6
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 






#################     JLQ_COMPANY_JLQ_TRANS    #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_TRANS",
  write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  A.*
  , B.PROB_85 AS JLQ_MULT
  , CASE
        WHEN B.PROB_85 = 1.0 THEN CAST(CEILING((A.JLQ * B.PROB_85) + A.STDDEV) AS INT64)
        WHEN B.PROB_85 = 1.5 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(1.5) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 2.0 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(2.0) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 2.5 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(2.5) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 3.0 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(3.0) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 3.5 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(3.5) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 4.0 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(4.0) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 4.5 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(4.5) * A.STDDEV)) AS INT64)
        WHEN B.PROB_85 = 5.0 THEN CAST(CEILING((A.JLQ * B.PROB_85) + (SQRT(5.0) * A.STDDEV)) AS INT64)
    END AS FULL_JLQ
  
  
  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_ADJUSTED_FREQ` A

  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_RTLT_FREQ_LKUP` B
    ON  A.RTLT = B.RTLT
    AND A.ADJ_FREQ = B.FREQ
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 



####################################################################

# SELECT
# FSCL_WK_END_DT,
# --B.SUB_DEPT_NBR,
# -- LAST_UPD_DT,
# COUNT(*),
# SUM( JLQ ) AS JLQ,
# SUM( FULL_JLQ ) AS FULL_JLQ
# 
# FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_TRANS_HIST` A
# INNER JOIN `pr-edw-views-thd.SHARED.SKU_HIER_FD` B
# ON A.SKU_NBR = B.SKU_NBR
# AND A.SKU_CRT_DT = B.SKU_CRT_DT
# 
# WHERE A.LAST_UPD_DT > DATE_SUB( CURRENT_DATE, INTERVAL 65 DAY )
# 
# GROUP BY 1--,2
# ORDER BY 1

#################     JLQ_COMPANY_JLQ_TRANS_HIST   #################


bq_job_wait(bq_perform_query(
  #project = project,
  #dataset = dataset,
  destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_TRANS_HIST", 
  ##update biweekly
  write_disposition = "WRITE_APPEND",
  # create_disposition = "CREATE_IF_NEEDED",
  # write_disposition = "WRITE_TRUNCATE",
  query = 
    "
  SELECT
  F.CAL_DT AS FSCL_WK_END_DT,
  A.*

  FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_TRANS` A
  
  CROSS JOIN (  SELECT
                A.CAL_DT,
                A.FSCL_WK_NBR
                FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` A
  
                JOIN (  SELECT
                        FSCL_YR_WK_KEY_VAL
                        FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` A
                        --WHERE a. CAL_DT IN ( CURRENT_DATE(), DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY))
                        --WHERE a. CAL_DT IN ( DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY), DATE_ADD(CURRENT_DATE, INTERVAL 14 DAY), DATE_ADD(CURRENT_DATE, INTERVAL 21 DAY))
                        WHERE a. CAL_DT IN ( DATE_ADD(CURRENT_DATE, INTERVAL 7 DAY), DATE_ADD(CURRENT_DATE, INTERVAL 14 DAY))  -- change for BI team
                      ) B
                  ON A.FSCL_YR_WK_KEY_VAL = B.FSCL_YR_WK_KEY_VAL

                WHERE  A.DAY_OF_WK_NBR = 7
                GROUP BY 1,2
              ) F
  
  ",
  use_legacy_sql = FALSE,
  max_pages = Inf,
  billing = project), 
  quiet = getOption( "bigrquery.quiet"), pause = 1
  
) 




######################################################################  END OF CALCULATION QUERY SET   #######################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################


##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
##############################################################################################################################################################
######################################################################  PARM AND IMPACT EXECUTION  ###########################################################
 


#################     JLQ_UPLOAD    #################

upload_2 <- query_exec( project = project,
                        #dataset = dataset,
                        destination_table = "IXL0858_ANALYTICS.JLQ_UPLOAD",
                        write_disposition = "WRITE_TRUNCATE",
                        query =
    "
                        
    WITH GetSKUInfo AS (
    
    SELECT
    
    a.*
    
    --a.SKU_NBR,
    --a.SKU_CRT_DT,
    --a.SKU_DESC, 
    --a.SKU_STAT_CD, 
    --a.SUB_CLASS_NBR, 
    --a.SUB_CLASS_DESC,
    --a.CLASS_NBR, 
    --a.CLASS_DESC,
    --a.DEPT_NBR, 
    --a.SUB_DEPT_NBR,
    --a.SUB_DEPT_DESC
    
    FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` a
    
    JOIN (  SELECT
            bb.SKU_NBR,
            MAX( bb.SKU_CRT_DT ) AS SKU_CRT_DT
            FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` bb
            GROUP BY 1
          ) b
      ON a.SKU_NBR = b.SKU_NBR
      AND a.SKU_CRT_DT = b.SKU_CRT_DT
    
    WHERE A.SUB_DEPT_NBR IS NOT NULL
    
    )
    
    
    SELECT
      5 AS Trumping_Level
    , 2 AS Type
    , NULL AS Store_Group
    , NULL AS Volume_Id
    , NULL AS Velocity_Id
    , A.SKU_NBR AS SKU
    , NULL AS SKU_Grp
    , CAST(A.STR_NBR AS INT64) AS Store
    , 1703 AS Parm_code
    , CAST( CASE
    
                # WHEN CAST(B.MKT_NBR AS INT64) = 2 AND G.EXT_SUB_CLASS_NBR IN ('027-001-002', '027-001-011', '027-004-002', '027-004-004', '027-006-002', '027-006-003', --  1309, 1317, 1344, 1345, 3475, 5160, 1367, 1368, 1369, 1370, 1371, 1372, 1374, 1375, 1376, 3334, 1378, 1379, 1391, 1392, 1393, 1395)
                # '027-006-004', '027-006-005', '027-006-006', '027-006-007', '027-006-009', '027-006-010',
                # '027-006-011', '027-006-014', '027-006-016', '027-008-004', '027-008-005', '027-008-007',
                # '027-008-009', '027-006-012', '027-004-006', '027-004-014') THEN A.FULL_JLQ --LINDSEY REEVES
                
                WHEN D.JLQ_METHOD = '1' THEN CEILING(A.JLQ)
                WHEN D.JLQ_METHOD = 'FULL' THEN A.FULL_JLQ
                END AS INT64) AS Parm_Value
    
    --CHANGE DATE TO NEXT DAY (FRIDAY) THEN 2 WEEKS OUT

    , FORMAT_DATE(\"%m/%d/%Y\",F.CAL_DT) AS Eff_Begin_date 
    , FORMAT_DATE(\"%m/%d/%Y\",DATE_ADD(F.CAL_DT, INTERVAL 22 DAY))  AS Eff_End_date

    --, F.CAL_DT AS Eff_Begin_date --CHANGE DATE!
    --, DATE_ADD(F.CAL_DT, INTERVAL 22 DAY) AS Eff_End_date --CHANGE DATE!

    , NULL AS Start_Fscl_Wk_nbr
    , NULL AS End_Fscl_Wk_nbr

    , CONCAT('JLQ FW', CAST(F.FSCL_WK_NBR AS STRING)) AS Param_Desc --CHANGE DATE!
    , NULL AS OOTL_Reason_Code
    
    FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_TRANS` a
    
    LEFT JOIN `pr-edw-views-thd.SHARED.STR_HIER_FD` b
      ON a.STR_NBR = b.STR_NBR
      AND b.STR_CLS_DT > CURRENT_DATE('America/New_York')
    
    JOIN GetSKUInfo g
      ON a.SKU_NBR = g.SKU_NBR
      AND a.SKU_CRT_DT = g.SKU_CRT_DT
    
    JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_COMPANY_JLQ_SC_LIST` d
      ON g.SUB_DEPT_NBR = d.SUB_DEPT
      AND g.CLASS_NBR = d.CLASS_NBR
      AND g.SUB_CLASS_NBR = d.SC_NBR
    
    CROSS JOIN (  SELECT
                  a.CAL_DT,
                  a.FSCL_WK_NBR
                  
                  FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` a
    
                  JOIN (  SELECT
                          bb.FSCL_YR_WK_KEY_VAL
                          FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` bb
                          WHERE bb.CAL_DT = CURRENT_DATE()
                        ) b
                    ON a.FSCL_YR_WK_KEY_VAL = b.FSCL_YR_WK_KEY_VAL
                  
                  WHERE  a.DAY_OF_WK_NBR = 5
                  GROUP BY 1,2
                ) f
    
    WHERE 1=1
    AND d.JLQ_METHOD <> 'BYPASS'
    
    /*
    AND A.SKU_NBR NOT IN ( 251430, 251528, 251791, 252846) --LINDSEY REEVES
    AND ( CAST(A.STR_NBR AS INT64) <> 1406 OR A.SKU_NBR <> 984590) --TIMI KRAFT
    AND ( CAST(A.STR_NBR AS INT64) <> 106 OR A.SKU_NBR <> 145785) --TRACY TURNER
    AND A.SKU_NBR <> 278803 --TIMI KRAFT
    AND A.SKU_NBR NOT IN ( 1000003071, 1000003072, 1000003073, 1000026937, 1000049543, 1000049544, 1000054878, 1000063103, 161167, 423599) --J THORNE
    AND ( CAST(A.STR_NBR AS INT64) <> 3828 OR A.SKU_NBR <> 827621) --MIKE ZIMMERMAN
    AND ( CAST(A.STR_NBR AS INT64) <> 3828 OR A.SKU_NBR <> 827638) --MIKE ZIMMERMAN
    AND ( CAST(A.STR_NBR AS INT64) <> 3828 OR A.SKU_NBR <> 827640) --MIKE ZIMMERMAN
    AND ( CAST(A.STR_NBR AS INT64) <> 3828 OR A.SKU_NBR <> 827643) --MIKE ZIMMERMAN
    AND ( CAST(A.STR_NBR AS INT64) <> 477 OR G.EXT_CLASS_NBR NOT IN ('030-020','030-021','030-027')) --(349, 350, 356) --MICHELLE SLICK
    AND ( CAST(A.STR_NBR AS INT64) <> 4013 OR G.EXT_CLASS_NBR NOT IN ('030-020','030-021','030-027')) --MICHELLE SLICK
    AND ( CAST(A.STR_NBR AS INT64) <> 6654 OR A.SKU_NBR <> 576387) --MARK SCHLEIER
    AND A.SKU_NBR <> 1001243800 --CJ BROOME
    AND ( CAST(A.STR_NBR AS INT64) <> 3605 OR A.SKU_NBR <> 964196) --CJ BROOME
    AND ( CAST(A.STR_NBR AS INT64) <> 405 OR A.SKU_NBR <> 158653) --CJ BROOME
    AND ( CAST(A.STR_NBR AS INT64) <> 1546 OR A.SKU_NBR <> 123460) --LATRENDA SMITH
    AND ( CAST(A.STR_NBR AS INT64) <> 707 OR A.SKU_NBR <> 1000031792) --ALEX NIZKOVSKI
    AND ( CAST(A.STR_NBR AS INT64) <> 485 OR A.SKU_NBR <> 315248) --CLEVELAND CANAR
    AND A.SKU_NBR NOT IN (191095, 191926, 193242, 193520, 1000021086, 1000021088, 1000021091) --CLEVELAND CANAR
    AND A.SKU_NBR NOT IN (1000001066, 1000001072,1000001071) --ASH GALE
    AND ( CAST(A.STR_NBR AS INT64) <> 126 OR A.SKU_NBR <> 169757) --ASHLEY TUPIN
    AND ( CAST(A.STR_NBR AS INT64) <> 2763 OR A.SKU_NBR <> 714338) --JOSHUA STERRITT
    AND ( CAST(A.STR_NBR AS INT64) <> 126 OR A.SKU_NBR <> 873343) --ASHLEY TUPIN
    AND ( CAST(A.STR_NBR AS INT64) <> 4647 OR A.SKU_NBR <> 792127) --JOSIAH SIN
    AND ( CAST(A.STR_NBR AS INT64) <> 1228 OR A.SKU_NBR <> 1000007379) --SHONTEL MARSH 2/18/16
    
    
    AND A.SKU_NBR NOT IN ( 1001599950, 1001599951, 1001599952, 1001600067, 1001650923, 1001600565, 1001600568, 1001360149, 1000027420) --NATHAN BROCK
    AND A.SKU_NBR NOT IN ( 1000017959, 1001293006, 1000047462 ) --  CHARLIE LOCKRIDGE
    AND (CAST(A.STR_NBR AS INT64) <> 6534 OR A.SKU_NBR <> 514208) --Allison Schmidt 10/25/2018
    AND (CAST(B.DIV_NBR AS INT64) <>  4 --NORTHERN DIVISION	 Jonathan Hung 11/27/2018
    OR ( G.DEPT_NBR <> 22 --BUILDING MATERIALS
    OR G.CLASS_NBR NOT IN (9, 10))) --CONCRETE AND ROOFING
    
    AND (CAST(B.DIV_NBR AS INT64) <>  1 --SOUTHERN DIVISION	 Elizabeth Clark 12/07/2018
    OR ( G.DEPT_NBR <> 22 --BUILDING MATERIALS
    OR G.CLASS_NBR NOT IN (10)) --ROOFING
    OR G.SUB_CLASS_NBR NOT IN ( 3, 4, 5, 6, 9, 10, 11, 12, 14, 15, 17, 18, 19 ))
    
    AND (CAST(B.DIV_NBR AS INT64) <>  2 --WESTERN DIVISION	 Elizabeth Clark 12/07/2018
    OR ( G.DEPT_NBR <> 22 --BUILDING MATERIALS
    OR G.CLASS_NBR NOT IN (10)) --ROOFING
    OR G.SUB_CLASS_NBR NOT IN ( 3, 4, 5, 6, 9, 10, 11, 12, 14, 15, 17, 18, 19 ))
    */    
                      
    "     ,
    use_legacy_sql = FALSE,
    max_pages = Inf,
    billing = project)



################   OUTL Impact    ##################

IMPACT <- query_exec(project = project,
                     dataset = dataset,
                     query = 
     "
     WITH ACTV_SKU_STR_LIST AS (

     SELECT
     
     a. SKU_NBR,
     CAST( d. STR_NBR AS INT64) AS STR_NBR,
     a. SKU_CRT_DT,
     -- a. SKU_DESC,
     -- a. SKU_STAT_CD, 
     REPLACE ( a.SKU_DESC, '\"', '' ) AS SKU_DESC,
     a. DEPT_NBR,
     a. SUB_DEPT_NBR,
     -- a. SUB_DEPT_DESC,
     a. CLASS_NBR,
     a. CLASS_DESC,
     -- CONCAT( CAST( a. CLASS_NBR AS STRING ), CAST ( ' - ' AS STRING ), CAST( a.CLASS_DESC AS STRING )) AS SKU_CLASS,
     a. SUB_CLASS_NBR,
     a. SUB_CLASS_DESC
     #-- CONCAT( CAST( a.SUB_CLASS_NBR AS STRING ), CAST( ' - ' AS STRING ), CAST( a.SUB_CLASS_DESC AS STRING )) AS SKU_SUBCLASS
     
     
     -------------------- get SKU info ---------------------
     FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` a
     
     JOIN ( SELECT
     SKU_NBR,
     MAX(SKU_CRT_DT) AS SKU_CRT_DT
     FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD`
     GROUP BY  1
     ) b
     ON a.SKU_NBR = b.SKU_NBR
     AND a.SKU_CRT_DT = b.SKU_CRT_DT
     
     
     ------------ get actively replenished SKUs ------------
     JOIN ( SELECT DISTINCT
     SKU_NBR,
     SKU_CRT_DT,
     STR_NBR
     FROM `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY`
     WHERE 1=1
     AND PARTITIONDATE = DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY)
     AND IPR_REPLE_IND = 1
     AND OK_TO_ORD_FLG = 'Y'
     ) d
     ON a.SKU_NBR = d.SKU_NBR
     AND a.SKU_CRT_DT = d.SKU_CRT_DT
     
     WHERE 1=1
     
     -- AND a.DEPT_NBR = 22
     -- AND a.SUB_DEPT_NBR = '0021'
     -- AND a.CLASS_NBR = 10
     -- AND a.SUB_CLASS_NBR = 2
     
     -- AND ( CASE  WHEN a. SKU_NBR = 775940 AND CAST( d. STR_NBR AS INT64) = 2906 THEN 'Y'
     --               WHEN a. SKU_NBR = 510890 AND CAST( d. STR_NBR AS INT64) =  563 THEN 'Y'
     --               WHEN a. SKU_NBR = 775276 AND CAST( d. STR_NBR AS INT64) = 6505 THEN 'Y'
     --               WHEN a. SKU_NBR = 510890 AND CAST( d. STR_NBR AS INT64) = 8976 THEN 'Y'
     --               ELSE 'N'
     --               END ) = 'Y'
     
    )

--------------------------------------------------------------------------------------------------------------------------------------------------------------


    ,LT_RT_QTY AS (
    
    SELECT
    
    a.STR_NBR,
    b.SKU_NBR,
    SUM( b.PARM_DEC_VAL ) AS LT_RT_QTY,
    SUM( b.PARM_INT_VAL ) AS LT_RT_DAYS
    
    FROM `pr-edw-views-thd.SHARED.STR_HIER_FD` a
    -- FROM `pr-edw-views-thd.SHARED.STR` a
    
    JOIN `pr-edw-views-thd.SCHN_INV.A_CRE_BU_SKU_PARM` b
    ON a.STR_BU_ID = b.BU_ID
    
    WHERE 1=1
    AND b.CRE_PARM_CD IN ( 1, 2 )
    
    -- AND b.SKU_NBR = 938076
    -- AND CAST( a.STR_NBR AS INT64 ) = 2115
    
    GROUP BY a.STR_NBR, b.SKU_NBR
    
    )
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,PARM_DLY_1 AS (
  
  SELECT
  
  a. PARTITIONDATE AS DATE,
  a.SKU_NBR AS SKU,
  CAST( a.STR_NBR AS INT64 ) AS STORE,
  sku_str.SKU_DESC,
  a.DEPT_NBR AS DEPT,
  sku_str.SUB_DEPT_NBR AS SUB_DEPT,
  a.ITEM_CLASS_CD AS CLASS,
  sku_str.CLASS_DESC,
  a.ITEM_SC_CD AS SUBCLASS,
  sku_str.SUB_CLASS_DESC,
  
  a.BUY_UOM_QTY AS BP_QTY,
  a.OH_QTY + a.OO_QTY + IFNULL(a.EXPCTD_ALLOC_QTY,0) + IFNULL(a.CONF_ALLOC_QTY,0) AS EFF_OO_QTY,
  
  a.CURR_RETL_AMT AS RETL,
  (( a.REV_TM_QTY + a.LEAD_TM_QTY ) / ( a.LEAD_TM_DAYS + a.REV_TM_DAYS_CNT )) AS DLY_ARS,
  IFNULL( a.SUBPARM_MIN_OH_QTY, 0 ) AS SUBPARM_MOHQ,
  a.MAX_INV_QTY AS MAX_QTY,
  a.LEAD_TM_DAYS AS LT_DAYS,
  a.REV_TM_DAYS_CNT AS  RT_DAYS,
  a.REV_TM_QTY AS RT_UNITS,
  a.LEAD_TM_QTY AS LT_UNITS,
  a.SFTY_STK_QTY AS SSU,
  IFNULL( a.CMTD_STK_QTY , 0 ) AS CMTD_STK,
  a.TRGT_OH_QTY,
  
  d.PARM_VALUE AS NEW_JLQ,
  
  
  ----------------------------------------------------------
  
  
  -- **** COMM MIN BUCKET ****
  
  a. CMTD_MIN_MAP_QTY,                                         -- MAP 2001
  a. CMTD_MIN_XMER_QTY,                                        -- CMAP 2002
  a. CMTD_MIN_BULK_WALL_QTY,                                   -- Bulk Wall 2003
  a. MOHQ_PRST_QTY,                                            -- SPI 1701
  
  
  -- **** MOHQ BUCKET ****
  a. MOHQ_AD_QTY,                                              -- Ad Min 1702
  a. MOHQ_JLQ_QTY,                                             -- JLQ 1703
  a. MOHQ_FLD_RQST_QTY,                                        -- Field RQST 1704
  a. MOHQ_WOS_MIN_QTY,                                         -- WOS MIN 1705
  a. MOHQ_SPPRT_NEW_ITEM_QTY,                                  -- New Item 1706
  a. MOHQ_BULK_DSPL_QTY,                                       -- Bulk 1707
  a. MOHQ_NON_SPI_PRST_QTY,                                    -- Non SPI 1708
  a. MOHQ_INV_SOLN_TEAM_QTY,                                   -- Solution 1709
  
  
  -- **** SSU BUCKET ****
  a. SFTY_STK_QTY,
  
  
  ----------------------------------------------------------
  
  GREATEST(
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(a. CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(a. CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(a. CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(a. MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  IFNULL(a. MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  IFNULL(a. MOHQ_JLQ_QTY,0),                                              -- JLQ 1703
  IFNULL(a. MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(a. MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(a. MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(a. MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(a. MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(a. MOHQ_INV_SOLN_TEAM_QTY,0),                                    -- Solution 1709
  
  
  -- **** SSU BUCKET ****
  a. SFTY_STK_QTY
  ) AS CURR_SYSMOHQ_QTY,
  
  a. SUBPARM_MIN_OH_QTY,
  a. MIN_OH_QTY,
  a. SUBPARM_CMTD_STK_QTY,
  a. CMTD_STK_QTY,
  
  ------------------------------------------------------------------ manually calculating CUR_OUTL_NO_MAX
  
  CEIL( 
  
  --IFNULL(a. LEAD_TM_QTY,0) +
  --IFNULL(a. REV_TM_QTY,0) +
  
  IFNULL( COALESCE( (a. LEAD_TM_QTY + a. REV_TM_QTY), e.LT_RT_QTY ), 0) +
  
  GREATEST(
  GREATEST( 
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  
  IFNULL(MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  IFNULL(MOHQ_JLQ_QTY,0),                                              -- JLQ 1703
  IFNULL(MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0)),                                    -- Solution 1709
  
  --IFNULL(a.SUBPARM_MIN_OH_QTY, 0), IFNULL(a.MIN_OH_QTY, 0) ),
  
  -- **** SSU BUCKET ****
  IFNULL(SFTY_STK_QTY, 0)
  
  ) +
  
  IFNULL(COALESCE( a. SUBPARM_CMTD_STK_QTY, CMTD_STK_QTY ),0)
  
  ) AS CURR_OUTL_QTY_NO_MAX_CALC,
  
  
  ------------------------------------------------------------------ manually calculating CUR_OUTL_QTY
  
  CEIL( CASE  WHEN ( 
  --IFNULL(a. LEAD_TM_QTY,0) +
  --IFNULL(a. REV_TM_QTY,0) +
  
  IFNULL( COALESCE( (a. LEAD_TM_QTY + a. REV_TM_QTY), e.LT_RT_QTY ), 0) +
  
  GREATEST(
  GREATEST( 
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  
  IFNULL(MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  IFNULL(MOHQ_JLQ_QTY,0),                                              -- JLQ 1703
  IFNULL(MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0)),                                    -- Solution 1709
  
  --IFNULL(a.SUBPARM_MIN_OH_QTY, 0), IFNULL(a.MIN_OH_QTY, 0) ),
  
  -- **** SSU BUCKET ****
  IFNULL(SFTY_STK_QTY, 0)
  
  ) +
  
  IFNULL(COALESCE( a. SUBPARM_CMTD_STK_QTY, CMTD_STK_QTY ),0)
  ) > a. MAX_INV_QTY
  
  THEN a. MAX_INV_QTY
  
  ELSE ( 
  --IFNULL(a. LEAD_TM_QTY,0) +
  --IFNULL(a. REV_TM_QTY,0) +
  
  --IFNULL( COALESCE( e.LT_RT_QTY, (a. LEAD_TM_QTY + a. REV_TM_QTY)), 0) +
  IFNULL( COALESCE( (a. LEAD_TM_QTY + a. REV_TM_QTY), e.LT_RT_QTY ), 0) +
  
  GREATEST(
  GREATEST( 
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  
  IFNULL(MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  IFNULL(MOHQ_JLQ_QTY,0),                                              -- JLQ 1703
  IFNULL(MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0)),                                    -- Solution 1709
  
  --IFNULL(a.SUBPARM_MIN_OH_QTY, 0), IFNULL(a.MIN_OH_QTY, 0) ),
  
  -- **** SSU BUCKET ****
  IFNULL(SFTY_STK_QTY, 0)
  
  ) +
  
  IFNULL(COALESCE( a. SUBPARM_CMTD_STK_QTY, CMTD_STK_QTY ),0)
  )
  END ) AS CURR_OUTL_CALC_QTY,
  
  ------------------------------------------------------------------ manually calculating NEW_OUTL_QTY
  
  CEIL( CASE  WHEN ( 
  --IFNULL(a. LEAD_TM_QTY,0) +
  --IFNULL(a. REV_TM_QTY,0) +
  
  IFNULL( COALESCE( (a. LEAD_TM_QTY + a. REV_TM_QTY), e.LT_RT_QTY ), 0) +
  
  GREATEST(
  GREATEST( 
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  
  IFNULL(MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  COALESCE( IFNULL(d.PARM_VALUE,0), IFNULL(MOHQ_JLQ_QTY,0)) ,          -- JLQ 1703 - substituting with calculated JLQ value
  IFNULL(MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0)),                                    -- Solution 1709
  
  --IFNULL(a.SUBPARM_MIN_OH_QTY, 0), IFNULL(a.MIN_OH_QTY, 0) ),
  
  -- **** SSU BUCKET ****
  IFNULL(SFTY_STK_QTY, 0)
  
  ) +
  
  IFNULL(COALESCE( a. SUBPARM_CMTD_STK_QTY, CMTD_STK_QTY ),0)
  ) > a. MAX_INV_QTY
  
  THEN a. MAX_INV_QTY
  
  ELSE ( 
  --IFNULL(a. LEAD_TM_QTY,0) +
  --IFNULL(a. REV_TM_QTY,0) +
  
  --IFNULL( COALESCE( e.LT_RT_QTY, (a. LEAD_TM_QTY + a. REV_TM_QTY)), 0) +
  IFNULL( COALESCE( (a. LEAD_TM_QTY + a. REV_TM_QTY), e.LT_RT_QTY ), 0) +
  
  GREATEST(
  GREATEST( 
  
  -- **** COMM MIN BUCKET ****
  (
  IFNULL(CMTD_MIN_MAP_QTY,0) +                                         -- MAP 2001
  IFNULL(CMTD_MIN_XMER_QTY,0) +                                        -- CMAP 2002
  IFNULL(CMTD_MIN_BULK_WALL_QTY,0) +                                   -- Bulk Wall 2003
  IFNULL(MOHQ_PRST_QTY,0)                                              -- SPI 1701
  ),
  
  
  -- **** MOHQ BUCKET ****
  
  IFNULL(MOHQ_AD_QTY,0),                                               -- Ad Min 1702
  COALESCE( IFNULL(d.PARM_VALUE,0), IFNULL(MOHQ_JLQ_QTY,0)) ,          -- JLQ 1703 - substituting with calculated JLQ value
  IFNULL(MOHQ_FLD_RQST_QTY,0),                                         -- Field RQST 1704
  IFNULL(MOHQ_WOS_MIN_QTY,0),                                          -- WOS MIN 1705
  IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0),                                   -- New Item 1706
  IFNULL(MOHQ_BULK_DSPL_QTY,0),                                        -- Bulk 1707
  IFNULL(MOHQ_NON_SPI_PRST_QTY,0),                                     -- Non SPI 1708
  IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0)),                                    -- Solution 1709
  
  --IFNULL(a.SUBPARM_MIN_OH_QTY, 0), IFNULL(a.MIN_OH_QTY, 0) ),
  
  -- **** SSU BUCKET ****
  IFNULL(SFTY_STK_QTY, 0)
  
  ) +
  
  IFNULL(COALESCE( a. SUBPARM_CMTD_STK_QTY, CMTD_STK_QTY ),0)
  )
  END ) AS NEW_OUTL_CALC_QTY
  
  ------------------------------------------------------------------
  
  
  FROM `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY` a
  
  JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.JLQ_UPLOAD` d
    ON a.SKU_NBR = d.SKU
    AND CAST(a.STR_NBR AS INT64)  = CAST(d.STORE AS INT64)
    AND d.PARM_VALUE <> 0
  
  JOIN ACTV_SKU_STR_LIST sku_str
  ON a.SKU_NBR = sku_str.SKU_NBR
  AND a.SKU_CRT_DT = sku_str.SKU_CRT_DT
  AND CAST(a.STR_NBR AS INT64) = sku_str.STR_NBR
  
  
  LEFT JOIN `LT_RT_QTY` e
  ON a. SKU_NBR = e. SKU_NBR
  AND CAST( a. STR_NBR AS INT64 ) = CAST( e. STR_NBR AS INT64 ) 
  
  
  WHERE 1=1
  AND a. PARTITIONDATE = DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY)
  AND a. IPR_REPLE_IND = 1
  AND a. OK_TO_ORD_FLG = 'Y'
  
  
  )
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,PARM_DLY_2 AS (
  
  SELECT
  a.*,
  
  ( a.TRGT_OH_QTY * a.RETL ) AS TRGT_OH_AMT,
  
  ( a.CURR_OUTL_QTY_NO_MAX_CALC * a.RETL ) AS CURR_OUTL_AMT_NO_MAX_CALC,
  
  ( a.CURR_OUTL_CALC_QTY * a.RETL ) AS CURR_OUTL_CALC_AMT,
  
  ( a.NEW_OUTL_CALC_QTY * a.RETL ) AS NEW_OUTL_CALC_AMT
  
  
  FROM PARM_DLY_1 a
  
  )
  
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,CAR_ORD_CALC_1 AS (
  
  SELECT
  a.*
  
  ,CEIL(CAST(
  (CASE WHEN (CURR_OUTL_CALC_QTY - EFF_OO_QTY) < 0
  THEN 0
  ELSE (CURR_OUTL_CALC_QTY - EFF_OO_QTY)
  END) / BP_QTY
  AS FLOAT64)
  ) * BP_QTY AS CURR_CAR_ORD_QTY
  
  
  ,CEIL(CAST(
  (CASE WHEN (NEW_OUTL_CALC_QTY - EFF_OO_QTY) < 0
  THEN 0
  ELSE (NEW_OUTL_CALC_QTY - EFF_OO_QTY)
  END) / BP_QTY AS FLOAT64)
  ) * BP_QTY AS NEW_CAR_ORD_QTY_CALC
  
  FROM PARM_DLY_2 a
  
  )
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,IMPACT_CALC_1 AS (
  
  SELECT
  a.*
  
  ---CURRENT OUTL AND CAR ORDER-----
  --, CURR_OUTL_QTY * CURR_RETL_AMT AS CURR_OUTL_AMT
  , CURR_CAR_ORD_QTY * RETL AS CURR_CAR_ORD_AMT
  
  ---NEW OUTL AND CAR ORDER-----
  --,NEW_OUTL_QTY * CURR_RETL_AMT AS NEW_OUTL_AMT
  ,NEW_CAR_ORD_QTY_CALC * RETL AS CAR_ORD_AMT_CALC
  
  FROM CAR_ORD_CALC_1 a
  )
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,IMPACT_CALC_2 AS (
  
  SELECT
  a.*,
  
  TRGT_OH_AMT - CURR_OUTL_CALC_AMT AS OUTL_DIFF_AMT,
  
  NEW_OUTL_CALC_AMT - TRGT_OH_AMT AS OUTL_IMPACT_AMT,
  -- CAR_ORD_AMT_CALC_2 - CURR_CAR_ORD_AMT AS CAR_ORD_IMPACT_AMT,
  
  CASE WHEN NEW_OUTL_CALC_AMT <> CURR_OUTL_CALC_AMT THEN 1 ELSE 0 END AS OUTL_IMPACT_SKU_STR_COUNT,
  -- CASE WHEN CAR_ORD_AMT_CALC_2 - CURR_CAR_ORD_AMT > 0 THEN 1 ELSE 0 END AS ORD_IMPACT_SKU_STR_COUNT,
  
  
  NEW_OUTL_CALC_AMT - CURR_OUTL_CALC_AMT AS OUTL_IMPACT_AMT_2
  
  FROM IMPACT_CALC_1 a
  
  )
  
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,IMPACT_SMRY_SUB_DEPT AS (
  
  SELECT
  
  --  DEPT
  SUB_DEPT
  -- , CLASS
  -- , CLASS_DESC
  -- , SUBCLASS
  -- , SUB_CLASS_DESC
  
  , COUNT(*) AS TOTAL_STR_SKU_CNT
  , SUM(OUTL_IMPACT_SKU_STR_COUNT) AS OUTL_IMPACT_SKU_STR_COUNT
  -- , SUM(ORD_IMPACT_SKU_STR_COUNT) AS ORD_IMPACT_SKU_STR_COUNT
  , SUM(TRGT_OH_QTY) AS TRGT_OH_QTY
  , SUM(TRGT_OH_AMT) AS TRGT_OH_AMT
  , SUM(CURR_OUTL_CALC_QTY) AS CURR_OUTL_CALC_QTY
  , SUM(CURR_OUTL_CALC_AMT) AS CURR_OUTL_CALC_AMT
  , SUM(OUTL_DIFF_AMT) AS OUTL_DIFF_AMT
  , SUM(NEW_OUTL_CALC_QTY) AS NEW_OUTL_CALC_QTY
  , SUM(NEW_OUTL_CALC_AMT) AS NEW_OUTL_CALC_AMT
  -- , SUM(OUTL_IMPACT_AMT) AS OUTL_IMPACT_AMT_1
  , SUM(OUTL_IMPACT_AMT_2) AS OUTL_IMPACT_AMT
  -- , SUM(CAR_ORD_IMPACT_AMT) AS CAR_ORD_IMPACT_AMT
  
  
  FROM IMPACT_CALC_2 a  
  
  GROUP BY 1--,2--,3,4,5,6
  
  ORDER BY 1 ASC
  
  )
  
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  ,IMPACT_SMRY_TOTAL AS (
  
  
  SELECT
  
  'TOTAL' AS SUB_DEPT
  
  , COUNT(*) AS TOTAL_STR_SKU_CNT
  , SUM(OUTL_IMPACT_SKU_STR_COUNT) AS OUTL_IMPACT_SKU_STR_COUNT
  -- , SUM(ORD_IMPACT_SKU_STR_COUNT) AS ORD_IMPACT_SKU_STR_COUNT
  , SUM(TRGT_OH_QTY) AS TRGT_OH_QTY
  , SUM(TRGT_OH_AMT) AS TRGT_OH_AMT
  , SUM(CURR_OUTL_CALC_QTY) AS CURR_OUTL_CALC_QTY
  , SUM(CURR_OUTL_CALC_AMT) AS CURR_OUTL_CALC_AMT
  , SUM(OUTL_DIFF_AMT) AS OUTL_DIFF_AMT
  , SUM(NEW_OUTL_CALC_QTY) AS NEW_OUTL_CALC_QTY
  , SUM(NEW_OUTL_CALC_AMT) AS NEW_OUTL_CALC_AMT
  -- , SUM(OUTL_IMPACT_AMT) AS OUTL_IMPACT_AMT_1
  , SUM(OUTL_IMPACT_AMT_2) AS OUTL_IMPACT_AMT
  -- , SUM(CAR_ORD_IMPACT_AMT) AS CAR_ORD_IMPACT_AMT
  
  
  FROM IMPACT_CALC_2 a  
  
  )
--------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  SELECT * FROM IMPACT_SMRY_SUB_DEPT UNION ALL
  
  SELECT * FROM IMPACT_SMRY_TOTAL

   ",
   use_legacy_sql = FALSE,
   max_pages = Inf,
   billing = project)


##############################################################################################################################################################


write.csv(IMPACT,str_c("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/JLQ Min Procedure  (DO NOT DELETE - Nirjhar Raina)/Investment Report/investment_report_",
                       str_trim(str_replace_all(Sys.Date(),pattern = ":",replacement = "-")),".csv"), #  quote = FALSE,
          row.names = FALSE)





upload<-as.data.table(upload_2)

colnames(upload)[colnames(upload)=="Trumping_Level"]<-"Trumping Level"
colnames(upload)[colnames(upload)=="Store_Group"]<-"Store Group"
colnames(upload)[colnames(upload)=="Volume_Id"]<-"Volume Id"
colnames(upload)[colnames(upload)=="Velocity_Id"]<-"Velocity Id"
colnames(upload)[colnames(upload)=="SKU_Grp"]<-"SKU Grp"
colnames(upload)[colnames(upload)=="Parm_code"]<-"Parm code"
colnames(upload)[colnames(upload)=="Parm_Value"]<-"Parm Value"
colnames(upload)[colnames(upload)=="Eff_Begin_date"]<-"Eff Begin date"
colnames(upload)[colnames(upload)=="Eff_End_date"]<-"Eff End date"
colnames(upload)[colnames(upload)=="Start_Fscl_Wk_nbr"]<-"Start Fscl Wk nbr"
colnames(upload)[colnames(upload)=="End_Fscl_Wk_nbr"]<-"End Fscl Wk nbr"
colnames(upload)[colnames(upload)=="Param_Desc"]<-"Param Desc"
colnames(upload)[colnames(upload)=="OOTL_Reason_Code"]<-"OOTL Reason Code"


counter<- nrow(upload)
i<-1
while (counter>0){
  write<- min(999999,counter)
  write.csv(upload[1:write],
            str_c("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/JLQ Min Procedure  (DO NOT DELETE - Nirjhar Raina)/Parm Uploads/JLQ_min_upload_",str_trim(str_replace_all(Sys.Date(),pattern = ":",replacement = "-")),"_",i,".csv"), row.names=FALSE, quote=FALSE, na="")
  upload<-upload[-1:-write]
  counter<-counter-write
  i<- i+1
}







