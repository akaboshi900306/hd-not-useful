# version 2.1 with added case statement to reduce bulk parms qty for selected SKUs


{
  options(java.parameters = "- Xmx1024m")
  packages = list("bigrquery", "data.table", "bigQueryR", "readr")
  for(i in 1:length(packages)) {
    if (is.element(packages[i],installed.packages()) == TRUE){
      library(packages[[i]],character.only=TRUE)
    } else {
      install.packages(packages[[i]])
      library(packages[[i]],character.only=TRUE)
    }
  }
}

setwd("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/BULK Min Procedure (DO NOT DELETE - Nirjhar Raina)/Bulk Inputs/")

library(bigrquery)
#library(RGoogleAnalytics)
library(data.table)
#library(stringr)
library(bigQueryR)
#library(readr)
#bqr_auth()
project = "analytics-supplychain-thd"
dataset = "IXL0858_ANALYTICS"


###########################     PULLING AND UPLOADING MERCH SETTINGS AND EXCLUSIONS    #########################


bulk_min_mult_exc <-fread("Bulk min preferences.csv")

query_exec( project = project,
            dataset = dataset,
            query = "DELETE FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_MULT_EXC` WHERE 1=1",
            use_legacy_sql = FALSE,
            max_pages = Inf,
            billing = project
)


bulk_min_mult_exc_schema = schema_fields(bulk_min_mult_exc)

bq_job_wait( bq_perform_upload(  x = "analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_MULT_EXC",
                                 values = bulk_min_mult_exc,
                                 fields = bulk_min_mult_exc_schema,
                                 create_disposition = "CREATE_IF_NEEDED",
                                 write_disposition = "WRITE_TRUNCATE",
                                 # source_format = "CSV",
                                 # nskip = 1,
                                 billing = project), 
             
             quiet = getOption( "bigrquery.quiet"), pause = 1
             
)


bulk_min_sku_exc <-fread("Bulk SKU exceptions.csv")

query_exec( project = project,
            dataset = dataset,
            query = "DELETE FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_SKU_EXC` WHERE 1=1",
            use_legacy_sql = FALSE,
            max_pages = Inf,
            billing = project
)

bulk_min_sku_exc_schema = schema_fields(bulk_min_sku_exc)

bq_job_wait( bq_perform_upload(  x = "analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_SKU_EXC",
                                 values = bulk_min_sku_exc,
                                 fields = bulk_min_sku_exc_schema,
                                 create_disposition = "CREATE_IF_NEEDED",
                                 write_disposition = "WRITE_TRUNCATE",
                                 # source_format = "CSV",
                                 # nskip = 1,
                                 billing = project), 
             
             quiet = getOption( "bigrquery.quiet"), pause = 1
             
)


sc_id <- query_exec(project = project,
                   dataset = dataset,
                   query = 
                    "
                  SELECT DISTINCT

                  A.SKU_NBR,
                  C.SUB_DEPT_NBR,
                  C.CLASS_NBR,
                  C.CLASS_DESC,
                  C.SUB_CLASS_NBR,
                  C.SUB_CLASS_DESC,
                  \'ON\' AS BULK_PRICE_INV_FEED,
                  \'1\' AS BULK_PRICE_MULTIPLIER

                  FROM ( SELECT DISTINCT
                         SKU_NBR
                         FROM ( SELECT
                                A1.*
                                FROM `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A1   --pr-supply-chain-thd
                                
                                JOIN ( SELECT
                                       SKU_NBR,
                                       MKT_NBR,
                                       TIER_LVL_NBR,
                                       MAX(EFF_BGN_DT) AS MAX_BGN_DT

                                       FROM  `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A2
                                       GROUP BY 1,2,3
                                      ) A2
                                  ON A1.SKU_NBR = A2.SKU_NBR
                                  AND A1.MKT_NBR =A2.MKT_NBR
                                  AND A1.TIER_LVL_NBR =A2.TIER_LVL_NBR
                                  AND A1.EFF_BGN_DT = A2.MAX_BGN_DT

                                WHERE CAST(BQ_EFF_END_DT AS DATE) = \'9999-12-31\' AND BQ_ACTN_IND  = \'I\'
                               ) A        
                        ) A

                  JOIN ( SELECT
                         A.*
                         FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A

                         JOIN ( SELECT
                                A.SKU_NBR,
                                MAX(SKU_CRT_DT) AS SKU_CRT_DT

                                FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A
                                GROUP BY  1) B
                           ON A.SKU_NBR = B.SKU_NBR
                           AND A.SKU_CRT_DT = B.SKU_CRT_DT
                        ) C
                    ON A.SKU_NBR = C.SKU_NBR

                  LEFT JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_MULT_EXC` E
                    ON C.SUB_DEPT_NBR  = CASE WHEN LENGTH(E.DEPT) = 2 THEN CONCAT(\'00\',E.DEPT) 
                                              WHEN LENGTH(E.DEPT) = 3 THEN CONCAT(\'0\', E.DEPT)
                                              ELSE E.DEPT END
                    AND C.CLASS_NBR = E.CLASS_NBR
                    AND C.SUB_CLASS_NBR = E.SC_NBR

                   JOIN ( SELECT DISTINCT
                          SKU_NBR
                          FROM `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY` hh
                          WHERE hh.CAL_DT = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY) 
                          AND hh.SKU_STAT_CD IN (100,200,300)
                          AND hh.IPR_REPLE_IND = 1 )H
                          -- AND ((hh.SKU_VLCTY_CD NOT IN (\'D\',\'E\') AND hh.DEPT_NBR NOT IN (25,28) ) OR hh.DEPT_NBR IN (25,28))) H                                 
                   ON A.SKU_NBR  = H.SKU_NBR
                   
                   WHERE e.SC_NBR IS NULL
                    ",
                   use_legacy_sql = FALSE,
                   max_pages = Inf,
                   billing = project)



if (nrow(sc_id) != 0) { 
  write.table(sc_id,"new_SC_IDs!!!.txt",quote =FALSE,sep = "\t",row.names = FALSE)
  quit(save = 'no', status = 0,runLast = FALSE)
  
}




bq_job_wait(bq_perform_query(
            #project = project,
            #dataset = dataset,
           destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_NEW_TIER_PRC_ALL",
           write_disposition = "WRITE_TRUNCATE",
           query = 
           "
              SELECT 
                A.SKU_NBR
               , A.MKT_NBR
               , C.SKU_CRT_DT
               ------------------- ADD EXCEPTIONS AND OVERRIDES TO `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE` -------------------
               , CASE WHEN G.SKU IS NULL 
               THEN 
               (CASE WHEN H.SKU IS NULL
               THEN A.VOL_DISC_MIN_QTY
               ELSE (CASE WHEN H.OVRD_TYPE = 1 THEN H.OVRD_QTY
               WHEN H.OVRD_TYPE = 2 THEN GREATEST(H.MIN_OVRD_QTY,A.VOL_DISC_MIN_QTY)
               WHEN H.OVRD_TYPE = 3 THEN LEAST(H.MAX_OVRD_QTY,A.VOL_DISC_MIN_QTY)          
               END)END) 
               ELSE 
               (CASE WHEN G.OVRD_TYPE = 1 THEN G.OVRD_QTY
               WHEN G.OVRD_TYPE = 2 THEN GREATEST(G.MIN_OVRD_QTY,A.VOL_DISC_MIN_QTY)
               WHEN G.OVRD_TYPE = 3 THEN LEAST(G.MAX_OVRD_QTY,A.VOL_DISC_MIN_QTY)          
               END)END
               AS VOL_DISC_MIN_QTY 
               ----------------------------------------------------------------------------------------------
               , A.VOL_DISC_PCT
               , B.LOC_NBR
               , A.EFF_END_DT
               , A.EFF_BGN_DT                  
               , 1 AS MKT_LVL
               ,F.SKU_STAT_CD
               ,F.IPR_REPLE_IND
               FROM (  SELECT A1.*
               FROM `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A1 
               JOIN (SELECT 
               SKU_NBR,
               MKT_NBR,
               TIER_LVL_NBR,
               MAX(EFF_BGN_DT) AS MAX_BGN_DT
               FROM  `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A2
               GROUP BY 1,2,3
               ) A2
               ON A1.SKU_NBR = A2.SKU_NBR
               AND A1.MKT_NBR =A2.MKT_NBR
               AND A1.TIER_LVL_NBR =A2.TIER_LVL_NBR
               AND A1.EFF_BGN_DT = A2.MAX_BGN_DT
               WHERE CAST(BQ_EFF_END_DT AS DATE) = \'9999-12-31\' AND BQ_ACTN_IND  = \'I\'
               ) A        
               JOIN `pr-edw-views-thd.SHARED.LOC_HIER_FD` B
               ON CAST(A.MKT_NBR AS INT64) = CAST(B.MKT_NBR AS INT64)
               JOIN (SELECT  
               AA.SKU_NBR,
               AA.SKU_CRT_DT,
               AA.SKU_STAT_CD, 
               AA.SUB_CLASS_NBR AS SC_NBR, 
               AA.SUB_CLASS_DESC AS SC_DESC,
               AA.CLASS_NBR, 
               AA.CLASS_DESC, 
               AA.SUB_DEPT_NBR AS DEPT
               FROM  `pr-edw-views-thd.SHARED.SKU_HIER_FD` AA
               JOIN (SELECT  AAA.SKU_NBR, MAX(SKU_CRT_DT) AS SKU_CRT_DT FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` AAA GROUP BY 1) BB
               ON AA.SKU_NBR = BB.SKU_NBR
               AND AA.SKU_CRT_DT = BB.SKU_CRT_DT
               AND AA.SKU_STAT_CD IN (100,200,300)) C
               ON A.SKU_NBR = C.SKU_NBR		  
               
               JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_DLY` F                  
               ON CAST(A.SKU_NBR AS INT64) = CAST(F.SKU_NBR AS INT64)
               AND CAST(B.LOC_NBR AS INT64) = CAST(F.STR_NBR AS INT64)
               AND F.CAL_DT = DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)
               AND F.SKU_STAT_CD IN (100,200,300)
               AND F.IPR_REPLE_IND = 1  
               
               JOIN  `pr-edw-views-thd.SHARED.FSCL_WK_HIER_FD` E
               ON CURRENT_DATE() BETWEEN E.FSCL_WK_BGN_DT AND E.FSCL_WK_END_DT
               
               LEFT JOIN `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE`  G
               ON  A.SKU_NBR = G.SKU
               AND CAST(B.LOC_NBR AS INT64) = G.STR
               AND CURRENT_DATE() BETWEEN G.BEG_DT AND G.END_DT
               
               LEFT JOIN `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE`  H
               ON  A.SKU_NBR = H.SKU
               AND CURRENT_DATE() BETWEEN H.BEG_DT AND H.END_DT
               
               WHERE 1=1 
               AND E.FSCL_WK_BGN_DT > A.EFF_BGN_DT 
               AND  E.FSCL_WK_BGN_DT <= A.EFF_END_DT  
               AND C.SKU_STAT_CD IN (100,200,300)    
               AND A.TIER_LVL_NBR = 1
               AND B.LOC_CLS_DT > CURRENT_DATE()
    
           ",
           use_legacy_sql = FALSE,
           max_pages = Inf,
           billing = project), 
           
  quiet = getOption( "bigrquery.quiet"), pause = 1
           
)







bq_job_wait(bq_perform_query(
                                    #dataset = dataset,
                                    destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_LW_TIER_PRC_ALL",
                                    write_disposition = "WRITE_TRUNCATE",
                                    query = 
                                    "SELECT 
                                       A.SKU_NBR
                                       ,A.MKT_NBR
                                       ,C.SKU_CRT_DT
                                                  ------------------- ADD EXCEPTIONS AND OVERRIDES TO `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE` -------------------
                                                  , CASE WHEN G.SKU IS NULL 
                                                    THEN 
                                                    (CASE WHEN H.SKU IS NULL
                                                    THEN A.VOL_DISC_MIN_QTY
                                                    ELSE (CASE WHEN H.OVRD_TYPE = 1 THEN H.OVRD_QTY
                                                    WHEN H.OVRD_TYPE = 2 THEN GREATEST(H.MIN_OVRD_QTY,A.VOL_DISC_MIN_QTY)
                                                    WHEN H.OVRD_TYPE = 3 THEN LEAST(H.MAX_OVRD_QTY,A.VOL_DISC_MIN_QTY)          
                                                    END)END) 
                                                    ELSE 
                                                    (CASE WHEN G.OVRD_TYPE = 1 THEN G.OVRD_QTY
                                                    WHEN G.OVRD_TYPE = 2 THEN GREATEST(G.MIN_OVRD_QTY,A.VOL_DISC_MIN_QTY)
                                                    WHEN G.OVRD_TYPE = 3 THEN LEAST(G.MAX_OVRD_QTY,A.VOL_DISC_MIN_QTY)          
                                                    END)END
                                                    AS VOL_DISC_MIN_QTY 
                                                   ----------------------------------------------------------------------------------------------
                                      , A.VOL_DISC_PCT
                                      , B.LOC_NBR
                                      , A.EFF_END_DT
                                      , A.EFF_BGN_DT                  
                                      , 1 AS MKT_LVL
                                    FROM ( SELECT A1.*
                                          FROM `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A1 
                                          JOIN (SELECT SKU_NBR,
                                               MKT_NBR,
                                               TIER_LVL_NBR,
                                               MAX(EFF_BGN_DT) AS MAX_BGN_DT
                                               FROM  `pr-edw-views-thd.SCHN_SHARED.SKU_MKT_TIER_PRC` A2
                                               GROUP BY 1,2,3
                                               ) A2
                                          ON A1.SKU_NBR = A2.SKU_NBR
                                          AND A1.MKT_NBR =A2.MKT_NBR
                                          AND A1.TIER_LVL_NBR =A2.TIER_LVL_NBR
                                          AND A1.EFF_BGN_DT = A2.MAX_BGN_DT
                                          WHERE CAST(BQ_EFF_END_DT AS DATE) = \'9999-12-31\' AND BQ_ACTN_IND  = \'I\'
                                           ) A        
                                    JOIN `pr-edw-views-thd.SHARED.LOC_HIER_FD` B
                                      ON CAST(A.MKT_NBR AS INT64) = CAST(B.MKT_NBR AS INT64)
                                    JOIN (SELECT  
                                            A.SKU_NBR,
                                            A.SKU_CRT_DT,
                                            A.SKU_STAT_CD, 
                                            A.SUB_CLASS_NBR AS SC_NBR, 
                                            A.SUB_CLASS_DESC AS SC_DESC,
                                            A.CLASS_NBR, 
                                            A.CLASS_DESC, 
                                            A.SUB_DEPT_NBR AS DEPT
                                          FROM    `pr-edw-views-thd.SHARED.SKU_HIER_FD` A
                                          JOIN    (SELECT    A.SKU_NBR, MAX(SKU_CRT_DT) AS SKU_CRT_DT FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A GROUP BY  1) B
                                            ON      A.SKU_NBR = B.SKU_NBR
                                            AND     A.SKU_CRT_DT = B.SKU_CRT_DT
                                            AND A.SKU_STAT_CD IN (100,200,300) 
                                        ) C
                                      ON A.SKU_NBR = C.SKU_NBR	

                                    JOIN  `pr-edw-views-thd.SHARED.FSCL_WK_HIER_FD` E
                                      ON DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY) BETWEEN E.FSCL_WK_BGN_DT AND E.FSCL_WK_END_DT  
                                    
                                    LEFT JOIN `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE`  G
                                        ON  A.SKU_NBR = G.SKU
                                        AND CAST(B.LOC_NBR AS INT64) = G.STR
                                        AND CURRENT_DATE() BETWEEN G.BEG_DT AND G.END_DT
                                        
                                    LEFT JOIN `analytics-supplychain-thd.JRS4297_BULK.SKU_STR_OVERRIDE`  H
                                        ON  A.SKU_NBR = H.SKU
                                        AND CURRENT_DATE() BETWEEN H.BEG_DT AND H.END_DT

                                    WHERE 1=1 
                                      AND E.FSCL_WK_BGN_DT > A.EFF_BGN_DT AND  E.FSCL_WK_BGN_DT <= A.EFF_END_DT  
                                      AND C.SKU_STAT_CD IN (100,200,300)    
                                      AND A.TIER_LVL_NBR = 1
                                      AND B.LOC_CLS_DT > CURRENT_DATE()
             
                                    ",
                                    use_legacy_sql = FALSE,
                                    max_pages = Inf,
                                    billing = project), 
            
            quiet = getOption( "bigrquery.quiet"), pause = 1
            
)

 #Sys.sleep(300)               # 2


bq_job_wait(bq_perform_query(
                                   #dataset = dataset,
                                   destination_table = "analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_DIAG",
                                   write_disposition = "WRITE_TRUNCATE",
                                   query = 
                                          "
                                          WITH BASE AS
                                          (
                                          SELECT
                                            A.*
                                            ,CASE WHEN  WNG_MOHQ_SUBPARM_CD = 1707 AND SUBPARM_MIN_OH_QTY > SFTY_STK_QTY
                                                    THEN SUBPARM_MIN_OH_QTY
                                                  ELSE 0 
                                              END AS CURRENT_BULK_DRIVEN_QTY
                                            
                                            ,CASE WHEN  WNG_MOHQ_SUBPARM_CD = 1707 AND SUBPARM_MIN_OH_QTY > SFTY_STK_QTY
                                                    THEN CURR_SYSMOHQ-NON_BULK_SYSMOHQ     
                                                  ELSE 0
                                              END AS CURR_BULK_INCREMENTAL
                                            
                                            ,CASE WHEN NEW_BULK_MIN_QTY = CURR_BULK_MIN_QTY 
                                                    THEN CURR_SYSMOHQ 
                                                  ELSE GREATEST(COALESCE(NON_BULK_SYSMOHQ,0), NEW_BULK_MIN_QTY ) 
                                              END AS NEW_SYSMOHQ
                                            
                                            ,GREATEST(NEW_BULK_MIN_QTY - NON_BULK_SYSMOHQ,0) AS NEW_BULK_INCREMENTAL
                                            ,CASE WHEN NEW_BULK_MIN_QTY = CURR_BULK_MIN_QTY 
                                                    THEN CURR_SYSMOHQ 
                                                  ELSE GREATEST( COALESCE(NON_BULK_SYSMOHQ,0), NEW_BULK_MIN_QTY ) 
                                              END - CURR_SYSMOHQ AS NEW_UNITS
                                            
                                          FROM (
                                                SELECT
                                                  A.SKU_NBR
                                                  ,A.SKU_CRT_DT
                                                  ,A.STR_NBR
                                                  ,CASE   WHEN G.SKU_NBR IS NULL THEN \'ADDED\'
                                                          WHEN B.SKU_NBR IS NULL THEN \'REMOVED\'
                                                          WHEN B.VOL_DISC_MIN_QTY<G.VOL_DISC_MIN_QTY THEN \'REDUCED\'
                                                          WHEN B.VOL_DISC_MIN_QTY>G.VOL_DISC_MIN_QTY THEN \'INCREASED\'
                                                          WHEN B.VOL_DISC_MIN_QTY=G.VOL_DISC_MIN_QTY THEN \'NO CHANGE\'
                                                          ELSE NULL 
                                                    END   AS STATUS
                                                  
                                                  ,MOHQ_PRST_QTY AS ADJ_MOHQ_PRST_SPI_QTY
                                                  ,A.CURR_RETL_AMT
                                                  ,IFNULL(MOHQ_PRST_QTY,0) AS MOHQ_PRST_QTY
                                                  ,IFNULL(MOHQ_AD_QTY,0) AS MOHQ_AD_QTY									-- Ad Min 1702
                                                  ,IFNULL(MOHQ_JLQ_QTY,0) AS MOHQ_JLQ_QTY								-- JLQ 1703
                                                  ,IFNULL(MOHQ_FLD_RQST_QTY,0)AS MOHQ_FLD_RQST_QTY					-- Field RQST 1704
                                                  ,IFNULL(MOHQ_WOS_MIN_QTY,0) AS MOHQ_WOS_MIN_QTY					-- WOS MIN 1705
                                                  ,IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0) AS MOHQ_SPPRT_NEW_ITEM_QTY	-- New Item 1706
                                                  ,IFNULL(MOHQ_BULK_DSPL_QTY,0) AS MOHQ_BULK_DSPL_QTY				-- Bulk 1707
                                                  ,IFNULL(MOHQ_NON_SPI_PRST_QTY,0) AS MOHQ_NON_SPI_PRST_QTY			-- Non SPI 1708
                                                  ,IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0) AS MOHQ_INV_SOLN_TEAM_QTY		-- Solution 1709
                                                  ,SUBPARM_MIN_OH_QTY
                                                  ,WNG_MOHQ_SUBPARM_CD
                                                  ,SFTY_STK_QTY
                                                  ,IFNULL(CMTD_MIN_MAP_QTY,0) + IFNULL( CMTD_MIN_XMER_QTY ,0) + IFNULL( CMTD_MIN_BULK_WALL_QTY ,0) + IFNULL(MOHQ_PRST_QTY,0) AS COM_MIN_QTY
                                          
                                                  -- ------------------------------------------CURRENT METRICS-------------------------------
                                                  ,MOHQ_BULK_DSPL_QTY AS CURR_BULK_MIN_QTY
                                                  ,GREATEST(SUBPARM_MIN_OH_QTY  ,SFTY_STK_QTY ) AS CURR_SYSMOHQ
                                          
                                          
                                                  ,GREATEST( IFNULL(CMTD_MIN_MAP_QTY,0) + IFNULL( CMTD_MIN_XMER_QTY ,0) + IFNULL( CMTD_MIN_BULK_WALL_QTY ,0) + IFNULL(MOHQ_PRST_QTY,0)  ---COMM MIN BUCKET
                                                            -- MOHQ BUCKET
                                                            ,IFNULL(MOHQ_AD_QTY,0) 								-- Ad Min 1702
                                                            ,IFNULL(MOHQ_JLQ_QTY,0)					-- JLQ 1703
                                                            ,IFNULL(MOHQ_FLD_RQST_QTY,0)					-- Field RQST 1704
                                                            ,IFNULL(MOHQ_WOS_MIN_QTY,0) 					-- WOS MIN 1705
                                                            ,IFNULL(MOHQ_SPPRT_NEW_ITEM_QTY,0) 	-- New Item 1706
                                                            --,IFNULL(MOHQ_BULK_DSPL_QTY,0) AS MOHQ_BULK_DSPL_QTY				-- Bulk 1707
                                                            ,IFNULL(MOHQ_NON_SPI_PRST_QTY,0) 			-- Non SPI 1708
                                                            ,IFNULL(MOHQ_INV_SOLN_TEAM_QTY,0) 		-- Solution 1709
                                                    
                                                            ,SFTY_STK_QTY -- SSU BUCKET                                                    
                                                            ) AS NON_BULK_SYSMOHQ
                                          
                                          
                                          
                                                  -- --------------------------------------------------NEW BULK METRICS ------------------------------------------------------------
                                                  ,G.VOL_DISC_MIN_QTY AS LW_BULK_PRICE_QTY
                                                  ,B.VOL_DISC_MIN_QTY AS NEW_BULK_PRICE_QTY
                                                  ,CASE WHEN (B.VOL_DISC_MIN_QTY * E.BULK_PRICE_MULTIPLIER) IS NULL  THEN 0 ELSE (B.VOL_DISC_MIN_QTY * E.BULK_PRICE_MULTIPLIER) END  AS NEW_BULK_MIN_QTY
                                          
                                        
                                                FROM  `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_PARM_DLY`  A
                                        
                                                LEFT JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_NEW_TIER_PRC_ALL` B
                                                  ON A.SKU_NBR = B.SKU_NBR
                                                  AND CAST(A.STR_NBR AS INT64) = CAST(B.LOC_NBR AS INT64)
                                        
                                                JOIN (SELECT  A.*  
                                                      FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A
                                                      JOIN (SELECT A.SKU_NBR, MAX(SKU_CRT_DT) AS SKU_CRT_DT FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A GROUP BY  1) B
                                                        ON A.SKU_NBR = B.SKU_NBR
                                                        AND A.SKU_CRT_DT = B.SKU_CRT_DT   
                                                      WHERE   A.SUB_DEPT_NBR IS NOT NULL 
                                                      ) C
                                                  ON A.SKU_NBR = C.SKU_NBR
                                                  AND A.SKU_CRT_DT = C.SKU_CRT_DT
                                        
                                                LEFT JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_MULT_EXC` E
                                                  ON C.SUB_DEPT_NBR  = CASE WHEN LENGTH(E.DEPT) = 2 THEN CONCAT(\'00\',E.DEPT) 
                                                                            WHEN LENGTH(E.DEPT) = 3 THEN CONCAT(\'0\', E.DEPT)
                                                                             ELSE E.DEPT END
                                                  AND C.CLASS_NBR = E.CLASS_NBR
                                                  AND C.SUB_CLASS_NBR = E.SC_NBR
                                        
                                                LEFT JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_SKU_EXC` F
                                                  ON cast(A.SKU_NBR as int64)= cast(F.SKU_NBR as int64)
                                        
                                                LEFT JOIN `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_LW_TIER_PRC_ALL` G
                                                  ON A.SKU_NBR = G.SKU_NBR
                                                  AND CAST(A.STR_NBR AS INT64) = CAST(G.LOC_NBR AS INT64)
                                        
                                                WHERE  1=1
                                                  AND (B.SKU_NBR IS NOT NULL OR G.SKU_NBR IS NOT NULL)
                                                  AND E.BULK_PRICE_INV_FEED = \'ON\'
                                                  AND F.SKU_NBR IS NULL
                                                  AND A.CAL_DT = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
                                                  AND A.RMETH_CD = 1
                                                  AND A.IPR_REPLE_IND = 1
                                                  AND ((A.SKU_VLCTY_CD NOT IN (\'D\',\'E\') AND A.DEPT_NBR NOT IN (25,28) ) OR A.DEPT_NBR IN (25,28))
                                                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25

                                                 ) A
                                                  )

                                                  SELECT A.*
                                                  
                                                  FROM BASE A
                                                  LEFT JOIN `pr-edw-views-thd.SCHN_INV.IPR_STR_SKU_VLCTY` B
                                                    ON A.SKU_NBR = B.SKU_NBR
                                                    AND CAST(A.STR_NBR AS INT64) = B. STR_NBR
                                                    
                                                  WHERE 1=1
                                                    AND ((B.SKU_VLCTY_CD NOT IN ('D','E') AND B.MER_DEPT_NBR NOT IN (25,28) ) OR B.MER_DEPT_NBR IN (25,28))

                                   ",
                                   use_legacy_sql = FALSE,
                                   max_pages = Inf,
                                   billing = project), 
 
 quiet = getOption( "bigrquery.quiet"), pause = 1
 
 )
 
 
 #Sys.sleep(300)               # 3
 
 
 invest <- query_exec(project = project,
                     dataset = dataset,
                     query = 
                       "
                      SELECT A.*
                     , C.CURRENT_OH_RET
                     , C.R8W_RET_SLS_2019
                     , D.R8W_RET_SLS_2018

                     FROM 
                             (SELECT
                             CASE WHEN SUBSTR(SUB_DEPT_NBR, 1, 1) = \'0\' THEN SUBSTR(SUB_DEPT_NBR, 2, 3)
                             ELSE SUB_DEPT_NBR
                             END AS DEPT
                             ,C.CLASS_NBR AS CLASS_NUMBER
                             ,C.CLASS_DESC AS CLASS_NAME
                             ,C.SUB_CLASS_NBR AS SUBCLASS
                             ,C.SUB_CLASS_DESC AS SUBCLASS_NAME
                             ,A.SKU_NBR AS SKU
                             ,COUNT(*) AS STORES_WITH_BULK_PRICE_QTY
                             ,SUM(CASE WHEN STATUS = \'ADDED\' THEN 1 ELSE 0 END) AS NEW_STORES_WITH_BULK_PRICE_QTY
                             ,SUM(CURR_SYSMOHQ*CAST(CURR_RETL_AMT AS FLOAT64)) AS  CURRENT_SYSMOHQ_DLRS
                             ,SUM(CURR_BULK_INCREMENTAL*CAST(CURR_RETL_AMT AS FLOAT64)) AS CURRENT_BULK_CONTRIBUTION_TO_SYSMOHQ           --changes
                             ,SUM(NEW_BULK_INCREMENTAL*CAST(CURR_RETL_AMT AS FLOAT64))  AS NEW_BULK_CONTRIBUTION_TO_SYSMOHQ                --changes
                             ,SUM(NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64)) AS NET_IMPACT_OF_BULK_CHANGES
                             ,SUM(CASE WHEN STATUS <> \'REMOVED\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END ) AS NET_IMPACT_OF_BULK_CHANGES_EFFECTIVE_THIS_WEEK
                             ,SUM(CASE WHEN STATUS =\'ADDED\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END ) AS NEW_STR_SKU_IMPACT
                             ,SUM(CASE WHEN STATUS =\'INCREASED\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END) AS HIGHER_BULK_PRICE_QTY_IMPACT
                             ,SUM(CASE WHEN STATUS =\'REMOVED\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END) AS REMOVED_STR_SKU_IMPACT
                             ,SUM(CASE WHEN STATUS =\'REDUCED\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END) AS LOWER_BULK_PRICE_QTY_IMPACT
                             ,SUM(CASE WHEN STATUS =\'NO CHANGE\' THEN NEW_UNITS*CAST(CURR_RETL_AMT AS FLOAT64) ELSE 0 END) AS OTHER_IMPACT
                             
                             FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_DIAG` A
                             
                            JOIN 
                                      (SELECT  A.*  
                                       FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A
                                       
                                      JOIN (SELECT A.SKU_NBR, MAX(SKU_CRT_DT) AS SKU_CRT_DT FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A GROUP BY  1) B
                                       ON A.SKU_NBR = B.SKU_NBR
                                       AND A.SKU_CRT_DT = B.SKU_CRT_DT   
                           
                                      WHERE   A.SUB_DEPT_NBR IS NOT NULL 
                                      ) C
           
                                      ON A.SKU_NBR = C.SKU_NBR
                                      AND A.SKU_CRT_DT = C.SKU_CRT_DT
                             
                            GROUP BY 1,2,3,4,5,6)  A
                     
                     LEFT JOIN 
                             (SELECT A.SKU_NBR AS SKU
                              , C.PARTITIONDATE
                              , CASE WHEN SUM(C.OH_QTY*C.CURR_RETL_AMT) IS NULL THEN 0 ELSE SUM(C.OH_QTY*C.CURR_RETL_AMT) END AS CURRENT_OH_RET
                              , CASE WHEN SUM(C.R8W_AVG_SLS_AMT) IS NULL THEN 0 ELSE (SUM(C.R8W_AVG_SLS_AMT)*8) END AS R8W_RET_SLS_2019
                               
                               FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_DIAG`  A
                               LEFT JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_WKLY`  C
                               ON A.SKU_NBR = C.SKU_NBR
                                AND A.STR_NBR = C.STR_NBR
                               GROUP BY 1,2   

                                HAVING PARTITIONDATE = 
                              (SELECT DISTINCT ENDDATE 
                                   FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` 
                                   WHERE fiscalweek = (SELECT MAX(fiscalweek) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE = (SELECT MAX(ENDDATE) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE < CURRENT_DATE))
                                   AND fiscalyear = (SELECT MAX(fiscalyear) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE = (SELECT MAX(ENDDATE) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE < CURRENT_DATE))
                                           )
                             
                             ) C
                             ON A.SKU = C.SKU

                     LEFT JOIN 
                          (SELECT A.SKU_NBR AS SKU
                          , C.PARTITIONDATE
                          , CASE WHEN SUM(C.R8W_AVG_SLS_AMT) IS NULL THEN 0 ELSE (SUM(C.R8W_AVG_SLS_AMT)*8) END AS R8W_RET_SLS_2018
                           
                           FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_MIN_DIAG`  A
                           LEFT JOIN `pr-edw-views-thd.SCHN_INV.STR_SKU_ACINV_WKLY`  C
                           ON A.SKU_NBR = C.SKU_NBR
                                AND A.STR_NBR = C.STR_NBR                           
                           GROUP BY 1,2

                            HAVING PARTITIONDATE = 
                              (SELECT DISTINCT ENDDATE 
                                   FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` 
                                   WHERE fiscalweek = (SELECT MAX(fiscalweek) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE = (SELECT MAX(ENDDATE) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE < CURRENT_DATE))
                                   AND fiscalyear = (SELECT MAX(fiscalyear)-1 FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE = (SELECT MAX(ENDDATE) FROM `pr-edw-views-thd.SCHN_FCST.FISCALCALENDAR` WHERE ENDDATE < CURRENT_DATE))
                                           )
                           
                           )  D                                                  
                          ON A.SKU = D.SKU
                     
                     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
                     
                     
                     ",
                     use_legacy_sql = FALSE,
                     max_pages = Inf,
                     billing = project)
 
 
 # 4
 
 
 removed <- query_exec(project = project,
                      dataset = dataset,
                      query = 
                      "
                      SELECT
                        A.SKU_CRT_DT
                        ,CASE WHEN SUBSTR(SUB_DEPT_NBR, 1, 1) = \'0\' THEN SUBSTR(SUB_DEPT_NBR, 2, 3)
                              ELSE SUB_DEPT_NBR
                          END AS DEPT
                        ,C.CLASS_NBR AS CLASS_NUMBER
                        ,C.CLASS_DESC AS CLASS_NAME
                        ,C.SUB_CLASS_NBR AS SUBCLASS
                        ,C.SUB_CLASS_DESC AS SUBCLASS_NAME
                        ,A.SKU_NBR AS SKU
                      
                      
                      FROM (SELECT DISTINCT SKU_NBR, SKU_CRT_DT FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_LW_TIER_PRC_ALL`) A
                      LEFT JOIN (SELECT DISTINCT SKU_NBR, SKU_CRT_DT FROM `analytics-supplychain-thd.IXL0858_ANALYTICS.BULK_NEW_TIER_PRC_ALL`) B
                        ON A.SKU_NBR = B.SKU_NBR
                        AND A.SKU_CRT_DT = B.SKU_CRT_DT
                      
                      JOIN (SELECT  A.*  
                            FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A
                            JOIN (SELECT A.SKU_NBR, MAX(SKU_CRT_DT) AS SKU_CRT_DT FROM `pr-edw-views-thd.SHARED.SKU_HIER_FD` A GROUP BY  1) B
                            ON A.SKU_NBR = B.SKU_NBR
                            AND A.SKU_CRT_DT = B.SKU_CRT_DT   
                            WHERE   A.SUB_DEPT_NBR IS NOT NULL 
                            ) C
                        ON A.SKU_NBR = C.SKU_NBR
                        AND A.SKU_CRT_DT = C.SKU_CRT_DT
                      
                      WHERE B.SKU_NBR IS NULL
                      ",
                      use_legacy_sql = FALSE,
                      max_pages = Inf,
                      billing = project)
 
 
 
 write.table(invest,"//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/BULK Min Procedure (DO NOT DELETE - NIrjhar Raina)/Investment Report/MON_investment_report.txt",quote = FALSE,sep = "\t",row.names = FALSE)
 
 write.table(removed,"//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/BULK Min Procedure (DO NOT DELETE - NIrjhar Raina)/Removed SKUs/removed_skus.txt",quote =FALSE,sep = "\t",row.names = FALSE)
 
 
 