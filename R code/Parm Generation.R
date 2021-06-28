library(bigrquery)
library(data.table)
library(bigQueryR)
library(stringr)
library(XML) 
library(tidyverse)
#devtools::install_github("hadley/tidyverse")

setwd("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/Seasonal ASL (DO NOT DELETE - Nirjhar Raina)/Parm Upload/")


project = "analytics-supplychain-thd"
dataset = "SSNL_ASL_PROD"


upload_2 <- query_exec(project = project,
                       #dataset = dataset,
                       #destination_table = "IXL0858_ANALYTICS.JLQ_UPLOAD",
                       #write_disposition = "WRITE_TRUNCATE",
                       query =     "
                        SELECT 
                           Trumping_Level
                          , Type
                          , Store_Group
                          , Volume_Id
                          , Velocity_Id
                          , SKU
                          , SKU_Grp
                          , Store
                          , Parm_code
                          , CASE WHEN Parm_Value > 9999 THEN 9999 ELSE Parm_Value end as Parm_Value
                          
                          , Eff_Begin_date
                          , Eff_End_date
                          , Start_Fscl_Wk_nbr
                          , End_Fscl_Wk_nbr
                          , Param_Desc --CHANGE DATE!
                          , OOTL_Reason_Code
                        FROM `analytics-supplychain-thd.SSNL_ASL_PROD.PARM_UPLOAD_WKLY` A
                       
                        JOIN (
                          SELECT
                            A.CAL_DT, A.FSCL_WK_NBR
                          FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` A
                          JOIN (SELECT FSCL_YR_WK_KEY_VAL FROM `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` A  WHERE CAL_DT = CURRENT_DATE('America/New_York')) B
                            ON A.FSCL_YR_WK_KEY_VAL = B.FSCL_YR_WK_KEY_VAL
                          WHERE  A.DAY_OF_WK_NBR = 1
                          GROUP BY 1,2
                          ) F  
                       ON A.UPLOAD_DT = F.CAL_DT
                        --WHERE TESTING_FIELD IS NULL

                       "     ,
                       use_legacy_sql = FALSE,
                       max_pages = Inf,
                       billing = project)





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
            str_c("//at2a5/vol4/DEPTS/TRAFFIC/PROJECTS/SCD/Supply Chain Analytics/Projects/Seasonal ASL (DO NOT DELETE - Nirjhar Raina)/Parm Upload/ssn_sfty_stk_soln_min_upload_",str_trim(str_replace_all(Sys.Date(),pattern = ":",replacement = "-")),"_",i,".csv"), row.names=FALSE, quote=FALSE, na="")
  upload<-upload[-1:-write]
  counter<-counter-write
  i<- i+1
}
