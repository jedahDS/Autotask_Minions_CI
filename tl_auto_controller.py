import sys
import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint 
import pandas
from datetime import date
from datetime import datetime
import datetime as dt
import calendar
import math
import tl_helper_functions as helper
import PNP_DailyVidTempJob_LEVT as daily_levt_pacing
import FEPConsTbl_Imp_Daily_Update_LEVT as fep_cons_levt
import PNP_DailyVidTempJob_CS as daily_cs_pacing
import FEPConsTbl_Imp_Daily_Update_CS as fep_cs
import Y2YFEPCons_frTemp2_wRepeatsCSV_Tableau as fep_cons_csv
import DailyMTDCSxTitleUpdate as cs_mtd_daily
import DailyMTDLEVTxTitleUpdate as levt_mtd_daily
import DailyMTDOmAllxTitleUpdate as omall_mtd_daily
import CNET_Monthly_Temp_Tbl_Update_IMP as cnet_temp
import CNET_GS_2_Tabl_CSV_IMP as cnet2csv
import CAN_xPartner_PriorMonth as can_part_pri
import LateNightCSVWrite as colbert_csv


# Connection declaration
print("Minions Starting")

conn=pyodbc.connect('DSN=Impala_64;UID=ttlee;PWD=KB8AMD_Cn$', autocommit=True)

data_cursor = conn.cursor()

# Daily portion ############################################################################################################

# Start Daily Vid KPI Table ###################################################################################################
#  PNP_DailyVidTempJob_LEVT

print("Start PNP_DailyVidTempJob_LEVT")

daily_levt_pacing.main(data_cursor)

print("End of PNP_DailyVidTempJob_LEVT")


# Start CS Daily Vid KPI Table ####################################################################################S

print("Start PNP_DailyVidTempJob_CS")

daily_cs_pacing.main(data_cursor)

print("End of PNP_DailyVidTempJob_CS")


# Start of FEP Consumption LEVT temp  ###############################################################################################
# Start FEPConsTbl_Imp_Daily_Update_LEVT

print("Start FEPConsTbl_Imp_Daily_Update_LEVT")

fep_cons_levt.main(data_cursor)

print("End of FEPConsTbl_Imp_Daily_Update_LEVT")


# Start FEP Consumption CS temp table ###################################################################
# FEPConsTbl_Imp_Daily_Update_CS.py

print("Start FEPConsTbl_Imp_Daily_Update_CS")

fep_cs.main(data_cursor)

print("End FEPConsTbl_Imp_Daily_Update_CS")


#  Write Y2Y FEP to CSV

# Start Y2YFEPCons_frTemp2_wRepeatsCSV_Tableau script  #################################################

print("Start Y2YFEPCons_frTemp2_wRepeatsCSV_Tableau")

fep_cons_csv.main(conn)

print("End Y2YFEPCons_frTemp2_wRepeatsCSV_Tableau")


# Start DailyMTDCSxTitleUpdate script  #################################################

print("Start DailyMTDCSxTitleUpdate")

cs_mtd_daily.main(data_cursor)

print("End DailyMTDCSxTitleUpdate")


# Start DailyMTDLEVTxTitleUpdate script  #################################################

print("Start DailyMTDLEVTxTitleUpdate")

levt_mtd_daily.main(data_cursor)

print("End DailyMTDLEVTxTitleUpdate")


# Start DailyMTDOmAllxTitleUpdate script  #################################################

print("Start DailyMTDOmAllxTitleUpdate")

omall_mtd_daily.main(data_cursor)

print("End DailyMTDOmAllxTitleUpdate")



# Monthly portion ###############################################################################################

# Individual scripts without begining of month filter have filter to only run at the beginning of the month.  May consider bringing up to cover these in the future

# Add to CNET Monthly Temp table: ###########################################
# CNET_Monthly_Temp_Tbl_Update_IMP.py

print("Start CNET_Monthly_Temp_Tbl_Update_IMP")

cnet_temp.main(data_cursor)

print("End CNET_Monthly_Temp_Tbl_Update_IMP")

# CNET_GS_2_Tabl_CSV_IMP.py

print("Start CNET_GS_2_Tabl_CSV_IMP")

cnet2csv.main(data_cursor, conn)

print("End CNET_GS_2_Tabl_CSV_IMP")

# Below scripts do not have check for beginning of month, so checking for date here
# if 1st of month, execute monthly block
today_dt = date.today()
today_day_of_month = today_dt.day

# change this back to 1 after confirming this works
if today_day_of_month == 1:

	# CAN_xPartner_PriorMonth.py
	print("Start CAN_xPartner_PriorMonth")
	can_part_pri.main(data_cursor, conn)
	print("End CAN_xPartner_PriorMonth")

	# LateNightCSVWrite.py
	print("Start LateNightCSVWrite")
	colbert_csv.main(data_cursor, conn)
	print("End LateNightCSVWrite")
