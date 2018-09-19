import sys
import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint 
import pandas
import pandas.io
import google.cloud.bigquery
import pandas_gbq
from datetime import date
from datetime import datetime
import datetime as dt
import calendar
import math
from impala.util import as_pandas


def get_filter(filter_name, client):
	print("getting " + filter_name)
	filter_file = client.open("Filters_and_Bandaids_Data")
	filter_sheet = filter_file.get_worksheet(1)
	filter_df = pandas.DataFrame(filter_sheet.get_all_records())
	filter_val = filter_df.loc[filter_df['filter_name'] == filter_name]['filter_value'].values[0]

	return filter_val


def get_filter_cs(filter_name, client):
	print("getting " + filter_name)
	filter_file = client.open("Filters_and_Bandaids_Data")
	filter_sheet = filter_file.get_worksheet(3)
	filter_df = pandas.DataFrame(filter_sheet.get_all_records())
	filter_val = filter_df.loc[filter_df['filter_name'] == filter_name]['filter_value'].values[0]

	return filter_val

def get_filter_csbq(filter_name, client):
  print("getting " + filter_name)
  filter_file = client.open("Filters_and_Bandaids_Data")
  filter_sheet = filter_file.worksheet('Clickstream_BQ')
  filter_df = pandas.DataFrame(filter_sheet.get_all_records())
  filter_val = filter_df.loc[filter_df['filter_name'] == filter_name]['filter_value'].values[0]

  return filter_val

def get_filter_om(filter_name, client):
	print("getting " + filter_name)
	filter_file = client.open("Filters_and_Bandaids_Data")
	filter_sheet = filter_file.get_worksheet(4)
	filter_df = pandas.DataFrame(filter_sheet.get_all_records())
	filter_val = filter_df.loc[filter_df['filter_name'] == filter_name]['filter_value'].values[0]

	return filter_val

# function to increment the month query dates
# Calculates new dates based on the current start of the date range (beginning_of_mth)
# Return list within beginning and end of query
def incr_mth_query_dates(beginning_of_mth, last_date):
  end_of_this_mth = date(beginning_of_mth.year, beginning_of_mth.month, calendar.monthrange(beginning_of_mth.year, beginning_of_mth.month)[1])
  new_start = end_of_this_mth + dt.timedelta(days=1)
  new_end = date(new_start.year, new_start.month, calendar.monthrange(new_start.year, new_start.month)[1])

  # Set end of query range to last date of the query (final_dt) if it exceeds the last date
  if new_end > last_date:
    new_end = last_date

  date_list = [new_start, new_end]
  date_str_list = [new_start.isoformat(), new_end.isoformat()]

  incr_list = [date_list, date_str_list]

  return incr_list


def add_to_tbl(main_logic, insert_clause_atl, create_clause_atl, this_iter_cnt, d_cursor):
  # if this is the first insert, create the table first

  insert_script_template = """
  -- create or insert clause below
  {}
  -- main sql logic below
  {}
  """

  if this_iter_cnt == 1:
    print(create_clause_atl)
    run_sql = insert_script_template.format(create_clause_atl, main_logic)
  else:
    print(insert_clause_atl)
    run_sql = insert_script_template.format(insert_clause_atl, main_logic)

  sql_runner(run_sql, d_cursor)

  print ('insert into table finished')


# one-off SQL executor; doesn't check for increment
def sql_runner(main_logic_sql, data_cursor):

  data_cursor.execute(main_logic_sql)
  data_cursor.commit()

  print ('SQL finished')


# Get the latest date of the table
def getLatestDate(date_field, table_name, data_cursor):

  # Pull max date
  max_dt_sql_template = "select max({}) from {}"
  max_dt_sql = max_dt_sql_template.format(date_field, table_name)

  print(max_dt_sql)
  data_cursor.execute(max_dt_sql)

  # Put results into dataframe
  max_dt_df=as_pandas(data_cursor)
  
  # Convert result into date format.  Most dates in Impala are stored as strings
  max_dt_str = max_dt_df.iloc[0][0]
  max_dt_datetime = datetime.strptime(max_dt_str , '%Y-%m-%d')
  max_dt = date(max_dt_datetime.year, max_dt_datetime.month, max_dt_datetime.day)
  print("max date of " + table_name + ": " + max_dt.isoformat())
  
  return max_dt

def getLatestDateBQStr(date_field, table_name):

	# Pull max date
	max_dt_sql_template = "select max({}) from {}"
	max_dt_sql = max_dt_sql_template.format(date_field, table_name)

	# Set up BQ connection
	projectid = "i-dss-ent-data"

	print(max_dt_sql)
	# Put results into dataframe
	max_dt_df = pandas.read_gbq(max_dt_sql, projectid, dialect='standard')

	# Convert result into date format.  Can't write dates to temp table, so this is if date is stored as string
	max_dt_str = max_dt_df.iloc[0][0]
	max_dt_datetime = datetime.strptime(max_dt_str , '%Y-%m-%d')
	max_dt = date(max_dt_datetime.year, max_dt_datetime.month, max_dt_datetime.day)
	print("max date of " + table_name + ": " + max_dt.isoformat())

	return max_dt


def getLatestMonth(date_field, table_name, data_cursor):

  # Pull max date
  max_dt_sql_template = "select max({}) from {}"
  max_dt_sql = max_dt_sql_template.format(date_field, table_name)

  print(max_dt_sql)
  data_cursor.execute(max_dt_sql)

  # Put results into dataframe
  max_dt_df=as_pandas(data_cursor)
  
  # Convert result into date format.  Most dates in Impala are stored as strings
  max_dt_month_id = max_dt_df.iloc[0][0]

  print("max monthid " + table_name + ": " + str(max_dt_month_id))
  
  return max_dt_month_id


def monthid_getyear(this_monthid):
  year_int = this_monthid/100

  return year_int


def monthid_getmonth(this_monthid):
  year_int = this_monthid/100
  this_month = this_monthid - (year_int * 100)

  return this_month



def month_id_sub_month(this_monthid, months_2_sub):
  # if subtracting month from this_monthid month will be <= 0, then subtract the year by 1

  # If subtract month > current month, then the ceiling of the difference will be >= 1
  # If subtracting the month lands us right at Dec (mod subtract month - curr month = 0), then subtract another year
  this_month = monthid_getmonth(this_monthid)
  monthdiff = months_2_sub - this_month
  this_year = this_monthid/100
  result_year = int(this_year - math.ceil((monthdiff * 1.0)/12) - (monthdiff % 12 == 0))

  # if monthdiff < 0 (exceeded current month), then take that negative number, and substract it from 12
  if monthdiff >= 0:
      result_month = 12 - (monthdiff % 12)
  else:
      result_month = this_month - months_2_sub

  result_monthid = (result_year * 100) + result_month

  return result_monthid

# Pull max effective date prior to date
def pull_max_eff_prior_to(prior_to_date, d_cursor):
  # Pull max effective date before start of month
  max_eff_query = """
  select max(effective_date) from dw_vw_live.networks_archive where effective_date <= '{}'
  """
  max_eff_query_w_param = max_eff_query.format(prior_to_date)
  print(max_eff_query_w_param)
  d_cursor.execute(max_eff_query_w_param)
  max_eff_query_df=as_pandas(d_cursor)
  datetime_max_eff_str = max_eff_query_df.iloc[0][0]
  max_eff_datetime = datetime.strptime(datetime_max_eff_str , '%Y-%m-%d')
  max_eff_dt = date(max_eff_datetime.year, max_eff_datetime.month, max_eff_datetime.day)
  print("max_eff_dt iso: " + max_eff_dt.isoformat())
  
  return max_eff_dt


# Define Function to generate Email on Success/Failure (SM)
def send_email(user, pwd, recipient, subject, body):
  import smtplib

  gmail_user = user
  gmail_pwd = pwd
  FROM = user
  TO = recipient if type(recipient) is list else [recipient]
  SUBJECT = subject
  TEXT = body

  # Prepare actual message
  message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
  """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
  try:
  		# server = smtplib.SMTP("smtp.gmail.com", 587)
  		server = smtplib.SMTP("smtp.cnet.com", 25)
  		server.ehlo()
  		# server.starttls()
  		# server.login(gmail_user, gmail_pwd)
  		server.sendmail(FROM, TO, message)
  		server.close()
  		print 'successfully sent an mail'
  except:
  		print "failed to send mail"

  print('Email Finished')
