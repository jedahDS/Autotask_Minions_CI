import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas
import pandas.io
import google.cloud.bigquery
import pandas_gbq
from datetime import date
from datetime import datetime
import datetime as dt
import calendar
import tl_helper_functions as helper


def main():

  # use creds to create a client to interact with the Google Drive API
  scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
  creds = ServiceAccountCredentials.from_json_keyfile_name('SampleVY_Example.json', scope)
  client = gspread.authorize(creds)

  # This is the table final result will be stored or updated
  result_table_name = "temp_tl.additive_cs_vid_day2"

  body = "Done.  Please check {}".format(result_table_name)

  # Check if the table is already up to date
  # Get the max date of result_table_name
  # If the max date is already through yesterday, then exit.
  # Otherwise, update for dates >= max date + 1

  # Check if table has already been updated through latest date
  date_field_name = 'day_dt'

  max_updated_dt = helper.getLatestDateBQStr(date_field_name, result_table_name)

  yesterday_dt = date.today() - dt.timedelta(days=1)

  # if last date of table is already yesterday, table is update to date.  exit program
  if max_updated_dt == yesterday_dt:
    print("Table has already been updated.  Exiting")
  else:
    # For monitoring purposes.  Check if max date is the day before yesterday.
    # If so, note in body of email, there might've been an issue

    day_before_yest = yesterday_dt - dt.timedelta(days=1)

    if max_updated_dt < day_before_yest:
      body = "Done.  Possible issue earlier. Please check {}".format(result_table_name)

    update_start_dt = max_updated_dt + dt.timedelta(days=1)

    update_start_dt_str = update_start_dt.isoformat()

    print("Start from " + update_start_dt_str)


    combined_insert_template = """
    with bq_vid_level1 as (
      SELECT
      cast(day_dt as string) as day_dt,
      v1_brand_nm,
      v9_rsid,
      v15_user_status_cd,
      v25_video_title_nm,
      v31_mpx_reference_guid,
      v32_video_partner_cd,
      v69_registration_id_nbr as reg_cookie,
      v_behind_paywall_ind,
      length_in_seconds,
      primary_category_nm,
      category_level1_nm,
      video_series_nm,
      video_content_owner_nm,
      device_os_nm,
      ms.device_type_nm,
      mobile_device_nm,
      cast(date(video_air_dt_UT, "America/New_York") as string) as video_air_dt_ET,
      video_title,
      media_start_cnt,
      hb_video_init_cnt,
      duration_sec_qty,
      ad_duration_sec_qty,
      {} as new_sec_calc,
      {} as new_ad_sec_calc,
      {} as region,
      {} as mvpd_provider,
      {} as gojivcaca,
      {} as t_platform,
      {} as ep_type
      FROM dw_vw.aa_cs_video_detail_day ms
      left outer JOIN dw_vw.mpx_video_content vc
      ON (vc.src_video_guid=ms.v31_mpx_reference_guid
        and vc.src_video_guid is not null
      )
      where
      day_dt >= '{}'
      and coalesce(v32_video_partner_cd, 'null') not in ('msn|caca', 'gojinews|caca', 'yahoo|caca', 'dailymotion|caca', 'roku|caca', '-')
      and coalesce(v9_rsid, 'null') <> 'gojiicbsca'
      and coalesce(v1_brand_nm, 'null') not like 'gojinews_site_%'
      and coalesce(v1_brand_nm, 'null') not like '%sports_site_%'
    ),  
    bq_vid_level2 as (
    select
    day_dt,
    region,
    gojivcaca,
    t_platform,
    mvpd_provider,
    video_title,
    v25_video_title_nm,
    v31_mpx_reference_guid,
    v32_video_partner_cd,
    ep_type,
    video_air_dt_ET,
    primary_category_nm,
    category_level1_nm,
    video_content_owner_nm,
    length_in_seconds,
    media_start_cnt,
    hb_video_init_cnt,
    new_sec_calc,
    new_ad_sec_calc,
    {} as cont_ad_sec,
    -- when VOD, measure 10% qualifier, when Live, this become 10min qualifier
    {} as gt10str,
    {} as caca_partner,
    {} as vid_form,
    {} as t_behind_paywall,
    {} as sub_or_not,
    {} as t_device, 
    {} as t_series,
    {} as discard_or_not
    from bq_vid_level1
    ),
    bq_vid_level3 as (
    select
    day_dt,
    region,
    gojivcaca,
    t_device,
    t_platform,
    mvpd_provider,
    caca_partner,
    t_series,
    ep_type,
    vid_form,
    t_behind_paywall,
    sub_or_not,
    discard_or_not,
    new_sec_calc,
    new_ad_sec_calc,
    primary_category_nm,
    category_level1_nm,
    video_content_owner_nm,
    length_in_seconds,
    video_air_dt_ET,
    gt10str,
    cont_ad_sec,
    --media_start_cnt,
    {} as media_start_cnt,
    {} as t_daypart
    from bq_vid_level2
    )
    select
    day_dt,
    region,
    gojivcaca,
    t_device,
    t_platform,
    mvpd_provider,
    caca_partner,
    t_series,
    ep_type,
    vid_form,
    t_behind_paywall,
    sub_or_not,
    discard_or_not,
    t_daypart,
    gt10str,
    {} as sea_no,
    {} as ent_or_not,
    sum(media_start_cnt) strms,
    sum(cast(new_sec_calc as float64)/60) duration_min_qty,
    sum(cast(cont_ad_sec as float64)/60) cont_ad_min_qty,
    sum(case when media_start_cnt > 0 then cast(length_in_seconds as float64)/60 else 0 end) avail_length
    from bq_vid_level3
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    """


    # Set up BQ connection
    projectid = "i-dss-ent-data"

    # Get filters
    new_sec_calc = helper.get_filter_csbq('new_sec_calc', client)
    new_ad_sec_calc = helper.get_filter_csbq('new_ad_sec_calc', client)
    region = helper.get_filter_csbq('region', client)
    mvpd_provider = helper.get_filter_csbq('mvpd_provider', client)
    gojivcaca = helper.get_filter_csbq('gojivcaca', client)
    t_platform = helper.get_filter_csbq('t_platform', client)
    cont_ad_sec = helper.get_filter_csbq('cont_ad_sec', client)
    ep_type = helper.get_filter_csbq('ep_type', client)
    gt10str = helper.get_filter_csbq('gt10str', client)
    caca_partner = helper.get_filter_csbq('caca_partner', client)
    vid_form = helper.get_filter_csbq('vid_form', client)
    t_behind_paywall = helper.get_filter_csbq('t_behind_paywall', client)
    sub_or_not = helper.get_filter_csbq('sub_or_not', client)
    t_device = helper.get_filter_csbq('t_device', client)
    t_series = helper.get_filter_csbq('t_series', client)
    discard_or_not = helper.get_filter_csbq('discard_or_not', client)
    media_start_cnt = helper.get_filter_csbq('media_start_cnt', client)
    t_daypart = helper.get_filter_csbq('t_daypart', client)
    sea_no = helper.get_filter_csbq('sea_no', client)
    ent_or_not = helper.get_filter_csbq('ent_or_not', client)


    combined_insert_script = combined_insert_template.format(new_sec_calc, new_ad_sec_calc, region, mvpd_provider, gojivcaca, t_platform, ep_type, update_start_dt_str,
    cont_ad_sec, gt10str, caca_partner, vid_form, t_behind_paywall, sub_or_not, t_device, t_series, discard_or_not, media_start_cnt, t_daypart, sea_no, ent_or_not)

    # pull data into a data frame
    print("Reading data into data frame")
    curr_df = pandas.read_gbq(combined_insert_script, projectid, dialect='standard')

    # insert into result table, append if it exists, create if it doesn't
    print("Writing data into table")
    pandas_gbq.to_gbq(curr_df, result_table_name, projectid, if_exists='append')


    from_address = 'jomama@pluckyduck.com'
    to_address = 'ben_dover@kyj.com'
    subject = "Your script is finished"

    helper.send_email(from_address,'',to_address, subject, body)

  print("Script finished.  Exiting")

main()
