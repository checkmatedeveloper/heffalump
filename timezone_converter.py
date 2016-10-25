import pytz
import datetime

def convert_utc_to_local(utc_dt, local_tz_str):
    #Expects a string of the form:
    # utc_str = '2014-01-06 03:49:48'
    # local_tz_str = 'US/Central'
    #utc_dt = datetime.datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S')
    local_tz = pytz.timezone(local_tz_str)
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_dt
