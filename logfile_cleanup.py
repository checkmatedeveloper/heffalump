#! /usr/bin/python

import os
import datetime

# Set logfile path depending on the server
if os.getenv('Chkm8WorkerServerType') == 'PROD':
    logpath = '/data/serverlogs/rabbitmq_workers_logs'
else:
    logpath = '/home/ec2-user/serverlogs/rabbitmq_workers_logs'
# Set the number of days to keep
num_of_days_to_keep = 7 
# Set base file names to be exempt from deletion
exempt_file_basenames = ['order_payment_convert_to_sale', 'order_modified_void_closed_sale', 'order_preauth_process_credit_card']
today_dt = datetime.datetime.now().date()
days_to_keep_td = datetime.timedelta(num_of_days_to_keep)
for (dirpath, dirnames, filenames) in os.walk(logpath):
    for filename in filenames:
        if not any([filename.startswith(x) for x in exempt_file_basenames]):
            start_date = None
            end_date = None
            split_filename = filename.rsplit('.')
            if split_filename[-1] == 'log':
                start_date = split_filename[-3]
                end_date = split_filename[-2]
            else:
                start_date = split_filename[-1]
            try:
                start_date_dt = datetime.datetime.strptime(start_date,'%Y%m%d').date()
            except Exception:
                continue
            if today_dt - start_date_dt > days_to_keep_td:
                os.remove(os.path.join(dirpath,filename))
                print "Deleted", os.path.join(dirpath,filename)
