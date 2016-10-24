import os
import sys
import datetime
from config import CheckMateConfig

checkmateconfig = CheckMateConfig();

day_of_week = datetime.datetime.today().strftime('%A')
logfile = checkmateconfig.BATCH_REPORT_LOG_PATH.format( day_of_week )

# erase existing logfile
if os.path.isfile( logfile ):

    file_obj = open(logfile, "rw+")
    file_obj.truncate()

else:
    
    file_obj = open(logfile, "w+")


file_obj.write( datetime.datetime.today().strftime('%A %m %d, %Y') )
file_obj.write('\n')

file_obj.close()
    
