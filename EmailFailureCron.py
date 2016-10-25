# Handle Failed Receipts Cron
import os
import sys
import pytz
import requests
import datetime
from datetime import timedelta
from db_connection import DbConnection
import events_db
from config import CheckMateConfig
import xlsxwriter

conn = DbConnection().connection
eventsDB = events_db.EventsDb( conn )

checkmateconfig = CheckMateConfig();
env_var = os.getenv('Chkm8WorkerServerType')
mailgun_api_key = checkmateconfig.MAILGUN_API_KEY
if env_var == "PROD":
    mailgun_host = "https://api.mailgun.net/v3/{0}/messages".format(checkmateconfig.MAILGUN_PROD_GENERAL_DOMAIN)
else:
    mailgun_host = "https://api.mailgun.net/v3/{0}/messages".format(checkmateconfig.MAILGUN_DEV_DOMAIN)

# get all failures from the current day
#failed = eventsDB.getFailedReceipts()

venueUids = sys.argv[1:]
for venue in venueUids:

    # get all failures from the current day
    failed = eventsDB.getFailedReceipts(venue)

    # If none failed, doesnt send
    if len( failed ) == 0:
        continue   
 
    # Find the notification emails
    contacts = eventsDB.getFailedEmailContacts(venue)
    
    # No contacts, dont send
    if len( contacts ) == 0:
        continue

    # Venue info for subject / body
    venue_name = eventsDB.getVenueInfo(venue)

    # Email Message
    body = '{0} Emailed Receipts failed to send today at {1}.  Please see the attached report for more detail.  If the addresses in the report look valid but the emails continue to fail please contact the individual and have them add receipts@bypassmobile.com to their "safe list" to increase deliverability.'.format(len(failed), venue_name);
    message = "<html><body style='font-family: Geneva,Verdana,Arial,Helvetica; font-size: smaller'><div style='width: 500px; height: 100%;'><table border=0; cellpadding=0; width=100% style='font-family: Geneva,Verdana,Arial,Helvetica;'><tr style='font-size: 12pt'><td style='text-align: left'>Hello,</td></tr><tr><td>&nbsp;</td></tr><tr style='font-size: 12pt'><td>&nbsp;&nbsp;&nbsp;{0}</td></tr><tr><td>&nbsp;</td></tr><tr style='font-size: 12pt'><td>Thank you,</td></tr><tr style='font-size: 12pt'><td>Parametric Staff</td></tr></table><br /></body></html>".format(body);

    # Create Excel Doc
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    date = str(yesterday).replace (" ", "_");
    filename = 'Parametric_Failed_Receipts_'+date+'.xlsx';
    
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()
   
    # Excel Formats
    header_format = workbook.add_format()
    header_format.set_bold()

    worksheet.set_column('A:G', 20)
 
    # Excel Header
    worksheet.write('A1', '{0} {1}: Parametric Failed Receipts'.format(venue, venue_name))
    worksheet.write('A2', str(yesterday))
    
    # Excel curr pos
    x = 0;
    y = 3;

    # Excel Report Headers
    headers = ['Event Name', 'Event Start', 'Patron', 'Unit', 'Order Uid', 'Email', 'Attempted At']
    header_key = ['event_name', 'event_start', 'patron', 'unit', 'order_uid', 'email', 'attempted_at']

    for col in headers:
        worksheet.write( y, x, col)
        x = x + 1

    # Excel Report Body
    for row in failed:
        x = 0 
        y = y + 1
        for col in header_key:
            worksheet.write( y, x, row[col] )
            x = x + 1

    workbook.close()

    # Email to Venue Contacts
    for contact in contacts:
    
        if contact is not None:

                r = requests.post(
                    mailgun_host,
                    auth=("api", mailgun_api_key),
                    files=[("attachment", open(filename))],
                    data={"from": "<receipts@bypassmobile.com>",
                        "to": contact,
                        "subject": "{0} {1}: Parametric Emailed Receipt Failure".format(venue, venue_name),
                        "html": message
                    }
                )

    os.remove(filename)


