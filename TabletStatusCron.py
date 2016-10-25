from db_connection import DbConnection
import TabletStatusMailer
import requests
import traceback
from config import CheckMateConfig

try:
    conn = DbConnection().connection

    cursor = conn.cursor()

    # get all of the venues that have tablets
    cursor.execute("SELECT DISTINCT venue_uid FROM tablets.venues_x_tablet")

    for venueRow in cursor.fetchall():
        venueUid = venueRow[0]

        #for each venue find the events between where the current time is within the 'seconds_before_event' interval.
        #I.E. are we within 12 hours of the event
        cursor.execute("SELECT events.id AS event_id FROM setup.events \
                        JOIN setup.tablet_report_controls ON events.venue_uid = tablet_report_controls.venue_uid \
                        WHERE events.venue_uid = %s \
                        AND event_date BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL seconds_before_event SECOND) \
                        AND tablet_alert_sent = 0 \
                        ORDER BY event_date  ASC", (str(venueUid)))

        #for each of these events send a tablet status alert email
        for eventRow in cursor.fetchall():
            eventUid = eventRow[0]
            if TabletStatusMailer.sendStatusEmail(venueUid, eventUid) == True:
                #assuming everything went well with the email update the event row to mark the tablet status email sent
                cursor.execute("UPDATE setup.events SET tablet_alert_sent = 1 WHERE id = %s", (eventUid))
                conn.commit()
except Exception, e:
        print "I caught the exception!!!"

        checkmateconfig = CheckMateConfig()
        auth_token = checkmateconfig.HIPCHAT_AUTH_TOKEN
        room_id = checkmateconfig.HIPCHAT_WORKERS_ROOM_ID

        '''
         hipchat_dict = {'auth_token':'987ce42cc059f3a404e18c9f9c9642',
                         'room_id':'518943',
                         'from':'TabletStatCron',
                         'message':'Something went wrong in TabletStatusCron: ' + str(e) + " - " + traceback.format_exc(),
                         'notify':'1',
                         'color':'red'}
        '''

        hipchat_dict = {'auth_token': auth_token,
                        'room_id': room_id,
                        'from':'TabletStatCron',
                        'message':'Something went wrong in TabletStatusCron: ' + str(e) + " - " + traceback.format_exc(),
                        'notify':'1',
                        'color':'red'}

        print hipchat_dict
        resp = requests.post('http://api.hipchat.com/v1/rooms/message', params=hipchat_dict)
        print resp
