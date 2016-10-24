from db_connection import DbConnection
import gmail
import HipChat
import pytz, datetime
from pytz import timezone


#TODO on upload to prod, put in the real emails
notify = ['nate@parametricdining.com']

conn = DbConnection().connection

cursor = conn.cursor()

eventToUpdate = cursor.execute('SELECT \
                                    events.id, \
                                    event_name, \
                                    event_date \
                                FROM setup.events \
                                JOIN setup.events_x_venues ON events.id = events_x_venues.event_uid \
                                JOIN setup.events_x_settings ON events.id = events_x_settings.event_uid \
                                WHERE events.venue_uid = 422 \
                                    AND event_setting_uid IN (1, 3) AND (value = "true" OR value = "1") \
                                    AND NOW() BETWEEN DATE_SUB(event_date, INTERVAL 90 MINUTE) AND event_date \
                                    AND events.id NOT IN (SELECT event_uid FROM setup.food_and_alcohol_opened_events) \
                                GROUP by events.id \
                                LIMIT 1')

if eventToUpdate > 0:

    eventToUpdate = cursor.fetchone()

    eventUid, eventName, eventDate = eventToUpdate

    success = True

    eventSettingsUpdated = cursor.execute('UPDATE setup.events_x_settings \
                                           SET VALUE = 0 \
                                           WHERE event_setting_uid IN (1, 3) AND event_uid = %s', (eventUid))

    if eventSettingsUpdated == 0:
        success = False


    eventsOpened = cursor.execute('INSERT INTO setup.food_and_alcohol_opened_events( \
                                        event_uid, \
                                        opened_at, \
                                        created_at \
                                   ) VALUES ( \
                                        %s, \
                                        NOW(), \
                                        NOW())',
                                   (eventUid))

    if eventsOpened == 0:
        success = False


    if success:
        conn.commit()

        emailBody = "Food and Beverage ordering for the following  event has been opened: \n\n"

        emailBody = emailBody + eventName + '\n'

        cursor.execute('SELECT local_timezone_long \
                        FROM setup.venues \
                        WHERE id = 422')
        localTimeZone = cursor.fetchone()[0]

        localDateTime = eventDate.replace(tzinfo=pytz.utc).astimezone(timezone(localTimeZone)) 

        emailBody = emailBody + localDateTime.strftime('%B  %-d, %Y %-I:%M %p')

     

        for email in notify:
            gmail.sendGmail('tech@parametricdining.com', 'fkTUfbmv2YVy', 'tech@parametricdining.com', email, 'Event Opened', emailBody, emailBody)


        print HipChat.sendMessage("Food and Beverage ordering open for event " + str(eventUid), "FandBCron", 447878, 'purple')

    else:
        print "Everything failed"

else:
    print "Nothing to do"
