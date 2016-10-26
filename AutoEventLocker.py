from db_connection import DbConnection
import sys
import HipChat
import traceback

try:
    conn = DbConnection().connection
    
    cursor = conn.cursor()

    cursor.execute('''
                    SELECT venue_uid FROM integrations.venues_levy
                    WHERE is_active = 1
                   ''')
    venueUids = cursor.fetchall()

    for venueUid in venueUids:
        venueUid = venueUid[0]
        
        print "Autolocking events for: " + str(venueUid)

        cursor.execute('''SELECT events.id FROM setup.events 
                        LEFT JOIN setup.event_controls ON event_controls.event_uid = events.id 
                        WHERE venue_uid = %s 
                        AND event_date BETWEEN date_sub(NOW(), INTERVAL 24 HOUR) AND NOW()
                        AND (is_locked = 0 OR is_locked is null)
                        AND event_type_uid != 11;''', (venueUid))

        unlockedEvents = cursor.fetchall()
        if len(unlockedEvents) == 0:
            HipChat.sendMessage("The Venue Locked all of yesterday's events!", "AutoLocker", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_GREEN)

        for event in unlockedEvents:
            eventUid = event[0]
            HipChat.sendMessage("The Venue failed to lock event " + str(eventUid) + ".  Locking for them...",  "AutoLocker", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_YELLOW)
            lockCursor = conn.cursor()
            lockCursor.execute("INSERT INTO setup.event_controls ( \
                                    event_uid, is_locked, locking_employee_uid, locked_at) \
                                VALUES (%s, 1, 1, NOW()) \
                                ON DUPLICATE KEY UPDATE is_locked = 1, locking_employee_uid = 1, locked_at = NOW()", (eventUid))

            conn.commit() 


except Exception as e:
    tb = traceback.format_exc()
    HipChat.sendMessage("Auto Lock Event Script Crashed: " + str(tb), "AutoLocker", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_RED)
    
