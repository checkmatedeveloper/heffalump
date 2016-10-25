import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback
import pytz, datetime

def integrate(dbCore):
    dbCore.addLogRow("Integrating Events") 
    tempEvents = dbCore.getTempEvents()

    #event row = (0:levy abreviated venue, 1: event number, 2: event name, 3, event_type, 4: event_date, 5: canceled

    success = True
    errorLogRows = []
    errorVenues = []
    eventNumber = ""
   
    for row in tempEvents:
        try:
            venue_uid = dbCore.getVenueUid(row[0])
        except:
            continue
        try:
            eventNumber = row[1]

            levyEvents = dbCore.getLevyEvents(venue_uid, row[1])
        
            eventTypeId = dbCore.getEventTypeUid(row[3])

            if len(levyEvents) == 0:
                #we don't have a mapping for this event yet
               
                
                #lets see if we know about the event though
                # a matching venue_uid + event_name + event_type + event_datetime   
                
                events = dbCore.findEvents(venue_uid, row[2], eventTypeId, row[4])
                if len(events) == 1:
                    dbCore.insertLevyEvent(row[1], events[0][0], venue_uid)
                    
                else:
                    
                    insert_uuid = uuid.uuid4()
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'events', 'venue_uid', venue_uid, False, row[1], auto_apply = True)
                    
                   # localTime = datetime.datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")                    
                    localTimeZone = dbCore.getVenueTimeZone(venue_uid)
                    localTime = localTimeZone.localize(row[4], is_dst=None)
                    utcTime = localTime.astimezone(pytz.utc)
                   # print str(row[1]) + " " + str(utcTime)
                    


                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'events', 'event_date', str(utcTime), False, row[1], auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'events', 'event_type_uid', eventTypeId, False, row[1], auto_apply = True)
                    
                    
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'events_x_venues', 'event_name', row[2], False, row[1], auto_apply = True) 
            elif len(levyEvents) == 1:
                dbCore.addLogRow("levyEvents[0][2]: " + str(levyEvents[0][2]))
                event = dbCore.getEvent(levyEvents[0][2])
                dbCore.addLogRow("EVENT: " + str(event))
                event_date = event[2]
                event_type_uid = event[3]
                event_name = event[4]

                #probably could comapre event[x] without saving it to a var but I wanted a *little* readbility

                localTimeZone = dbCore.getVenueTimeZone(venue_uid)
                localTime = localTimeZone.localize(row[4], is_dst=None)
                utcTime = localTime.astimezone(pytz.utc)

                
                print str(event_date) + " vs " + utcTime.strftime("%Y-%m-%d %H:%M:%S")
                if str(event_date) != utcTime.strftime("%Y-%m-%d %H:%M:%S"):
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'events', 'event_date', event[0], event_date, str(utcTime), False)
                
                if event_type_uid != eventTypeId: #the second one is looked up above, stupid naming scheme on my part
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'events', 'event_type_uid', event[0], event_type_uid, eventTypeId, False)

                if event_name != row[2]:
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'events_x_venues', 'event_name', event[5], event_name, row[2], False) #event[5] = events_x_venues uid 
            else:
                #uh oh
                dbCore.addLogRow("Multiple rows in events_levy... I don't know what to do")
        except:
            success = False
            tb = traceback.format_exc()
            logRowId = dbCore.addLogRow("Error processing event row (event_number= " + str(eventNumber) + ") Stacktrace: " + tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venue_uid)

    return success, errorLogRows, errorVenues;


            




