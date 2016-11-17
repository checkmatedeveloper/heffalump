import requests
import shutil
import xml.etree.ElementTree as ElementTree
import os
import hashlib
import traceback
import gmail
import MailGun

from  UCRSSDB import UCRSSDB
from Levy_DB import Levy_Db

from db_connection import DbConnection
import datetime
import pytz
import IntegrationTools


#create ourselves a nice little database connection
conn = DbConnection().connection
UCRSSDB = UCRSSDB(conn)
levy_db = Levy_Db(conn, None)

VENUE_UID = 201



def updateEvent(eventUid, newName, newSubtitle, newDescription, eventImageUrl):
    oldName, oldSubtitle, oldDescription = UCRSSDB.findEventsXVenue(eventUid)[0]

    eventXVenueUid = UCRSSDB.findEventXVenueUid(eventUid)

    print "Updating: " + str(eventUid)

    if oldName != newName:
        IntegrationTools.confirmUpdate(levy_db, VENUE_UID, 'setup', 'events_x_venues', 'event_name', eventXVenueUid, oldName, newName, False, None)
    if oldSubtitle != newSubtitle: 
        IntegrationTools.confirmUpdate(levy_db, VENUE_UID, 'setup', 'events_x_venues', 'subtitle', eventXVenueUid, oldSubtitle, newSubtitle, False, None)
    if oldDescription != newDescription:
        IntegrationTools.confirmUpdate(levy_db, VENUE_UID, 'setup', 'events_x_venues', 'description', eventXVenueUid, oldDescription, newDescription, False, None)
    
    if needNewImage(eventUid, eventImageUrl):
        print "Inserting new image row into purgatory"
        IntegrationTools.confirmImage(levy_db, VENUE_UID, 'media', 'images', 'events_x_venues', eventXVenueUid, eventImageUrl)
     
   

def downloadImage(imageUrl, imagePath):
    r = requests.get(imageUrl, stream=True)
    if r.status_code == 200:
        with open(imagePath, 'w+') as imageFile:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, imageFile)

def hashImage(imagePath):
    return hashlib.md5(open(imagePath).read()).hexdigest()

#returns true if a new image should be inserted
def needNewImage(eventUid, eventImageUrl):
    print "Comparing Images"
    eventXVenueUid = UCRSSDB.findEventXVenueUid(eventUid)

    currentImageFilePath = '/data/media/201/images/events_x_venues/' + str(eventXVenueUid) + ".png"
          
    print currentImageFilePath

    if os.path.isfile(currentImageFilePath):
        
        #hash the current image
        currentImageHash = hashImage(currentImageFilePath)
        
        #download temp file
        tempImagePath = '/data/media/201/images/events_x_venues/temp.png'
        downloadImage(eventImageUrl, tempImagePath)

        #hash temp image
        tempImageHash = hashImage(tempImagePath)

        print "Comparing Image Hashes: " + currentImageHash + " vs " + tempImageHash
        if(tempImageHash == currentImageHash):
            os.remove(tempImagePath)   
            print "New Image not needed"
            return False
        else:   
            os.remove(tempImagePath)
            print "We need a new image"
            return True
                
    else:
        print "We don't have an image... yet!"
        return True #if we don't have an image then we need to get one

def convertDateStringToUTCDateTime(dateString):

    print "Converting: " + dateString + " to datetime object"
    eventDate = datetime.datetime.strptime(dateString, "%a, %d %b %Y %H:%M:%S CST")

    centralTimeZone = pytz.timezone("America/Chicago")
    eventDate = centralTimeZone.localize(eventDate, is_dst=None)
    utcEventDate = eventDate.astimezone(pytz.utc)

    return utcEventDate

def dateTimeToString(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")

#lets go get the UC's event data from their goofy rss feed
eventsXMLString = requests.get('http://www.unitedcenter.com/rss/events.aspx')


#Remove the XML Byte Order Marker (BOM)
eventsXMLStringNoBom = eventsXMLString.text[3:]

eventsXML = ElementTree.fromstring(eventsXMLStringNoBom.encode('utf-8'))

issues = []

def logIssue(levy_event_name, issue_message):
    issueMessage = "<p>Levy Event - " + levy_event_name + ":</p><ul><li>" + issue_message + "</li></ul>"
    issues.append(issueMessage)

#run through each event and integrate it's data into our database
for eventXML in eventsXML.findall('.//item'):
    try:
        eventDateString = eventXML.find('.//event_date').text

        eventName = eventXML.find('.//title').text
        eventSubtitle = eventXML.find('.//subtitle').text
        eventDescription = eventXML.find('.//more_info').text

        eventImageUrl = eventXML.findall('.//{http://search.yahoo.com/mrss/}content')[1].attrib['url']

        #print "EVENT NAME: " + eventName
        #print "EVENT SUBTITLE: " + eventSubtitle
        #print "EVENT DESCRIPTION: " + eventDescription
        #print "EVENT IMAGE URL: " + eventImageUrl

        if "-" in eventDateString:
         #   print "Multi Event"

            startDateString, endDateString = [ i.strip() for i in eventDateString.split("-", 1) ] #this line splits the datestring on the '-' and stips the whitespace from both strings
            
            startDate = convertDateStringToUTCDateTime(startDateString)

            endDate = convertDateStringToUTCDateTime(endDateString)

         #   print "Operative UTC Date Range: " + dateTimeToString(startDate) + " to " + dateTimeToString(endDate)

            eventUids = UCRSSDB.findEventsByDateRange(startDate, endDate)

            if eventUids is None or len(eventUids) == 0:
                logIssue(eventName, "Multi-Event had no matching parametric events.  \n Event Date Range: " + dateTimeToString(startDate) + "  to  " + dateTimeToString(endDate))

         #   print "Updating " + str(len(eventUids)) + " events:"
         #   print "      " + str(eventUids)
            for eventUid in eventUids:
                
                updateEvent(eventUid[0], eventName, eventSubtitle, eventDescription, eventImageUrl)
           
     
        else:
            print "Single Event"
            print "Local Time: " + eventDateString

            # evnetDateString example: Fri, 12 Jun 2015 19:30:00 CST
           
            utcEventDate = convertDateStringToUTCDateTime(eventDateString)

            utcEventDateString = dateTimeToString(utcEventDate)

            print "UTC date: " + str(utcEventDateString)

            eventUids = UCRSSDB.findEventByDate(utcEventDateString)

            print str(eventUids)

            if len(eventUids) > 1:
                print "I have multiple events, I don't know what to do"
                logIssues(eventName, "Event had multiple matching parametric events.  \n Event Date: " + dateTimeToString(utcEventDate))
            elif len(eventUids) == 0:
                print "I have 0 events, I don't know what to do"
                logIssue(eventName, "Event had no matching parametric event.  \n Event Date: " + dateTimeToString(utcEventDate))
            else:
                eventUid = eventUids[0][0]

                print "Updating event " + str(eventUid)
                updateEvent(eventUid, eventName, eventSubtitle, eventDescription, eventImageUrl)
          
                print '\n\n'
          

    except:
        logIssue("Event Level EXCEPTION", traceback.format_exc())


if len(issues) == 0:
    emailMessage = "Levy Event RSS Integration was successful!"
else:

    emailMessage = "<h3>Levy Event RSS Integration finished with errors:</h3>"

    for issue in issues:
        emailMessage += issue

#gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "uc_rss_integration@parametricdining.com", "nate@checkmatetablet.com", "UC RSS integration summary", emailMessage, "html")
MailGun.sendEmail("mail@bypassmobile.copm", "nate@checkmatetablet.com", "UC RSS integration summary", emailMessage)
