#Training Event Cron

from db_connection import DbConnection
from TrainingEventDB import TrainingEventDB
import HipChat
from config import CheckMateConfig
import redis

checkmateconfig = CheckMateConfig()
host = checkmateconfig.REDIS_ORDERS_HOST
port = checkmateconfig.REDIS_ORDERS_PORT
db = checkmateconfig.REDIS_ORDERS_DB
password = checkmateconfig.REDIS_ORDERS_PASSWORD
redisInstance = redis.Redis(host, port, db, password)

for key in redisInstance.keys('*:orders_by*'):
    redisInstance.delete(key)

conn = DbConnection().connection
dbCore = TrainingEventDB(conn)

trainingEvents = dbCore.getTrainingEvents()

HipChat.sendMessage("Moving " + str(len(trainingEvents)) + " training events", "Training", 447878, "purple")

for eventUid, venueUid in trainingEvents:

    print "Event UID: " + str(eventUid) + "  Venue UID: " + str(venueUid)
    
    #move the event to today
    dbCore.moveEventToToday(eventUid)

    #clear the orders
    dbCore.clearEventOrders(eventUid)

    #clear the messages
    dbCore.clearEventMessages(eventUid)

    #clear the activities
    dbCore.clearEventActivities(eventUid)

    #restore the unit_x_patrons
    dbCore.insertUnitXPatrons(venueUid)

    #restore the base shinfo
    dbCore.restoreBaseShinfo(venueUid)

    #clear the event shinfo
    dbCore.clearEventInfo(eventUid)  #--- WARNING DO NOT UNCOMMENT THIS UNTIL YOU ARE 100% SURE WE HAVE ALL OF BECKY'S CONFIGS 
                                         #CAPTURED IN THE BASE SHINFO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    dbCore.restoreSuiteAssignments(eventUid, venueUid)
   
    dbCore.createEventPreorders(venueUid, eventUid)
   
    dbCore.restoreEmployeeAssignments(eventUid, venueUid) 

    dbCore.restorePoints(venueUid, eventUid)

HipChat.sendMessage("Training Events have been moved and reset.", "Training", 447878, "purple")
