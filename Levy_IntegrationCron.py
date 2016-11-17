import sys
import os
import time
import traceback
import gmail
import MailGun

import Levy_FillTempTables

import Levy_IntegrateCustomers
import Levy_IntegrateVenuesXEmployees
import Levy_IntegrateMenu
import Levy_IntegrateUnits
import Levy_IntegrateEvents
import Levy_IntegrateTaxRates
import Levy_IntegratePackages

from db_connection import DbConnection
from Levy_DB import Levy_Db

from config import CheckMateConfig
import redis

import HipChat

INTEGRATION_HIPCHAT_ROOM = 1066556;

checkmateconfig = CheckMateConfig()
host = checkmateconfig.REDIS_CACHE_HOST
port = checkmateconfig.REDIS_CACHE_PORT
db = checkmateconfig.REDIS_CACHE_DB
password = checkmateconfig.REDIS_CACHE_PASSWORD
redisInstance = redis.Redis(host, port, db, password)

conn = DbConnection().connection
dbCore = Levy_Db(conn, redisInstance)

def sendErrorEmail(body):        
    recipients = ['nate@parametricdining.com', 'jonathan@parametric.com']
    for to in recipients:
        #gmail.sendGmail('tech@parametricdining.com', 'fkTUfbmv2YVy', 'tech@parametricdining.com', to, 'Daily Integration Crashed', 'Daily Integration Crashed: ' + body, 'enable html to read this')
        MailGun.sendEmail('mail@bypassmobile.com', to, 'Daily Integration Crashed', 'Daily Integration Crashed: ' + body) 

def findIntegrationFile(dumpDir, fileNameRoot):
    for f in os.listdir(dumpDir):
        if f.startswith(fileNameRoot):
            return f

dbCore.addLogRow("STARTING INTEGRATION CRON")
dbCore.addLogRow("LOCK AQUIRED")

#find the scripts
args = sys.argv
args.pop(0) #remove the script name from the args list

try:
    FILE_PATH = args[0]
    
except:
    HipChat.sendMessage("(failed) \n Cron failed, no path to integration files supplied", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")
    dbCore.addLogRow("ERROR: No path to integration files supplies")
    dbCore.addLogRow("LOCK RELEASED")
    sendErrorEmail("No path to integration files supplied")
    sys.exit()

skipTempFiles = False
try:
    skipTempFilesRaw = args[0]
    if skipTempFilesRaw == '--skip-temp-files':
        skipTempFiles = True
except:
    print "No big deal we are going to default to NOT skipping temp files"

if skipTempFiles == False:
    #truncate all of the levy file names
    integrationFiles = os.listdir(FILE_PATH)


    dbCore.addLogRow("Filling Temp Tables")
    #make temp tables for menu sync

    success, missingFiles, sqlErrorRows = Levy_FillTempTables.makeTempTables(FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_EMPLOYEE_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_SUITE_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_CUSTOMER_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_EVENT_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_ITEM_MASTER_')), 
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_ITEM_PRICE_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_TAX_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_SERVICE_CHARGE_')),
                                     FILE_PATH + "/" + str(findIntegrationFile(FILE_PATH, 'OMS_PACKAGE_DEFINITION_')),
                                     dbCore)

    if success == False:
        HipChat.sendMessage("The following integration files were missing: " + ", ".join(str(x) for x in missingFiles), "InteCron", INTEGRATION_HIPCHAT_ROOM, "yellow")
     
    if len(sqlErrorRows) > 0:
        HipChat.sendMessage("There were  errors when inserting into temp levy tables. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in sqlErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "yellow")



#clear out any 
dbCore.purgePurgatory()
 

#run our scripts

integrationError = False

try:

    success, eventsErrorRows, eventsErrorVenues = Levy_IntegrateEvents.integrate(dbCore)
    if success == False:
        integrationError = True
        HipChat.sendMessage("There were  errors when integrating EVENTS. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in eventsErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")


    success, customersErrorRows, customerErrorVenues = Levy_IntegrateCustomers.integrate(dbCore)
    if success == False:   
        integrationError = True
        HipChat.sendMessage("There were  errors when integrating CUSTOMERS. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in customersErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")
        
#    success, venuesXEmployeesErrorRows, venuesXEmployeesErrorVenues = Levy_IntegrateVenuesXEmployees.integrate(dbCore)
#    if success == False:
#        integrationError = True
#        HipChat.sendMessage("There were  errors when integrating EMPLOYEES. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in venuesXEmployeesErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")

    success, unitsErrorRows, unitsErrorVenues = Levy_IntegrateUnits.integrate(dbCore)
    if success == False:
        integrationError = True
        HipChat.sendMessage("There were  errors when integrating UNITS. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in unitsErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")


    success, taxRatesErrorRows, taxRatesErrorVenues, = Levy_IntegrateTaxRates.integrate(dbCore)
    if success == False:
        integrationError = True
        HipChat.sendMessage("There were errors when integrating TAX RATES.  Use \n SELECT * FROM integrations.integration_actions WHERE id in ("+ ','.join(str(x) for x in taxRatesErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")
    

    success, menuErrorRows, menuErrorVenues = Levy_IntegrateMenu.integrate(dbCore)
    if success == False:
        integrationError = True
        HipChat.sendMessage("There were  errors when integrating MENUS. Use \n SELECT * FROM integrations.integration_actions WHERE id in (" + ','.join(str(x) for x in menuErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")    


    success, packageErrorRows, packageErrorVenues = Levy_IntegratePackages.integrate(dbCore)
    if success == False:
        integrationError = True
        HipChat.sendMessage("There were errors when integrating PACKAGE DEFINITIONS.  Use \n SELECT * FROM integrations.integration_actions WHERE id in ("+ ','.join(str(x) for x in packageErrorRows) + ")\n for more info", "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")



 



except:
    tb = traceback.format_exc()
    
    HipChat.sendMessage("(failed) \n An unexptected error has occuered, YOUR SCRIPT CRASHED!!!! \n STACKTRACE: " + tb, "IntCron", INTEGRATION_HIPCHAT_ROOM, "red")
    sys.exit()

if integrationError:
    HipChat.sendMessage("Daily Integrations finished with errors.  Partial Integrations avaliable for approval https://controlcenter.paywithcheckmate.com/index.php#manageUpdates", "IntCron", INTEGRATION_HIPCHAT_ROOM, "yellow")
else:
    
    HipChat.sendMessage("Daily Integrations ready for approval https://controlcenter.paywithcheckmate.com/index.php#manageUpdates", "IntCron", INTEGRATION_HIPCHAT_ROOM, "green")

dbCore.addLogRow("LOCK RELEASED")


#Send Summary Email
summaryInfo = [
                {'data':'Customers', 'pointer_table':'patrons', 'errors':customerErrorVenues},
               # {'data':'Employees', 'pointer_table':'employees', 'errors':venuesXEmployeesErrorVenues},
                {'data':'Events', 'pointer_table':'events', 'errors':eventsErrorVenues},
                {'data':'Menu Items', 'pointer_table':'menu_items', 'errors':menuErrorVenues},
                {'data':'Menu X Menu Items', 'pointer_table':'menu_x_menu_items', 'errors':None},
                {'data':'Units', 'pointer_table':'units', 'errors':unitsErrorVenues},
                {'data':'Unit X Patrons', 'pointer_table':'unit_x_patrons', 'errors':None},
                {'data':'Venues X Suite Holders', 'pointer_table':'venues_x_suite_holders', 'errors':None},
                {'data':'Events X Venues', 'pointer_table':'events_x_venues', 'errors':None},
                {'data':'Taxes', 'pointer_table':'menu_taxes', 'errors':taxRatesErrorRows},
                {'data':'Packages', 'pointer_table':'menu_packages_x_items', 'errors':packageErrorRows},
                {'data':'Pars', 'pointer_table':'par_menu_items', 'errors':None}
              ]

recipients = ['nate@parametricdining.com', 'jonathan@parametricdining.com']


venues = dbCore.getAllLevyIntegrationVenues();

emailBody = 'Integration has completed and purgatory is awaiting approval, please go here to approve: <a href="https://controlcenter.paywithcheckmate.com/index.php#manageUpdates"> Approve </a>'

allVenuesCount = 0

for venue in venues:
    venue_uid = venue[0]

    dbCore.markVenueIntegrated(venue_uid)

    if venue_uid == 309:
        continue
    
    venueRowCount = dbCore.countAllVenuePurgatoryRows(venue_uid)

    if venueRowCount < 1:
        continue
 
    emailBody = emailBody + "<h3>Venue " + str(venue_uid) + "</h3>"
    for info in summaryInfo:
        
        totalCount = 0;

        addRows = dbCore.countAddPurgatoryRows(venue_uid, info['pointer_table'])
        editRows = dbCore.countEditPurgatoryRows(venue_uid, info['pointer_table'])
        deactivateRows = dbCore.countDeactivatePurgatoryRows(venue_uid, info['pointer_table'])
        reactivateRows = dbCore.countReactivatePurgatoryRows(venue_uid, info['pointer_table'])
        removeRows = dbCore.countRemovePurgatoryRows(venue_uid, info['pointer_table'])

        totalCount = addRows + editRows + deactivateRows + reactivateRows + removeRows
        if info['errors'] is not None:
            totalCount += info['errors'].count(venue_uid)


        if totalCount < 1:
            continue

        allVenuesCount += totalCount

        emailBody = emailBody + "<p>" + info['data'] + ":\n" 
        emailBody = emailBody + str(dbCore.countAddPurgatoryRows(venue_uid, info['pointer_table'])) + " new, "
        emailBody = emailBody + str(dbCore.countEditPurgatoryRows(venue_uid, info['pointer_table'])) + " updated,"
        emailBody = emailBody + str(dbCore.countDeactivatePurgatoryRows(venue_uid, info['pointer_table'])) + " deactivated, "
        emailBody = emailBody + str(dbCore.countReactivatePurgatoryRows(venue_uid, info['pointer_table'])) + " reactivated, "
        emailBody = emailBody + str(dbCore.countRemovePurgatoryRows(venue_uid, info['pointer_table'])) + " removed, "
        if info['errors'] is not None:
            errorCount = info['errors'].count(venue_uid)
            emailBody = emailBody + str(errorCount) + " errors</p>"


#os.system("/usr/bin/python2.6 /home/ec2-user/crons/repo/Levy_PurgatoryMonitorCron.py")

print "All Venues Count: " + str(allVenuesCount)

if allVenuesCount != 0:
    print "Sending Emails!!!"
    for to in recipients:
        #gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "integration@parametricdining.com", to, 'Daily Integration Report', emailBody, 'You need to enable HTML emails to view this message')
        MailGun.sendEmail("mail@bypassmobile.com", to, 'Daily Integration Report', emailBody)
else:
    print "Nothing to notify Nate and Jonathan about, not sending email"
