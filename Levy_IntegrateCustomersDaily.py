import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import sys
import traceback
import os
import gmail
import MailGun
from IntegrationEmailerDb import EmailerDb

try:

    DAILY_CUSTOMER_FILE_LOCATION = "/data/integration_files/daily_customer"

    print str(os.listdir(DAILY_CUSTOMER_FILE_LOCATION))

    conn = DbConnection().connection
    dbCore = Levy_Db(conn, None)

    successBody = ""

    for dailyCustomerFileName in os.listdir(DAILY_CUSTOMER_FILE_LOCATION):
        if os.path.isfile(DAILY_CUSTOMER_FILE_LOCATION + "/" + dailyCustomerFileName):
            
            if len( open( DAILY_CUSTOMER_FILE_LOCATION + "/" + dailyCustomerFileName ).readlines()) == 0: #read line should be ok here because the file is small
                os.remove(DAILY_CUSTOMER_FILE_LOCATION + "/" + dailyCustomerFileName) # the file is empty, just delete it and move on
                continue

            with open(DAILY_CUSTOMER_FILE_LOCATION + "/" + dailyCustomerFileName, "rb") as dailyCustomerFile:
                
                reader = CSVUtils.parseCSVFile(dailyCustomerFile)
            
                for row in reader:
                    
                    print str(row) 
        
                    customerNumber = row[0]
                    customerName = row[1]
                    entityCode = row[2]
                    suiteNumber = row[3]
                   
                    try: 
                        venueUid = dbCore.getVenueUid(entityCode)                
                    except:
                        continue #venue is not supported for integration yet

                    levyPatron = dbCore.getLevyPatron(customerNumber)

                    if levyPatron is None:
                        print "This is a new customer"

                        dbCore.insertDailyPatron(venueUid, customerNumber, customerName)
                        successBody += "Added New Patron: " + customerName + "</br>"

                    else:
                        print "Existing Customer"
                         

                    #by this point we should have a pretty firm guarentee that there is a patron so lets handle
                    #assigning him to suites
                    added = dbCore.insertDailySuiteAssignment(customerNumber, venueUid, suiteNumber)
                    if added:
                        successBody += "Added " + customerName + " to Suite " + str(suiteNumber) + "</br>"

                #email success
                if len(successBody) > 0:
                    conn = DbConnection().connection
                    emailerDb = EmailerDb(conn)
                    emailAddresses = emailerDb.getEmailAddresses(venueUid)
                    for address in emailAddresses:
                        #gmail.sendGmail("tech@parametricdining.com", 
                        #                "fkTUfbmv2YVy", 
                        #                "integration@parametricdining.com", 
                        #                address[0], 
                        #                'Instant Patron Integration Applied', 
                        #                "Instant Patron Integration has completed.  The following changes have been applied: </br>" + successBody,  'please open this in an HTML compatable email client')
    `                   MailGun.sendEmail("mail@bypassmobile.com", address[0], 'Instant Patron Integration Applied', "Instant Patron Integration has completed.  The following changes have been applied: </br>" + successBody)
                    else:
                        print "Email body too short to send"

            os.remove(DAILY_CUSTOMER_FILE_LOCATION + "/" + dailyCustomerFileName)
        else:
            print "Not a file"

except:
     tb = traceback.format_exc()
    # gmail.sendGmail("tech@parametricdining.com", 
    #                 "fkTUfbmv2YVy", 
    #                 "integration@parametricdining.com", 
    #                 'nate@parametricdining.com', 
    #                 'ERROR: Instant Patron Integration', "An error occured during instant patron integration: " + tb,                      'please open this in an HTML compatable email client')
    MailGun.sendEmail("mail@bypassmobile.com", "nate@parametricdining.com", "ERROR: Instant Patron Integration", "An error occured during instant patron integration: " + tb)


