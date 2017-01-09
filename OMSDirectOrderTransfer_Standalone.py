import sys
from db_connection import DbConnection
import HipChat
import csv
import traceback
import paramiko
from OMSDirectOrderTransferDB import OMSDirectOrderTransferDB
from OMSTransferLogger import OrderTransferLogger
import pytz
from pytz import timezone
import datetime
import locale
import gmail
import MailGun

EXPORT_FILE_PATH = "/data/integration_files/OMS_DIRECT_EXPORTS/"

def transferPALAC():
    eventDate = dbCore.getEventDate(eventUid)
    datestamp = eventDate.strftime('%Y%m%d')

    palacUsersHeader, palacUsers = dbCore.getPALACUsers()
    makeCSVFile('PALAC-MSTR-USERS', palacUsersHeader, palacUsers)

    palacRoleAssocHeader, palacRoleAssoc = dbCore.getPALACRoleAssoc()
    makeCSVFile('PALAC-MSTR-ROLEASSOC', palacRoleAssocHeader, palacRoleAssoc)

    creditUsageHeader, creditUsage = dbCore.getPALACCreditUsage(eventUid)
    creditUsageFileName = 'CREDITUSAGE_' + entityCode + "_" + levyEventNumber
    makeCSVFile(creditUsageFileName, creditUsageHeader, creditUsage)
    
    palacPatronHeader, palacPatrons = dbCore.getPALACPatrons()
    makeCSVFile('PALAC-MSTR-CUSTOMERS', palacPatronHeader, palacPatrons)

    palacUnitsHeader, palacUnits = dbCore.getPALACUnits()
    makeCSVFile('PALAC-MSTR-SUITES', palacUnitsHeader, palacUnits)

    palacItemsHeader, palacItems = dbCore.getPALACItems()
    makeCSVFile('PALAC-MSTR-ITEMS', palacItemsHeader, palacItems)

    host = 'files.palacenet.com'
    port = 22
    username = 'bypasssms-ftp' 
    password = 'Fush3foo8Xe2eil@eelu'

    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    
    sftp.put(EXPORT_FILE_PATH + 'PALAC-MSTR-USERS.csv', 'MSTR-USERS_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'PALAC-MSTR-ROLEASSOC.csv', 'MSTR-ROLEASSOC_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'MSTR-ROLES.csv', 'MSTR-ROLES_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + orderHeaderFileName + '.csv', orderHeaderFileName + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + orderDetailsFileName + '.csv', orderDetailsFileName + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + orderPaymentsFileName + '.csv', orderPaymentsFileName + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + creditUsageFileName + '.csv', creditUsageFileName + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'MSTR-PAYMENTTYPES' + '.csv', 'MSTR-PAYMENTTYPES' + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'PALAC-MSTR-CUSTOMERS' + '.csv', 'MSTR-CUSTOMERS' + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'PALAC-MSTR-SUITES' + '.csv', 'MSTR-SUITES' + '_%s.csv' % (datestamp))
    sftp.put(EXPORT_FILE_PATH + 'PALAC-MSTR-ITEMS' + '.csv', 'MSTR-ITEMS' + '_%s.csv' % (datestamp))


    sftp.close()
    
    transport.close()

    HipChat.sendMessage("PALAC Export Completed!!!", "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_GREEN)


def convertUTCToLocalTime(utcDate, timezoneString):
    return utcDate.replace(tzinfo=pytz.utc).astimezone(timezone(timezoneString))

'''
builds and sends a successful notification email after an order has successfully
transferred
'''
def sendSuccessfulTransferEmail(eventUid, venueUid, dbCore):


    recipientList = dbCore.getOrderTransferEmailRecipientList(venueUid)

    eventName, eventDateUTC, levyEventId, localTimeZone, venueName, employeeFirstName, employeeLastName, orderCount = dbCore.getTransferedOrderData(eventUid)

    total = dbCore.getTransferedOrderTotal(eventUid, localTimeZone, venueUid)

    print "Total: " + str(total)

    localEventDate = convertUTCToLocalTime(eventDateUTC, localTimeZone)

    subject = "Event: " + eventName + " successfully transferred"

    currentTimeUTC = datetime.datetime.now()
    currentTime = convertUTCToLocalTime(currentTimeUTC, localTimeZone)

    failedOrders = dbCore.getFailedTransferOrders(eventUid)

    locale.setlocale( locale.LC_ALL, '' )

    emailBody = ""

    openOrders = dbCore.getOpenOrders(eventUid)
    if len(openOrders) > 0:
        emailBody += "<h3> There were " + str(len(openOrders)) + " open orders when the event was locked.</h3>"

#    print "TOTAL: " +  str(total)

    if total is None:
        total = 0

    emailBody += "<p>Event: " + eventName + " - " + localEventDate.strftime("%b %-d, %Y @ %-I:%M %p") + " </p><p>Venue: " + venueName + "</p><p>Closed By: " + employeeFirstName + " " + employeeLastName + "</p><p>Transfer Time: " + currentTime.strftime("%b %-d, %Y @ %-I:%M %p") + " </p><p>Orders: " + str(orderCount) + "</p><p>Gross Total: " + locale.currency(total, grouping=True) + " </p>"


    if len(failedOrders) > 0:
        subject = "Event: " + eventName + " transferred with errors"
        emailBody += "<p>The following orders experienced errors while transfering, please investigate and close on a terminal</p><ul>"
        for failedOrder in failedOrders:
            emailBody += "<li>" + str(failedOrder[0]) + "</li>"

        emailBody += "</ul>"


    openOrders = dbCore.getOpenOrders(eventUid)
    if len(openOrders) > 0:
        emailBody += "<p>Not all orders were closed when event was locked.  The following orders did NOT transfer: <ul>"

    for to in recipientList:
        print "Sending Transfer Email to " + str(to[0])
        #gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "OrderTransfer@parametricdining.com", str(to[0]), subject, emailBody, "You need to enable HTML to view this message")
        MailGun.sendEmail('mail@bypassmobile.com', str(to[0]), subject, emailBody)


def makeCSVFile(fileName, header, data):
    with open(EXPORT_FILE_PATH + fileName + ".csv", 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(data)

logger = OrderTransferLogger()

try:
    


    conn = DbConnection().connection
    dbCore = OMSDirectOrderTransferDB(conn)



    venueUidResults = dbCore.getVenueUids()
    venueUids = []
    for venueUidRow in venueUidResults:
        venueUids.append(venueUidRow[0])

    print str(venueUids)


    if dbCore.shouldRun(venueUids):
        print "I found events to transfer, I'm running"
        HipChat.sendMessage("OMS Direct Integration Starting.  Stay tuned for more messages", "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_PURPLE)
        logger.log("---------- OMS Direct Export Started ----------")
    else:
        print "No events to transfer"
        sys.exit()
    
    logger.log("Venue Uids: " + str(venueUids))

    logger.log("Starting Generic Exports")

    voidReasonsHeader, voidReasons = dbCore.getVoidReasons()
    makeCSVFile("MSTR-VOIDREASONS", voidReasonsHeader, voidReasons)

    checkTypesHeader, checkTypes = dbCore.getCheckTypes()
    makeCSVFile("MSTR-CHECKTYPES", checkTypesHeader, checkTypes)

    usersHeader, users = dbCore.getUsers(venueUids)
    makeCSVFile("MSTR-USERS", usersHeader, users)

    userLocationHeader, userLocations = dbCore.getUserLocations(venueUids)
    makeCSVFile("MSTR-USERLOCATIONS", userLocationHeader, userLocations)

    userRolesHeader, userRoles = dbCore.getUserRoles(venueUids)
    makeCSVFile("MSTR-ROLES", userRolesHeader, userRoles)

    roleAssocsHeader, roleAssocs = dbCore.getRoleAssociations(venueUids)
    makeCSVFile("MSTR-ROLEASSOC", roleAssocsHeader, roleAssocs)

    discountsHeader, discounts = dbCore.getDiscounts()
    makeCSVFile("MSTR-DISCOUNTS", discountsHeader, discounts)

    paymentTypesHeader, paymentTypes = dbCore.getPaymentTypes()
    makeCSVFile("MSTR-PAYMENTTYPES", paymentTypesHeader, paymentTypes)

    menusHeader, menus = dbCore.getMenus(venueUids)
    makeCSVFile("MSTR-MENUS", menusHeader, menus)

    logger.log("Generic Exports Finished, event transfers starting")

#    exit()

    for venueUid in venueUids:
        print str(venueUid)
        closedEvents = dbCore.getClosedEvents(venueUid)
        print str(closedEvents)
        for event in closedEvents:
            eventUid = event[0]
            if dbCore.TPGDisabled(eventUid):
                
                logger.log("Transfering event " + str(eventUid) + " @ " + str(venueUid))
                HipChat.sendMessage("OMS Direct Integration of event " + str(eventUid) + " @ " + str(venueUid) + " has started", "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_GREEN)
                dbCore.markEventTransferStarted(eventUid)

                entityCode = dbCore.getLevyVenueEntityCode(venueUid)
                levyEventNumber = str(dbCore.getLevyEventNumber(eventUid))

                orderHeadersHeader, orderHeaders = dbCore.getOrderHeaders(eventUid)
                orderHeaderFileName = "ORDHDR_" + entityCode + "_" +  levyEventNumber
                makeCSVFile(orderHeaderFileName, orderHeadersHeader, orderHeaders)
                
                orderDetailsHeader, orderDetails = dbCore.getOrderDetails(eventUid)
                orderDetailsFileName = "ORDDTL_" + entityCode + "_" + levyEventNumber
                makeCSVFile(orderDetailsFileName, orderDetailsHeader, orderDetails)

                orderPaymentsHeader, orderPayments = dbCore.getOrderPayments(eventUid)
                orderPaymentsFileName = "ORDPMT_" + entityCode + "_" + levyEventNumber
                makeCSVFile(orderPaymentsFileName, orderPaymentsHeader, orderPayments)

                dbCore.markAllOrdersTransfered(eventUid)
                dbCore.markEventTransferSuccessful(eventUid)
                
                dbCore.markEventTransfered(eventUid)            
            
                HipChat.sendMessage("Event " + str(eventUid) + " transfered successfully", "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_GREEN)


                #PALAC special transfer
                if venueUid == 315:
                    try:

                        transferPALAC()
                   except:
                        try:
                            transferPALAC()
                        except:
                            tb = traceback.format_exc()
                            HipChat.sendMessage("Issue transfering PALAC files: " + str(tb), "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_RED)

                sendSuccessfulTransferEmail(eventUid, venueUid, dbCore)    
            else:
                logger.log("The TPG is enabled, don't transfer this event")
                print "TPG is enabled, don't transfer this event directly"
except Exception as e:
    tb = traceback.format_exc()
    HipChat.sendMessage("@nate @jonathan_removethis OMS Direct Integration Script CRASHED " + str(tb), "OrderTransfers", HipChat.INTEGRATIONS_ROOM, HipChat.COLOR_RED)
                


