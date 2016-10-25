import sys
from db_connection import DbConnection
import HipChat
import csv
import traceback
import paramiko
from OMSDirectOrderTransferDB import OMSDirectOrderTransferDB

EXPORT_FILE_PATH = "/data/integration_files/OMS_DIRECT_EXPORTS/"

def makeCSVFile(fileName, header, data):
    with open(EXPORT_FILE_PATH + fileName + ".csv", 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(data)


venueUids = sys.argv[1:]

print str(venueUids)

conn = DbConnection().connection
dbCore = OMSDirectOrderTransferDB(conn)

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

for venueUid in venueUids:
    print str(venueUid)
    closedEvents = dbCore.getClosedEvents(venueUid)
    for event in closedEvents:
        eventUid = event[0]
        print str(eventUid)
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

        

