import sys
from db_connection import DbConnection
import HipChat
import csv
import traceback
import paramiko
from PalaceExportDB import PalaceExportDB
import pytz
from pytz import timezone
import datetime
import locale


EXPORT_FILE_PATH = "/data/integration_files/PALACE_EXPORTS/"

def log(indent, message):
    for x in range(indent):
        message = " " + message

    print message

def makeCSVFile(fileName, data):
    with open(EXPORT_FILE_PATH + fileName + ".txt", 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerows(data)



conn = DbConnection().connection
dbCore = PalaceExportDB(conn)


#DISCOUNT
discountRows = dbCore.getDiscountRows()
makeCSVFile('DISCOUNT070', discountRows)

#EMP
employeeRows = dbCore.getEmployeeRows()
makeCSVFile('EMP070', employeeRows)

#GRATUITY
gratuityRows = dbCore.getGratuityRows()
makeCSVFile('GRATUITY070', gratuityRows)

#ORDHDR
orderHeaderRows = dbCore.getOrderHeaderRows()
makeCSVFile('ORDHDR070', orderHeaderRows)

#ORDTAX
orderTaxRows = dbCore.getOrderTaxRows()
makeCSVFile('ORDTAX070', orderTaxRows)

#ORITEM
orderItemRows = dbCore.getOrderItemRows()
makeCSVFile('ORITEM070', orderItemRows)

#REVENUE
revenueRows = dbCore.getRevenueRows()
makeCSVFile('REVENUE070', revenueRows)

#SERVER
serverRows = dbCore.getServerRows()
makeCSVFile('SERVER070', serverRows)

#TENDER
tenderRows = dbCore.getTenderRows()
makeCSVFile('TENDER070', tenderRows) 

#DOC ???? what the fuck is this shit?

