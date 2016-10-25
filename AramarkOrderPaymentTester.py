from db_connection import DbConnection
from Aramark_DB import Aramark_Db
from suds.client import Client
import sys
import datetime
import traceback
import SuiteWizard.SuiteWizard as SuiteWizard

conn = DbConnection().connection
dbCore = Aramark_Db(conn)

wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'
client = Client(wsdlUrl)


orderSummary = client.service.GetOrderSummaryComplete(FacilityID = 22, OrderSummaryID= 'e4083c81-5937-42a0-a2a0-3b6d047a13e4')

orderPayments = orderSummary.OrderPayments

print len(orderPayments)

orderPayment = orderPayments[0][0]

orderPayment.Gratuity = 123.45

client.service.SaveOrderPayment(orderPayment)

