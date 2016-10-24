import sys
from Aramark_DB import Aramark_Db
import HipChat
import traceback
from db_connection import DbConnection
from suds.client import Client
import json
import requests
import hashlib
from requests.auth import HTTPBasicAuth
import uuid


ARAMARK_PREORDER_URL = "http://endpointer.checkmatetablet.com//api/v1/x/external/orders/aramark"
PREORDER_SECRET_KEY = "ee2aaba2fdb11f59db156d8259d761dbd6d7d7604f6d6d27b86c89dba72388d3"
PREORDER_PARTNER_ID = 'levy'
PREORDER_USER_NAME = 'nate'
PREORDER_PASSWORD = '$up3rD3v3l0p3r'


venueUid = sys.argv[1]

conn = DbConnection().connection
dbCore = Aramark_Db(conn)

wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'
soapClient = Client(wsdlUrl)

facilityId = dbCore.getFacilityId(venueUid)

aramarkAccounts = dbCore.getAllAccounts(venueUid)
#eventUids = dbCore.getUpcomingEvents(venueUid)
eventUids = dbCore.getPreorderInProgressEvents(venueUid)

print "Ingesting Preorders for the following events: " + str(eventUids)

#this first section is going to change once they provide us with a 

for eventUid in eventUids:
    eventUid = eventUid[0]
    print "Ingesting Preorders for event: " + str(eventUid)
    #for account in aramarkAccounts:
    #    accountId = account[0]
            
    eventId = dbCore.getEventIdFromEventUid(eventUid)
    print "Event ID: " + str(eventId)
    orders = soapClient.service.GetOrdersForFacility(EventCalendarID = eventId, FacilityID = facilityId)                

    print str(orders)
    
    preorderSuccess = True

    if len(orders) != 0:
        print "There are " + str(len(orders[0])) + " orders" 
        for order in orders[0]:
            try: 
#                print str(order)
                

                orderSummaryComplete = soapClient.service.GetOrderSummaryComplete(OrderSummaryID = order.OrderSummaryID, FacilityID = facilityId)

#                print str(orderSummaryComplete)
                print "\n Ingesting Preorder: " + str(order.OrderSummaryID) + '\n'

                #fetch all the fields!!!!

                preorder = {}
            
                preorder['venue_uid'] = venueUid
                preorder['event_uid'] = eventUid
                
                preorder['customer_id'] = orderSummaryComplete.AccountID
                preorder['suite_id'] = orderSummaryComplete.SuiteID
                preorder['order_id'] = orderSummaryComplete.OrderSummaryID 

                preorder['items'] = []

                preorder['device_uid'] = 'CMaramarksuitewiza';

                preorder['uuid'] = str(uuid.uuid4())

                preorder['order_pay_method_uid'] = 8

                preorder['tpg_disabled'] = True # a relic that is left over from TPG integration

                orderDetails = orderSummaryComplete.OrderDetails
                for lineNumber, orderDetail in enumerate(orderDetails[0]):
                    #print orderDetail
                    item = {}
                    item['line_number'] = lineNumber + 1
                    item['menu_item_id'] = orderDetail.MenuItemID
                    item['quantity'] = orderDetail.Quantity
                    item['package_flag'] = str(orderDetail.IsPackage)
                    item['price'] = orderDetail.UnitPrice
               
                    preorder['items'].append(item)     
                
                #print str(preorder)
                preorder['payments'] = []

                #TODO payments 

                orderPayments = orderSummaryComplete.OrderPayments
                for orderPayment in orderPayments[0]:
                    payment = {}
                    payment['payment_method'] = orderPayment.PaymentType
                    payment['cardholder_name'] = orderPayment.CardHolderName
                    expirationYear = orderPayment.ExpirationDateYYYY
                    expirationMonth = orderPayment.ExpirationDateMM
                    if expirationYear is not None and expirationMonth is not None:
                        payment['expiration_date'] = expirationYear[:2] + expirationMonth
                    
                    preorder['payments'].append(payment)


                #we're having issues where they keep sending us preorders for patrons not assigned to the suite the order was in
                #we're going to fix this by jamming the patron into the suite 
                suiteId = orderSummaryComplete.SuiteID
                unitUid = dbCore.getUnitUidFromSuiteId(suiteId)

                accountId = orderSummaryComplete.AccountID
                patronUid = dbCore.getPatronUidFromAccountId(accountId)

                dbCore.insertUnitXPatron(patronUid, unitUid, venueUid)
            
 
                payload = {}
                jsonPreorder = json.dumps(preorder)
                payload['data'] = jsonPreorder
                
                params = payload
               # params['partner_id'] = PREORDER_PARTNER_ID
                params['hash'] = hashlib.md5(jsonPreorder + PREORDER_SECRET_KEY).hexdigest()
                params['partner_id'] = PREORDER_PARTNER_ID

                print "REQUEST: " + str(params) + '\n'

                r = requests.post(ARAMARK_PREORDER_URL, params = params, data = payload, auth=HTTPBasicAuth(PREORDER_USER_NAME, PREORDER_PASSWORD))

                print "RESPONSE: " + str(r.text) + '\n'
                response = json.loads(r.text) 

                print str(response)
            except Exception as e:
                preorderSuccess = False;
                print "There was an issue transfering order  " + traceback.format_exc()

        if preorderSuccess:
            print "Thats it, we're all done!"
            dbCore.markPreordersComplete(eventUid)
        else:
            print "Finished with errors"
            dbCore.markPreordersFailed(eventUid)
    else:
        print "There are no orders"
