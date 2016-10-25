from db_connection import DbConnection
from Aramark_DB import Aramark_Db
from suds.client import Client
import sys
import datetime
import traceback
import SuiteWizard.SuiteWizard as SuiteWizard

'''
1. Check if there are any events to transfer

2. get all the orders from that event

3. send those orders to SuiteWizard

'''

PREORDER = 8


#TODO: this should be a db table that maps payment types by venue
ARAMARK_PAYMENTS = {}
ARAMARK_PAYMENTS[1] = SuiteWizard.invoicePayment 
ARAMARK_PAYMENTS[2] = SuiteWizard.genericCashPayment
ARAMARK_PAYMENTS[3] = SuiteWizard.genericCashPayment
ARAMARK_PAYMENTS[6] = SuiteWizard.genericCashPayment
ARAMARK_PAYMENTS[4] = SuiteWizard.cashPayment

    

conn = DbConnection().connection
dbCore = Aramark_Db(conn)

wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'
client = Client(wsdlUrl)

eventsToTransfer = dbCore.getEventsToTransfer()

logFile = ""

def printRow(indent, message):

    
    for x in range(indent):
        message = " " + str(message) 
   
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %-H:%-M:%-S")
    logRow = timestamp + " - " + message + '\n'
    with open(logFile, "a") as log:
        log.write(logRow)
     
    print message 

def getOrderCustomer(patronUid, unitUid):

    customerId, accountId = dbCore.getAramarkCustomerData(patronUid)
    suiteId = dbCore.getAramarkSuiteId(unitUid)

    return SuiteWizard.Customer(customerId, accountId, suiteId)
#START 

def getOrderItem(itemUid, quantity, unitPrice):
    itemId, title = dbCore.getAramarkItemData(itemUid)

    return SuiteWizard.OrderItem(itemId, quantity, title, unitPrice)

def processMultiPayment(orderUid, orderPayments):
    description = "Multiple Payments, add the following payments: " 
    for orderPayment in orderPayments:
        orderPaymentUid, subtotal, discount, tip, tax, orderPayMethodUid = orderPayment
        paymentName, pamentTypeUid, typeCode = dbCore.getAramarkPaymentData(venueUid, orderPayMethodUid)
        description += "<br>" + paymentName + "$" + str(subtotal + tip + tax)
    addOrderTransferAction(ordersAramarkUid, 'split pay', description)

print "Starting Transfer"

for eventUid, venueUid in eventsToTransfer:

    logFile = "/var/log/aramark_order_transfer/event_" + str(eventUid) + ".log" 

    printRow(0, "Transfering event: " + str(eventUid) + " at venue: " + str(venueUid))

    ordersToTransfer = dbCore.getOrdersToTransfer(eventUid)
        
    for orderUid, orderTypeUid, patronUid, unitUid in ordersToTransfer:
       
        try: 
            printRow(5, "Transfering order: " + str(orderUid))

            customer = getOrderCustomer(patronUid, unitUid)

            printRow(5, customer)
            

            dbCore.clearOrderTransferTasks(orderUid)

            if (orderTypeUid == PREORDER):
                printRow(10, "It's a preorder")          
               
                print str(orderUid)
     
                ordersAramarkUid, orderId  = dbCore.findOrdersAramarkUid(orderUid, venueUid)

                orderSummary = SuiteWizard.getOrderSummaryComplete(client, orderId)

               
                printRow(20, "Adding new preorder items")
                newPreorderItems = dbCore.getOrderItemsAddedToPreorder(orderUid)
                for orderItemUid, revenueCenterUid, menuXMenuItemUid, menuItemUid, price, qty in newPreorderItems:
                    item = getOrderItem(menuItemUid, qty, price)
                    printRow(25, item)

                    status = SuiteWizard.addItemToOrder(client, orderId, item, logFile)
                    printRow(30, "Successful add: " + str(status))

                #add gratuity

                printRow(15, "Adding Grat to preorder")
                
                orderPayments = orderSummary.OrderPayments
                if orderPayments is not None and len(orderPayments) > 0:
                    orderPaymentsList = orderPayments[0]
                    if orderPaymentsList is not None and len(orderPaymentsList) > 0:
                        if len(orderPaymentsList) == 1:
                            printRow(20, "There is one payment")
                            orderPayment = orderPaymentsList[0]

                            payments = dbCore.getOrderPayments(orderUid)
                            if len(payments) == 1: 
                                printRow(20, "We have 1 payment in our db, we're in buisness")
                                gratuity = payments[0][3]
                                orderPayment.Gratuity = gratuity
                                client.service.SaveOrderPayment(orderPayment)
                        else:
                            printRow(20, "Too many order payments, I can't help")
                
                

                SuiteWizard.savePreorder(client, orderId, logFile) 
                    
                #handle the voided items
                printRow(15, "Getting Voided Items")
                voidedItemNames = dbCore.getVoidedOrderItems(orderUid)
                for name in voidedItemNames:
                    name = name[0]
                    dbCore.addOrderTransferAction(ordersAramarkUid, 'void', "Void item: " + name)
                    
                
        
            else:
                printRow(10, "It's a regular order")

                eventId = dbCore.getEventId(eventUid)

                #create order 
                orderId = SuiteWizard.createOrder(client, eventId, customer, logFile)
                printRow(10 , "Order ID: " + orderId) 

                orderSummary = SuiteWizard.getOrderSummaryComplete(client, orderId)

                 #a unique id that is displayed to the users in SuiteWizard, we want to use this when creating order transfer tasks
                orderNumber = orderSummary.OrderNum


                #save an orders_aramark row
                ordersAramarkUid = dbCore.saveOrderAramark(orderId, venueUid, orderUid, orderSummary.OrderNum)
                printRow(10, "Orders Aramark Row saved: " + str(ordersAramarkUid))        
         
                #add items to order
                orderItems = dbCore.getOrderItems(orderUid)

                printRow(10, "Adding Items to order")
                for orderItemUid, revenueCenterUid, menuXMenuItemUid, menuItemUid, price, quantity in orderItems:
                    item = getOrderItem(menuItemUid, quantity, price)
                    printRow(15, item)

                    status = SuiteWizard.addItemToOrder(client, orderId, item, logFile)
                    printRow(15,  "Successful add: " + str(status))

                printRow(10, "Checking order payments")
                orderPayments = dbCore.getOrderPayments(orderUid)

                print str(orderPayments)
                if len(orderPayments) == 0:
                    printRow(15, "There are no payments for this order")
                elif len(orderPayments) == 1:
                    printRow(15, "I found 1 payment")
                    orderPayment = orderPayments[0]
                    orderPaymentUid, subtotal, discount, tip, tax, orderPayMethodUid = orderPayment
                    paymentName, paymentTypeId, typeCode = dbCore.getAramarkPaymentData(venueUid, orderPayMethodUid)

                    printRow(15, "I got payment data") 
                    payment = SuiteWizard.Payment(paymentTypeId, typeCode)
                    payment.setGratuity(tip)
                    printRow(15, "I got payment object")
                    try:
                        SuiteWizard.addPaymentToOrder(client, orderId, customer, payment, logFile)
                        printRow(15, "Payment added Successfully")
                    except:
                        printRow(15, "Adding payment failed")
                        print str(client.last_sent())
                        tb = traceback.format_exc()
                        printRow(20, tb)
                        exit()
                else:
                    printRow(15, "There are too many payments, I can't transfer them")
                    processMultiPayment(orderUid, orderPayments)

                SuiteWizard.saveOrder(client, orderId, logFile)

            #CLOSE ORDER
            totalPayment, totalGratuity = dbCore.getOrderPaymentTotal(orderUid)

            paymentSummary = "(PaymentTotal = " + str(totalPayment) + ", Gratuity = " + str(totalGratuity) + ")"

            dbCore.addOrderTransferAction(ordersAramarkUid, 'close', 'Close Order ' + paymentSummary)

            dbCore.markOrderTransferSuccessful(orderUid)
        except:
            tb = traceback.format_exc()
            
            printRow(50, "ORDER FAILED TO TRANSFER") 
            printRow(55, tb)

    dbCore.markEventTransfered(eventUid)
