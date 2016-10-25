import sys
from OrderTransferDB import OrderTransfer_Db
from db_connection import DbConnection
from Agilysys.Order_Transfer_Logger import OrderTransferLogger
from Agilysys.Agilysys import Agilysys
import traceback
import HipChat
import csv
import gmail
import datetime
import pytz
from pytz import timezone
import os, os.path



TRANSFER_ORDERS_LOCK = "transfer_orders.lock"

'''
will attempt to lock the mutex, if it has already been locked by another
script, this method will return False, indicating that the calling thread
should die.  If another thread has not already locked the mutex this method
will return True
'''
def lockMutex():
    if(os.path.isfile(TRANSFER_ORDERS_LOCK)):  
      return False
    else:
        #HipChat.sendMessage("Order Transfer Locked", "MUTEX", "1066556", 'gray')
        open(TRANSFER_ORDERS_LOCK, 'w+')
        return True

'''
will unlock the mutex, should be called whenever this script finishes
executing (or fails)
'''
def unlockMutex():
    #HipChat.sendMessage("Order Transfer Unlocked", "MUTEX", "1066556", 'gray')
    os.remove(TRANSFER_ORDERS_LOCK)

'''
converts a UTC based datetime object into a local timezone based datetime object
'''
def convertUTCToLocalTime(utcDate, timezoneString):
    return utcDate.replace(tzinfo=pytz.utc).astimezone(timezone(timezoneString))

'''
builds and sends a successful notification email after an order has successfully
transfered
'''
def sendSuccessfulTransferEmail(eventUid, venueUid, orders, OTDB):
    

    recipientList = OTDB.getOrderTransferEmailRecipientList(venueUid) 
    print str(recipientList)


    eventName, eventDateUTC, levyEventId, localTimeZone, venueName, employeeFirstName, employeeLastName, total = OTDB.getTransferedOrderData(eventUid)

    localEventDate = convertUTCToLocalTime(eventDateUTC, localTimeZone)
    subject = "Event: " + eventName + " successfully transfered"

    currentTimeUTC = datetime.datetime.now()
    currentTime = convertUTCToLocalTime(currentTimeUTC, localTimeZone)

    failedOrders = OTDB.getFailedTransferOrders(eventUid)

 

    emailBody = "<p>Event: " + eventName + " - " + localEventDate.strftime("%b %-d, %Y %-I:%M %p") + " </p><p>Venue: " + venueName + "</p><p>Successfully Transferred " + str(len(orders)-len(failedOrders)) + " orders at " + currentTime.strftime("%-I:%M %p") + " on " + currentTime.strftime("%b %-d, %Y") + " for a total of $" + "{0:.2f}".format(total) + ".</p>" + "<p>Event was closed by " + employeeFirstName + " " + employeeLastName + ".</p>"

    if len(failedOrders) > 0:
        subject = "Event: " + eventName + " transfered with errors"
        emailBody += "<p>The following orders experienced errors while transfering, please investigate and close on a terminal</p><ul>"
        for failedOrder in failedOrders:
            emailBody += "<li>" + str(failedOrder[0]) + "</li>"

        emailBody += "</ul>"

    for to in recipientList:
        print "Sending Transfer Email to " + str(to[0])
        gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "OrderTransfer@parametricdining.com", str(to[0]), subject, emailBody, "You need to enable HTML to view this message")

'''
builds an exports the order data csv file and places it in Levy's 
outgoing sftp directoy
'''
def exportEventData(venueUid, eventUid, OTDB):
    
    entityCode = OTDB.getLevyVenueEntityCode(venueUid)
    levyEventNumber = OTDB.getLevyEventNumber(eventUid)

    csvFileName = str(entityCode) + "-" + str(levyEventNumber) + ".csv"

    description, tipRows = OTDB.getLevyExportData(eventUid)

    #formatting changes as requested by Todd at Levy
    #print "Tip Rows: " + str(tipRows)
    reformattedTipRows = []
    for tipRow in tipRows:
        tipRow = list(tipRow)
        #convert our int boolean to a char
        
        if tipRow[5] == 0:
            tipRow[5] = 'N'
        else:
            tipRow[5] = 'Y'

        #truncate decimals to two places
        tipRow[2] = "{0:.2f}".format(tipRow[2])
        tipRow[3] = "{0:.2f}".format(tipRow[3])
        tipRow[4] = "{0:.2f}".format(tipRow[4])
        tipRow[6] = "{0:.2f}".format(tipRow[6])
        tipRow[7] = "{0:.2f}".format(tipRow[7])
        tipRow[8] = "{0:.2f}".format(tipRow[8])
        tipRow[9] = "{0:.2f}".format(tipRow[9])
        tipRow[10] = "{0:.2f}".format(tipRow[10])    
        reformattedTipRows.append(tipRow)
    #print str(tipRows) 

    with open("/data/integration_files/order_exports/" + csvFileName, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow([ i[0] for i in description ])
        writer.writerows(reformattedTipRows)


def exportVoidedOrder(venueUid, eventUid, OTDB, orderUid, orderNumber):

    entityCode = OTDB.getLevyVenueEntityCode(venueUid)
    levyEventNumber = OTDB.getLevyEventNumber(eventUid)
    
    csvFileName = str(entityCode) + "-" + str(levyEventNumber) + ".csv"

    #since it's a voided order none of this data matters, Todd just needs a row with the first 2 columns
    voidedOrderExportRow = [orderUid, orderNumber, 0, 0, 0, 'N', 0, 0, 0, 0, 0]

    with open("/data/integration_files/order_exports/" + csvFileName, 'a') as fout:
        writer = csv.writer(fout)
        writer.writerow(voidedOrderExportRow)

if not lockMutex():
    print "Mutex is locked, exiting"
    exit()

try:

    PREORDER = 8

    LEVY_MISC_FOOD_ITEM = 11110

    conn = DbConnection().connection

    OTDB = OrderTransfer_Db(conn)

    #LEVY TENDER TYPES -- for right now these only work for the UC, but hopefully we can get time installed at all venues
    DIRECT_BILL = 9
    AMERICAN_EXPRESS = 19
    DISCOVER = 20
    MASTERCARD = 21
    VISA = 22

    #Levy Generic Account Numbers -- see note above
    accountNumbers = {19:'3333', 20:'6666', 21:'5555', 22:'4444', 13:'5555'} #13 is for testing against the dev tpg

    TIP_ITEM_NUMBER = 9999999 #DEPRICATED

    #Test creds
#    agilysys = Agilysys('http://73.165.252.236:7008', '999', 'BBQ')

    #THIS ONLY WORKS FOR THE UC, WE ARE GOING TO HAVE TO SWITCH BASED ON VENUE, OR BETTER YET LOOK IT UP IN THE DB
    agilysys = Agilysys('http://85.190.177.240:7008', '65300', 'LEVYUC')

    venueUid = sys.argv[1]

    #get all of the closed events
    closedEvents = OTDB.getClosedEvents(venueUid) 

    print "There are: " + str(len(closedEvents)) + " closed events"


    #transfer all fo the closed events one by one, there should only be one, but there for loop is here just in case
    for event in closedEvents:

        #event[0] = event_uid, event[1] = venue_uid
        logger = OrderTransferLogger(event[1], event[0])
        agilysys.setLogger(logger)
        OTDB.setLogger(logger)
        logger.log("*************Transfering orders for EVENT " + str(event[0]) + " *************")
        
        eventTransfered = True    
        failedOrders = []

        orders = OTDB.getOrders(event[0])
       
        logger.log(str(len(orders)) + " orders to transfer") 
    
        #sendSuccessfulTransferEmail(2238, 201, orders, OTDB)
        #exit()

        #transfer orders 1 by 1
        for order in orders:
        
            try: 
                logger.log("     -------ORDER------- " + str(order[0]))

                #a bunch of random data used to make orderHeaders
                venueInfo = OTDB.getLevyVenueInfo(event[1])
                suiteInfo = OTDB.getLevyUnit(order[2])
                suiteId = suiteInfo[0]
                suiteName = suiteInfo[1]
           
                tableName = venueInfo[0] + str(suiteName)
                employeeId = OTDB.getLevyEmployeeId(event[1], order[3])
                profitCenterId = venueInfo[1]        

                #gather all of the items associated with the order
                items = OTDB.getOrderItems(order[0])
                itemObjects = []
                logger.log("Fetching Items")


                orderVoided = OTDB.isOrderVoided(order[0])


                #turn the items database rows into an array of Agilysys.OrderItem objects
                itemTotal = 0.0
                numberOfItems = 0
                for item in items:

                    item_uid = item[3] #TODO, this needs to be translated through the integration tables
                    levy_item_number = OTDB.getLevyItemNumber(item_uid)[0]
                    
                    price = round(item[4], 2)
                    if orderVoided: 
                        #if the item is voided we transfer it with a $0 price
                        price = 0

                    logger.log("Price: " + str(price))
                    itemTotal = itemTotal + (price * int(item[5]))
                    price = int(round(price * 100)) #convert to an int because the TPG doesn't have to deal with decimal points
                                    

                    logger.log("Item Price: " + str(price)); 

                    qty = item[5]
                    numberOfItems = numberOfItems + qty
                    itemObject = Agilysys.OrderItem(levy_item_number, qty,  price, 'no', 1, 1)
                    itemObjects.append(itemObject)



      
                #serviceCharge
             #   serviceCharge = OTDB.getOrderServiceChargeSum(order[0])
             #   if serviceCharge > 0:
             #       serviceCharge = int(rount(serviceCharge * 100)) #convert the service charge into pennies
             #       logger.log("Total Service Charge: " + str(serviceCharge))
             #   else:
             #       serviceCharge = None #set in to None so that the Agilysys methods will ignore it
             #       logger.log("No service charge")
                #adjust for sloppy rounding


                logger.log("Fetching Payments")
                payments = OTDB.getOrderPayment(order[0])
                paymentObjects = []
               
                #turn the payment db rows into an array of Agilysys,OrderPayment objects
                paymentTotal = 0
                taxTotal = 0

                discountTotal = 0
                for payment in payments:
                    logger.log("Appending Payment")
                    #just to make reading a little easier
                    orderPaymentUid = payment[0]
                    subtotal = payment[1]
                      
                    discount = payment[2]
                    discountTotal = discountTotal + discount
                    tip = payment[3]
                    tax = payment[4]
                    taxTotal = taxTotal + tax
                    logger.log('TAX: ' + str(tax))
                    orderPayMethodUid = payment[5]
                
                    #discount = 0 # since we have split the discount over the items we don't want this discount anymore
     
                    logger.log("Tender Amount Total = " + str(subtotal) + " + " + str(tax) + " + " + str(tip) + " - " + str(discount) )    
                    tenderAmountTotal = int(round(((subtotal + tax) - discount) * 100)) # *100 because Levy does all of their transactions in cents (no decimal points!!!!)

                    paymentTotal = paymentTotal + int(round((subtotal + tax) * 100))

                    tenderUid = OTDB.getLevyTenderType(orderPaymentUid, orderPayMethodUid)
                   
                    #tip = int(tip * 100) #for the tip hack, don't convert this into an int 
                    paymentObject = Agilysys.OrderPayment(tenderUid, tenderAmountTotal, tip)  

                    paymentObjects.append(paymentObject)

#TODD changes his mind and no longer wants the discount split over the items
                #divide the total order discount over all of the items
#                discountPercent = OTDB.getOrderDiscount(order[0])
#                
#                if not (discountPercent == 0):
#                    logger.log("Order wide discount percent = " + str(discountPercent))
#                    itemTotal = 0
#                    for itemObject in itemObjects:
#                        print itemObject.item_price
#                        itemObject.item_price = int(itemObject.item_price * (1.0 - (discountPercent / 100.0)))                        
#                        print itemObject.item_price
#                        itemTotal = itemTotal + (itemObject.item_price * itemObject.item_quantity)
            
               # discountTotal = 0 #we don't want to use this guy anymore since we just distributed it to the items
            
    

                #pre-calculate the ig order total            
                igOrderDetails = agilysys.calculateOrder(itemObjects) 
                igOrderTotal = igOrderDetails['orderAmount']
                            
                logger.log("Item Total: " + str(itemTotal)); 
                logger.log("Processing order with: " + str(len(itemObjects)) + " ITEMS") 



       
                subtotalsAndTaxes = OTDB.getOrderSubtotalAndTaxes(order[0])
                itemTotal = 0;
                taxTotal = 0;
                discountTotal = 0;
                for st in subtotalsAndTaxes:
                    itemTotal = itemTotal + st[0]
                    taxTotal = taxTotal + st[1] 
                    discountTotal = discountTotal + st[2]
                logger.log("Tax Total: " + str(taxTotal))
               
                parametricSubtotal = int(round((itemTotal + taxTotal - discountTotal) * 100))
                logger.log("PARAMETRIC SUBTOTAL: " + str(parametricSubtotal))
                logger.log("IG SUBTOTAL: " + str(igOrderTotal))
                difference = parametricSubtotal - int(igOrderTotal)
                logger.log("DIFFERENCE: " + str(difference))
                
                
                taxTotal = int(round(taxTotal * 100))

                if difference < 0:
                    logger.log("We didn't give them enough money... padding first payment")
                    paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total + abs(difference)

                if difference > 0:
                    logger.log("We gave them too much money... removing from first payment")
                    paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total - difference
                #if difference > 0:
                #    logger.log("We gave them too much bloody money... subtracting from the first payment")
                #    paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total - difference


                ############################
                # ~ ~ ~ ~ TIP HACK ~ ~ ~ ~ #
                ############################

                #The goal here is to convert the tip_amount in to a value in our export file
                #We need to do this because the TPG is bugged and refuses to accept a single 
                #penny more than the order total (no tips)

                # This hack should work for single and multiple payments 

                logger.log("Totaling up tips")

                orderTipTotal = 0
                orderPaidTips = 0
                for paymentObject in paymentObjects:
                    if(paymentObject.tender_id in (AMERICAN_EXPRESS, DISCOVER, MASTERCARD, VISA)):
                        logger.log("Adding " + str(paymentObject.tip_amount) + " to order tip total")
                        orderTipTotal = orderTipTotal + paymentObject.tip_amount
                        orderPaidTips = orderPaidTips + paymentObject.tip_amount
                        logger.log("New tip total = " + str(orderTipTotal))
                        paymentObject.tip_amount = 0
                    else:
                        orderTipTotal = orderTipTotal + paymentObject.tip_amount
                        paymentObject.tip_amount = 0
                        
                    #if order[4] == 1:
                    #    logger.log("This order is tax exempt") 
                #    tax = igOrderDetails['tax'] 
                #    logger.log("Tax to remove: " + tax)
                #    paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total - int(tax)                                       

                result = None    
                if order[1] == PREORDER:
                    logger.log("Its a preorder!")
                    #find the tpg order number
                    referenceNumber = OTDB.getTPGReferenceNumber(order[0])[0]
                    logger.log("Reference Number = " + str(referenceNumber))
                
                    MAGIC_PREORDER_EMPLOYEE_UID = 584 # this only works for 201
                    employeeId = OTDB.getLevyEmployeeId(event[1], MAGIC_PREORDER_EMPLOYEE_UID)
       
                    #PREORDER SAFTEY CHECK
                    noItems, noPayments = agilysys.checkPreorder(profitCenterId, employeeId, referenceNumber)

                    if not noItems:
                        itemObjects = None

                    if not noPayments:
                        paymentObjects = None

                    #put together the orderHeader with the referenceNumber
                    checkType = OTDB.getCheckType(event[1], preorder=True)
                    
                    #with check type
                    orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, orderNumber=referenceNumber, guestCount = 1, checkTypeId= checkType, receiptRequired = 'yes')
                    #without check type
                    #orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, orderNumber=referenceNumber, guestCount = 1, receiptRequired = 'yes')

                else:
                    logger.log("It's a regular order")                
                    checkType = OTDB.getCheckType(event[1], preorder=False)
     
                    #with check type
                    orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, guestCount=1, checkTypeId=checkType, receiptRequired='yes')
                    #without check type
                    #orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, guestCount=1, receiptRequired='yes')

     
                logger.log("Preparing to send NEW order to Agilysys")
                logger.logParams(orderHeader, "ORDER HEADER: ")
                logger.logParams(itemObjects, "ITEMS: ")
                logger.logParams(paymentObjects, "PAYMENTS: ")

                                 

                if len(paymentObjects) > 1:
                    #logger.log("I CAN'T CLOSE THIS ORDER, THERE ARE MULTIPLE PAYMENTS!!!!")
                    #failedOrder.append(order[0])
                    logger.log("OPENING ORDER (with multiple payments)")
                    openOrderResponse = agilysys.openOrder(orderHeader, itemObjects)
                    if openOrderResponse['success']:
                        logger.log("Order opened successfully")
                        orderNumber = openOrderResponse['orderNumber']
                        orderHeader.order_number = orderNumber
                        patronUid = order[5]

                        accountNumber, guestName = OTDB.getLevyPatronData(patronUid, venueUid)

                        logger.log("Adding Payments")

                        paymentSuccess = True

                        for payment in paymentObjects:
                            if payment.tender_id == DIRECT_BILL:
                                logger.log("Adding DIRECT BILL payment")
                                accountNumber, guestName = OTDB.getLevyPatronData(patronUid, venueUid)
                                roomAuthorizationResponse = agilysys.authorizeRoomCharge(orderNumber,
                                                                                         profitCenterId,
                                                                                         tableName,
                                                                                         employeeId,
                                                                                         payment.tender_id,
                                                                                         payment.tender_amount_total,
                                                                                         payment.tip_amount,
                                                                                         tableName,
                                                                                         guestName,
                                                                                         accountNumber)
                                if(roomAuthorizationResponse['success']):
                                    logger.log("Direct Bill Payment successfully added")
                                else:
                                    logger.log("Direct Bill Payment failed to add")
                                    paymentSuccess = False
                            elif payment.tender_id in (AMERICAN_EXPRESS, DISCOVER, MASTERCARD, VISA):
                                logger.log("Adding CREDIT CARD payment")
                                
                                accountNumber = accountNumbers[payment.tender_id]
                                
                                genericAuthResult = agilysys.authorizeGenericAccount(orderNumber,
                                                                                     profitCenterId,
                                                                                     employeeId,
                                                                                     payment.tender_id,
                                                                                     payment.tender_amount_total,
                                                                                     payment.tip_amount,
                                                                                     accountNumber)
                                if(genericAuthResult['success']):
                                    logger.log("Generic Auth Payment Successfully added")
                                else:
                                    logger.log("Gerneirc Auth Payment failed to add")
                                    paymentSuccess = False


                        if paymentSuccess == False:
                            #the order has failed to apply all payments
                            logger.log("Order can not be closed, one or more payments failed to complete")
                            if order[1] == PREORDER:
                                OTDB.markPreorderTransferFailed(order[0])
                            failedOrders.append(order[0])
                            continue #processing orders
                        
                        
                        else:
                            logger.log("All payments successfully added, attempting to close order")
                            finalizeResponse = agilysys.finalizeOrder(orderNumber, employeeId)       
                            
                            if finalizeResponse['success']:
                                logger.log("Order Finalized Successfully")
                                OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference)
                                OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
                                OTDB.markOrderTransfered(order[0])
                                OTDB.recordOrderTipTotal(openOrderResponse['orderNumber'], order[0], orderTipTotal, orderPaidTips)
                            else:
                                logger.log("Order failed to Finalize")
                                if order[1] == PREORDER:
                                    OTDB.markPreorderTransferFailed(order[0])
                                failedOrders.append(order[0])

                        
                    
                elif len(paymentObjects) == 1: 
                    payment = paymentObjects[0] #because there is only one

                    logger.log("New single payment order")
     
                    if(payment.tender_id == DIRECT_BILL):
                        logger.log("OPENING ORDER");
                        openOrderResponse = agilysys.openOrder(orderHeader, itemObjects)
                        if(openOrderResponse['success']):
                            logger.log("Order opened successfully")  
                            orderNumber = openOrderResponse['orderNumber']
                            orderHeader.order_number = orderNumber 
                            patronUid = order[5]
                        
                            accountNumber, guestName = OTDB.getLevyPatronData(patronUid, venueUid)

                            logger.log("Tender Amount Total = " + str(payment.tender_amount_total))

                            roomAuthorizationResponse = agilysys.authorizeRoomCharge(orderNumber,
                                                                                     profitCenterId,
                                                                                     tableName,
                                                                                     employeeId,
                                                                                     payment.tender_id,
                                                                                     payment.tender_amount_total,
                                                                                     payment.tip_amount,
                                                                                     tableName,
                                                                                     guestName,
                                                                                     accountNumber
                                                                                    )      

                            if(roomAuthorizationResponse['success']):
                                logger.log("Room Charge Successfully authorized")
                                referenceKey = roomAuthorizationResponse['referenceKey']
                                agilysys.closeOrder(orderHeader, payment.tender_amount_total, referenceKey)
                            
                                # recording the difference
                                #if difference != 0:
                                #    OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference)    
                                
                                logger.log("SUCCESS!!!!!!\n\n")
                                OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference)
                                OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
                                OTDB.markOrderTransfered(order[0])
                                OTDB.recordOrderTipTotal(openOrderResponse['orderNumber'], order[0], orderTipTotal, orderPaidTips)

                                OTDB.markOrderTransfered(order[0])
                    
                            else:
                                logger.log("***ERROR Room Charge Failed")
                                logger.log(str(roomAuthorizationResponse))
                                if order[1] == PREORDER:
                                    OTDB.markPreorderTransferFailed(order[0])

                                failedOrders.append(order[0])        

                        else:
                            logger.log("***ERROR: Open order failed!")
                            errorMessage = openOrderResponse['errorMessage']
                            logger.log("     " + errorMessage)
                            if order[1] == PREORDER:
                                OTDB.markPreorderTransferFailed(order[0])

                            failedOrders.append(order[0])
                       
                

                    elif payment.tender_id in (AMERICAN_EXPRESS, DISCOVER, MASTERCARD, VISA):
    #                    logger.log("CREDIT CARD ORDER")
    #                    openOrderResponse = agilysys.openOrder(orderHeader, itemObjects, paymentObjects, closeImmedietly = True)
    #                  
    #                    if openOrderResponse['success']:
    #                        # recording the difference
    #                        if difference != 0:
    #                            OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference) 
    #                            logger.log("SUCCESS!!!!!!\n\n")
    #                            OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
    #                    else:
    #                        logger.log("***ERROR: Open order failed!")
    #                        errorMessage = openOrderResponse['errorMessage']
    #                        logger.log("     " + errorMessage)
    #                        failedOrders.append(order[0])
                        logger.log("CREDIT CARD ORDER")

                        openOrderResponse = agilysys.openOrder(orderHeader, itemObjects)
                        
                        if openOrderResponse['success']:
                            logger.log("Order opened successfully")
                            orderNumber = openOrderResponse['orderNumber']

                            accountNumber = accountNumbers[payment.tender_id]
                            logger.log("Account Number = " + accountNumber)
                            genericAuthResult = agilysys.authorizeGenericAccount(orderNumber,
                                                                                 profitCenterId,
                                                                                 employeeId,
                                                                                 payment.tender_id,
                                                                                 payment.tender_amount_total,
                                                                                 payment.tip_amount,
                                                                                 accountNumber)
                            if(genericAuthResult['success']):
                                logger.log("Generic Auth Payment Successfully added")
                                logger.log("Attempting to finalize the order")
                                logger.log(str(orderNumber) + " " + str(employeeId))
                                finalizeResponse = agilysys.finalizeOrder(orderNumber, employeeId)
                        
                                if finalizeResponse['success']:
                                    logger.log("Order Finalized Successfully")
                                    OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference)
                                    OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
                                    OTDB.markOrderTransfered(order[0])
                                    OTDB.recordOrderTipTotal(openOrderResponse['orderNumber'], order[0], orderTipTotal, orderPaidTips)
                                else:
                                    logger.log("Order failed to finalize")
                                    if order[1] == PREORDER:
                                        OTDB.markPreorderTransferFailed(order[0])

                                    failedOrders.append(order[0])

                            else:
                                logger.log("Gerneirc Auth Payment failed to add")
                                if order[1] == PREORDER:
                                    OTDB.markPreorderTransferFailed(order[0])

                                failedOrders.append(order[0])
                        else:
                            logger.log("Order failed to open: " + openOrderResponse['errorMessage'])
                            if order[1] == PREORDER:
                                OTDB.markPreorderTransferFailed(order[0])

                            failedOrders.append(order[0])    

                    else:
                        logger.log("Payment is not direct bill or a credit card, I don't know what to do with it")
                        if order[1] == PREORDER:
                            OTDB.markPreorderTransferFailed(order[0])

                        failedOrders.append(order[0])
                else:
                    if order[1] != PREORDER:
                        logger.log("EMPTY DOE ORDER: There are no payments")
                        logger.log("SKIPPING TRANSFER")
                        OTDB.markOrderTransfered(order[0])
                             

            except Exception as e:
                tb = traceback.format_exc()
                logger.log(tb)
                eventTransfered = False
                if order[1] == PREORDER:
                    OTDB.markPreorderTransferFailed(order[0])

                failedOrders.append(order[0])


        #remove this
        #eventTransfered = True
        #failedOrders = []

        logger.log("Hipchatting transfer status")
        try:
            if eventTransfered and len(failedOrders) == 0:
                hipChatColor = 'green'
                message = "Event " + str(event[0]) + " transfered with no problems!"
                OTDB.markEventTransfered(event[0]) 
                logger.log("Event marked transfered")
                exportEventData(venueUid, event[0], OTDB)
                HipChat.sendMessage(message, "OrderTransfers", "1066556", hipChatColor)
                sendSuccessfulTransferEmail(event[0], venueUid, orders, OTDB)
            else:
                hipChatColor = 'red'
                message = "Event " + str(event[0]) + " encounted failures when trying to transfer orders: " + "".join((str(w) + ", " for w in failedOrders))
                HipChat.sendMessage(message, "OrderTransfers", "1066556", hipChatColor)
        except:
            tb = traceback.format_exc()
            logger.log(tb)

        logger.log("*** Cleaning up open TPG orders ***")
        try:
            voidedOrders = OTDB.getVoidedPreorders(event[0])
            for voidedOrder in voidedOrders:
                try:
                    logger.log("--- Cleaning Order --- " + str(voidedOrder[0]))
                    venueInfo = OTDB.getLevyVenueInfo(event[1])
                    suiteInfo = OTDB.getLevyUnit(voidedOrder[2])
                    suiteName = str(suiteInfo[1])
                    tableName = venueInfo[0] + suiteName
            
                    employeeId = OTDB.getLevyEmployeeId(event[1], voidedOrder[3])

                    profitCenterId = venueInfo[1]
                    voidedOrderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, orderNumber=voidedOrder[4], guestCount=1, checkTypeId=checkType, receiptRequired='yes')       

                    agilysys.closeVoidedOrder(voidedOrderHeader) 

                    exportVoidedOrder(venueUid, event[0], OTDB, voidedOrder[0], voidedOrder[4])
                except:
                    tb = traceback.format_exc()
                    logger.log("FAILED to clean up open TPG Order")
                    logger.log(tb)
        except:
            tb = traceback.format_exc()
            logger.log(tb)

    #assuming nothing has gone wrong we should unlock our mutex here
    unlockMutex()       

except Exception as e:
    #if something did go wrong we will unlock our mutex here
    tb = traceback.format_exc()
    #logger.log("SCRIPT CRASHED!!!!")
    #logger.log(tb)
    HipChat.sendMessage("SCRIPT CRASHED: " + str(tb), "OrderTransfers", "1066556", "red")
    unlockMutex()
