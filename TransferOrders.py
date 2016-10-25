import sys
from OrderTransferDB import OrderTransfer_Db
from db_connection import DbConnection
from Agilysys.Order_Transfer_Logger import OrderTransferLogger
from Agilysys.Agilysys import Agilysys
import traceback
import HipChat

PREORDER = 8

LEVY_MISC_FOOD_ITEM = 11110

conn = DbConnection().connection

OTDB = OrderTransfer_Db(conn)

#LEVY TENDER TYPES
DIRECT_BILL = 9
AMERICAN_EXPRESS = 19
DISCOVER = 20
MASTERCARD = 21
VISA = 22

#Levy Generic Account Numbers
accountNumbers = {19:'3333', 20:'6666', 21:'5555', 22:'4444'}


#Test creds
#agilysys = Agilysys('http://73.165.252.236:7008', '999', 'BBQ')

#THIS ONLY WORKS FOR THE UC, WE ARE GOING TO HAVE TO SWITCH BASED ON VENUE, OR BETTER YET LOOK IT UP IN THE DB
agilysys = Agilysys('http://85.190.177.240:7008', '65300', 'LEVYUC')

venueUid = sys.argv[1]

#get all of the closed events
closedEvents = OTDB.getClosedEvents(venueUid) 

print "There are: " + str(len(closedEvents)) + " closed events"

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
            for item in items:

                item_uid = item[3] #TODO, this needs to be translated through the integration tables
                levy_item_number = OTDB.getLevyItemNumber(item_uid)[0]
                
                price = round(item[4], 2)
                if orderVoided:
                    price = 0

                logger.log("Price: " + str(price))
                itemTotal = itemTotal + (price * int(item[5]))
                price = int(round(price * 100)) #convert to an int so that the TPG doesn't have to deal with decimal points
                                

                logger.log("Item Price: " + str(price)); 

                qty = item[5]
                itemObject = Agilysys.OrderItem(levy_item_number, qty,  price, 'no', 1, 1)
                itemObjects.append(itemObject)


            #pre-calculate the ig order total            
            igOrderDetails = agilysys.calculateOrder(itemObjects) 
            igOrderTotal = igOrderDetails['orderAmount']
                        
            logger.log("Item Total: " + str(itemTotal)); 
            
            #serviceCharge
            serviceCharge = OTDB.getOrderServiceChargeSum(order[0])
            if serviceCharge > 0:
                serviceCharge = int(rount(serviceCharge * 100)) #convert the service charge into pennies
                logger.log("Total Service Charge: " + str(serviceCharge))
            else:
                serviceCharge = None #set in to None so that the Agilysys methods will ignore it
                logger.log("No service charge")
            #adjust for sloppy rounding


            logger.log("Fetching Payments")
            payments = OTDB.getOrderPayment(order[0])
            paymentObjects = []
           
            #turn the payment db rows into an array of Agilysys,OrderPayment objects
            paymentTotal = 0
            taxTotal = 0
            for payment in payments:
                logger.log("Appending Payment")
                #just to make reading a little easier
                orderPaymentUid = payment[0]
                subtotal = payment[1]
                

                
                discount = payment[2]
                tip = payment[3]
                tax = payment[4]
                taxTotal = taxTotal + int(round(tax * 100))
                logger.log('TAX: ' + str(tax))
                orderPayMethodUid = payment[5]
                 
                logger.log("Tender Amount Total = " + str(subtotal) + " + " + str(tax) + " + " + str(tip) + " - " + str(discount) )    
                tenderAmountTotal = int(round(((subtotal + tax + tip) - discount) * 100)) # *100 because Levy does all of their transactions in cents (no decimal points!!!!)

                paymentTotal = paymentTotal + int(round((subtotal + tax) * 100))

                tenderUid = OTDB.getLevyTenderType(orderPaymentUid, orderPayMethodUid)
               
                tip = int(tip * 100) 
                paymentObject = Agilysys.OrderPayment(tenderUid, tenderAmountTotal, tip)  

                paymentObjects.append(paymentObject)


            
            parametricSubtotal = int(round(itemTotal * 100)) + taxTotal
            logger.log("PARAMETRIC SUBTOTAL: " + str(parametricSubtotal))
            logger.log("IG SUBTOTAL: " + str(igOrderTotal))
            difference = parametricSubtotal - int(igOrderTotal)
            logger.log("DIFFERENCE: " + str(difference))
            


            if difference < 0:
                logger.log("We didn't give them enough money... padding first payment")
                paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total + abs(difference)

            #if order[4] == 1:
            #    logger.log("This order is tax exempt") 
            #    tax = igOrderDetails['tax'] 
            #    logger.log("Tax to remove: " + tax)
            #    paymentObjects[0].tender_amount_total = paymentObjects[0].tender_amount_total - int(tax)                                       

            result = None    
            if order[1] == PREORDER:
                logger.log("Its a preorder!")
                #find the tpg order number
                referenceNumber = OTDB.getTPGReferenceNumber(order[0])
                logger.log("Reference Number = " + str(referenceNumber[0]))
                #put together the orderHeader with the referenceNumber
                checkType = OTDB.getCheckType(event[1], preorder=True)
                
                orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, orderNumber=referenceNumber, guestCount = 1, checkTypeId= checkType, receiptRequired = 'yes')
            else:
                
                checkType = OTDB.getCheckType(event[1], preorder=False)
 
                orderHeader = Agilysys.OrderHeader(tableName, employeeId, profitCenterId, guestCount=1, checkTypeId=checkType, receiptRequired='yes')
            
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

                    accountNumber, guestName = OTDB.getLevyPatronData(patronUid)

                    logger.log("Adding Payments")

                    paymentSuccess = True

                    for payment in paymentObjects:
                        if payment.tender_id == DIRECT_BILL:
                            logger.log("Adding DIRECT BILL payment")
                            accountNumber, guestName = OTDB.getLevyPatronData(patronUid)
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
                        elif payment in (AMERICAN_EXPRESS, DISCOVER, MASTERCARD, VISA):
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
                        failedOrders.append(order[0])
                        continue #processing orders
                    else:
                        logger.log("All payments successfully added, attempting to close order")
                        finalizeResponse = agilysys.finalzieOrder(orderNumber, employeeId)       
                        
                        if finalizeResponse['success']:
                            logger.log("Order Finalized Successfully")
                        else:
                            logger.log("Order failed to Finalize")
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
                    
                        accountNumber, guestName = OTDB.getLevyPatronData(patronUid)

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
                            if difference != 0:
                                OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference)    
                                logger.log("SUCCESS!!!!!!\n\n")
                                OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
                                OTDB.markOrderTransfered(order[0])
                
                        else:
                            logger.log("***ERROR Room Charge Failed")
                            logger.log(str(roomAuthorizationResponse))
                            failedOrders.append(order[0])        

                    else:
                        logger.log("***ERROR: Open order failed!")
                        errorMessage = openOrderResponse['errorMessage']
                        logger.log("     " + errorMessage)
                        failedOrders.append(order[0])
                   
            

                elif payment.tender_id in (AMERICAN_EXPRESS, DISCOVER, MASTERCARD, VISA):
                    logger.log("CREDIT CARD ORDER")
                    openOrderResponse = agilysys.openOrder(orderHeader, items =itemObjects, payments = paymentObjects, closeImmedietly = True)
                  
                    if openOrderResponse['success']:
                        # recording the difference
                        if difference != 0:
                            OTDB.recordOrderDifference(order[0], openOrderResponse['orderNumber'], parametricSubtotal, igOrderTotal, difference) 
                            logger.log("SUCCESS!!!!!!\n\n")
                            OTDB.recordTPGOrderNumber(openOrderResponse['orderNumber'], event[1], order[0])
                    else:
                        logger.log("***ERROR: Open order failed!")
                        errorMessage = openOrderResponse['errorMessage']
                        logger.log("     " + errorMessage)
                        failedOrders.append(order[0])
#                    logger.log("CREDIT CARD ORDER")
#
#                    openOrderResponse = agilysys.openOrder(orderHeader, itemObjects)
#                    
#                    if openOrderResponse['success']:
#                        logger.log("Order opened successfully")
#                        orderNumber = openOrderResponse['orderNumber']
#
#                        accountNumber = accountNumbers[payment.tender_id]
#                        logger.log("Account Number = " + accountNumber)
#                        genericAuthResult = agilysys.authorizeGenericAccount(orderNumber,
#                                                                             profitCenterId,
#                                                                             employeeId,
#                                                                             payment.tender_id,
#                                                                             payment.tender_amount_total,
#                                                                             payment.tip_amount,
#                                                                             accountNumber)
#                        if(genericAuthResult['success']):
#                            logger.log("Generic Auth Payment Successfully added")
#                            logger.log("Attempting to finalize the order")
#                            logger.log(str(orderNumber) + " " + str(employeeId))
#                            finalizeResponse = agilysys.finalizeOrder(orderNumber, employeeId)
#                    
#                            if finalizeResponse['success']:
#                                logger.log("Order Finalized Successfully")
#                            else:
#                                logger.log("Order failed to finalize")
#                                failedOrders.append(order[0])
#
#                        else:
#                            logger.log("Gerneirc Auth Payment failed to add")
#                            failedOrders.append(order[0])
#                        

                else:
                    logger.log("Payment is not direct bill or a credit card, I don't know what to do with it")
                    failedOrders.append(order[0])
            else:
                logger.log("EMPTY ORDER: There are no payments")
                logger.log("SKIPPING TRANSFER")
                OTDB.markOrderTransfered(order[0])
                         

        except Exception as e:
            tb = traceback.format_exc()
            logger.log(tb)
            eventTransfered = False
            failedOrders.append(order[0])


    #remove this
    #eventTransfered = True
    #failedOrders = []

    if eventTransfered and len(failedOrders) == 0:
        hipChatColor = 'green'
        message = "Event " + str(event[0]) + " transfered with no problems!"
        OTDB.markEventTransfered(event[0]) 
        logger.log("Event marked transfered")
   	HipChat.sendMessage(message, "OrderTransfers", "1066556", hipChatColor)
    else:
        hipChatColor = 'red'
        message = "Event " + str(event[0]) + " encounted failures when trying to transfer orders: " + "".join((str(w) + ", " for w in failedOrders))
        HipChat.sendMessage(message, "OrderTransfers", "1066556", hipChatColor)
 
