import SOAP
import xml.etree.ElementTree as ElementTree
import time
class Agilysys: 
    
    #use this as the base of any xml element object abstraction
    class XMLElement:
        def getXmlElement(self):
            #all classes that inherit from this class need to have a field called 
            #elementName

            fields = self.__dict__
            
            E = ElementTree.Element(fields['elementName'])
            for field in fields.keys():
                if field != 'elementName' and  fields[field] is not None:
                    element = ElementTree.Element(field.replace('_', '-'))
                    element.text = str(fields[field])
                    E.append(element)
            return E

    #used in just about every order request
    class OrderHeader(XMLElement):
        def __init__(self, tableName, employeeId, profitCenter, teamId = None, guestCount = None, checkTypeId = None, receiptRequired = 'yes', orderNumber=None):
            self.elementName = 'order-header'
            self.order_number = orderNumber
            self.table_name = tableName
            self.team_id = teamId
            self.employee_id = employeeId
            self.guest_count = guestCount
            self.profitcenter_id = profitCenter
            self.check_type_id = checkTypeId
            self.receipt_required = receiptRequired

    class OrderItem(XMLElement):
        def __init__(self, id, quantity, price, kitchenPrintIndicator, seatNumber, courseNumber):
            self.elementName = 'item'
            self.item_id = id
            self.item_quantity = quantity
            self.item_price = price
            self.item_kitchen_print_indicator = kitchenPrintIndicator
            self.seat_number = seatNumber
            self.course_number = courseNumber

        def adjustPrice(self, difference):
            #print "Adjusting: " + str(self.item_price) + " by " + str(difference) 
            if self.item_quantity == 1:
                self.item_price = self.item_price + int(difference)
                return None
            else:
                self.item_quantity = self.item_quantity - 1
                return Agilysys.OrderItem(self.item_id, 1, self.item_price + int(difference), self.item_kitchen_print_indicator, self.seat_number, self.course_number)
    
    class OrderPayment(XMLElement):
        def __init__(self, tenderId, tenderAmountTotal, tipAmount):
            self.elementName = 'payment-data'
            self.tender_id = tenderId
            self.tender_amount_total = tenderAmountTotal
            self.tip_amount = tipAmount

    

    def __init__(self, url, clientId, authenticationCode, sessionId=None):
        self.url = url
        self.clientId = str(clientId)
        self.authenticationCode = str(authenticationCode)
        self.sessionId = sessionId
        self.logger = None
    
    def setLogger(self, logger):
        self.logger = logger

    def getTransServiceHeader(self):
        transServiceHeader = ElementTree.Element('trans-services-header')
        
        clientId = ElementTree.Element('client-id')
        clientId.text = self.clientId
        transServiceHeader.append(clientId)
        
        authenticationCode = ElementTree.Element('authentication-code')
        authenticationCode.text = self.authenticationCode
        transServiceHeader.append(authenticationCode)

        if self.sessionId is not None:        
            sessionId = ElementTree.Element('session-id')
            sessionId.text = self.sessionId
            transServiceHeader.append(sessionId)        

        return transServiceHeader
    
    def xmlPackageHeader(self, headerParams):
        #header = ElementTree.Element('header')

        if headerParams is None:
            return header #if we don't have any header params just return the empty element        
 
        for key in headerParams.keys():
            element = ElementTree.Element(key)
            element.text = str(headerParams[key]) 
            paramsElement.append(element)
         
        return header
    
    def xmlPackageParams(self, elementName, params):
        paramsElement = ElementTree.Element(elementName)

        for key in params.keys():
            element = ElementTree.Element(key)
            element.text = str(params[key])
            paramsElement.append(element)

        return paramsElement

    #returns the FIRST xml value with matching tag name
    def getXmlValue(self, root, name):
        for element in  root.getiterator(name): #please ignore this for loop, it was the only way to get data our of the iter :(
            return element.text
        return None


    def buildResponseDict(self, xmlResponse):
        response = {}
        response['raw'] = SOAP.xmlToString(xmlResponse)
        serviceCompletionStatus = self.getXmlValue(xmlResponse, 'service-completion-status')
       
         
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True

        if response['success']:
            response['orderNumber'] = self.getXmlValue(xmlResponse, 'order-number')

            #this might not be present if a user closes the order immedietly
            response['orderGuid'] = self.getXmlValue(xmlResponse, 'order-guid')    
        else:
            response['serviceErrorId'] = self.getXmlValue(xmlResponse, 'service-error-id')
            response['serviceErrorDisplayMessage'] = self.getXmlValue(xmlResponse, 'service-error-display-message')
        return response
          

    def calculateOrder(self, items, service_charge = None):
        
        envelope = SOAP.getSOAPEnvelope() 

        calculateOrderAmountRequestBody = ElementTree.Element('calculate-order-amount-request-Body')
        calculateOrderAmountRequest = ElementTree.Element('calculate-order-amount-request')

        calculateOrderAmountRequest.append(self.getTransServiceHeader())

        orderBody = ElementTree.Element('order-body')
        for item in items:
            orderBody.append(item.getXmlElement())

        if service_charge is not None:
            serviceCharge = ElementTree.Element('service-charge')
            serviceCharge.text = str(service_charge)
            orderBody.append(serviceCharge)

        calculateOrderAmountRequest.append(orderBody)

        calculateOrderAmountRequestBody.append(calculateOrderAmountRequest)
      
        envelope.append(calculateOrderAmountRequestBody)

        if self.logger is not None:
            self.logger.log("CALC ORDER -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")
 
        order = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("CALC ORDER -- TPG RESPONSE: \n\n" + SOAP.xmlToString(order) + "\n\n")

        response = {}

        response['raw'] = SOAP.xmlToString(order)
        serviceCompletionStatus = self.getXmlValue(order, 'service-completion-status')

        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True

        if response['success']:
            response['orderAmount'] = self.getXmlValue(order, 'order-amount')
            response['tax'] = self.getXmlValue(order, 'tax')
        return response

    def checkPreorder(self, profitCenterId, employeeId, orderNumber):
        envelope = SOAP.getSOAPEnvelope()
    
        orderDetailRequestBody = ElementTree.Element('order-detail-request-Body')

        orderDetailRequest = ElementTree.Element('order-detail-request')

        orderDetailRequest.append(self.getTransServiceHeader())

        profitcenter_id = ElementTree.Element('profitcenter-id')
        profitcenter_id.text = str(profitCenterId)
        orderDetailRequest.append(profitcenter_id)

        employee_id = ElementTree.Element('employee-id')
        employee_id.text = str(employeeId)
        orderDetailRequest.append(employee_id)

        order_number = ElementTree.Element('order-number')
        order_number.text = str(orderNumber)
        orderDetailRequest.append(order_number)

        orderDetailRequestBody.append(orderDetailRequest)
    
        envelope.append(orderDetailRequestBody)

        if self.logger is not None:
            self.logger.log("ORDER DETAILS -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        #first attempt
        orderDetails = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("ORDER DETAILS -- TPG RESPONSE: \n\n" + SOAP.xmlToString(orderDetails) + "\n\n")

        #check if we got a good return, if not try twice more
        serviceCompletionStatus = self.getXmlValue(orderDetails, 'service-completion-status')
        if serviceCompletionStatus != 'ok':
            if self.logger is not None:
                self.logger.log("The TPG Seems to think that this order doesn't exist: " + str(orderNumber))
            retries = 2            
            while retries > 0:
                time.sleep(20) #sleep for 5 seconds
                orderDetails = SOAP.sendSOAP(self.url, envelope) #retry the request
                if self.logger is not None:
                    self.logger.log("ORDER DETAILS ( R E T R Y ) -- TPG RESPONSE: \n\n" + SOAP.xmlToString(orderDetails) + "\n\n")
                serviceCompletionStatus = self.getXmlValue(orderDetails, 'service-completion-status')
                if serviceCompletionStatus == 'ok': 
                    break #if we are breaking out of the loop we must be good
                retries = retries - 1 #one less retry       
        
        #do one last check to make sure that after all those retries we finally get a result
        serviceCompletionStatus = self.getXmlValue(orderDetails, 'service-completion-status')
        if serviceCompletionStatus != 'ok':
            raise Exception("order-details-request FAILED") #if not we have nothing we can do except fail the order

        #if we got here then we must be good
        orderData = orderDetails.find(".//order-data")
        
        orderDataCount = len(list(orderData))
        noItems = True
        if orderDataCount > 0:
            noItems = False

        paymentData = orderDetails.find(".//payment-data")
        paymentDataCount = len(list(paymentData))
        noPayments = True
        if paymentDataCount > 0:
            noPayments = False

        return noItems, noPayments
       

    def addItemsToOrder(self, orderHeader,
                             items,
                             payments):
        
        envelope = SOAP.getSOAPEnvelope()

        processOrderRequestBody = ElementTree.Element('process-order-request-Body')

        processOrderRequest = ElementTree.Element('process-order-request')

        processOrderRequest.append(self.getTransServiceHeader())
        
        

        orderType = ElementTree.Element('order-type')
        orderType.text = 'open' #might need to be close
        processOrderRequest.append(orderType)

        processOrderRequest.append(orderHeader.getXmlElement())

        #order body
        orderBody = ElementTree.Element('order-body')
        for item in items:
            orderBody.append(item.getXmlElement())
        processOrderRequest.append(orderBody)

        #order_payment
        orderPayment = ElementTree.Element('order-payment')
        for payment in payments:
            orderPayment.append(payment.getXmlElement())
        processOrderRequest.append(orderPayment)

        processOrderRequestBody.append(processOrderRequest)

        envelope.append(processOrderRequestBody)

       # print SOAP.xmlToString(envelope)

        #send SOAP
        order = SOAP.sendSOAP(self.url, envelope)

        response = {}
       
        response['raw'] = SOAP.xmlToString(order)
        serviceCompletionStatus = self.getXmlValue(order, 'service-completion-status')

        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True

        if response['success']:
            response['orderNumber'] = self.getXmlValue(order, 'order-number')
        else:
            response['errorMessage'] = self.getXmlValue(order, 'service-error-display-message') 
        
        return response


    #will hit the TPG server and open a new order
    # will return a dict with the following values: success, orderNumber, orderGuid
    def openOrder(self, orderHeader, items = None, service_charge = None, payments = None, closeImmedietly = False):

        envelope = SOAP.getSOAPEnvelope()
        
        processOrderRequestBody = ElementTree.Element('process-order-request-Body')        

       # header = self.xmlPackageHeader(None)
       # processOrderRequestBody.append(header)


        processOrderRequest = ElementTree.Element('process-order-request')
        
        processOrderRequest.append(self.getTransServiceHeader())



        #ORDER TYPE
        orderType = ElementTree.Element('order-type')
        #allow immediet closing if certian conditions are met
    

        #there can only be one payment to close immedietly
        if closeImmedietly == True and payments is not None and len(payments) != 0 and not len(payments) > 1:
            orderType.text = 'closed'
            if self.logger is not None:
                self.logger.log("Marking order closed")
        else:
            orderType.text = 'open'
            if self.logger is not None and payments is not None:
                self.logger.log("Marking order open: closeImmedietly: " + str(closeImmedietly) + " number of payments: " + len(payments))

        processOrderRequest.append(orderType)



        if items is not None:
            orderBody = ElementTree.Element('order-body')
            for item in items:
                orderBody.append(item.getXmlElement())

            if service_charge is not None:
                serviceCharge = ElementTree.Element('service-charge')
                serviceCharge.text = str(service_charge)
                orderBody.append(serviceCharge)

            processOrderRequest.append(orderBody)

            #your not allowed to add a payment until you add items, 
            #also you can only pass in a payment this way if you are only sending one
            if payments != None and len(payments) == 1:
                orderPayment = ElementTree.Element('order-payment')
                for payment in payments:
                    orderPayment.append(payment.getXmlElement())
                processOrderRequest.append(orderPayment)
        
        processOrderRequest.append(orderHeader.getXmlElement())

        processOrderRequestBody.append(processOrderRequest)

        envelope.append(processOrderRequestBody)

       # print "\n\n" + SOAP.xmlToString(envelope) + "\n\n"
    
        if self.logger is not None:
            self.logger.log("OPEN ORDER -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")
        openOrder = SOAP.sendSOAP(self.url, envelope)
        
        if self.logger is not None:
            self.logger.log("OPEN ORDER -- TPG RESPONSE: \n\n" + SOAP.xmlToString(openOrder) + "\n\n") 
#        return self.buildResponseDict(openOrder)

        response = {}

        response['raw'] = SOAP.xmlToString(openOrder)
        serviceCompletionStatus = self.getXmlValue(openOrder, 'service-completion-status')

        #first attempt
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True
        else:
            #if we got here then the response was a failure
            if orderHeader.order_number is not None:
                if self.logger is not None:
                    self.logger.log("The TPG can't find our order: " + str(orderHeader.order_number))
                #if orderHeader.order_number is not none then this is a PREORDER and we only want to retry if its a preorder
                retries = 2
                while retries > 0:
                    time.sleep(20) #sleep for 5 seconds
                    openOrder = SOAP.sendSOAP(self.url, envelope) #retry the request
                    if self.logger is not None:
                        self.logger.log("OPEN ORDER ( R E T R Y ) -- TPG RESPONSE: \n\n" + SOAP.xmlToString(openOrder) + "\n\n")
                    serviceCompletionStatus = self.getXmlValue(openOrder, 'service-completion-status')
                    if serviceCompletionStatus == 'ok':
                        break #if we are breaking out of the loop we must be good
                    retries = retries - 1 #one less retry       

        #do one last check to make sure that after all those retries we finally get a result
        serviceCompletionStatus = self.getXmlValue(openOrder, 'service-completion-status')
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True
        else:
             raise Exception("open-order-request FAILED") #hard fail 


        if response['success']:
            response['orderNumber'] = self.getXmlValue(openOrder, 'order-number')
        else:
            response['errorMessage'] = self.getXmlValue(openOrder, 'service-error-display-message')

        return response

    def finalizeOrder(self, order_number, employee_id):
        if self.logger is not None:
            self.logger.log("Finalizing Order with params:")
            self.logger.logParams(locals())

        envelope = SOAP.getSOAPEnvelope()
        
        finalizeOrderRequestBody = ElementTree.Element('finalize-order-request-Body')
        finalizeOrderRequest = ElementTree.Element('finalize-order-request')
        finalizeOrderRequest.append(self.getTransServiceHeader())

        finalizeOrderData = ElementTree.Element('finalize-order-data')
        
        orderNumber = ElementTree.Element('order-number')
        orderNumber.text = str(order_number)
        finalizeOrderData.append(orderNumber)

        employeeId = ElementTree.Element('employee-id')
        employeeId.text = str(employee_id)
        finalizeOrderData.append(employeeId)

        receiptRequired = ElementTree.Element('receipt-required')
        receiptRequired.text = 'yes'
        finalizeOrderData.append(receiptRequired)

        finalizeOrderRequest.append(finalizeOrderData)
        
        finalizeOrderRequestBody.append(finalizeOrderRequest)
        
        envelope.append(finalizeOrderRequestBody)

        if self.logger is not None:
            self.logger.log("FINALIZE ORDER -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        finalizedOrderResponse = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("FINALIZE ORDER -- TPG RESPONSE: \n\n" + SOAP.xmlToString(finalizedOrderResponse))
        
        response = {}
    
        response['raw'] = SOAP.xmlToString(finalizedOrderResponse)
        serviceCompletionStatus = self.getXmlValue(finalizedOrderResponse, 'service-completion-status')   
     
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True
        
        return response
       

    def closeOrder(self, orderHeader, tender_amount_total, reference_number = None):
        if self.logger is not None:
            self.logger.log("Closing Order with params:")
            self.logger.logParams(locals())
        envelope = SOAP.getSOAPEnvelope()
        
        processOrderRequestBody = ElementTree.Element('process-order-request-Body')
        processOrderRequest = ElementTree.Element('process-order-request')
        processOrderRequest.append(orderHeader.getXmlElement())
        processOrderRequest.append(self.getTransServiceHeader())

        orderType = ElementTree.Element('order-type')
        orderType.text = "closed"
        processOrderRequest.append(orderType)

        orderPayment = ElementTree.Element('order-payment')
        paymentData = ElementTree.Element('payment-data')

        referenceKey = ElementTree.Element('reference-key')
        referenceKey.text = reference_number
        paymentData.append(referenceKey)

        tenderAmountTotal = ElementTree.Element('tender-amount-total')
        tenderAmountTotal.text = str(tender_amount_total)
        paymentData.append(tenderAmountTotal)

        orderPayment.append(paymentData)

        processOrderRequest.append(orderPayment)

        processOrderRequestBody.append(processOrderRequest)

        envelope.append(processOrderRequestBody)

        if self.logger is not None:
            self.logger.log("CLOSE ORDER -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        closedOrder = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("CLOSE ORDER -- TPG RESPONSE: \n\n" + SOAP.xmlToString(closedOrder) + "\n\n")

    def closeVoidedOrder(self, orderHeader):
        
        if self.logger is not None:
            self.logger.log("Closing a voided TPG order")
            self.logger.logParams(locals())

        envelope = SOAP.getSOAPEnvelope()
        
        processOrderRequestBody = ElementTree.Element('process-order-request-Body')
        processOrderRequest = ElementTree.Element('process-order-request')
        processOrderRequest.append(orderHeader.getXmlElement())
        processOrderRequest.append(self.getTransServiceHeader())

        orderType = ElementTree.Element('order-type')
        orderType.text = "closed"
        processOrderRequest.append(orderType)

        orderPayment = ElementTree.Element('order-payment')
        paymentData = ElementTree.Element('payment-data')

        tenderId = ElementTree.Element('tender-id')
        tenderId.text = "1" #tender-type = cash
        paymentData.append(tenderId)

        tipAmount = ElementTree.Element('tip-amount')
        tipAmount.text = "0"
        paymentData.append(tipAmount)

        tenderAmountTotal = ElementTree.Element('tender-amount-total')
        tenderAmountTotal.text = '0'
        paymentData.append(tenderAmountTotal)

        orderPayment.append(paymentData)
        processOrderRequest.append(orderPayment)

        processOrderRequestBody.append(processOrderRequest)

        envelope.append(processOrderRequestBody)

        if self.logger is not None:
            self.logger.log("CLOSE VOIDED ORDER -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        closedVoidedOrder = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("CLOSE VOIDED ORDER -- TPG RESPONSE: \n\n" + SOAP.xmlToString(closedVoidedOrder) + "\n\n")

        return SOAP.xmlToString(closedVoidedOrder)
    def addCashPayment(self, orderNumber, tableName, employeeUid,  payment):
        
        if self.logger is not None:
            self.logger.log("Adding CASH payment")
            self.logger.logParams(locals())

        envelope = SOAP.getSOAPEnvelope()
        
        addCashPaymentRequestBody = ElementTree.Element('add-cash-payment-request-Body')
        
        addCashPaymentRequest = ElementTree.Element('add-cash-payment-request')

        addCashPaymentRequest.append(self.getTransServiceHeader())

        addCashPaymentRequestData = ElementTree.Element('add-cash-payment-request-data')

        orderNumber = ElementTree.Element('order-number')
        orderNumber.text = str(orderNumber)
        addCashPaymentRequGestData.append(orderNumber)

        tableName = ElementTree.Element('table-name')
        tableName.text = str(tableName)
        addCashPaymentRequestData.append(tableName)

        employeeId = ElementTree.Element('employee-name')
        employeeId.text = str(employeeId)
        addCashPaymentRequestData.append(employeeId)        

        paymentAmount = ElementTree.Element('payment-amount')
        paymentAmount.attrib['tender-amount'] = payment.payment_tender_amount_total 
        paymentAmount.attrib['tip-amount'] = payment.tip_amount
        addCashPaymentRequestData.append(paymentAmount)

        addCashPaymentRequest.append(addCashPaymentRequestData)

        addCashPaymentRequestBody.append(addCashPaymentRequest)
        
        envelope.append(addCashPaymentRequestBody)

        if self.logger is not None:
            self.logger.log("ADD CASH PAYMENT -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        response = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("ADD CASH PAYMENT -- TPG RESPONSE: \n\n" + SOAP.xmlToString(response) + "\n\n")

        response = {}
        response['raw'] = SOAP.xmlToString(response)
    
        serviceCompletionStatus = self.getXmlValue(response, 'service-completion-status')
        response['success'] = False
        if serviceCompletionStatus == 'ok':
             response['success'] = True

        return response

    
    def authorizeGenericAccount(self, order_number, profit_center_id, employee_id, tender_id, tender_amount, tip_amount, account_number):
        
        envelope = SOAP.getSOAPEnvelope()
    
        genericAuthRequestBody = ElementTree.Element('generic-auth-request-Body')
        
        genericAuthRequest = ElementTree.Element('generic-auth-request')

        genericAuthRequest.append(self.getTransServiceHeader())

        genericAuthRequestData = ElementTree.Element('generic-auth-request-data')

        orderNumber = ElementTree.Element('order-number')
        orderNumber.text = str(order_number)
        genericAuthRequestData.append(orderNumber)
        
        profitCenterId = ElementTree.Element('profit-center-id')
        profitCenterId.text = str(profit_center_id)
        genericAuthRequestData.append(profitCenterId)

        employeeId = ElementTree.Element('employee-id')
        employeeId.text = str(employee_id)
        genericAuthRequestData.append(employeeId)

        tenderId = ElementTree.Element('tender-id')
        tenderId.text = str(tender_id)
        genericAuthRequestData.append(tenderId)        

        authAmount = ElementTree.Element('auth-amount')
        authAmount.attrib['tender-amount'] = str(tender_amount)
        authAmount.attrib['tip-amount'] = str(tip_amount)
        genericAuthRequestData.append(authAmount)
            
        accountNumber = ElementTree.Element('account-number')
        accountNumber.text = account_number
        genericAuthRequestData.append(accountNumber)

        genericAuthRequest.append(genericAuthRequestData)
        genericAuthRequestBody.append(genericAuthRequest)
        envelope.append(genericAuthRequestBody)

        if self.logger is not None:
            self.logger.log("GENERIC AUTH -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        authorization = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("GENERIC AUTH -- TPG RESPONSE: \n\n" + SOAP.xmlToString(authorization) + "\n\n")
    
        response = {}
        response['raw'] = SOAP.xmlToString(authorization)

        serviceCompletionStatus = self.getXmlValue(authorization, 'service-completion-status')
        
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True

        return response


    def authorizeRoomCharge(self, order_number, profit_center_id, table_name, employee_id, tender_id, tender_amount, tip_amount, room_number, guest_name, account_number): 

        envelope = SOAP.getSOAPEnvelope()
        
        roomAuthRequestBody = ElementTree.Element('room-auth-request-Body')  
        roomAuthRequest = ElementTree.Element('room-auth-request')
    
        roomAuthRequest.append(self.getTransServiceHeader())

        roomAuthRequestData = ElementTree.Element("room-auth-request-data")

        orderNumber = ElementTree.Element('order-number')
        orderNumber.text = str(order_number)        
        roomAuthRequestData.append(orderNumber)

        profitCenterId = ElementTree.Element('profit-center-id')
        profitCenterId.text = str(profit_center_id)
        roomAuthRequestData.append(profitCenterId)        

        tableName = ElementTree.Element('table-name')
        tableName.text = table_name
        roomAuthRequestData.append(tableName)

        employeeId = ElementTree.Element('employee-id')
        employeeId.text = str(employee_id)
        roomAuthRequestData.append(employeeId)

        authAmount = ElementTree.Element('auth-amount')
        authAmount.attrib['tender-amount'] = str(tender_amount)
        authAmount.attrib['tip-amount'] = str(tip_amount)
        roomAuthRequestData.append(authAmount)        

        roomNumber = ElementTree.Element('room-number')
        roomNumber.text = str(room_number)
        roomAuthRequestData.append(roomNumber)

        guestName = ElementTree.Element('guest-name')
        guestName.text = guest_name
        roomAuthRequestData.append(guestName)

        accountNumber = ElementTree.Element('account-number')
        accountNumber.text = str(account_number)
        roomAuthRequestData.append(accountNumber)


        roomAuthRequest.append(roomAuthRequestData)
        roomAuthRequestBody.append(roomAuthRequest)
        envelope.append(roomAuthRequestBody)
    
        if self.logger is not None:
            self.logger.log("ROOM AUTH -- TPG REQUEST: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        authorization = SOAP.sendSOAP(self.url, envelope)

        if self.logger is not None:
            self.logger.log("ROOM AUTH -- TPG RESPONSE: \n\n" + SOAP.xmlToString(envelope) + "\n\n")

        response = {}
        response['raw'] = SOAP.xmlToString(authorization)
        

        serviceCompletionStatus = self.getXmlValue(authorization, 'service-completion-status')
        response['success'] = False
        if serviceCompletionStatus == 'ok':
            response['success'] = True

        if(response['success']):
            response['referenceKey'] = self.getXmlValue(authorization, 'reference-key')

	return response
         
        
        
         


