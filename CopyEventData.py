'''
Copy one event's data to another event.
'''

from CopyEventDB import CopyEventDB
from db_connection import DbConnection

import sys

conn = DbConnection().connection
copyEventDB = CopyEventDB(conn)

fromEventUid = sys.argv[1]
toEventUid = sys.argv[2]

orders = copyEventDB.getFromEventOrders(fromEventUid)

for order in orders:

    fromOrderUid = order[0]
    orderData = order[2:]

    toOrderUid = copyEventDB.addOrderToEvent(toEventUid, orderData)

    subOrders = copyEventDB.getSubOrders(fromOrderUid)

    for subOrder in subOrders:
        subOrderData = subOrder[2:]
        toSubOrderUid = copyEventDB.addSubOrder(toOrderUid, subOrderData)
        fromSubOrderUid = subOrder[0]
        orderItems = copyEventDB.getOrderItems(fromSubOrderUid)
        print str(orderItems)
        for orderItem in orderItems:
            orderItemUid = orderItem[0]
            orderItemData = orderItem[2:]
            toOrderItemUid = copyEventDB.addOrderItem(toSubOrderUid, orderItemData)

            orderItemXModificationsData = copyEventDB.getOrderItemsXModifications(orderItemUid)
            for data in orderItemXModificationsData:
                copyEventDB.addOrderItemsXModifications(toOrderItemUid, data)
            
            orderItemOptionsData = copyEventDB.getOrderItemOptions(orderItemUid)
            for data in orderItemOptionsData:
                copyEventDB.addOrderItemOptions(toOrderItemUid, data)            

            orderItemSplitsData = copyEventDB.getOrderItemSplits(orderItemUid)
            for data in orderItemSplitsData:
                copyEventDB.addOrderItemSplits(toOrderItemUid, data)            

            orderItemOriginalPricesData = copyEventDB.getOrderItemOriginalPrices(orderItemUid)
            for data in orderItemOriginalPricesData:
                copyEventDB.addOrderItemOriginalPrices(toOrderItemUid, orderItemOriginalPricesData)


    ordersXRevenueCenters = copyEventDB.getOrdersXRevenueCenters(fromOrderUid)
    for center in ordersXRevenueCenters:
        copyEventDB.addOrdersXRevenueCenters(toOrderUid, center[2:])


    orderPaymentPreauths = copyEventDB.getOrderPaymentPreauths(fromOrderUid)
    for preauth in orderPaymentPreauths:
        copyEventDB.addOrderPaymentPreauth(toOrderUid, preauth[2:])

    orderPayments = copyEventDB.getOrderPayments(fromOrderUid)
    for payment in orderPayments:
        fromOrderPaymentUid = payment[0]
        toOrderPaymentUid = copyEventDB.addOrderPayments(toOrderUid, payment[2:])
        
        orderPaymentXRevenueCenters = copyEventDB.getOrderPaymentXRevenueCenters(fromOrderPaymentUid)
        for pxrc in orderPaymentXRevenueCenters:
            copyEventDB.addOrderPaymentXRevenueCenters(toOrderPaymentUid, pxrc[2:])

    orderModifications = copyEventDB.getOrderModifications(fromOrderUid)
    for mod in orderModifications:
        newOrderModificationUid = copyEventDB.addOrderModifications(toOrderUid, mod)


    ordersXGratuities = copyEventDB.getOrdersXGratuities(fromOrderUid)
    for grat in ordersXGratuities:
        toOrderXGratuitiesUid = copyEventDB.addOrdersXGratuities(toOrderUid, grat[2:])
        fromOrderGratuityUid = grat[0]
        orderGratuitiesXRevenueCenters = copyEventDB.getOrderGratuitiesXRevenueCenters(fromOrderGratuityUid)
        for gxrc in orderGratuitiesXRevenueCenters:
            copyEventDB.addOrderGratuitiesXRevenueCenters(toOrderXGratuitiesUid, gxrc[2:])
        

    ordersXDiscounts = copyEventDB.getOrdersXDiscounts(fromOrderUid)
    for discount in ordersXDiscounts:
        toOrdersXDiscountsUid = copyEventDB.addOrdersXDiscounts(toOrderUid, discount[2:])
        fromOrderDiscountUid = discount[0]
        orderDiscountsXRevenueCenters = copyEventDB.getOrderDiscountsXRevenueCenters(fromOrderDiscountUid)
        for dxrc in orderDiscountsXRevenueCenters:
            copyEventDB.addOrderDiscountsXRevenueCenters(toOrdersXDiscountsUid, dxrc[2:])
