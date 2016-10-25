from Agilysys.Agilysys import Agilysys
from Agilysys.Order_Transfer_Logger import OrderTransferLogger


agilysys = Agilysys('http://73.165.252.236:7008', '999', 'BBQ')

logger = OrderTransferLogger(2258, 201) #test data

agilysys.setLogger(logger)

items, payments = agilysys.checkPreorder(57, 1, 9991913)

print "Has No Items: " + str(items) + ", Has No Payments: " + str(payments)
