import SOAP
import xml.etree.ElementTree as ElementTree
from Agilysys import Agilysys

#test Sox prod
#agilysys = Agilysys('http://85.190.181.143:7008', '7300', 'levyso')

#test Nats prod
#agilysys = Agilysys('http://31.220.68.61:7008', '62300', 'levywa')

#test UC prod
#agilysys = Agilysys('http://85.190.177.240:7008', '65300', 'LEVYUC')

#DEV TPG from outside of the office
#agilysys = Agilysys('http://73.165.252.236:7008', '999', 'BBQ')

#DEV TPG from inside of the office
#agilysys = Agilysys('http://10.1.10.139:7008', '999', 'BBQ', '51')

#201 prod test server
#agilysys = Agilysys('http://153.92.36.65:7008', '65300', 'LEVYUC')

#experimental
agilysys = Agilysys('http://209.234.188.46:7008', '65300', 'LEVYUC')

orderHeader = Agilysys.OrderHeader(123, 1, 57) #add an extra param for order number

#item = (id, quantity, price, item-kitchen-print-indicator, seat-number)
#agedWhiteCheddar = Agilysys.OrderItem(1116, 2, 3.29, 'yes', 1)
items = []
items.append(Agilysys.OrderItem(1, 1, 5000, 'no' , 1, 1))



#payment = (tenderId, tenderAmountTotal, tipAmount)
payment = Agilysys.OrderPayment(1, 2000, 5)

#print SOAP.xmlToString(item.getXmlElement())
print "Sending Calc Request"
print  agilysys.calculateOrder(items)



