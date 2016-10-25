from suds.client import Client

wsdlUrl =  'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'

client = Client(wsdlUrl)

menus = client.service.GetMenuList(FacilityID = 98)


def getOrders():
    return client.service.GetOrdersForFacility(EventCalendarID='a84df6e2-d71e-408d-8ee3-6f1fa4e0159f', FacilityID = 98)

def countOrders():
    orders = getOrders()
    return len(orders[0])


