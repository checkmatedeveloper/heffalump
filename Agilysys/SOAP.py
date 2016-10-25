import requests
import xml.etree.ElementTree as ElementTree

SOAPEnvelope = '''
                    <SOAP-ENV:Envelope xmlns:xsi="http://www.w3.org/1999/XMLSchema/instance" 
                     xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope" 
                     xsi:schemaLocation="http://www.infoGenesis.com/schemas/ver1.4/POSTransGatewaySchema.xsd">
                    
                    </SOAP-ENV:Envelope>
                '''
def sendSOAP(url, body):

    #handle input as string or xml
    if isinstance(body, str):
        request = requests.post(url, body, timeout=31)
    else:
        request = requests.post(url, xmlToString(body), timeout=31)
    
    #return the result as an xml object
    return ElementTree.fromstring(request.text)


def getSOAPEnvelope():
    return ElementTree.fromstring(SOAPEnvelope)

def xmlToString(xmlObj):
    #if not isinstance(xmlObj, ElementTree.Element):
    #    print "Its not an element!!!"
    return ElementTree.tostring(xmlObj, encoding='utf8')


    
    
