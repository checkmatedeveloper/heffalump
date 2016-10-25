from time import strftime
import os
import os.path
from Agilysys import Agilysys

class OrderTransferLogger:

    def __init__(self, venueUid, eventUid):
        self.venueUid = str(venueUid)
        self.eventUid = str(eventUid)
       
       
        
        try:
            os.makedirs(os.path.dirname(self.getLogFilePath())); 
        except:
            print "Dir eixsts"

        if not os.path.isfile(self.getLogFilePath() + self.getLogFileName()):
            self.logFile = open(self.getLogFilePath() + self.getLogFileName(), 'a+')
        else:
            self.logFile = open(self.getLogFilePath() + self.getLogFileName(), 'a')
      
 
        
    def getLogFilePath(self):
        date = strftime('%Y%m%d')
        return '/var/log/order_transfer/venue_' + self.venueUid + '/'

    def getLogFileName(self):
        date = strftime('%Y%m%d')
        return date + '_' + 'event_' + self.eventUid + ".log"

    def log(self, message):
        logMessage = strftime("%Y-%m-%d %H:%M:%S") + " - " + str(message)
        self.logFile.write(logMessage + "\n")


    def logParams(self, params, label="PARAMS: "):
        #print str(params)
        logMessage = "                           " + label

        
        if isinstance(params, list):
            self.logFile.write("                           " + label + "\n")
            for param in params:
                self.logParams(param, "     - ") #print the params with a bullet point in front of them    
            return #were done here

        if isinstance(params, Agilysys.XMLElement):
            objectParams = {}
            for field in dir(params):
                if not field.startswith('__') and not callable(getattr(params,field)) and field != 'self':
                    objectParams[field] = getattr(params, field)
            #objectParams = [a for a in dir(params) if not a.startswith('__') and not callable(getattr(params,a))]
            for key in objectParams.keys():
                logMessage = logMessage + key + ": " + str(objectParams[key]) + ", "
        else:

            for key in params.keys():
                if key != 'self':
                    logMessage = logMessage + key + ": " + str(params[key]) + ", " 


        self.logFile.write(logMessage + "\n")
            
