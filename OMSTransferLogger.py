from time import strftime
import os
import os.path

class OrderTransferLogger:

    def __init__(self):

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
        return '/var/log/order_transfer/DIRECT_TRANSFER/'

    def getLogFileName(self):
        date = strftime('%Y%m%d')
        return date + ".log"

    def log(self, message):
        logMessage = strftime("%Y-%m-%d %H:%M:%S") + " - " + str(message)
        self.logFile.write(logMessage + "\n")
