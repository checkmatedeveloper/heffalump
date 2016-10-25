import threading
import os.path
import traceback
import CSVUtils
class FillTempTable(threading.Thread):

    def __init__(self, csvFilePath, clearTableFunction, insertRowFunction, dbCore):
        super(FillTempTable, self).__init__()
        self.csvFilePath = csvFilePath
        self.clearTableFunction = clearTableFunction
        self.insertRowFunction = insertRowFunction
        self.dbCore = dbCore

    def run(self):
        print "processing temp file: " + str(self.csvFilePath)
        if os.path.isfile(self.csvFilePath):
            self.dbCore.addLogRow("processing integration file: " + str(self.csvFilePath))
            self.clearTableFunction()

            with open(self.csvFilePath) as csvFile:
                reader = CSVUtils.parseCSVFile(csvFile)

                for row in reader:
                    try:
                        self.insertRowFunction(row)
                    except:
                        tb = traceback.format_exc()
                        errorRow = self.dbCore.addLogRow(tb)
            
        else:
            print "I can't find that file"            

