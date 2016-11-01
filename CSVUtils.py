import csv

def parseCSVFile(csvFile):
    fileSample = csvFile.read(1024)
    dialect = csv.Sniffer().sniff(fileSample)
    csvFile.seek(0)
    reader = csv.reader(csvFile, dialect)

    #skip the header if the sniffer detects one
    if csv.Sniffer().has_header(fileSample):
        csvFile.seek(0) #rewind
        next(reader, None) #skip the header

    del fileSample #make sure that we clean up after ourselves 

    return reader
