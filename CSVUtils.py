import csv

def parseCSVFile(csvFile):
    dialect = csv.Sniffer().sniff(csvFile.read(1024))
    csvFile.seek(0)
    reader = csv.reader(csvFile, dialect)

    #skip the header if the sniffer detects one
    if csv.Sniffer().has_header(csvFile.read(1024)):
        csvFile.seek(0) #rewind
        next(reader, None) #skip the header

    return reader
