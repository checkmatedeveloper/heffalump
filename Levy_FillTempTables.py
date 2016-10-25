import os.path
import CSVUtils
import traceback
#from Levy_ThreadTempTables import FillTempTable

def makeTempTablesThreaded(employeesCSV, suitesCSV, customersCSV, eventsCSV, itemsCSV, pricesCSV, taxesCSV, serviceChargesCSV, packageDefinitionsCSV, dbCore):

    suitesThread = FillTempTable(taxesCSV, dbCore.clearTempLevyTaxesTable, dbCore.insertTempLevyTax, dbCore)
    eventsThread = FillTempTable(suitesCSV, dbCore.clearTempLevySuiteTable, dbCore.insertTempLevySuite, dbCore)
    itemsThread = FillTempTable(itemsCSV, dbCore.clearTempLevyItemTable, dbCore.insertTempLevySuite, dbCore)
    pricesThread = FillTempTable(pricesCSV, dbCore.clearTempLevyItemPriceTable, dbCore.insertTempLevyItemPrice, dbCore)
    taxesThread = FillTempTable(taxesCSV, dbCore.clearTempLevyTaxesTable, dbCore.insertTempLevyTax, dbCore)
    serviceChargeThread = FillTempTable(serviceChargesCSV, dbCore.clearTempLevyServiceChargesTable, dbCore.insertTempLevyServiceCharge, dbCore)
    packageDefinitionThread = FillTempTable(packageDefinitionsCSV, dbCore.clearTempLevyPackageDefinitionsTable, dbCore.insertTempLevyPackageDefinition, dbCore)

    suitesThread.start()
    eventsThread.start()
    itemsThread.start()
    pricesThread.start()
    taxesThread.start()
    serviceChargeThread.start()
    packageDefinitionThread.start()

    return True, [], []

def makeTempTables(employeesCSV, suitesCSV, customersCSV, eventsCSV, itemCSV, priceCSV, taxesCSV, serviceChargesCSV, packageDefinitionsCSV, dbCore):
    
    success = True
    missingFiles = []
    sqlErrorRows = []
    try:
    #########   EMPLOYEES 
#        if os.path.isfile(employeesCSV):
#            dbCore.addLogRow("Attempting to populate levy_temp_employees")
#            dbCore.clearTempLevyEmployeeTable()
#            with open(employeesCSV, 'rb') as csvFile:
#                reader = CSVUtils.parseCSVFile(csvFile)
#
#                for row in reader:
#                    try:
#                        dbCore.insertTempLevyEmployee(row)
#                    except:
#                        tb = traceback.format_exc()
#                        errorRow = dbCore.addLogRow(tb)
#                        sqlErrorRows.append(errorRow)
#        else: 
#            success = False
#            missingFiles.append('Employees')
#            dbCore.addLogRow(employeesCSV + " dosn't exist")

    #########   SUITES
        if os.path.isfile(suitesCSV):
            dbCore.addLogRow("Attempting to populate levy_temp_suites")
            dbCore.clearTempLevySuiteTable()
            with open(suitesCSV) as csvFile:
                reader = CSVUtils.parseCSVFile(csvFile)

                for row in reader:
                    try:
                        dbCore.insertTempLevySuite(row)
                    except:
                        tb = traceback.format_exc()
                        errorRow = dbCore.addLogRow(tb)
                        sqlErrorRows.append(errorRow)
        else:
            success = False
            missingFiles.append("Suites")
            dbCore.addLogRow(suitesCSV + "doesn't exist")

    ########    CUSTOMERS
        if os.path.isfile(customersCSV):
            dbCore.addLogRow("Attempting to populate levy_temp_customers")
            dbCore.clearTempLevyCustomerTable()
            with open(customersCSV) as csvFile:
                reader = CSVUtils.parseCSVFile(csvFile)
                for row in reader:
                    try:
                        dbCore.insertTempLevyCustomer(row)
                    except:
                        tb = traceback.format_exc()
                        errorRow = dbCore.addLogRow(tb)
                        sqlErrorRows.append(errorRow)
        else: 
            success = False
            missingFiles.append("Customers")
            dbCore.addLogRow(customersCSV + " doesn't exist")

    ########    EVENTS
        if os.path.isfile(eventsCSV):
            dbCore.addLogRow("Attempting to populate levy_temp_events")
            dbCore.clearTempLevyEventTable()
            with open(eventsCSV) as csvFile:
                reader = CSVUtils.parseCSVFile(csvFile)
                for row in reader:
                    try:
                        dbCore.insertTempLevyEvent(row)
                    except:
                        tb = traceback.format_exc()
                        errorRow = dbCore.addLogRow(tb)
                        sqlErrorRows.append(errorRow)
        else:
            success = False
            missingFiles.append("Events")
            dbCore.addLogRow(eventsCSV + " doesn't exist")

    ########    ITEMS
        if os.path.isfile(itemCSV):
            dbCore.addLogRow("Attempting to populate levy_temp_menu_items")
            dbCore.clearTempLevyItemTable()
            with open(itemCSV, 'rb') as csvFile:
            
                reader = CSVUtils.parseCSVFile(csvFile)
            
                for row in reader:
                    print str(row)
                    try:
                        dbCore.insertTempLevyItem(row)
                    except:
                        tb = traceback.format_exc()
                        errorRow = dbCore.addLogRow(tb)
                        sqlErrorRows.append(errorRow)
        else:
            success = False
            missingFiles.append("Menu Items")
            dbCore.addLogRow(itemCSV + " doesn't exist")
     
    ########    ITEM PRICES
        if os.path.isfile(priceCSV):
            dbCore.addLogRow("Attempting to populate levy_temp_menu_prices")
            dbCore.clearTempLevyItemPriceTable()
            with open(priceCSV, 'rb') as csvFile:
                
                reader = CSVUtils.parseCSVFile(csvFile)
            
                for row in reader:
                    try:
                        dbCore.insertTempLevyItemPrice(row)
                    except:
                        tb = traceback.format_exc()
                        dbCore.addLogRow(str(row))
                        errorRow = dbCore.addLogRow(tb)
                        sqlErrorRows.append(errorRow)
        else:
            success = False
            missingFiles.append("Item Prices")
            dbCore.addLogRow(priceCSV + " doesn't exist")
        
    except:
        #just in case something go wrong along the way that isn't related to db row insertion :)
        tb = traceback.format_exc()
        dbCore.addLogRow(tb)        

    ########### TAXES: might need to be updated onece we get files from Todd
    if os.path.isfile(taxesCSV):
        dbCore.addLogRow("Attempting to populate levy_temp_taxes")
        dbCore.clearTempLevyTaxesTable()
        with open(taxesCSV, 'rb') as csvFile:
            reader = CSVUtils.parseCSVFile(csvFile)

            for row in reader:
                try:
                    dbCore.insertTempLevyTax(row)
                except:
                    tb = traceback.format_exc()
                    print tb
                    dbCore.addLogRow(str(row))
                    errorRow = dbCore.addLogRow(tb)
                    sqlErrorRows.append(errorRow)
    else:
        
        success = False
        missingFiles.append("Taxes")
        dbCore.addLogRow(taxesCSV + " doesn't exist")

    ######### SERVICE CHARGES
    if os.path.isfile(serviceChargesCSV): 
        dbCore.addLogRow("Attempting to populate levy_temp_service_charges")
        dbCore.clearTempLevyServiceChargesTable()
        with open(serviceChargesCSV, 'rb') as csvFile:
            reader = CSVUtils.parseCSVFile(csvFile)

            for row in reader:
                try:
                    dbCore.insertTempLevyServiceCharge(row)
                except:
                    tb = traceback.format_exc()
                    print tb
                    dbCore.addLogRow(str(row))
                    errorRow = dbCore.addLogRow(tb)
                    sqlErrorRows.append(errorRow)

        
    else:
        success = False
        missingFiles.append("Service Charges")
        dbCore.addLogRow(serviceChargesCSV + " doesn't exist")

    
    ############# PACKAGE DEFINITIONS
    if os.path.isfile(packageDefinitionsCSV):
        dbCore.addLogRow("Attempting to populate levy_temp_package_definitions")
        dbCore.clearTempLevyPackageDefinitionsTable()
        with open(packageDefinitionsCSV, 'rb') as csvFile:
            reader = CSVUtils.parseCSVFile(csvFile)
            
            for row in reader:
                try:
                    dbCore.insertTempLevyPackageDefinition(row) 
                except:
                    tb = traceback.format_exc()
                    dbCore.addLogRow(str(row))
                    errorRow = dbCore.addLogRow(tb)
                    sqlErrorRows.append(errorRow)

    else:
        success = False
        missingFiles.append("Package Definitions")
        dbCore.addLogRow(packageDefinitionsCSV + " doesn't exist")
        

    return success, missingFiles, sqlErrorRows
