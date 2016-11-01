import os.path
import CSVUtils
import traceback
from Levy_ThreadTempTables import FillTempTable

def makeTempTablesThreaded(employeesCSV, suitesCSV, customersCSV, eventsCSV, itemsCSV, pricesCSV, taxesCSV, serviceChargesCSV, packageDefinitionsCSV, dbCore):

    suitesThread = FillTempTable(taxesCSV, dbCore.clearTempLevyTaxesTable, dbCore.insertTempLevyTax)
    eventsThread = FillTempTable(suitesCSV, dbCore.clearTempLevySuiteTable, dbCore.insertTempLevySuite)
    itemsThread = FillTempTable(itemsCSV, dbCore.clearTempLevyItemTable, dbCore.insertTempLevySuite)
    pricesThread = FillTempTable(pricesCSV, dbCore.clearTempLevyItemPriceTable, dbCore.insertTempLevyItemPrice)
    taxesThread = FillTempTable(taxesCSV, dbCore.clearTempLevyTaxesTable, dbCore.insertTempLevyTax)
    serviceChargeThread = FillTempTable(serviceChargesCSV, dbCore.clearTempLevyServiceChargesTable, dbCore.insertTempLevyServiceCharge)
    packageDefinitionThread = FillTempTable(packageDefinitionsCSV, dbCore.clearTempLevyPackageDefinitionsTable, dbCore.insertTempLevyPackageDefinition)

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
                print "File opened"
                reader = CSVUtils.parseCSVFile(csvFile)

                errorRows = insertRows(reader, "levy_temp_suites", ["suite_id", "suite_number", "entity_code", "suite_end_date", "created_at"], dbCore)
                if len(errorRows) > 0:
                    errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_suites: " + str(errorRows))
                    sqlErrorRows.append(errorLogRow)
 
                
                #for row in reader:
                #    try:
                #        dbCore.insertTempLevySuite(row)
                #    except:
                #        tb = traceback.format_exc()
                #        errorRow = dbCore.addLogRow(tb)
                #        sqlErrorRows.append(errorRow)
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
                
                errorRows = insertRows(reader, "levy_temp_customers", ["customer_number", "customer_name", "entity_code", "suite_number", "customer_suite_end_date", "created_at"], dbCore)
                if len(errorRows) > 0:
                    errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_customers: " + str(errorRows))
                    sqlErrorRows.append(errorLogRow)
                #for row in reader:
                #    try:
                #        dbCore.insertTempLevyCustomer(row)
                #    except:
                #        tb = traceback.format_exc()
                #        errorRow = dbCore.addLogRow(tb)
                #        sqlErrorRows.append(errorRow)
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
                
                errorRows = insertRows(reader, "levy_temp_events", ["entity_code", "event_id", "event_name", "event_type", "event_datetime", "cancelled", "created_at"], dbCore)
                if len(errorRows) > 0:
                    errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_events: " + str(errorRows))
                    sqlErrorRows.append(errorLogRow)
                #for row in reader:
                #    try:
                #        dbCore.insertTempLevyEvent(row)
                #    except:
                #        tb = traceback.format_exc()
                #        errorRow = dbCore.addLogRow(tb)
                #        sqlErrorRows.append(errorRow)
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
            
                errorRows = insertRows(reader, "levy_temp_menu_items", ["item_number", "package_flag", "item_name", "pos_button_1", "pos_button_2", "pos_printer_label", "pos_prod_class_id", "pos_product_class", "item_classification", "rev_id", "tax_id", "cat_id", "created_at"], dbCore)
                if len(errorRows) > 0:
                    errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_menu_items: " + str(errorRows))
                    sqlErrorRows.append(errorLogRow)

                #for row in reader:
                #    print str(row)
                #    try:
                #        dbCore.insertTempLevyItem(row)
                #    except:
                #        tb = traceback.format_exc()
                #        errorRow = dbCore.addLogRow(tb)
                #        sqlErrorRows.append(errorRow)
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
            
                errorRows = insertRows(reader, "levy_temp_menu_prices", ['entity_code', 'item_number', 'min_order', 'max_order', 'pos_level_id', 'main_price', 'pos_doe_level_id', 'doe_price', 'location_name', 'created_at'], dbCore)
                
                if len(errorRows) > 0:
                    errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_menu_prices: " + str(errorRows))
                    sqlErrorRows.append(errorLogRow)

                #for row in reader:
                #    try:
                #        dbCore.insertTempLevyItemPrice(row)
                #    except:
                #        tb = traceback.format_exc()
                #        dbCore.addLogRow(str(row))
                #        errorRow = dbCore.addLogRow(tb)
                #        sqlErrorRows.append(errorRow)
        else:
            success = False
            missingFiles.append("Item Prices")
            dbCore.addLogRow(priceCSV + " doesn't exist")
        
    except:
        #just in case something go wrong along the way that isn't related to db row insertion :)
        tb = traceback.format_exc()
        dbCore.addLogRow(tb)        
        print tb
    ########### TAXES: might need to be updated onece we get files from Todd
    if os.path.isfile(taxesCSV):
        dbCore.addLogRow("Attempting to populate levy_temp_taxes")
        dbCore.clearTempLevyTaxesTable()
        with open(taxesCSV, 'rb') as csvFile:
            reader = CSVUtils.parseCSVFile(csvFile)
    
            errorRows = insertRows(reader, "levy_temp_tax_rates", ['entity_code', 'category_name', 'tax_id', 'tax_rate', 'rev_id', 'cat_id', 'created_at'], dbCore)
            if len(errorRows) > 0:
                errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_taxes: " + str(errorRows))
                sqlErrorRows.append(errorLogRow)

            #for row in reader:
            #    try:
            #        dbCore.insertTempLevyTax(row)
            #    except:
            #        tb = traceback.format_exc()
            #        print tb
            #        dbCore.addLogRow(str(row))
            #        errorRow = dbCore.addLogRow(tb)
            #        sqlErrorRows.append(errorRow)
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

            errorRows = insertRows(reader, "levy_temp_service_charge_rates", ['entity_code', 'category_name', 'rev_id', 'sc_rate', 'tax_flag', 'discount_flag', 'cat_id', 'created_at'], dbCore)
            if len(errorRows) > 0:
                errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_service_charges: " + str(errorRows))
                sqlErrorRows.append(errorLogRow)

            #for row in reader:
            #    try:
            #        dbCore.insertTempLevyServiceCharge(row)
            #    except:
            #        tb = traceback.format_exc()
            #        print tb
            #        dbCore.addLogRow(str(row))
            #        errorRow = dbCore.addLogRow(tb)
            #        sqlErrorRows.append(errorRow)

        
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
            
            errorRows = insertRows(reader, "levy_temp_package_definitions", ['entity_code', 'package_item_number', 'assigned_item_number', 'matrix_1', 'matrix_2', 'matrix_3', 'matrix_4', 'created_at'], dbCore)
            if len(errorRows) > 0:
                errorLogRow = dbCore.addLogRow("Rows failed to insert into levy_temp_package_definitions: " + str(errorRows))
                sqlErrorRows.append(errorLogRow)

            #for row in reader:
            #    try:
            #        dbCore.insertTempLevyPackageDefinition(row) 
            #    except:
            #        tb = traceback.format_exc()
            #        dbCore.addLogRow(str(row))
            #        errorRow = dbCore.addLogRow(tb)
            #        sqlErrorRows.append(errorRow)

    else:
        success = False
        missingFiles.append("Package Definitions")
        dbCore.addLogRow(packageDefinitionsCSV + " doesn't exist")
        

    return success, missingFiles, sqlErrorRows

def insertRows(reader, tableName, columns, dbCore):


    print "Inserting rows into: " + tableName

    rows = []

    x = 0
    allFailedRows = []

    for row in reader:
        x = x + 1
        rows.append(row)
        if x == 100:
            failedRows = dbCore.batchInsertTempTableRows(tableName, columns, rows)
            if failedRows is not None:
                allFailedRows = allFailedRows + failedRows
            rows = []
            x = 0

    if len(rows) > 0:
        dbCore.batchInsertTempTableRows(tableName, columns, rows)
        if failedRows is not None:
            allFailedRows = allFailedRows + failedRows 

    return allFailedRows
