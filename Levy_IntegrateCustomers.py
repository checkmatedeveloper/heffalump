import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import sys
import traceback


def updateVenuesXSuiteHolders(venue_uid, patron_uid, levy_pointer, dbCore):

    venuesXSuiteHolders = dbCore.getVenuesXSuiteHolders(venue_uid, patron_uid)

    if len(venuesXSuiteHolders) == 0:
        print "Inserting venuesXSuiteHolders"
        insert_uuid = uuid.uuid4()
        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'venue_uid', venue_uid, False, levy_pointer, auto_apply = True)
        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'patron_uid', patron_uid, False, levy_pointer, auto_apply = True)
    else:
        for venueXSuiteHolder in venuesXSuiteHolders:
            if venueXSuiteHolder[3] == 0:
                IntegrationTools.confirmUpdate(dbCore, venue_uid, 'patrons', 'venues_x_suite_holders', 'is_active', venueXSuiteHolder[0], 0, 1)

def updateUnitsXPatrons(venue_uid, unit_uid, patron_uid, levy_pointer, dbCore):

    unitsXPatrons = dbCore.getUnitsXPatrons(unit_uid, patron_uid)

    if len(unitsXPatrons) == 0:
        if unit_uid is not None: #this might cause issues
            print "Inserting unitsXPatrons"
            insert_uuid = uuid.uuid4()
            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'unit_uid',  unit_uid, False, levy_pointer, True, auto_apply = True)
            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'patron_uid', patron_uid, False, levy_pointer, True, auto_apply = True)
    else:
        for unitXPatron in unitsXPatrons:
            if unitXPatron[5] == 0:
                IntegrationTools.confirmUpdate(dbCore, venue_uid, 'info', 'unit_x_patrons', 'is_active', unitXPatron[0], 0, 1)


def integrate(dbCore):
    
    dbCore.addLogRow("Integrating Customers")
   
    tempCustomers = dbCore.getTempCustomers()
    #Customer row = (0:'customer_number', 1:'customer_name', 2:'levy abreviated venue', 3:'suite number', 4:NULL)
    
    success = True;
    errorLogRows = []
    errorVenues = []
    customerNumber = ""

    for row in tempCustomers:
        try:
            venue_uid = dbCore.getVenueUid(row[2])
        except:
            print "Venue not mapped, can't integrate"
            continue
            
        try:
            customerNumber = row[0]
            levyPatron = dbCore.getLevyPatron(row[0])
            
            if levyPatron is None:
                print "No Customer mapping for " + str(row[0]) + " found yet"     

                hashedCustomerName = IntegrationTools.hashString(row[1])
                
               # print "Searching for customer with matching name"
                patrons = dbCore.getAllPatronsByNameHash(IntegrationTools.hashString(row[1]), venue_uid)
                
                if patrons is None or len(patrons) == 0:
                    #we have zero knowledge of a patron by this name or number
                    
                    print "No customer found, inserting new one"
                    import_uuid = uuid.uuid4()     
                  
                    #attempt to insert a new patron into the parametric table     
                    IntegrationTools.confirmInsert(dbCore, venue_uid, import_uuid, 'patrons', 'patrons', 'company_name', row[1], True, row[0], auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venue_uid, import_uuid, 'patrons', 'patrons', 'company_name_hashed', hashedCustomerName, False, row[0], auto_apply = True);
                    IntegrationTools.confirmInsert(dbCore, venue_uid, import_uuid, 'patrons', 'patrons', 'is_encrypted', '1', False, row[0], auto_apply = True);
                elif len(patrons) == 1:
                    print "      Match Found !!!"
                    # A customer by that name was in our system but not the _levy tables
                    
                    #add the customer to the levy table
                    lastLevyPatronId = dbCore.insertLevyPatron(row[0], venue_uid, patrons[0][0])

                    #then find all of the customers venues/suites and add him to the database in those places
                    customerSuites = dbCore.getCustomerSuites(row[0], row[2])
                    for suiteNumber in customerSuites:
                  #      print "processing suit"
                        suiteNumber = suiteNumber[0]
                    
                        unitUid = dbCore.findUnit(suiteNumber, venue_uid)
                        unitUid = unitUid[0]
                       

                   #     print str(patrons) 
                        patronUid = patrons[0][0]
                        
                    
                    #    dbCore.insertPatronXTables(venue_uid, unitUid, patronUid, lastLevyPatronId);
                    dbCore.insertPatronXUnitsLevy(unitUid, lastLevyPatronId);

        #            insert_uuid = uuid.uuid4()
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'venue_uid', venue_uid, False, row[0])
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'patron_uid', patronUid, False, row[0])
                    updateVenuesXSuiteHolders(venue_uid, patronUid, row[0], dbCore)

        #            insert_uuid = uuid.uuid4()
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'unit_uid',  unit_uid, False, row[0], True)
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'patron_uid', patron_uid, False, row[0], True)
                    updateUnitsXPatrons(venue_uid, unit_uid, patronUid, row[0], dbCore)                    

                else:
                    print "     We have multiple customers with that name: " + hashedCustomerName      
                    dbCore.addLogRow("I found multple customers with name: " + row[1] + "   I don't know what to do!!!")
                   
            else:
                print "A mapping was found"
                #check if the patrons row needs updating
                parametricPatron = dbCore.getParametricPatron(levyPatron[3]) # levyPatron[3] = the parametric patron_uid
                parametricClonePatron = dbCore.getParametricClonePatron(parametricPatron[0])
                if parametricPatron[4] != IntegrationTools.hashString(row[1]): #compare the hashes, since the actual name is encrypted
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'patrons', 'patrons', 'company_name', parametricPatron[0], parametricPatron[3], row[1], True)
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'patrons', 'patrons', 'company_name_hashed', parametricPatron[0], parametricPatron[4], IntegrationTools.hashString(row[1]), False)
                    if parametricClonePatron is not None:
                        IntegrationTools.confirmUpdate(dbCore, venue_uid, 'patrons', 'clone_patrons', 'company_name', parametricPatron[0], parametricClonePatron[3], row[1], False)
                
                customerSuites = dbCore.getCustomerSuites(row[0], row[2])
                for suiteNumber in customerSuites:
               #     print "Processing Suite"
                    suiteNumber = suiteNumber[0]
                    
                    unitUid = dbCore.findUnit(suiteNumber, venue_uid)
                    
                    if unitUid is None:
			            continue;
                    unitUid = unitUid[0]
                      
                    parametricPatron = dbCore.getParametricPatron(levyPatron[3]) # levyPatron[3] = the parametric patron_uid
                    patronUid = parametricPatron[0]
                    patronsLevyUid = levyPatron[0]
                    
                    dbCore.insertPatronXUnitsLevy(unitUid, patronsLevyUid);
                     
        #            insert_uuid = uuid.uuid4()
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'venue_uid', venue_uid, False, row[0])
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'patrons', 'venues_x_suite_holders', 'patron_uid', levyPatron[3], False, row[0])
                    updateVenuesXSuiteHolders(venue_uid, patronUid, row[0], dbCore)            
                    
        #            insert_uuid = uuid.uuid4()
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'unit_uid',  unitUid, False, row[0], ignoreUniqueConstraint=True)
        #            IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'info', 'unit_x_patrons', 'patron_uid', levyPatron[3], False, row[0], ignoreUniqueConstraint=True)
                    updateUnitsXPatrons(venue_uid, unitUid, patronUid, row[0], dbCore)
 
                   
        except:
            success = False
            tb = traceback.format_exc()
            logRowId = dbCore.addLogRow("Error processing customer row (customer_number= " + str(customerNumber) + ") Stacktrace: " + tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venue_uid)
   
    #Clear out customer that are no longer in the integration file
    venues = dbCore.getAllLevyIntegrationVenues()
 

    print "Checking for inactive patrons"   
    #need to do this for all venues
    for venue in venues:
        venue_uid = venue[0]
        inactivePatrons = []
        inactiveUnitXPatronRows = dbCore.getUnitXPatronsToInactivate(venue_uid)
        for unitXPatronsUid, patron_uid in inactiveUnitXPatronRows:
            IntegrationTools.confirmDeactivate(dbCore, venue_uid, 'info', 'unit_x_patrons', unitXPatronsUid, auto_apply = True)
            if patron_uid not in inactivePatrons:
                inactivePatrons.append(patron_uid)

        for patron_uid in inactivePatrons:
            
            venuesXSuiteHolderUid = dbCore.getVenuesXSuiteHolders(venue_uid, patron_uid) #should only ever return 1
            if len(venuesXSuiteHolderUid) != 0:
                venuesXSuiteHolderUid = venuesXSuiteHolderUid[0][0]
                IntegrationTools.confirmDeactivate(dbCore, venue_uid, 'patrons', 'venues_x_suite_holders', venuesXSuiteHolderUid, auto_apply = True)


    return success, errorLogRows, errorVenues;


