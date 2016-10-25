import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

def integrate(dbCore):
    
    dbCore.addLogRow("Integrating Units")
    tempSuites = dbCore.getTempSuites()

    success = True
    errorLogRows = []
    errorVenues = []
    suiteId = ""

    #Unit row = (0:'suite_id', 1:'suite_name', 2:'levy abreviated venue', 3:NULL)
    for row in tempSuites:
        try:
            venue_uid = dbCore.getVenueUid(row[2])
        except:
            continue
        try:
            suiteId = row[0]
            levyUnits = dbCore.getLevyUnits(venue_uid, row[0])
            
            if len(levyUnits) == 0:
                #there is no mapping for this unit
                
                #look it up in our existing units table, maybe checkmate knew about it before integration
                units = dbCore.findUnits(venue_uid, row[1])
                if len(units) == 0:
                    #we don't have any record of this unit
                    
                    #insertConfirm into units and levy_units
                    insert_uuid = uuid.uuid4()
                    
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'units', 'location_uid', 6, False, row[0], auto_apply = True) #a hack for an unsuported table :( 
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'units', 'venue_uid', venue_uid, False, row[0], auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'units', 'name', row[1], False, row[0], auto_apply = True)
                    #there is more info that this about the unit but we don't have anywhere to get it from at this time
                    
                if len(units) == 1:
                    #we have exactly one unit that matches.  This is the one
                    dbCore.insertUnitsLevy(row[0], venue_uid, units[0][0])


            elif len(levyUnits) == 1:
                #we already have a mapping for this unit

                #double check that out suite name is up to date
                if levyUnits[0][3] != None: #if there is no matching parametric unit_uid this s a suite that we should skip messing with
                    unit = dbCore.getUnit(levyUnits[0][3])
                    if unit[3] != row[1]: #if the names are not the same
                        IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'units', 'name', unit[0], unit[3], row[1], False)
            else:
                #uh oh
                dbCore.addLogRow("Multiple Rows in units_levy.  I don't know what to do!!!")
        except:
            success = False
            tb = traceback.format_exc()
            logRowId = dbCore.addLogRow("Error processing Unit row (suite_id=" + str(suiteId) + ") Stacktrace: " + tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venue_uid)

          
    return success, errorLogRows, errorVenues;
