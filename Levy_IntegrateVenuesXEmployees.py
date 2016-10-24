import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

def integrate(dbCore):
    
    dbCore.addLogRow("Integrating Employees")
    tempEmployees = dbCore.getTempEmployees()

    #Employee row = (0:employee_number, 1:levy abbreviated venue, 2:venue number??? 3:pos name?, 4:first_name, 5:last_name
 
    success = True 
    errorLogRows = []
    employeeNumber = ""
    errorVenues = []

    for row in tempEmployees:
        try:
            venue_uid = dbCore.getVenueUid(row[1])
        except:
            continue
        try:        
            employeeNumber = row[0]
           

            levyVenuesXEmployees =dbCore.getLevyVenuesXEmployees(row[0])

            if len(levyVenuesXEmployees) == 0:
                #uh oh we don't have mapping for this employee
                

                #try looking them up in out parametric db
                
                #we need first name and last name
               
             
                employees = dbCore.findEmployees(row[4], row[5], venue_uid)
                if len(employees) == 0:
                    print "inserting employee"
                    insert_uuid = uuid.uuid4()
                
                    #TODO: im not sure what to do with this because we don't have any of the password fields  
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'employees', 'first_name', row[4], False, row[0])
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'employees', 'last_name', row[5], False, row[0])
                    IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'setup', 'employees', 'pos_name', row[3], False, row[0])
                elif len(employees) == 1:
                    print "mapping"
                    dbCore.insertLevyVenuesXEmployees(row[0], venue_uid, employees[0][1])
                else:
                    print "found multiple employees with parameters: " + str(row[4]) + " " + str(row[5]) + " " + str(venue_uid)
            elif len(levyVenuesXEmployees) == 1:
              	print "A mapping exists for " + str(row[4]) + " " + str(row[5]) + " " + str(venue_uid) 
            
                #go ahead and follow that mapping
                employee = dbCore.getEmployee(levyVenuesXEmployees[0][3])
                first_name = employee[1]
                last_name = employee[2]
                pos_name = employee[3]
                            

                #just make sure that our data is up to date
                if first_name != row[4]:
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'employees', 'first_name',  employee[0], first_name, row[4], False) 

                if last_name != row[5]:
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'employees', 'last_name', employee[0], last_name, row[5], False)

                if pos_name != row[3]:
                    IntegrationTools.confirmUpdate(dbCore, venue_uid, 'setup', 'employees', 'pos_name', employee[0], pos_name, row[3], False)
            else:
                #oh fuck
                dbCore.addLogRow("Multiple matching employees found")
        except:
            success = False
            tb = traceback.format_exc()
            logRowId = dbCore.addLogRow("Error processing employee row (employee_number= " + str(employeeNumber) + ") Stacktrace: " + tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venue_uid)

    return success, errorLogRows, errorVenues;

