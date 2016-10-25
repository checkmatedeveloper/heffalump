'''
    This handles sending an email to venue employees after integration changes have been applied.
'''
from db_connection import DbConnection
from IntegrationEmailerDb import EmailerDb
import datetime
import gmail
import IntegrationTools


def sendEmail(allAppliedActions):

    
    print "Preparing Integration Email"

    #convert the uuid keyed dict of actions into a list of actions (we don't care about the uuids anymore
#    tempActions = []
#    for action in allAppliedActions:
#        tempActions.append(allAppliedActions[action])
#
#    allAppliedActions = tempActions

    appliedActions = allAppliedActions

    conn = DbConnection().connection
    emailerDb = EmailerDb(conn)

    venueUids = emailerDb.getVenueUids()
    print "VENUE UIDS: " + str(venueUids)
    for venueUid in venueUids:
        venueUid = venueUid[0] #DON'T FORGET python will return single values from mysql as an array with only one item 
        appliedActions = getVenueAppliedActions(allAppliedActions, venueUid)
        print "Starting email for venue " + str(venueUid)
        print str(appliedActions)

        venueName = emailerDb.getVenueName(venueUid)

        sendEmail = False

        if len(appliedActions) > 0:
            sendEmail = True
            emailBody = "<html>Daily Integration has been completed for, " + venueName + ".  The following changes have been applied:"   
        
            #EMPLOYEES:
            employeeActions = getAppliedActionsByType(appliedActions, "setup", "employees")
            if len(employeeActions) > 0:
                emailBody += "<h3>EMPLOYEES:</h4>"
                
                addEmployeeActions = getInsertActions(employeeActions)
                if len(addEmployeeActions) > 0:
                    emailBody += "     New Employees:<ul>"
                    for addAction in addEmployeeActions:
                        levyTempPointerUid = addAction[0][13]
                        emailBody += getNewEmployeeLine(emailerDb, levyTempPointerUid)
                    emailBody += "</ul>"

                editEmployeeActions = getUpdateActions(employeeActions)
                if len(editEmployeeActions) > 0:
                    emailBody += "Updated Employees:<ul>"
                    for editAction in editEmployeeActions:
                        emailBody += getUpdatedEmployeeLine(emailerDb, editAction)
                    emailBody += "</ul>"
            #CUSTOMERS
            customerActions = getAppliedActionsByType(appliedActions, "patrons", "patrons")
            suiteAssignmentActions = getAppliedActionsByType(appliedActions, "info", "unit_x_patrons")

            if len(customerActions) + len(suiteAssignmentActions) > 0:
                sendEmail = True
                emailBody += "<h3>CUSTOMERS:</h3>" 

                addCustomerActions = getInsertActions(customerActions)
                if len(addCustomerActions) > 0:
                    emailBody += "New Customers:<ul>"
                    for addAction in addCustomerActions:
                        levyTempPointerUid = addAction[0][13]
                        emailBody += getNewCustomerLine(emailerDb, levyTempPointerUid)    
                    emailBody += "</ul>"

                editCustomerActions = getUpdateActions(customerActions)
                if len(editCustomerActions) > 0:
                    emailBody += "Updated Customers:<ul>"
                    for editAction in editCustomerActions:
                        emailBody += getUpdatedCustomerLine(emailerDb, editAction)
                    emailBody += "</ul>"
                
 
                addSuiteAssignmentActions = getInsertActions(suiteAssignmentActions)
                if len(addSuiteAssignmentActions) > 0:
                    emailBody += "Suite Assignments:<ul>"
                    for addAction in addSuiteAssignmentActions:
                        emailBody += getNewSuiteAssignmentLine(emailerDb, addAction)
                    emailBody += "</ul>"

                deactivateSuiteAssignments = getDeactivateActions(suiteAssignmentActions)
                print "Deactivate: " + str(deactivateSuiteAssignments)
                if len(deactivateSuiteAssignments) > 0:
                    emailBody += "Suite Deassignments:<ul>"
                    for deactivateAction in deactivateSuiteAssignments:
                        emailBody += getDeactivateSuiteAssignmentLine(emailerDb, deactivateAction)
                    emailBody += "</ul>"

            #EVENTS
            eventActions = getAppliedActionsByType(appliedActions, "setup", "events")
            if len(eventActions) > 0:
                sendEmail = True
                emailBody += "<h3>EVENTS:</h3>"
                
                addEventActions = getInsertActions(eventActions)
                if len(addEventActions) > 0: 
                    emailBody += "New Events: <ul>"
                    for addAction in addEventActions:
                        levyTempPointerUid = addAction[0][13]
                        emailBody += getNewEventLine(emailerDb, levyTempPointerUid)
                    emailBody += "</ul>"

            eventXVenuesActions = getAppliedActionsByType(appliedActions, "setup", "events_x_venues")
            updateEventXVenuesActions = getUpdateActions(eventXVenuesActions)
            updateEventActions = getUpdateActions(eventActions)
            updateAllEventActions = eventXVenuesActions + updateEventActions 
            
            if len(updateAllEventActions) > 0:
                sendEmail = True
                emailBody += "UpdatedEvents:<ul>"
                for updateAction in updateAllEventActions:
                    emailBody += getUpdatedEventLine(emailerDb, updateAction)
                emailBody += "</ul>"
                

            #ITEMS
            updateItems = False
            itemActions = getAppliedActionsByType(appliedActions, "menus", "menu_items")
            if len(itemActions) > 0:
                sendEmail = True
                emailBody += "<h3>ITEMS:</h3>"

                addItemActions = getInsertActions(itemActions) 
                if len(addItemActions) > 0:
                    emailBody += "New Items:<ul>"
                    for addAction in addItemActions:
                        levyTempPointerUid = addAction[0][13]
                        emailBody += getNewItemLine(emailerDb, levyTempPointerUid)
                    emailBody += "</ul>"
                
                updateMenuItemActions = getUpdateActions(itemActions)
                
                if len(updateMenuItemActions) > 0:
                    updateItems = True
                    emailBody += "Updated Items:<ul>"
                    for updateAction in updateMenuItemActions:
                        emailBody += getUpdatedMenuItemLine(emailerDb, updateAction)

            menuXMenuActions = getAppliedActionsByType(appliedActions, "menus", "menu_x_menu_items")
            updateMenuXMenuItemActions = getUpdateActions(menuXMenuActions)
            if len(updateMenuXMenuItemActions) > 0:
                sendEmail = True
                if updateItems == False:
                    updateItems = True
                    emailBody  += "Updated Items:<ul>"
                for updateAction in updateMenuXMenuItemActions:
                    emailBody += getUpdatedMenuXMenuItemLine(emailerDb, updateAction)
                            

            if updateItems == True:
                emailBody += "</ul>"
                
            #SUITES
            unitActions = getAppliedActionsByType(appliedActions, "setup", "units")
            if len(unitActions) > 0:
                sendEmail = True
                emailBody += "<h3>UNITS:</h3>"
            
                addUnitActions = getInsertActions(unitActions)
                if len(addUnitActions) > 0:
                    emailBody += "New Units:<ul>"
                    for addAction in addUnitActions:
                        levyTempPointerUid = addAction[0][13]
                        emailBody += getNewUnitLine(emailerDb, levyTempPointerUid)
                    emailBody += "</ul>"
                editUnitActions = getUpdateActions(unitActions)
                if len(editUnitActions) > 0:
                    emailBody += "Updated Units:<ul>"
                    for editAction in editUnitActions:
                        emailBody += getUpdatedUnitLine(emailerDb, editAction)
                    emailBody += "</ul>"

                else:
                    print "NO UNIT UPDATES WERE MADE"
            emailBody += "</html>"

            print str(venueUid) + " OUTPUT: " + emailBody

            if sendEmail:
                emailAddresses = emailerDb.getEmailAddresses(venueUid)
                for address in emailAddresses:
                    address = address[0]
                    gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "integration@parametricdining.com", address, 'Integration Complete', emailBody, 'please open this in an HTML compatable email client') 
            else:
                print "Don't send email, nothing to tell the venue about"
        else:
            print "No actions were applied, Canceling Email"
            

#####Get Lines
def getNewEmployeeLine(emailerDb, levyTempPointerUid):    
    firstName, lastName = emailerDb.getEmployeeName(levyTempPointerUid)
    return "<li>" + firstName + " " + lastName + "</li>"

def getUpdatedEmployeeLine(emailerDb, updateAction):
    levyTempPointerUid = updateAction[0][13]
    return "<li>" + emailerDb.getUpdateEmployeeName(updateAction) + getUpdateInfo(updateAction) + "</li>"

def getNewCustomerLine(emailerDb, levyTempPointerUid):

    customerXSuiteNumbers = emailerDb.getCustomerNameAndSuites(levyTempPointerUid)
    customerName = customerXSuiteNumbers[0][0]
    newCustomerLine = "<li>" + customerName + " - Assigned to suites: "
    for row in customerXSuiteNumbers:
        suiteName = row[1]
        newCustomerLine += suiteName + ", "

    return newCustomerLine[:-2] + "</li>" #remove that pesky trailing comma

def getUpdatedCustomerLine(emailerDb, updateAction):
    newCustomerName = emailerDb.getUpdateCustomerName(updateAction)
    levyCustomerNumber = emailerDb.getCustomerNumber(updateAction[0][6])
    return "<li> Customer Number: " + str(levyCustomerNumber) + " changed name to " + newCustomerName  +  "</li>"

def getNewSuiteAssignmentLine(emailerDb, addSuiteAssignmentAction):
    levyTempPointerUid = addSuiteAssignmentAction[0][13]
    patronName = emailerDb.getCustomerNameAndSuites(levyTempPointerUid)[0][0]
    unitUid = None
    for row in addSuiteAssignmentAction:
        if row[5] == "unit_uid":
            unitUid = row[8]
            break;


    unitName = emailerDb.getUnitNameByUid(unitUid)
    return "<li>" + patronName + " assigned to Suite " + unitName + "</li>"

def getDeactivateSuiteAssignmentLine(emailerDb, deactivateAction):
    unitXPatronsUid = deactivateAction[0][6]
    patronUid, unitName = emailerDb.getPatronUidAndUnitNameFromUnitXPatronsUid(unitXPatronsUid)
    companyName = IntegrationTools.decryptPatron(emailerDb, patronUid)
    return '<li>' + str(companyName) + " removed from suite " + str(unitName) + "</li>"

def getNewEventLine(emailerDb, levyTempPointerUid):
    eventName, eventDate = emailerDb.getEventNameAndDate(levyTempPointerUid)
    return '<li>"' + eventName  + '" on ' + eventDate.strftime("%B %d, %Y at %-I:%M %p") + "</li>"

def getUpdatedEventLine(emailerDb, updateAction):
    pointerTable = updateAction[0][4]
    venueUid = updateAction[0][1]
    eventName = ""
    if pointerTable == "events":
        pointerField = updateAction[0][5]
        if pointerField == 'event_date':
             return ': Updated "' + str(updateAction[0][5]) + '" - changed "' +  str(emailerDb.convertTimeZone(updateAction[0][7], venueUid)) + '" to "' + str(emailerDb.convertTimeZone(updateAction[0][8], venueUid)) + '"'            

        eventUid = updateAction[0][6]
        eventName = emailerDb.getEventNameFromEventUid(eventUid)
    elif pointerTable == "events_x_venues":
        eventXVenueUid = updateAction[0][6]
        eventName = emailerDb.getEventNameFromEventXVenueUid(eventXVenueUid)
    return '<li>' + eventName + getUpdateInfo(updateAction) + "</li>"

def getNewItemLine(emailerDb, levyPointerUid):
    itemName = emailerDb.getItemName(levyPointerUid)
    return '<li>' + itemName + " (" + str(levyPointerUid) + ") </li>"

def getUpdatedMenuItemLine(emailerDb, updateAction):
    menuItemUid = updateAction[0][6]
    itemNumber = updateAction[0][13] 
    itemName = emailerDb.getItemNameByUid(menuItemUid)
    return "<li>" + itemName + " (" + str(itemNumber) +") " + getUpdateInfo(updateAction) + "</li>"

def getUpdatedMenuXMenuItemLine(emailerDb, updateAction):
    menuXMenuItemUid = updateAction[0][6]
    itemNumber = updateAction[0][13]
    itemName = emailerDb.getItemNameByMenuXMenuItemUid(menuXMenuItemUid)
    menuName = emailerDb.getMenuName(menuXMenuItemUid)
    return "<li>" + itemName + " (" + str(itemNumber) + " ) " + "(" + menuName + ") " + getUpdateInfo(updateAction) + "</li>"

#def getUpdatedEventLine(emailerDb, updateAction):
#    levyTempPointerUid = updateAction[0][13]
#    eventName, eventDate = emailerDb.getEventnameAndDate(levyTempPointerUid)
#    return "<li>" + eventName + "(" + eventDate.strftime("%B %d, %Y at %-I:%M %p") + ")" + getUpdateInfo(updateAction) + "</li>"

def getNewUnitLine(emailerDb, levyTempPointer):
    return "<li>" + emailerDb.getSuiteName(levyTempPointer) + "</li>"

def getUpdatedUnitLine(emailerDb, updateAction):
    return "<li>" + emailerDb.getUpdateSuiteName(updateAction) + getUpdateInfo(updateAction) + "</li>"
    
def getUpdateInfo(updateAction):
    return ': Updated "' + str(updateAction[0][5]) + '" - changed "' +  str(updateAction[0][7]) + '" to "' + str(updateAction[0][8]) + '"'



def getVenueAppliedActions(allAppliedActions, venueUid):
    venueActions = []
    for action in allAppliedActions:
        actionVenue = action[0][1]
        if actionVenue == venueUid:
            venueActions.append(action)

    return venueActions

'''
Will get all of the appliedActions that have a given schema and table
'''
def getAppliedActionsByType(appliedActions, targetSchema, targetTable):
    targetActions = []
    for action in appliedActions:
        schema = action[0][3]
        table = action[0][4]

        if(schema == targetSchema and table == targetTable):
            targetActions.append(action)

    return targetActions


def getInsertActions(actions):
    insertActions = []
    for action in actions:
        requiredAction = action[0][9]

        if requiredAction == 'add':
            insertActions.append(action)
 
    return insertActions

def getUpdateActions(actions):
    updateActions = []
    for action in actions:
        requiredAction = action[0][9]
    
        if requiredAction == "edit":
            updateActions.append(action)

    return updateActions


def getDeactivateActions(actions):
    deactivateActions = []
    for action in actions:
        requiredAction = action[0][9]
        
        if requiredAction == 'deactivate':
            deactivateActions.append(action)

    return deactivateActions



################MAIN
#print "Started Emailer"
#sendEmail([])
