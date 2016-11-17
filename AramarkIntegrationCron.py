from db_connection import DbConnection
from Aramark_DB import Aramark_Db
from suds.client import Client
import HipChat
import sys
import pytz, datetime
from dateutil.relativedelta import relativedelta
import traceback
import gmail
import MailGun
import csv

ACTION_TYPE_INSERT = 0
ACTION_TYPE_UPDATE = 1

NULL_ID = '00000000-0000-0000-0000-000000000000'

class AramarkIntegrationError:
    
    def __init__(self, field, data, stacktrace):
        self.field = field
        self.data = data
        self.stacktrace = stacktrace

    def getHTMLFormattedRow(self):
        return "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (self.field, self.data, self.stacktrace)

class AramarkRequiredAction:

    def __init__(self, field, action, target):
        self.field = field
        self.action = action
        self.target = target

    def getHTMLFormattedRow(self):
        return "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (self.field, self.action, self.target)

'''
Used to create the summary of actions applied email
'''
class AppliedIntegration:
    
    def __init__(self, venueUid, actionType, pointerSchema, pointerTable, pointerUid, oldValue, newValue, aramarkData, updatedField = None):
        self.venueUid = venueUid
        self.actionType = actionType
        self.pointerSchema = pointerSchema
        self.pointerTable = pointerTable 
        self.pointerUid = pointerUid #used for updates
        self.oldValue = oldValue #used for updates
        self.newValue = newValue #used for updates
        self.aramarkData = aramarkData # this is the xml node as it comes in from aramerk, can be used for extra formatting data when building the email row
   
    def formatEmailRow(self):
        if self.pointerSchema == 'setup' and self.pointerTable == 'units':
            if self.actionType == ACTION_TYPE_INSERT:
                return "<li>" + self.aramarkData.SuiteNumber + "</li>"
            elif self.actionType == ACTION_TYPE_UPDATE:
                #TODO
                return "<li>" + self.aramarkData.SuiteNumber + ": updated" + updatedField + "- changed " + str(oldValue) + " to " + str(newValue)

        if self.pointerSchema == 'setup' and self.pointerTable == 'events':
            if self.actionType == ACTION_TYPE_INSERT:
                #2016-02-26T13:00:00
                prettyEventDate = self.aramarkData.EventDateTime.strftime("%B %d, %Y at %-I:%M %p")
                return '<li>"' + self.aramarkData.Description + '" on ' + prettyEventDate + "</li>"
            elif self.actionType == ACTION_TYPE_UPDATE:
                return "<li>" + self.aramarkData.SuiteNumber + ": updated" + updatedField + "- changed " + str(oldValue) + " to " + str(newValue)
        if self.pointerSchema == 'menus' and self.pointerTable == 'menu_items':
            if self.actionType == ACTION_TYPE_INSERT:
                return '<li>' + self.aramarkData.Title + " (" + self.aramarkData.MenuItemID + ")</li>"
            elif self.actionType == ACTION_TYPE_UPDATE:
                return "<li>" + self.aramarkData.SuiteNumber + ": updated" + updatedField + "- changed " + str(oldValue) + " to " + str(newValue)

        if self.pointerSchema == 'patrons' and self.pointerTable == 'patrons':
            if self.actionType == ACTION_TYPE_INSERT:
                return '<li>' + self.aramarkData.FirstName + ' ' + self.aramarkData.LastName  + '</li>'
            elif self.actionType == ACTION_TYPE_UPDATE:
                return "<li>" + self.aramarkData.SuiteNumber + ": updated" + updatedField + "- changed " + str(oldValue) + " to " + str(newValue)

    def __str__(self):
        return '[' + ', '.join([self.venueUid, str(self.actionType), str(self.pointerSchema), str(self.pointerTable), str(self.pointerUid), str(self.oldValue), str(self.newValue)]) + ']'

    def __repr__(self):
        return self.__str__()

def getAppliedIntegrationsByVenue(venueUid, appliedIntegrations):
    venueFilteredIntegrations = []
    for integration in appliedIntegrations:
        if integration.venueUid == venueUid:
            venueFilteredIntegrations.append(integration)

    return venueFilteredIntegrations

def getAppliedInserts(appliedIntegrations):
    appliedInserts = []
    for integration in appliedIntegrations:
        if integration.actionType == ACTION_TYPE_INSERT:
            appliedInserts.append(integration)

    return appliedInserts

def getAppliedUpdates(appliedIntegrations):
    appliedUpdates = []
    for integration in appliedIntegrations:
        if integration.actionType == ACTION_TYPE_UPDATE:
            appliedUpdates.append(integration)

    return appliedUpdates

def getAppliedIntegrationsByTable(pointerSchema, pointerTable, appliedIntegrations):
    matchingIntegrations = []
    for integration in appliedIntegrations:
        if integration.pointerSchema == pointerSchema and integration.pointerTable == pointerTable:
            matchingIntegrations.append(integration)

    return matchingIntegrations

def sendAppliedIntegrationSummary(venueUid, appliedIntegrations):
    
    emailBody = "Aramark Integration has completed, the following changes have been applied"
    sendIntegrationEmail = False;

    appliedIntegrations = getAppliedIntegrationsByVenue(venueUid, appliedIntegrations) 

    if len(appliedIntegrations) > 0:
        sendIntegrationEmail = True

    appliedInserts = getAppliedInserts(appliedIntegrations)
    appliedUpdates = getAppliedUpdates(appliedIntegrations)

   

    #units
    insertedUnits = getAppliedIntegrationsByTable('setup', 'units', appliedInserts)
    updatedUnits = getAppliedIntegrationsByTable('setup', 'units', appliedUpdates)

    if len(insertedUnits) + len(updatedUnits) != 0:
        emailBody += "<h3>UNITS:</h3>"
        if len(insertedUnits) != 0:
            emailBody += "New Units:<ul>"
            for insertedUnit in insertedUnits:
                emailBody += insertedUnit.formatEmailRow()
            emailBody += "</ul>"
        if len(updatedUnits) != 0:
            emailBody = "Updated Units:<ul>"
            for updatedUnit in updatedUnits:
                emailBody += updatedUnit.formatEmailRow()
            emailBody += "</ul>"

    #events
    insertedEvents = getAppliedIntegrationsByTable('setup', 'events', appliedInserts)
    updatedEvents = getAppliedIntegrationsByTable('setup','events', appliedUpdates)

    if len(insertedEvents) + len(updatedEvents) != 0:
        emailBody += "<h3>EVENTS:</h3>"
        if len(insertedEvents) != 0:
            emailBody += "New Events:<ul>"
            for insertedEvent in insertedEvents:
                emailBody += insertedEvent.formatEmailRow()
            emailBody += "</ul>"
        if len(updatedEvents) != 0:
            emailBody += "Updated Events:<ul>"
            for updatedEvent in updatedEvents:
                emailBody += updatdEvent.formatEmailRow()
            emailBody += "</ul>"

    #menu_categories

    #menus

    #menu_items
    insertedMenuItems = getAppliedIntegrationsByTable('menus', 'menu_items', appliedInserts)
    updatedMenuItems = getAppliedIntegrationsByTable('menus', 'menu_items', appliedUpdates)

    if len(insertedMenuItems) + len(updatedMenuItems) != 0:
        emailBody += "<h3> MENU ITEMS:</h3>"
        if len(insertedMenuItems) != 0:
            emailBody += "New Menu Items:<ul>"
            for insertedMenuItem in insertedMenuItems:
                emailBody += insertedMenuItem.formatEmailRow()
            emailBody += "</ul>"
        if len(updatedMenuItems) != 0:
            emailBody += "Updated Menu Items:<ul>"
            for updatedMenuItem in updatedMenuItems:
                emailBody += updatedMenuItem.formatEmailRow()
            emailBody += "</ul>"

    #customers
    insertedPatrons = getAppliedIntegrationsByTable('patrons', 'patrons', appliedInserts)
    updatedPatrons = getAppliedIntegrationsByTable('patrons', 'patrons', appliedUpdates)

    if len(insertedPatrons) + len(updatedPatrons) > 0:
        emailBody += "<h3>PATRONS: </h3>"
        if len(insertedPatrons) > 0:
            emailBody += "New Patrons:<ul>"
            for insertedPatron in insertedPatrons:
                emailBody += insertedPatron.formatEmailRow()
            emailBody +='</ul>'

        if len(updatedPatrons) > 0:
            emailBody += "Updated Patrons: <ul>"
            for updatedPatron in updatedPatrons:
                emailBody += updatedPatron.formatEmailRow()
            emailBody+= '</ul>'


    if sendIntegrationEmail:
        recipients =dbCore.getAramarkEmailRecipients(venueUid)
        for recipient in recipients:
            email = recipient[0]
            gmail.sendGmail("tech@parametricdining.com",
                        "fkTUfbmv2YVy",
                        "aramark_integration@parametricdining.com",
                        email,
                        "Aramark Integration Summary",
                        emailBody,
                        "Please view this email in an html compatable email client"
                        )

def sendIntegrationErrorSummary(venueUid, errors):

    body = "<h3> Aramark Integration Encountered " + str(len(errors)) + " Errors</h3>"
    body += '<table border="1">'
    body += '<tr><th>Integration Field</th><th>Integration Data</th><th>Error Stack Trace</th></tr>' 
    for error in errors:
        body += error.getHTMLFormattedRow()

    body += "</table>"
    
    toEmails = dbCore.getErrorEmailRecipients(venueUid)
    print "Error Emails: " + str(toEmails)
    for toEmail in toEmails:
        toEmail = toEmail[0]
        print "Script crashed, sending an email to " + toEmail
        gmail.sendGmail("tech@parametricdining.com",
                        "fkTUfbmv2YVy",
                        "aramark_integration@parametricdining.com",
                        toEmail,
                        "ERRORS: Aramark Integration",
                        body,
                        "enable html"
                        )

def sendRequiredActionSummary(venueUid, requiredActions):
    
    body = "<h3> Aramark Integration Complete: Manual Alignment Required <h3>" 
    body += '<table border = "1">'
    for action in requiredActions:
        body += action.getHTMLFormattedRow()

    body += "</table>"

    toEmails = dbCore.getErrorEmailRecipients(venueUid)
    for toEmail in toEmails:
        toEmail = toEmail[0] 
        gmail.sendGmail("tech@parametricdining.com",
                        "fkTUfbmv2YVy",
                        "aramark_integration@parametricdining.com",
                        toEmail,
                        "ACTION REQUIRED: Aramark Integration",
                        body,
                        "enable html"
                        )


def getPackageDetails(client, facilityId, venueUid, package):
    eventId, customerId = dbCore.getPackageDetailsData(venueUid)
    packageDetails =  client.service.GetPackageDetail(FacilityID = facilityId, CustomerID = customerId, EventId = eventId, PackageID = package.PackageID)
    return packageDetails

def addPackageOnlyItem(venueUid, item, menuUid):
   
    print "Menu Item Details: " + str(item)

    menuCategoryUid = dbCore.getPackageCategory(menuUid, venueUid)   
    menuTaxUid = dbCore.getMenuTaxUid()
    itemName = item.Title
    price = item.Price
    numberServed = item.NumberServed
    printerCategory = dbCore.getPrinterCategory()
    cost = item.Cost

    menuItemUid = dbCore.insertMenuItem(venueUid, menuTaxUid, itemName, price, numberServed, printerCategory, cost)

    menuXMenuItemUid = dbCore.insertMenuXMenuItem(menuUid, menuCategoryUid, menuItemUid, price)
    
    dbCore.addMenuItemMapping(venueUid, menuItemUid, item.MenuItemID, itemName, datetime.datetime.now())

    dbCore.commit()

    return menuItemUid 

def addNewPackageToMenu(package, packageDetails, menuUid, venueUid):
    menuCategoryUid = dbCore.getPackageCategory(menuUid, venueUid)
    menuTaxUid = dbCore.getMenuTaxUid()
    itemName = package.Title
    price = 0.0
    numberServed = package.NumberServed
    printerCategory = dbCore.getPrinterCategory()
    cost = 0.0

    price = packageDetails.Price    


    menuItemUid = dbCore.insertMenuItem(venueUid, menuTaxUid, itemName, price, numberServed, printerCategory, cost)

    menuXMenuItemUid = dbCore.insertMenuXMenuItem(menuUid, menuCategoryUid, menuItemUid, price)

    dbCore.addMenuItemMapping(venueUid, menuItemUid, package.PackageID, itemName, datetime.datetime.now())

    dbCore.commit()

    return menuItemUid

def integrateMenuItem(menuItem, categoryId, menuId, venueUid):
    try:
        menuItemMappings = dbCore.getMenuItemMappings(menuItem.MenuItemID)

        menuUid = dbCore.getMenuUid(menuId)
        print "Finding menu cat for: " + categoryId
        menuCategoryUid = dbCore.getMenuCategoryUid(venueUid, categoryId)
        if menuCategoryUid is None:
            print "I can't find it"
        
        if len(menuItemMappings) == 0:
            print "Menu Cat = " + str(menuCategoryUid)
            #new item, add it to the db
            menuTaxUid = dbCore.getMenuTaxUid()
            printerCategory = dbCore.getPrinterCategory()
            
            #add menu item
            menuItemUid = dbCore.insertMenuItem(venueUid, menuTaxUid, menuItem.Title, menuItem.Price, menuItem.NumberServed, printerCategory,  menuItem.Cost)

            #add menu x menu item (s)
            dbCore.insertMenuXMenuItem(menuUid, menuCategoryUid, menuItemUid, menuItem.Price)

            #add menu item mapping
            dbCore.addMenuItemMapping(venueUid, menuItemUid, menuItem.MenuItemID, menuItem.Title, menuItem.LastUpdated)

            appliedIntegrations.append(AppliedIntegration(venueUid, ACTION_TYPE_INSERT, 'menus', 'menu_items', None, None, menuItem.Title, menuItem))

            dbCore.commit()
            
        elif len(menuItemMappings) == 1:
            #already have this mapping check it
            print "Menu Item mapping exists, updateing"
            mappingUid, menuItemUid, menuItemId, lastUpdated = menuItemMappings[0]
            #if lastUpdated != menuItem.LastUpdated:
            dbCore.updateMenuItem(menuItemUid, menuItem.Title, menuItem.Price, menuItem.NumberServed, menuItem.Cost)
#            dbCore.updateMenuXMenuItem(menuItemUid, menuUid, menuCategoryUid, menuItem.Price)
            dbCore.updateLastUpdated("menu_items_aramark", menuItem.LastUpdated, mappingUid)
            dbCore.commit()
        else:
            print "Multiple mappings for menu item, this is bad"
            raise Exception("Multiple MenuItem mappings found")
    except:
        tb = traceback.format_exc()
        errors.append(AramarkIntegrationError("Menu Item", menuItem, tb))

def getEvents():
    thisMonth = datetime.date.today()
    oneMonth = relativedelta(months = 1)
    nextMonth = thisMonth + oneMonth
    nextNextMonth = nextMonth + oneMonth

    months = [thisMonth, nextMonth, nextNextMonth]

    allEvents = []
    
    for month in months:
        events = client.service.GetEventCalendar(Month = month.month, Year = month.year, FacilityID = facilityId)
        if len(events) > 0:
            allEvents.extend(events[0])

    print "All Events: " + str(allEvents)

    return allEvents
##START

try:

    venueUid = sys.argv[1] 

    appliedIntegrations = []

    try:
        print "Integrating"
        conn = DbConnection().connection
        dbCore = Aramark_Db(conn)

        wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'

        client = Client(wsdlUrl)

        facilityId = dbCore.getFacilityId(venueUid)

        #SYNC UNITS

        print "Integrating Units"

        suites = client.service.GetSuites(FacilityID = facilityId)

        print "UNITS: " + str(suites)

        errors = []
        requiredActions = []

        for suite in suites[0]:
            try:
            #    print suite
                suiteMappings = dbCore.getUnitMappings(suite.ID) 
                
                if len(suiteMappings) == 0:
                    #no mapping for this suite yet
                    print "No Mapping"
                    unitUids = dbCore.findUnitByName(suite.SuiteNumber, venueUid)
                    
                    if len(unitUids) == 0:
                        #no matching units found, add new unit
                        unitUid = dbCore.insertUnit(venueUid, suite.SuiteNumber)
                        dbCore.addUnitMapping(venueUid, unitUid, suite.ID, suite.LastUpdated, suite.SuiteNumber)     
                        appliedIntegrations.append(AppliedIntegration(venueUid, ACTION_TYPE_INSERT, 'setup', 'units', None, None, suite.SuiteNumber, suite))
                        dbCore.commit()
                    elif len(unitUids) == 1:
                        #mapping found, create mapping row
                        unitUid = unitUids[0][0]
                        dbCore.addUnitMapping(venueUid, unitUid, suite.ID, suite.LastUpdated, suite.SuiteNumber)
                        dbCore.commit()
                    else:
                        print "Two units with the same name, I don't know what to do"
                        #shit we have two units with the same name
                        #TODO : handle this 
                elif len(suiteMappings) == 1:
                    #Mapping Found, check
                    mappingUid, suiteId, unitUid, isActive, lastUpdated = suiteMappings[0]
                    if(lastUpdated != suite.LastUpdated):
                        dbCore.updateSuite(unitUid, suite.SuiteNumber)
                        dbCore.updateLastUpdated("units_aramark", suite.LastUpdated, mappingUid)
                        dbCore.commit()

                else: 
                    print "multiple suite mappings, this is bad"
                    raise Exception("Multiple Suite mappings found")
            except:
                tb = traceback.format_exc()
                errors.append(AramarkIntegrationError("Suite", suite, tb))

        #SYNC EVENTS

        print "Integrating Events"

        events = getEvents()

        menuIds = [] 


        for event in events:
            print "Integrating Event: " + event.Description
            try:
                if event.MenuID not in menuIds:
                    menuIds.append(event.MenuID)

                eventMappings = dbCore.getEventMappings(event.EventID)

                if len(eventMappings) == 0:
                    #no matching events found
                    eventUids = dbCore.findEventByNameAndDate(event.Description, event.EventDateTime)

                    if len(eventUids) == 0:
                        #no matching events found, add new event
                        venueLocalEventDate = event.EventDateTime
                        localTimeZone = dbCore.getVenueTimeZone(venueUid) 
                        localTime = localTimeZone.localize(venueLocalEventDate, is_dst=None)
                        utcTime = localTime.astimezone(pytz.utc)

                        eventUid = dbCore.insertEvent(venueUid, utcTime)
                        dbCore.insertEventXVenue(venueUid, eventUid, event.Description)
                        dbCore.addEventMapping(eventUid, event.EventID, event.MenuID)
                        appliedIntegrations.append(AppliedIntegration(venueUid, ACTION_TYPE_INSERT, 'setup', 'events', None, None, event.Description, event))
                        dbCore.commit()
                    elif len(eventUids) == 1:
                        #mapping found
                        eventUid = eventUids[0][0]
                        dbCore.addEventMapping(eventUid, event.EventID, event.MenuID)
                        dbCore.commit()
                        
                    else:
                        print "I found multiple events with the same name/date I don't know what to do"

                elif len(eventMappings) == 1:
                    #mapping found, check
                    mappingUid, eventId, eventUid, menuId, lastUpdated = eventMappings[0]

                    venueLocalEventDate = event.EventDateTime
                    localTimeZone = dbCore.getVenueTimeZone(venueUid)
                    localTime = localTimeZone.localize(venueLocalEventDate, is_dst=None)
                    utcTime = localTime.astimezone(pytz.utc)        

                    dbCore.updateEvent(eventUid, utcTime)
                    dbCore.updateEventXVenue(venueUid, eventUid, event.Description)
                    dbCore.commit()
                else:
                    print "multiple event mappings, this is bad"
                    raise Exception("Multiple Event mappings found")
            except:
                tb = traceback.format_exc()
                errors.append(AramarkIntegrationError("Event", event, tb))
       
        print str(menuIds)
        #SYNC MENU CATEGORIES

        print "Integrating Menu Categories"


        menus = client.service.GetMenuList(FacilityID = facilityId)
        for menu in menus[0]:
            menuId = menu.MenuID
            menuCategories = client.service.GetMenuCategoriesList(FacilityID = facilityId, MenuID = menuId)
            print str(menuId)
            if len(menuCategories) != 0:
                for menuCategory in menuCategories[0]:
                    try:
                        menuCategoryMappings = dbCore.getMenuCategoryMappings(menuCategory.CategoryID, venueUid)

                        if len(menuCategoryMappings) == 0:
                            #NEW CATEGORY, do something about it
                            dbCore.addMenuCategoryMapping(menuCategory.CategoryID, menuCategory.Title, venueUid)
                            dbCore.commit()
                            requiredActions.append(AramarkRequiredAction("Menu Category", "Manually Map to menu_category", menuCategory.Title))
                        elif len(menuCategoryMappings) == 1:
                            #mapping exists, do nothing
                            print "I found my mapping, loving life!"
                        else:
                            print "Multiple Menu Category Mappings, this is bad"
                            raise Exception("Multiple Menu Category mappings found")

                        menuSubCategories = client.service.GetMenuSubCategoriesList(FacilityID=facilityId, MenuID = menuId, CategoryID=menuCategory.CategoryID)

                        if len(menuSubCategories) != 0:
                            for menuSubCategory in menuSubCategories[0]:
                                menuCategoryMappings = dbCore.getMenuCategoryMappings(menuSubCategory.SubCategoryID, venueUid)

                            if len(menuCategoryMappings) == 0:
                                #NEW CATEGORY, do something about it
                                dbCore.addMenuCategoryMapping(menuSubCategory.SubCategoryID, menuSubCategory.Title, venueUid)
                                dbCore.commit()
                                requiredActions.append(AramarkRequiredAction("Menu Category", "Manually Map to menu_category", menuCategory.Title))
                            elif len(menuCategoryMappings) == 1:
                                #mapping exists, do nothing
                                print "I found my mapping, loving life!"
                            else:
                                print "Multiple Menu Category Mappings, this is bad"
                                raise Exception("Multiple Menu Category mappings found")
                        else:   
                            print "Category " + str(menuCategory.Title) + " seems to have no sub cats, this is ok"
                    except:
                        tb = traceback.format_exc()
                        errors.append(AramarkIntegrationError("Menu Category", menuCategory, tb))
            else:
                print "Menu " + str(menuId) + " seems to have no categories"
        #SYNC MENUS

        print "Integrating Menus"

        for menu in menus[0]:
            try:
                menuId = menu.MenuID
                menuMappings = dbCore.getMenuMappings(menuId)

                if len(menuMappings) == 0:
                    #no existing menu mappings, this must be a new menu, add it to our system
                    menuTypeUid = dbCore.getMenuTypeUid()
                    menuUid = dbCore.insertMenu(venueUid, menu.MenuName, menuTypeUid)
                    dbCore.addMenuMapping(menuUid, menuId)
                    dbCore.commit()

                elif len(menuMappings) == 1:
                    #ah, just right
                    print "Menu mapping found, all is right with the world"
                else:
                    print "There are multiple menu mappings, this is bad"
            except:
                tb = traceback.format_exc()
                errors.append(AramarkIntegrationError("Menu", menuId, tb))


        #SYNC MENU ITEMS
        print "Integrating Menu Items"

        for menu in menus[0]:
            menuId = menu.MenuID
            menuCategories = client.service.GetMenuCategoriesList(FacilityID = facilityId, MenuID = menuId)
           
            if len(menuCategories) > 0: 
                for menuCategory in menuCategories[0]:

                    menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID)

                    if len(menuItems) > 0:
                        for menuItem in menuItems[0]:
                            integrateMenuItem(menuItem, menuCategory.CategoryID, menuId, venueUid)


                    menuSubCategories = client.service.GetMenuSubCategoriesList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID)

                    #if a category has subcats you need to specify the sub cat id to get it's items
                
                    if len(menuSubCategories) > 0 and  len(menuSubCategories[0]) > 0:
                        for menuSubCategory in menuSubCategories[0]:
                            menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID, SubCategoryID = menuSubCategory.SubCategoryID)
                            if len(menuItems) != 0:
                                for menuItem in menuItems[0]:
                                    integrateMenuItem(menuItem, menuSubCategory.SubCategoryID, menuId, venueUid)
                    else:
                        print "No categories for this menu" 
        unCategorizedMenuXMenuItems = dbCore.getUnCategorizedMenuXMenuItems(venueUid)
        for item in unCategorizedMenuXMenuItems:
            requiredActions.append(AramarkRequiredAction("MenuXMenuItem", "Manually set a category for this menu_x_menu_item", item[1] + ", mxm_uid = " + str(item[0])))

        #SYNC MENU PACKAGES
        
        for menu in menus[0]:
            menuId = menu.MenuID
            menuUid = dbCore.getMenuUid(menuId)
            menuPackages = client.service.GetMenuPackagesList(FailiityID = facilityId, MenuID = menuId)

            if len(menuPackages) > 0:
                for menuPackage in menuPackages[0]:
                    
                    #1. Check if the package is an item
                    menuItemId = menuPackage.PackageID
                    menuItemUids = dbCore.getMenuItemMappings(menuItemId)

                    packageDetails = getPackageDetails(client, facilityId, venueUid, menuPackage)

                    if len(menuItemUids) == 1:
                        #do nothing for now
                        packageItemUid = menuItemUids[0][1]
                    elif len(menuItemUids) > 1:
                        print "Double package item mapping this is bad"
                    else:
                        #new package to add to the menu
                        packageItemUid = addNewPackageToMenu(menuPackage, packageDetails, menuUid, venueUid)
                         
                    #2. Get all of the items in the package
                    packageItems = packageDetails.PackageMenuItems

                    if packageItems is not None and  len(packageItems) != 0:
                        for packageItem in packageItems[0]:
                            menuItemUids = dbCore.getMenuItemMappings(packageItem.MenuItemID)
                            if len(menuItemUids) == 1:
                                #we are good the menu item already is integrated
                                menuItemUid = menuItemUids[0][1]
                            elif len(menuItemUids) > 1:
                                print "too many item mappings"
                            else:
                                #new item add it to a package only menu
                                itemDetails = client.service.GetMenuItemDetail(FacilityID = facilityId, MenuItemID = packageItem.MenuItemID)
                                itemDetails = itemDetails[0][0]                      
                                print str(itemDetails)
                                menuItemUid = addPackageOnlyItem(venueUid, itemDetails, menuUid)
                                 
                            dbCore.insertMenuPackageXItem(packageItemUid, menuItemUid, 1, 'order')
                    else:
                        print "This package has no items... ... weird"
            else:
                print "No Packages for this menu"



 
        #TEMP SYNC CUSTOMERS -- i we need the our super patrons system to go live for this to work

        CUSTOMER_PAGE_SIZE = 10
        currentPage = 1
   
        while True:
            customers = client.service.GetCustomerAccountsForFacility(FacilityID = facilityId, CurrentPage = currentPage, PageSize = CUSTOMER_PAGE_SIZE)
            if len(customers) == 0 or len(customers[0]) == 0:
                break
            currentPage = currentPage + 1
            print "Customer Page Length: " + str(len(customers[0]))

            x = 0
            for customer in customers[0]:
                print "Integrating customer"
                x = x + 1
                print str(x)
                print "CUSTOMERS: " + str(customer)
                customerId = customer.CustomerID
                customerName = customer.CompanyName
                accountId = customer.AccountID
                accountFirst = customer.FirstName
                accountLast = customer.LastName
                #accountSuiteId = 
                suiteId =  customer.DefaultSuiteID
            
                patronUid = None

                #handle the customer -> super patron mappings
                customerMappings = dbCore.getCustomerMappings(customerId)
                if len(customerMappings) == 0:
                    print "adding super patron"
                    superpatronUid = dbCore.insertSuperpatron(venueUid, customerName)
                    dbCore.addSuperpatronMapping(venueUid, superpatronUid, customerId, customerName)
                    dbCore.commit() 

                superpatronUid = dbCore.getSuperpatronUid(venueUid, customerId)

                #handle the account -> patron mappings
                accountMappings = dbCore.getAccountMappings(accountId)
                if len(accountMappings) == 0:
                    patronUid = dbCore.insertPatron(accountFirst, accountLast, superpatronUid)
                    dbCore.insertVenuesXSuiteHolders(venueUid, patronUid)
                    dbCore.addPatronMapping(venueUid, patronUid, accountId, accountFirst, accountLast)
                    appliedIntegrations.append(AppliedIntegration(venueUid, ACTION_TYPE_INSERT, 'patrons', 'patrons', None, None, customerName, customer))
                    dbCore.commit()
                    print "Customer Inserted"
                elif len(accountMappings) == 1:
                    print "Under Construction"
                    patronUid = accountMappings[0][1]
                else:
                    print "There are multiple customer mappings, this is bad"

                if suiteId != NULL_ID and patronUid is not None:
                    unitMappings = dbCore.getUnitMappings(suiteId)
                    if unitMappings is not None and len(unitMappings) > 0:
                        unitUid = unitMappings[0][2]
                    
                        dbCore.insertUnitXPatron(patronUid, unitUid, venueUid) 
            
        if len(errors) > 0:
            sendIntegrationErrorSummary(venueUid, errors)
            sys.exit()

        if len(requiredActions) > 0:
            print "Manual Integration Required"
            sendRequiredActionSummary(venueUid, requiredActions)
            

               
                


        '''
           Temporarily Aramark Customers are going to be imported via a CSV file.  I think eventually the goal is going to be to add
            an endpoint to the SOAP api but for now this is what we are working with. 
        '''
        '''old csv based customer integration
        print "Integrating Customers"

        with open('aramark_temp_customers.csv', 'rb') as csvfile:
            customerReader = csv.reader(csvfile, delimiter=',')
            customerReader.next() #skip header
            for row in customerReader:
                customerId, customerName, accountId, accountFirst, accountLast, accountSuiteId, suiteNumber = row
                
                customerMappings = dbCore.getCustomerMappings(accountId)
                print str(customerMappings)
                if len(customerMappings) == 0:
                    patronUid = dbCore.insertPatron(accountFirst, accountLast)
                    dbCore.insertVenuesXSuiteHolders(venueUid, patronUid) 
                    dbCore.addPatronMapping(patronUid, accountId, accountFirst, accountLast)
                    appliedIntegrations.append(AppliedIntegration(venueUid, ACTION_TYPE_INSERT, 'patrons', 'patrons', None, None, customerName, row))  
                    dbCore.commit()
                    print "Customer Inserted"
                elif len(customerMappings) == 1:
                    print "Under Construction"
                else:
                    print "There are multiple customer mappings, this is bad"

                unitUid = dbCore.getUnitUidFromSuiteId(accountSuiteId.lower())
                patronUid = dbCore.getPatronUidFromAccountId(accountId.lower())

                unitXPatronUid = dbCore.insertUnitXPatrons(unitUid, patronUid)
                print str(unitUid) + " " + str(patronUid)
                if unitXPatronUid != 0: #unitXPatronUid will be 0 if we didn't create a new mapping
                    dbCore.insertUnitPatronInfo(unitXPatronUid, venueUid) 
        
                dbCore.commit()
            '''
        sendAppliedIntegrationSummary(venueUid, appliedIntegrations)
    except:
        print "Inner Except " + venueUid
        tb = traceback.format_exc()
        print tb
        toEmails = dbCore.getErrorEmailRecipients(venueUid)
        print str(toEmails)
        for toEmail in toEmails:
            toEmail = toEmail[0]
            print "Script crashed, sending an email to " + toEmail
            print traceback.format_exc()
            #gmail.sendGmail("tech@parametricdining.com",
            #                "fkTUfbmv2YVy",
            #                "aramark_integration@parametricdining.com",
            #                toEmail,
            #                "!CRASHED!: ARAMARK INTEGRATION",
            #                "<h3> Your script crashed: </h3>" + traceback.format_exc(),
            #                "enable html"
            #                ) 
            MailGun.sendEmail("mail@bypassmobile.com", toEmail, "!CRASHED!: ARAMARK INTEGRATION", "<h3> Your script crashed: </h3>" + traceback.format_exc())
except:
    tb = traceback.format_exc()
    #gmail.sendGmail("tech@parametricdining.com", 
    #                "fkTUfbmv2YVy", 
    #                "aramark_integrations@parametricdining.com", 
    #                "nate@parametricdining.com", 
    #                "!CRASHED!: ARAMARK INTEGTRATION", 
    #                "<h3>Your script encountered an non-recoverable error: </h3>" + traceback.format_exc(), "enable html")
    MailGun.sendEmail("mail@bypassmobile.com", toEmail, "!CRASHED!: ARAMARK INTEGTRATION",  "<h3>Your script encountered an non-recoverable error: </h3>" + traceback.format_exc())

    
