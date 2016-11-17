import redis
from config import CheckMateConfig
import logging
import traceback
from Levy_DB import Levy_Db
import HipChat
import uuid
from db_connection import DbConnection
import os
import sys
import HipChat
import hashlib
import requests
import shutil
import IntegrationEmailer

class LevyIntegrationWorker():
    
    LOCK_STRING = "LOCK ACQUIRED"
    UNLOCK_STRING = "LOCK RELEASED"

    insertCount = 0
    insertFailCount = 0
        
    updateCount = 0
    updateFailCount = 0

    imageCount = 0
    imageFailCount = 0

    removeCount = 0
    removeFailCount = 0

    db = None

    def __init__(self, levyDB):
        self.db = DbConnection().connection    
        self.levyDB = levyDB

    def setLock(self):
        self.levyDB.addLogRow(self.LOCK_STRING)

    def releaseLock(self):
        self.levyDB.addLogRow(self.UNLOCK_STRING)
        
    def isLocked(self):
        lockStatus = self.levyDB.getLastAction()
        print "LOCK STAT: " + str(lockStatus)        
        if lockStatus != self.UNLOCK_STRING or lockStatus is None: #is none in case this is the first row
            return True;
        else:
            return False;

    def handleError(self, errorMessage):
        #might not be the best idea to send all of these we were hammering hipchat pretty hard until it flow rated us
        #HipChat.sendMessage(errorMessage, "IntWorker", 1066556, "yellow")
        self.levyDB.addLogRow(errorMessage)
       
        
    def consolidatePurgatoryRows(self, applyActionRows):
        actions = {}

        for row in applyActionRows:
            if row[2] != None:
                if actions.get(row[2]) == None:
                    actions[row[2]] = []
                actions[row[2]].append(row)
            else:
                temp_uuid = uuid.uuid4()
                actions[temp_uuid] = []
                actions[temp_uuid].append(row)

        return actions
        

    def processAction(self, action):
        
        requiredAction = action[0][9] # all actions are the same
        self.levyDB.addLogRow("Processing Purgatory Row: " + str(action[0][0]))        

        if requiredAction == 'remove':
            removeAction = action[0] #there should only ever be one row

            pointerSchema = action[0][3]
            pointerTable = action[0][4]
            pointerField = action[0][5]
            uid = action[0][6]

            print "REMOVING: " + str(pointerSchema) + " " + str(pointerTable) + " " + str(pointerField) + " " + str(uid)

            success, errorMessage = self.levyDB.removeRow(pointerSchema, pointerTable, pointerField, uid)

            if success:
                print "removed" + str(uid)
                self.removeCount += 1
                self.levyDB.purgatoryRowApplied(action[0][0])
                return True
            else:
                print errorMessage
                self.removeFailCount += 1
                self.levyDB.purgatoryRowFailed(action[0][0], errorMessage)
                handleError(errorMessage)
                return False


        if requiredAction == 'image':
            imageAction = action[0] # there should only ever be one row
            
            #1. Download the iamge to the correct directory (and name it correctly in the process)
            imageUrl = imageAction[8]
            venueUid = imageAction[1]
            imageType = imageAction[5]
            imagePointer = imageAction[6]
            imageName = str(imagePointer) + ".png"  
            imagePath = '/data/media/201/images/events_x_venues/' + imageName 
            print "Downloading Image From: " + imageUrl
            downloadRequest = requests.get(imageUrl, stream = True)
            if downloadRequest.status_code == 200:
                with open(imagePath, 'w+') as imageFile:
                    downloadRequest.raw.decode_content = True
                    shutil.copyfileobj(downloadRequest.raw, imageFile)
                    
            else:
                errorMessage =  "Image failed to download.  Status code: " + str(downloadRequest.status_cod)
                self.levyDB.purgatoryRowFailed(action[0]. errorMessage)
                handleError(errorMessage)
                self.imageFailCount += 1
                return False
            
            #2. Hash the file
            imageHash = hashlib.md5(open(imagePath).read()).hexdigest()      
    
            #3. Insert a row into media.images
            success, errorMessage = self.levyDB.insertImageRow(imagePointer, imageType, imageHash)
            if success:
                self.imageCount += 1
                self.levyDB.markEventXVenueHasImage(imagePointer)
                self.levyDB.purgatoryRowApplied(imageAction[0])
                return True
            else:
                self.imageFailCount += 1
                self.levyDB.purgatoryRowFailed(imageAction[0], errorMessage)
                self.handleError(errorMessage)
                return False
       
        if requiredAction == 'edit' or requiredAction == 'deactivate' or requiredAction == 'reactivate':
        
            success, errorMessage = self.levyDB.updateRow(action[0][3], action[0][4], action[0][5], action[0][6], action[0][8])
           

 
            if success:

                
                if requiredAction == 'reactivate' and action[0][3] == 'menus' and action[0][4] == 'menu_items':
                    itemClassification = levyDB.getItemClassificationFromMenuItemUid(action[0][1], action[0][6])
                    BEVERAGE_ITEM_CLASSIFICATIONS = ["BEV-HOT BEVERAGES", "BAR MIXERS", "LIQUOR-MISC.", "BEV-JUICE", "BEV-SOFT DRINKS", "BEER-IMPORTED", "BEER-DOMESTIC", "LIQUOR-VODKA", "LIQUOR-SCOTCH", "BEV-WATER", "BEVERAGE PACKAGES", "WINE-RED", "WINE-WHITE", "WINE-SPARKLING", "LIQUOR-WHISKEY", "LIQUOR-TEQUILA", "LIQUOR-RUM", "LIQUOR-GIN", "FOOD PACKAGE"]
                    if itemClassification in BEVERAGE_ITEM_CLASSIFICATIONS:
                        self.levyDB.insertParMenuItem(venueUid, menuUid, message)


                self.updateCount += 1
                self.levyDB.purgatoryRowApplied(action[0][0])
                return True
            else:
                print errorMessage
                self.updateFailCount += 1
                self.levyDB.purgatoryRowFailed(action[0][0], errorMessage)
                handleError(errorMessage)
                return False

        if requiredAction == 'add':
                
            fields = list()
            values = list()

            eventName = ""

            for fieldRow in action:
                        
                if fieldRow[4] == 'events_x_venues' and fieldRow[5] == 'event_name':
                    
                    eventName = fieldRow[8] 
                    action.remove(fieldRow) #we don't actually want to use the value of the event name for the auto insert
                                            # but we will need it later for inserting in to the cross table
                    self.levyDB.purgatoryRowApplied(fieldRow[0])
                else:
                    fields.append(fieldRow[5])
                    values.append(fieldRow[8])                   

            #message = the last inserted id     
            print str(action)
            if action[0][3] == 'patrons' and action[0][4] == 'venues_x_suite_holders':
                 success, message = self.levyDB.insertRow(action[0][3], action[0][4], fields, values, insertIgnore=True)

            elif action[0][3] == 'info' and action[0][4] == 'unit_x_patrons':
                 success, message = self.levyDB.insertRow(action[0][3], action[0][4], fields, values, insertIgnore=True)      
            
            else:
                 success, message = self.levyDB.insertRow(action[0][3], action[0][4], fields, values)
            
               

            if success:
                print "SUCCESS"
                #warning, we had to get a little hacky to insert into the X tables
                if action[0][3] == 'setup' and action[0][4] == 'employees':
                   
                    success, newVenuesXEmployeeUid = self.levyDB.insertRow('setup', 'venues_x_employees', ['venue_uid', 'employee_uid'], [action[0][1], message] )
              
                print "PP, EVENT NAME: " + str(eventName) 
                if action[0][3] == 'setup' and action[0][4] == 'events' and eventName != "":
                    #events_x_venues
                    print "Adding to events_x_venues"
                    self.levyDB.insertRow('setup', 'events_x_venues', ['event_uid', 'venue_uid', 'event_name'], [message, action[0][1], eventName]) 

                    #default printer set
                    print "Adding to default printer set"
                    defaultPrinterSet = self.levyDB.getDefaultPrinterSet(action[0][1]) 
                    self.levyDB.setEventPrinterSet(message, defaultPrinterSet)                    

                    #events_x_egos
                    print "Adding default ego"
                    
                    eventTypeUid = 8 #8 = other 
                    for fieldRow in action:
                        if fieldRow[5] == "event_type_uid":
                            eventTypeUid = fieldRow[8]
                    defaultEgo = self.levyDB.getDefaultVenueEgo(action[0][1], eventTypeUid)
                    self.levyDB.setEventXEgo(message, defaultEgo)

                    #events_x_settings
                    print "adding default events_x_settings"
                    defaultSettings = self.levyDB.getDefaultEventSettings(action[0][1])
                    for defaultSetting in defaultSettings:
                        eventSettingUid, defaultValue = defaultSetting
                        self.levyDB.setDefaultEventSetting(message, eventSettingUid, defaultValue)
                    
                    #settings.event_settings
                    print "Adding default event_settings"
                    #defaultSuitemateSettings = self.levyDB.getDefaultSuitemateSettings(action[0][1])
                    self.levyDB.setEventSuitemateSettings(action[0][1], message)

                   
                if action[0][3] == 'patrons' and action[0][4] == 'patrons':
                    
                    print " --- NEW PATRON INSERTED --- "
                    self.levyDB.insertRow('patrons', 'venues_x_suite_holders', ['venue_uid', 'patron_uid'], [action[0][1], message])
                    #get levy temp customer, 
                    levyCustomer = self.levyDB.getTempLevyCustomer(action[0][13])
                    print "Levy temp customer: " + str(levyCustomer)
                    #insert into the clone patrons table
                    self.levyDB.insertRow('patrons', 'clone_patrons', ['id', 'company_name'], [message, levyCustomer[1]])
 
                    unit_uid = self.levyDB.findUnit(levyCustomer[3], action[0][1])
                    print "Found unit = " + str(unit_uid)
                    try:
                        unit_uid = unit_uid[0] 
                    except:
                        unit_uid = None

                    if unit_uid is not None:
                        print "unit uid is not none"
                        unitPatronUid = self.levyDB.insertUnitXPatrons(unit_uid, message)                    
                        print "UnitXPatronUid = " + str(unitPatronUid)
                        self.levyDB.insertUnitPatronInfo(unitPatronUid, action[0][1])
                    print "Done with patron insert"
                if action[0][3] == 'info' and action[0][4] == 'unit_x_patrons':
                    print "Inserting unit patron info: " + str(message)
                    self.levyDB.insertUnitPatronInfo(message, action[0][1])

                if action[0][3] == 'setup' and action[0][4] == 'units':
                    unitUid = message
                    venueUid = action[0][1]
                    print "Adding anon patron to unit: " + str(unitUid)
                    self.levyDB.insertAnonPatron(unitUid)
                if action [0][3] == 'menus' and action[0][4] == 'menu_items':
                    #insert new menu_item in to the menu_x_menu_items table
                    venueUid = action[0][1]
                    itemNumber = action[0][13]
                    levyTempItem = self.levyDB.getTempLevyItem(itemNumber, venueUid)
                    itemClassification, mainPrice, doePrice = levyTempItem
                   
                    print "CLASSIFICATION: " + str(itemClassification)    
                    menuCategoryUid = self.levyDB.getMenuCategoryFromClassification(itemClassification, venueUid)
                    
                    MAIN_MENU = 1
                    PREORDER_MENU = 2
                    DOE_MENU = 3
                  
                    menuTypes = [1, 2]
                    
                    for menuType in menuTypes:
                        menuUid = self.levyDB.findVenueMenu(venueUid, menuType)
                        
                        if menuType == MAIN_MENU:
                            newPrice = doePrice
                        elif menuType == PREORDER_MENU:
                            newPrice = mainPrice
                        #elif menuType == DOE_MENU:
                        #    newPrice = doePrice

                        print "MENU_UID: " + str(menuUid)
                        ordinal = self.levyDB.getMenuXMenuItemOrdinal(menuUid)
                        success, failReason = self.levyDB.insertRow('menus', 'menu_x_menu_items', ['menu_uid', 'menu_category_uid', 'menu_item_uid', 'price', 'ordinal'],
                            [menuUid, menuCategoryUid, message, newPrice, ordinal])
                        print failReason               
                        
                        if(success):
                            self.levyDB.insertMenuXMenuCategory(menuUid, menuCategoryUid) 
                            
                            BEVERAGE_ITEM_CLASSIFICATIONS = ["BEV-HOT BEVERAGES", "BAR MIXERS", "LIQUOR-MISC.", "BEV-JUICE", "BEV-SOFT DRINKS", "BEER-IMPORTED", "BEER-DOMESTIC", "LIQUOR-VODKA", "LIQUOR-SCOTCH", "BEV-WATER", "BEVERAGE PACKAGES", "WINE-RED", "WINE-WHITE", "WINE-SPARKLING", "LIQUOR-WHISKEY", "LIQUOR-TEQUILA", "LIQUOR-RUM", "LIQUOR-GIN"]
                            if itemClassification in BEVERAGE_ITEM_CLASSIFICATIONS:
                                self.levyDB.insertParMenuItem(venueUid, menuUid, message)
                                
 
                    #insert into menu_item_x_option_groups

                    defaultOptionGroups = self.levyDB.getDefaultOptionGroups(action[0][1])
                    for option in defaultOptionGroups:
                        self.levyDB.setOptionGroup(message, option[0])
                
                    

                #repoint the encryption row from purgatory to the newly inserted row 
                for fieldRow in action:
                    self.levyDB.updateEncryptionKey(fieldRow[3], fieldRow[4], fieldRow[0], message)
                    
                self.insertCount += 1
                for fieldRow in action:
                    self.levyDB.purgatoryRowApplied(fieldRow[0])
    
                

                #insert into the levy integrations table for the mapping between this new item and its levy row
                if action[0][13] is not None:
                    tempId = action[0][13]
                    if action[0][4] == 'employees':
                        tempField = 'employee_id'
                        tempTable = 'levy_temp_employees'
                        tempData = self.levyDB.getTempData(tempId, tempField, tempTable)
                        print str(tempData[0]) +  " " + str(action[0][1]) + "  " + str(newVenuesXEmployeeUid)
                        self.levyDB.insertLevyVenuesXEmployees(tempData[0], action[0][1], newVenuesXEmployeeUid)

                    if action[0][4] == 'units':
                        tempField = 'suite_id'
                        tempTable = 'levy_temp_suites'
                        tempData = self.levyDB.getTempData(tempId, tempField, tempTable)
                        self.levyDB.insertUnitsLevy(tempData[0], action[0][1], message)

                    if action[0][4] == 'patrons':
                        tempField = 'customer_number'
                        tempTable = 'levy_temp_customers'
                        tempData = self.levyDB.getTempData(tempId, tempField, tempTable)
                        self.levyDB.insertLevyPatron(tempData[0], action[0][1], message)

                    if action[0][4] == 'events':
                        tempField = 'event_id'
                        tempTable = 'levy_temp_events'
                        tempData = self.levyDB.getTempData(tempId, tempField, tempTable)
                        print "Inserting Levy Event"
                        self.levyDB.insertLevyEvent(tempData[1], message, action[0][1])

                    if action[0][4] == 'menu_items':
                        
                        tempField = 'item_number'
                        tempTable = 'levy_temp_menu_items'
                        tempData = self.levyDB.getTempData(tempId, tempField, tempTable)
                        print "Inserting levy menu item: " + str(tempId) + " " + tempField + " " + tempTable
                        self.levyDB.insertLevyMenuItem(tempData[0], message, action[0][1], tempData[6], tempData[10], tempData[9])

                    if action[0][4] == 'menu_taxes':
                        venueUid = action[0][1]
                        taxId = action[0][8] 

                        taxId, revId, catId = action[0][13].split(',')                       
 
                        self.levyDB.insertMenuTaxesXTaxGroupsLevy(message, taxId, revId, catId, venueUid) 
                        
                return True        
                
            else:
                self.insertFailCount += 1
                for fieldRow in action:
                    self.levyDB.purgatoryRowFailed(fieldRow[0], message)
                    self.handleError(message)
    
                return False


    def main(self):

#        IntegrationEmailer.sendEmail()
#        exit()              
       
        self.insertCount = 0
        self.insertFailCount =0
        
        self.updateCount = 0
        self.updateFailCount = 0

        self.imageCount = 0
        self.imageFailCount = 0       

        if self.isLocked():
            self.handleError("WARNING: Attempted to run an integration worker while another one is running")
            HipChat.sendMessage("WARNING: Attempted to run an integration worker while another one is running", "IntCron", 1066556, "yellow")    
            sys.exit()  
     
        self.setLock()          
    
        applyActionRows = self.levyDB.getPurgatoryRowsToApply()
        
        if len(applyActionRows) == 0:
            self.releaseLock()
            sys.exit() 
        actions = self.consolidatePurgatoryRows(applyActionRows)
     
      
        print str(actions) 

#############TESTING EMAILER                
        #print "ACTIONS: " + str(actions)
            
#        IntegrationEmailer.sendEmail(actions)
#            
#        self.releaseLock()
#        sys.exit()        
##############END TESTING EMAILER
            
        appliedActions = []
        for action in actions:
            try: 
                if self.processAction(actions[action]):
                    #add action to applied actions
                    appliedActions.append(actions[action])
            except:
                tb = traceback.format_exc()
                self.levyDB.addLogRow(tb)
        
        message = "Integrations Applied: \n " + str(self.insertCount) + " new rows inserted "
        if self.insertFailCount > 0:
            message += "(" + str(self.insertFailCount) + " inserts failed)"

        message += "\n"

        message += str(self.updateCount) + " rows updated "
        if self.updateFailCount > 0:
            message += "(" + str(self.updateFailCount) + " updates failed)"

        message += "\n"

        message += str(self.imageCount) + " images inserted"
        if self.imageFailCount > 0:
            message += "(" + str(self.imageFailCount) + " images failed)"

        message += "\n"

        #:TODO
        if self.updateFailCount + self.insertFailCount + self.imageFailCount > 0:
            message += "Failures: http://www.animatedgif.net/underconstruction/btrainbow1_e0.gif"

        HipChat.sendMessage(message, "Integrations", "1066556", "purple")
    
        self.levyDB.addLogRow("Integrations Worker Completed")
        
        IntegrationEmailer.sendEmail(appliedActions)


        self.releaseLock() 





