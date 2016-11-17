import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

MAIN_MENU = 1
PREORDER_MENU = 2
DOE_MENU = 3

ALL_MENUS = [1, 2]

def updateMenuItem(dbCore, menuItem, itemName, taxUid, minOrder, maxOrder, mainPrice, doePrice, menuCategoryUid, itemNumber, newServiceChargeRate):
    uid, venueUid, menuTaxUid, posMenuItemId, name, displayName, description, serverDescription, ageVerification, price, points, minQty, maxQty, servings, printer, showImage, cost, serviceChargeRate, createdAt, updatedAt = menuItem

    
    if dbCore.checkItemActiveStatus(venueUid, uid) == False:
        print "Item needs to be activated"
        #this item must have been recently reactivated
        #reactivate the menu item levy row
        menuItemLevyUid = dbCore.getMenuItemLevyUid(venueUid, uid)
        IntegrationTools.confrimReActivate(dbCore, venueUid, 'integrations', 'menu_items_levy', menuItemLevyUid)        
        mxmRows = dbCore.getMXMRowsByItemUid(uid)
        for mxm in mxmRows:
            mxmUid = mxm[0]
            IntegrationTools.confrimReActivate(dbCore, venueUid, 'menus', 'menu_x_menu_items', mxmUid)
    else:
        print "Item is active"
    print "TAX UPDATE ( " + str(uid) + " ):  current tax uid = " + str(menuTaxUid) + " new tax uid = " + str(taxUid)
    if(menuTaxUid != taxUid):
        print "      tax uid CHANGED"
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'menu_tax_uid', uid, menuTaxUid, taxUid, levy_temp_pointer=itemNumber) 

    if(name != itemName):
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'name', uid, name, itemName, levy_temp_pointer=itemNumber)

    if(displayName != itemName):
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'display_name', uid, displayName, itemName, levy_temp_pointer=itemNumber)    

    if(minQty != minOrder):
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'minimum_qty', uid, minQty, minOrder, levy_temp_pointer = itemNumber)
    if(maxQty != maxOrder):
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'maximum_qty', uid, maxQty, maxOrder, levy_temp_pointer = itemNumber)


    print "Service Charge Update: " + str(serviceChargeRate) + " vs " + str(newServiceChargeRate * 100)
    if  float(serviceChargeRate) != (newServiceChargeRate * 100):
        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'service_charge_rate', uid, serviceChargeRate, newServiceChargeRate * 100, levy_temp_pointer = itemNumber)

    

    menuXMenuItemDatas = dbCore.getMenuXMenuItemData(uid, venueUid)
    
    inMenu = []
    
    #update the menu_x_menu_item prices
    for menuXMenuItemData in menuXMenuItemDatas:
        mxmID, price, menu_type_uid = menuXMenuItemData
        
        inMenu.append(menu_type_uid);
        
        if menu_type_uid == PREORDER_MENU:
            newPrice = mainPrice
        else:
            newPrice = doePrice

        if float(price) != float(newPrice):
            IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_x_menu_items', 'price', mxmID, price,  newPrice, levy_temp_pointer = itemNumber)
    
    for menuType in ALL_MENUS:
        if menuType not in inMenu:
            print "Adding item to menu"
            #add it
          
            if menuType == MAIN_MENU:
                newPrice = doePrice
            elif menuType == PREORDER_MENU:
                newPrice = mainPrice
          #  elif menuType == DOE_MENU:
          #      newPrice = doePrice
            else:
                continue
            #find the right menu
            menuUid = dbCore.findVenueMenu(venueUid, menuType)
            
            ordinal = dbCore.getMenuXMenuItemOrdinal(menuUid)
 
            insert_uuid = uuid.uuid4()
            #print "menuUid: " + str(menuUid)
            IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_uid', menuUid, False, auto_apply = True)
            IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_category_uid', menuCategoryUid, False, auto_apply = True)
            IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_item_uid', uid, False, auto_apply = True)
            IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_x_menu_items', 'price', newPrice, False, auto_apply = True)
            IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_x_menu_items', 'ordinal', ordinal, False, auto_apply = True)

def integrate(dbCore):

    dbCore.addLogRow("Integrating Menus")
    print "Integrating Menus"
    menuRows = dbCore.getLevyTempMenuData() 

    success = True
    errorLogRows = []
    errorVenues = []
    itemNumberE = ""
    entityCodeE = ""

    AGE_VERIFICATION_PROD_CLASSES = [31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 53, 57]
    for row in menuRows:
        try: 
            itemNumber, itemName, posProdClass, taxId, entityCode, venueUid, minOrder, maxOrder, mainPrice, doePrice, revId, locationName, classification, catId = row
        except:
            continue

        itemName = locationName #levy is now providing a location name that they want us to use        

        try: 
            itemNumberE = itemNumber
            entityCodeE = entityCode
            print "getting tax uid: " + str(taxId) + " " + str(revId) + " " + str(venueUid)
            taxUid = dbCore.getTaxId(taxId, revId, venueUid, catId)
            print "Got tax uid: " + str(taxUid)


            levyMenuItem = dbCore.getLevyMenuItem(itemNumber, venueUid)
        
            if(len(levyMenuItem) == 0):
                #we don't have a mapping for that item at that venue
                
                #attempt to find a matching item
                menuItems = dbCore.findMenuItems(venueUid, itemName)
                menuItems = []  # this line is so, we will ALWAYS insert unmapped items as a new menu_items, we will not look for name matches
              
                if len(menuItems) == 0:
                    #we couldn't find a matching item at that venue :(, 
                    #print "No menu item found, CREATING"
                    #so were going to have to add one
                    insert_uuid = uuid.uuid4()
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'venue_uid', venueUid, False, itemNumber, auto_apply = True)


                    serviceChargeData = dbCore.getLevyServiceChargeRate(entityCode, revId, catId)
                    if serviceChargeData is None:
                        serviceCharageRate = 0.0
                    else:
                        serviceChargeRate = serviceChargeData[1]

                    #TODO: find correct tax it
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'menu_tax_uid', taxUid, False, itemNumber, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'name', itemName, False, itemNumber, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'display_name', itemName, False, itemNumber, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'minimum_qty', minOrder, False, itemNumber, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'maximum_qty', maxOrder, False, itemNumber, auto_apply = True)

                    printerCategory = dbCore.getPrinterCategory(revId);

                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'printer_category', printerCategory , False, itemNumber, auto_apply = True)
                    
                    if posProdClass in AGE_VERIFICATION_PROD_CLASSES:
                        IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'requires_age_verification', 1, False, itemNumber, auto_apply = True)
                    else:
                        IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'requires_age_verification', 0, False, itemNumber, auto_apply = True)

                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_items', 'service_charge_rate', serviceChargeRate * 100, False, itemNumber, auto_apply = True)

                    #when the insert is approved we are going to add the new items to the venues menus, but for now we can't

                else:
                    #we found a match, add it to menu_items_levy
                    uid =  menuItems[0][0]
                    print str(taxId)
                    dbCore.insertLevyMenuItem(itemNumber, uid, venueUid, posProdClass, taxId, revId)
                    #then follow the mapping to make sure its up to date
                    menuItem = dbCore.getMenuItem(uid)
                    menuCategoryUid = dbCore.getMenuCategoryFromClassification(classification, venueUid)

                    serviceChargeData = dbCore.getLevyServiceChargeRate(entityCode, revId, catId)

                    if serviceChargeData is None:
                        serviceCharageRate = 0.0
                    else:
                        serviceChargeRate = serviceChargeData[1]

                    updateMenuItem(dbCore, menuItems[0], itemName, taxUid, minOrder, maxOrder, mainPrice, doePrice, menuCategoryUid, itemNumber, serviceChargeRate) 
            
            else:
                print "Mapping Found, UPDATING"
                #we DO have a mapping for that item at that venue
                uid = levyMenuItem[0][2]
                serviceChargeData = dbCore.getLevyServiceChargeRate(entityCode, revId, catId)
                
                if serviceChargeData is None:
                    serviceChargeRate = 0.0
                else:
                    serviceChargeRate = serviceChargeData[1]
                

                menuItem = dbCore.getMenuItem(uid)
                menuCategoryUid = dbCore.getMenuCategoryFromClassification(classification, venueUid)
                updateMenuItem(dbCore, menuItem, itemName, taxUid, minOrder, maxOrder, mainPrice, doePrice, menuCategoryUid, itemNumber, serviceChargeRate)
                dbCore.updateLevyMenuItem(itemNumber, venueUid, posProdClass, taxId, revId)
        except:
            success = False
            tb = traceback.format_exc()
            dbCore.addLogRow(str(row))
            logRowId = dbCore.addLogRow("Error processing menu row (item_number= " + str(itemNumberE) + ", entity_code' " + str(entityCodeE) + ") Stacktrace: " + tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venueUid)
                    
    venues = dbCore.getAllLevyIntegrationVenues()
        
    print "Checking for inactive menu_items"

    for venue in venues:
        venue_uid = venue[0]
        print str(venue_uid)
        
        inactiveMenuXMenuItems = dbCore.getMenuXMenuItemsToInactivate(venue_uid)
        
        
        print str(len(inactiveMenuXMenuItems))
        for menuXMenuItem in inactiveMenuXMenuItems:
            mxmUid = menuXMenuItem[0]
            menuItemUid = menuXMenuItem[1]
            print "Deactivating " + str(mxmUid)
            
            IntegrationTools.confirmDeactivate(dbCore, venue_uid, 'menus', 'menu_x_menu_items', mxmUid)
            menuItemLevyUid = dbCore.getMenuItemLevyUid(venue_uid, menuItemUid)
            IntegrationTools.confirmDeactivate(dbCore, venue_uid, 'integrations', 'menu_items_levy', menuItemLevyUid)

            #also remove the par items
            IntegrationTools.confirmRemove(dbCore, venue_uid, 'info', 'par_menu_items', 'menu_item_uid', menuItemUid)

            IntegrationTools.confirmRemove(dbCore, venue_uid, 'info', 'unit_patron_par_items', 'menu_item_uid', menuItemUid)

    return success, errorLogRows, errorVenues;


