import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback
import math

def integrate(dbCore):

    print "Integrating Menu Packages"
    dbCore.addLogRow("Integrating Menu Pacakges")
    
    tempPackageDefinitions = dbCore.getTempLevyPackageDefinitions()

    success = True
    errorLogRows = []
    errorVenues = []

    for row in tempPackageDefinitions:
        entityCode, packageItemNumber, assignedItemNumber, matrix1, matrix2, matrix3, matrix4 = row

        print str(row)

        try:
            venueUid = dbCore.getVenueUid(entityCode)
        except:
            continue #venue not included in integration at this time

        try: 

            packageUids = dbCore.getLevyMenuItem(packageItemNumber, venueUid)
            menuItemUids = dbCore.getLevyMenuItem(assignedItemNumber, venueUid)

            if len(packageUids) < 1:
                print "Error, no package_item_mapping"
            
            elif len(menuItemUids) < 1:
               #no assignedItemNumberMapping, this could potentially be a pacakge only item, lets check

                print "         ASSIGNED ITEM MISSING !!!!!"

                if dbCore.isPackageOnlyItem(entityCode, assignedItemNumber):
                    #insert it into the db
                    print "package only item"
                    itemNumber, packageFlag, itemName, posProductClass, itemClassification, revId, taxId = dbCore.getLevyPackageOnlyTempMenuItemData(assignedItemNumber)[0]
        
                    menuItems = dbCore.findPackageMenuItems(venueUid, itemName)

                    if len(menuItems) == 0:
                        #no item with the same name, we are clear to add a new item

                        packageTaxId, packageRevId, packageCatId = dbCore.getPackageTaxData(packageItemNumber)

                        menuTaxUid = dbCore.getTaxId(packageTaxId, packageRevId, venueUid, packageCatId) 

                        price = 0
                        minimumQty = 1
                        maximumQty = 10
                        printerCategory = dbCore.getPrinterCategory(revId)
                        serviceChargeData = dbCore.getLevyServiceChargeRate(entityCode, revId)
                        if serviceChargeData is None:
                            serviceChargeRate = 0.0
                        else:
                            serviceChargeRate = serviceChargeData[1] 
                        
                        menuUid = dbCore.getPackagesMenu(venueUid)
                        menuCategoryUid = dbCore.getMenuCategoryFromClassification(itemClassification, venueUid)
                        
                        dbCore.insertNewPackageOnlyMenuItem(venueUid, menuTaxUid, itemName, price, minimumQty, maximumQty, printerCategory, serviceChargeRate * 100, menuUid, menuCategoryUid, itemNumber, posProductClass, taxId, revId)
                    else:
                                                

                        #there is already a menu_item with this name, we need to pair them up 
                        dbCore.insertLevyMenuItem(itemNumber, menuItems[0][0], venueUid, posProductClass, taxId, revId)    
                                  
                else:
                    print "regular item"
 
            else:

                if dbCore.isPackageOnlyItem(entityCode, assignedItemNumber):
                    packageTaxId, packageRevId, packageCatId = dbCore.getPackageTaxData(packageItemNumber)

                    menuTaxUid = dbCore.getTaxId(packageTaxId, packageRevId, venueUid, packageCatId)

                    menuItem = dbCore.getMenuItem(menuItemUids[0][2])

                    print "updating"

                    #if menuItem[2] != menuTaxUid:
                    #    IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_items', 'menu_tax_uid', menuItem[0], menuItem[2], menuTaxUid, None)


                packageUid = packageUids[0][2]
                menuItemUid = menuItemUids[0][2]
                menuXPackageItems = dbCore.getMenuPackageXItems(packageUid, menuItemUid)
                
                qty = round((float(matrix1) / float(matrix2) + .0001), 4)

                if len(menuXPackageItems) == 0:
        
                                        

                    #new package item mapping
                    insert_uuid = uuid.uuid4()

                    print str(insert_uuid) + " " + str(packageUid) + " " + str(menuItemUid) + " " + str(qty)

                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_packages_x_items', 'package_uid', packageUid, False, None, True, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_packages_x_items', 'menu_item_uid', menuItemUid, False, None, True, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_packages_x_items', 'qty', qty, False, None, True, auto_apply = True)
                    IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_packages_x_items', 'qty_per', 'item', False, None, True, auto_apply = True)
                elif len(menuXPackageItems) == 1:
                    #update existing mapping
                    oldQty = menuXPackageItems[0][3]
                    if qty != float(oldQty):
                        IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_packages_x_items', 'qty', menuXPackageItems[0][0], oldQty, qty, False, None, auto_apply = True)
                else:
                    #uh oh
                    print str(packageUid) + " " + str(menItemUid) + ' too many menuXPackageItem mappings, this should be impossible'
                    
                
        except:
            success = False
            tb = traceback.format_exc()
            print tb
            logRowId = dbCore.addLogRow("Error Processing Menu Packages Row (package Number = " + str(packageItemNumber) + ", assigned number = " + str(assignedItemNumber) + ")")
            dbCore.addLogRow(tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venueUid)

    packageItemsToRemove = dbCore.getItemsRemovedFromPackages()
    for packageItem in packageItemsToRemove:
        IntegrationTools.confirmRemove(dbCore, 1, 'menus', 'menu_packages_x_items', 'id', packageItem[0], auto_apply = True)
    
    dbCore.deactivatePackageItems()

    return success, errorLogRows, errorVenues
