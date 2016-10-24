import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

MAIN_MENU = 1
PREORDER_MENU = 2
DOE_MENU = 3

def integrate(dbCore):
    dbCore.addLogRow("Integrating Menu Prices") 
    tempMenuPrices = dbCore.getTempMenuPrices()
    # menu price row:
    #  0:venue_entity_code, 1:item_number, 2:min_order, 3: max_order, 4:pos_level_id, 5:main_price, 6:pos_doe_level_id, 7:doe_price
    

    for row in tempMenuPrices:
        print"ITEM NUMBER: " + str(row[1])
        try: 
            venue_uid = dbCore.getVenueUid(row[0])

            levyMenuItems = dbCore.getLevyMenuItem(row[1], venue_uid)
                      
            if len(levyMenuItems) == 0:
                continue #can't really do anything with this, because if there is no mapping we can't know the items menu_uid
            else:

                #get the venues main, preorder and DOE menus
                menus = dbCore.getVenueMenus(venue_uid)
                for menu in menus:
                    #menu: 0 = id, 1 = menu_name (not sure if we will need this), 2 = menu_type_uid
                
                    #determine if the menu in question has an entry for the row above
                    menu_item = dbCore.getMenuXMenuItem(menu[0], levyMenuItems[0][2])
                   
                        
                    minOrder = row[2]
                   
                  
                    if menu[2] == MAIN_MENU:
                        price = row[7]    
                    elif menu[2] == PREORDER_MENU:
                        price = row[5] 
                    elif menu[2] == DOE_MENU:
                        price = row[7]

                    if  menu_item is not None:
                        #check to make sure the item data is up to date        
                        print "          match found"
                        if float(menu_item[5]) != float(price):
                            IntegrationTools.confirmUpdate(dbCore, venue_uid, 'menus', 'menu_x_menu_items', 'price', menu_item[0], menu_item[5], price, False, row[1])
                        
                        min_order = row[2]
                        max_order = row[3]
                        realMenuItem = dbCore.getMenuItem(menu_item[4])
                        if realMenuItem[10] != min_order:
                            print str(menu_item[10]) + " vs " + str(min_order)
                            IntegrationTools.confirmUpdate(dbCore, venue_uid, 'menus', 'menu_items', 'minimum_qty', realMenuItem[0], realMenuItem[10], min_order)
                        if realMenuItem[11] != max_order:
                            IntegrationTools.confirmUpdate(dbCore, venue_uid, 'menus', 'menu_items', 'maximum_qty', realMenuItem[0], realMenuItem[11], max_order) 
                    else:
                        print "          no match"
                        #insert item
                        insert_uuid = uuid.uuid4()
                        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_uid', menu[0], False)
                        
                        menuCategoryUid = dbCore.getMenuCategory(levyMenuItems[0][2], venue_uid)                    

                        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_category_uid', menuCategoryUid, False)

                        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'menus', 'menu_x_menu_items', 'menu_item_uid', levyMenuItems[0][2], False)

                        IntegrationTools.confirmInsert(dbCore, venue_uid, insert_uuid, 'menus', 'menu_x_menu_items', 'price', price, False)
             
        except:
            tb = traceback.format_exc()
            dbCore.addLogRow(tb)
                          
                     
                

            
                
            
