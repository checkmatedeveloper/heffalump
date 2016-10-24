import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

def integrate(dbCore):
    
    dbCore.addLogRow("Integrating Menu Items")

    tempMenuItems = dbCore.getTempMenuItems()
        
    #Menu Item Row: 
    #0:item number, 1:package flag  2:item name  3:pos button 1 4 :pos button 2 5:pos printer label  6:pos prod class id  7:pos product class  8:rev id  9:tax id, created_at, updatedAt
       
    
    
    for row in tempMenuItems:
       
        try:  
           
            # So we have an interesting setup.  Levy only has one menu.  We have one menu per venue.
            # In order to accomadate this any new items are going to have to be added to all of 
            venues = dbCore.getAllLevyVenues() 

            # levyMenuItems = dbCore.getLevyMenuItem(row[0])
            for venue in venues:
                levyMenuItems =   dbCore.getLevyMenuItem(row[0], venue[0])
                
                #this venue does not have a mapping for that menu item
                if len(levyMenuItems) == 0:
                  
                    #see if there is a matching item that we can map
                    menu_items = dbCore.findMenuItems(venue[0], row[2])

                    if len(menu_items) >= 1:
                        
                        matchingItemFound = True # a matching item was found!!!
                        for menu_item in menu_items:
                            dbCore.insertLevyMenuItem(row[0], menu_item[0], venue[0], row[6], row[9])
                    
                    else:
                    #we didn't find a single matching menu item, so create a new menu item
                        print str(venue[0]) + " : " + str(row)
                        insert_uuid = uuid.uuid4()
                        
                        #    menu_tax_uid = dbCore.getMenuTaxUid(row[9])
                        #we are going to have to do a bunch of things after this insert is confirmed
                        IntegrationTools.confirmInsert(dbCore, venue[0], insert_uuid, 'menus', 'menu_items', 'venue_uid', venue[0], False, row[0])
                        # IntegrationTools.confirmInsert(dbCore, venue[0], insert_uuid, 'menus', 'menu_items', 'menu_tax_uid', menu_tax_uid, False)
                        taxId = dbCore.getTaxId(row[9], row[8], venue[0])
                        IntegrationTools.confirmInsert(dbCore, venue[0], insert_uuid, 'menus', 'menu_items', 'menu_tax_uid', taxId, False, row[0])
                        IntegrationTools.confirmInsert(dbCore, venue[0], insert_uuid, 'menus', 'menu_items', 'name', row[2], False, row[0])
                        IntegrationTools.confirmInsert(dbCore, venue[0], insert_uuid, 'menus', 'menu_items', 'display_name', row[2], False, row[0])
                        


                else:
                #hey perfect we already have at least one mapping we just need to check all of them to make sure that all the data is up to date

                    for levyMenuItem in levyMenuItems:
                        name = row[2]
                        menuItem = dbCore.getMenuItem(levyMenuItem[2])
                        
                        if menuItem[4] != name:
                            print menuItem[4] + " vs " + name
                            IntegrationTools.confirmUpdate(dbCore, venue[0], 'menus', 'menu_items', 'name', menuItem[0], menuItem[4], name, False)
                   
        except: 
           
            tb = traceback.format_exc()
            dbCore.addLogRow(tb)

   
