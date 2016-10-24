from db_connection import DbConnection
from Aramark_DB import Aramark_Db
from suds.client import Client
import SuiteWizard.SuiteWizard as SuiteWizard

conn = DbConnection().connection
dbCore = Aramark_Db(conn)


wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'
client = Client(wsdlUrl)

def processSpecialItemPrice(venueUid, superpatronUid,  menu, menuItem):
    specialPrice = menuItem.Price
    specialTaxes = menuItem.Taxes

    if specialTaxes == 0:
        specialTaxExempt = True
    else:
        specialTaxExempt = False   

    menuUid, mxmUid, regularPrice, itemName, regularTaxExempt = dbCore.getMenuXMenuItemData(menu.MenuID, menuItem.MenuItemID)

    if float(regularPrice) != float(specialPrice):
        print str(specialPrice) + " vs. " + str(regularPrice) + "   SPECIAL PRICING!!!"

        patrons = dbCore.getPatronsFromSuperPatron(superpatronUid)

        for patron in patrons:
            patronUid = patron[0]
            dbCore.insertSpecialPricingRow(venueUid, patronUid, mxmUid, specialPrice, specialTaxExempt)

        

specialPricingPatrons = dbCore.getSpecialPricingCustomers()

print str(specialPricingPatrons)

for specialPricingPatron in specialPricingPatrons:
    venueUid, superpatronUid, customerId = specialPricingPatron

    facilityId = dbCore.getAramarkFacilityId(venueUid)

    menus = client.service.GetMenuList(FacilityID = facilityId)

    for menu in menus[0]:
            menuId = menu.MenuID
            print menu.MenuName
            menuCategories = client.service.GetMenuCategoriesList(FacilityID = facilityId, MenuID = menuId)

            if len(menuCategories) > 0:
                for menuCategory in menuCategories[0]:

                    menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID, CustomerID = customerId)

                    if len(menuItems) > 0:
                        for menuItem in menuItems[0]:
                            #####ADD BULLSHIT HERE
                            processSpecialItemPrice(venueUid, superpatronUid, menu, menuItem)
                    menuSubCategories = client.service.GetMenuSubCategoriesList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID, CustomerID = customerId)

                    #if a category has subcats you need to specify the sub cat id to get it's items

                    if len(menuSubCategories) > 0 and  len(menuSubCategories[0]) > 0:
                        for menuSubCategory in menuSubCategories[0]:
                            menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID, SubCategoryID = menuSubCategory.SubCategoryID)
                            if len(menuItems) != 0:
                                for menuItem in menuItems[0]:
                                    ####### ADD BULLSHIT HERE
                                    processSpecialItemPrice(venueUid, superpatronUid,  menu, menuItem)
                    else:
                        print "No categories for this menu"
 
