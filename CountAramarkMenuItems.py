from suds.client import Client


wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'
client = Client(wsdlUrl)

facilityId = 22
#menuId = '7657949f-7bb4-43ea-b6f3-41d2c512cdf0'
#menuId = '527844dc-ba4e-421e-8592-a8e729076fc4'


itemCount = 0



menus = client.service.GetMenuList(FacilityID = facilityId)

for menu in menus[0]:
    menuId = menu.MenuID
    print menu.MenuName
    menuCategories = client.service.GetMenuCategoriesList(FacilityID = facilityId, MenuID = menuId)

    if len(menuCategories) > 0:
        for menuCategory in menuCategories[0]:
            print "     " + menuCategory.Title
            menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID)

            if len(menuItems) > 0:
                for menuItem in menuItems[0]:
                    itemCount += 1
                    print "          " + menuItem.Title + " " + menuItem.MenuItemID
            menuSubCategories = client.service.GetMenuSubCategoriesList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID)

            #if a category has subcats you need to specify the sub cat id to get it's items

            if len(menuSubCategories) > 0 and  len(menuSubCategories[0]) > 0:
                for menuSubCategory in menuSubCategories[0]:
                    print "     --" + menuSubCategory.Title
                    menuItems = client.service.GetMenuMenuItemsList(FacilityID = facilityId, MenuID = menuId, CategoryID = menuCategory.CategoryID, SubCategoryID = menuSubCategory.SubCategoryID)
                    if len(menuItems) != 0:
                        for menuItem in menuItems[0]:
                            itemCount += 1
                            print "          " + menuItem.Title + " " + menuItem.MenuItemID
            else:
                print "No categories for this menu"


    print "Found " + str(itemCount) + " total items"
