from suds.client import Client

wsdlUrl = 'https://www.suitewizardapi.com/SuiteWizardAPI.svc?wsdl'

client = Client(wsdlUrl)

facilityId = 22

menus = client.service.GetMenuList(FacilityID = 22)

for menu in menus[0]:
    print menu.MenuName + " : " + menu.MenuID
    
    menuId = menu.MenuID

    menuCategories = client.service.GetMenuCategoriesList(FacilityID = 22, MenuID = menuId)

    if len(menuCategories) == 0:
        print "     NO CATEGORIES"
    else:
        
        for category in menuCategories[0]:
            print "     " + category.Title + " : " + category.CategoryID

            subCategories = client.service.GetMenuSubCategoriesList(FacilityID = 22, MenuID = menuId, CategoryID = category.CategoryID)
            if len(subCategories) == 0:
                print "          NO SUB CATS"
            else:
                for subCategory in subCategories[0]:
                    print "          " + subCategory.Title + " : " + subCategory.SubCategoryID
