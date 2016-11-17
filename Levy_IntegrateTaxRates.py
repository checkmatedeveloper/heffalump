import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid
import traceback

def integrate(dbCore):

    dbCore.addLogRow("Integration Taxs")
    tempTaxes = dbCore.getTempTaxes()

    success = True
    errorLogRows = []
    errorVenues = []
    
    for row in tempTaxes:
        entityCode, taxName, taxId, taxRate, revId, catId= row
        try:
            venueUid = dbCore.getVenueUid(entityCode)
        except:
            continue

        try:
    
            levyTaxRates = dbCore.getLevyTaxRate(taxId, revId, catId, venueUid)

            if len(levyTaxRates) > 1:
                print "Uh oh, we have a double mapping"

            elif len(levyTaxRates) == 1:
                #perfect, we have a tax rate mapping already, lets just double check that we are up to date
                levyTaxRate = levyTaxRates[0]
                menuTaxUid, currentTaxRate = levyTaxRate

                print "TAX RATE: " + str(currentTaxRate) + " vs " + str(taxRate)
                print "TAX RATE COMPARE ROUNDED: " + str(round(currentTaxRate, 6)) + " vs " + str(round((taxRate * 100),6))
                if round(currentTaxRate, 6)  != round((taxRate * 100), 6): #compare the old to the new rates
                    print "NOT EQUAL!!!!"
                    IntegrationTools.confirmUpdate(dbCore, venueUid, 'menus', 'menu_taxes', 'tax_rate', menuTaxUid, currentTaxRate, round((taxRate * 100), 6), False, None, auto_apply = True) 
            elif len(levyTaxRates) == 0:
                
                #no tax rate mapped yet, lets sort that out :)
                insert_uuid = uuid.uuid4()

                #need to use this since there is no single unique id for taxes: UC,1,23,34 (example)
                taxGroupId = str(taxId) + "," + str(revId) + "," + str(catId)                

                #venueUid, name, taxt_rate, tax_inclusive, minimum_amount
#                IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_taxes', 'venue_uid', venueUid, False, taxGroupId, False, False)
    
#                IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid, 'menus', 'menu_taxes', 'name', taxName, False, taxGroupId, False, False)

#                IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid,'menus', 'menu_taxes', 'tax_rate', round((taxRate * 100),4), False, taxGroupId, False, False), 

                fields = ['venue_uid', 'name', 'tax_rate']
                values = [venueUid, taxName, round((taxRate * 100),4)]

                success, menuTaxUid = dbCore.insertRow('menus', 'menu_taxes', fields, values)

                if success:
                    fields = ['menu_tax_uid', 'tax_id', 'cat_id', 'rev_id']
                    values = [menuTaxUid, taxId, catId, revId]
                    dbCore.insertRow('integrations', 'menu_taxes_x_tax_groups_levy', fields, values) 
                else:
                    print "problem inserting menu tax uid"

                IntegrationTools.confirmInsert(dbCore, venueUid, insert_uuid,'menus', 'menu_taxes', 'tax_rate', round((taxRate * 100),6), False, taxGroupId, False, auto_apply = True)
            else:
                print "Something has gone very, very wrong :(((("

        except:
            success = False
            tb = traceback.format_exc()
            logRowId = dbCore.addLogRow("Error Processing Tax Row  entity code = " + str(entityCode))
            dbCore.addLogRow(tb)
            errorLogRows.append(logRowId)
            errorVenues.append(venueUid)


    return success, errorLogRows, errorVenues
