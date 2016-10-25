class EmailerDb:
        
    def __init__(self, db):
        self.db = db

    def getVenueUids(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT venue_uid \
                        FROM integrations.venues_levy \
                        WHERE is_active = 1")
        return cursor.fetchall()   

    def getEmployeeName(self, levyTempPointerUid):
        cursor = self.db.cursor()

        cursor.execute("SELECT first_name, last_name \
                        FROM integrations.levy_temp_employees \
                        WHERE employee_id = %s", (levyTempPointerUid))

        return cursor.fetchone()

    def getUpdateEmployeeName(self, updateAction):
        pointerSchema = updateAction[0][3]
        pointerTable = updateAction[0][4]
        pointerUid = updateAction[0][6]

        print "EMP Id: " + str(pointerUid)

        firstName, lastName = self.genericUpdateGet(pointerSchema, pointerTable, pointerUid, "first_name, last_name")

        return firstName + " " + lastName

    def getCustomerNameAndSuites(self, levyTempPointerUid):
        cursor = self.db.cursor()

        cursor.execute("SELECT customer_name, suite_number \
                        FROM integrations.levy_temp_customers \
                        WHERE customer_number = %s", (levyTempPointerUid))

        return cursor.fetchall()

    def getUpdateCustomerName(self, updateAction):
        pointerSchema = "patrons"
        pointerTable = "clone_patrons"
        pointerUid = updateAction[0][6]

        print "Customer UID: " + str(pointerUid)

        customerName = self.genericUpdateGet(pointerSchema, pointerTable, pointerUid, "company_name")

        return customerName[0] #TODO: we might have to factor in first name/ last name too

    def getCustomerNumber(self, patronUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT customer_number FROM integrations.patrons_levy WHERE patron_uid = %s''', (patronUid))
        return cursor.fetchone()[0]

    def getEventNameAndDate(self, levyTempPointerUid):
        print str(levyTempPointerUid)
        cursor = self.db.cursor()

        cursor.execute("SELECT event_name, event_datetime \
                        FROM integrations.levy_temp_events \
                        WHERE event_id = %s", (levyTempPointerUid))

        return cursor.fetchone()

    #def getUpdateEventName(self, updateAction):
        #TODO       

    def getSuiteName(self, levyTempPointerUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT suite_number \
                        FROM integrations.levy_temp_suites \
                        WHERE suite_id = %s", (levyTempPointerUid))

        return cursor.fetchone()[0] #just return the field we actually want

    def getUnitNameByUid(self, unitUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT name \
                        FROM setup.units \
                        WHERE id = %s", (unitUid))
        return cursor.fetchone()[0]

    def getUpdateSuiteName(self, updateAction):
        pointerSchema = updateAction[0][3]
        pointerTable = updateAction[0][4]
        pointerUid = updateAction[0][6]

        unitName = self.genericUpdateGet(pointerSchema, pointerTable, pointerUid, "name")
        return unitName[0]

    def getItemName(self, levyTempPointerUid):
        cursor = self.db.cursor() 
        cursor.execute("SELECT item_name \
                        FROM integrations.levy_temp_menu_items \
                        WHERE item_number = %s", (levyTempPointerUid))
        return cursor.fetchone()[0]

    def getEventNameFromEventUid(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT event_name \
                        FROM setup.events_x_venues \
                        JOIN setup.events on events.id = events_x_venues.event_uid \
                        WHERE events.id = %s", (eventUid))
        return cursor.fetchone()[0]

    def getEventNameFromEventXVenueUid(self, eventXVenueUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT event_name \
                        FROM setup.events_x_venues \
                        WHERE id = %s", (eventXVenueUid))
        return cursor.fetchone()[0]

    def getItemNameByUid(self, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT display_name \
                        FROM menus.menu_items \
                        WHERE id = %s", (menuItemUid))
        return cursor.fetchone()[0]

    def getMenuName(self, menuXMenuItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT menu_name FROM menus.menu_x_menu_items \
                        JOIN menus.menus on menus.id = menu_x_menu_items.menu_uid \
                        WHERE menu_x_menu_items.id = %s", (menuXMenuItemUid))
        return cursor.fetchone()[0]

    def getItemNameByMenuXMenuItemUid(self, menuXMenuItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT display_name \
                        FROM menus.menu_x_menu_items \
                        JOIN menus.menu_items ON menu_items.id = menu_x_menu_items.menu_item_uid \
                        WHERE menu_x_menu_items.id = %s", (menuXMenuItemUid))
        return cursor.fetchone()[0]




    def genericUpdateGet(self, pointerSchema, pointerTable, pointerUid, getFields):
        
        cursor = self.db.cursor()
        cursor.execute("SELECT " + getFields + " FROM " + pointerSchema + "." + pointerTable + " WHERE id = %s", (pointerUid))
        return cursor.fetchone()


    def getEmailAddresses(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT email \
                        FROM notifications.email_notifications \
                        JOIN notifications.notifications_x_venues ON email_notifications.notification_venue_uid = notifications_x_venues.id \
                        JOIN notifications.notifications ON notifications_x_venues.notification_uid = notifications.id \
                        WHERE venue_uid = %s and notifications.id = 8", (venueUid))
        return cursor.fetchall()

    def getVenueName(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT name FROM setup.venues WHERE id = %s''', (venueUid))
        return cursor.fetchone()[0]

    def getPatronUidAndUnitNameFromUnitXPatronsUid(self, unitXPatronsUid):
        cursor = self.db.cursor()
        cursor.execute(''' SELECT 
                          patron_uid,
                          units.name as unit_name 
                          FROM info.unit_x_patrons
                          LEFT JOIN setup.units ON units.id = unit_x_patrons.unit_uid
                          WHERE unit_x_patrons.id = %s''', (unitXPatronsUid))
        return cursor.fetchone()


    def getEncryptedPatronCompanyNameAndKey(self, patron_uid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT company_name, e_key FROM patrons.patrons
                                   JOIN operations.data_keys ON data_keys.pointer_uid = patrons.id 
                                        AND data_keys.pointer_schema = 'patrons' 
                                        AND data_keys.pointer_table = 'patrons'
                                   WHERE patrons.id = %s''', (patron_uid))
        return cursor.fetchone()

    def convertTimeZone(self, UTCTime, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT CONVERT_TZ(%s, 'GMT', (SELECT local_timezone_long FROM setup.venues WHERE id = %s))''', (UTCTime, venueUid))

        return cursor.fetchone()[0]
