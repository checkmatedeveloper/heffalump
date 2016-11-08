from config import CheckMateConfig
import pytz, datetime
import hashlib

class Levy_Db:
    
    def __init__(self, db, redisInstance):
        """Initializes a new instance of a Levy_Db object
        
        params:
        db -- a mysqldb database connection object
        """
        self.db = db
        self.checkmateconfig = CheckMateConfig()
        self.redis = redisInstance

#    def bustRedisCache 

    """
        //////////////////////////////  
        // Insert Levy Mapping Rows //
        //////////////////////////////

        The following methods are used when a mapping from levy data to parametic data is found.  They all insert a 
        row into their respecitve _levy tables and return nothing
    """

    def insertPatronsXUnits(self, patrons_levy_uid, unit_uid):
        """Inserts a row into integrations.patrons_x_units, used when a valid mapping of levy data to parametric data is found"""
        cursor = self.db.cursor()
        
        cursor.execute("INSERT IGNORE INTO integrations.patrons_x_units_levy (patrons_levy_uid, unit_uid, is_active, created_at, updated_at) VALUES \
                        (%s, %s, 1, NOW(), CURRENT_TIMESTAMP)", (patrons_levy_uid, unit_uid))
       
        self.db.commit() 

    def getAllLevyVenues(self):
        """Returns a list of all levy venues.  Gets the list from the integrations.venues_levy table"""
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.venues_levy WHERE is_active = 1")
        return cursor.fetchall()
    
    def insertLevyPatron(self, customer_number, venue_uid, patron_uid):
        """Adds a row to the integrations.patrons_levy table.  Used when a valid mapping of levy data to parametric patron data is found"""
        cursor = self.db.cursor()
        
        cursor.execute("INSERT INTO integrations.patrons_levy (customer_number, venue_uid, patron_uid, is_active, created_at, updated_at) \
                        VALUES (%s, %s, %s, 1, NOW(), CURRENT_TIMESTAMP)", (customer_number, venue_uid, patron_uid))
        self.db.commit()
        
        return cursor.lastrowid

    def insertLevyEvent(self, event_id, event_uid, venue_uid): #event_id -> levy, event_uid -> parametric
        """Adds a row to integrations.events_levy.  Used when a valid mapping is found"""
        cursor = self.db.cursor()
        
        cursor.execute("INSERT INTO integrations.events_levy (event_id, event_uid, venue_uid, is_active, created_at, updated_at) VALUES \
                        (%s, %s, %s, 1, NOW(), CURRENT_TIMESTAMP)", (event_id, event_uid, venue_uid))
        self.db.commit()

    def insertLevyVenuesXEmployees(self, employee_id, venue_uid, venue_employee_uid):
        """Adds a row to integrations.venues_x_employees_levy.  Used when a valid mapping is found"""
        cursor = self.db.cursor()
        
        cursor.execute("INSERT INTO integrations.venues_x_employees_levy (employee_id, venue_uid, venue_employee_uid, is_active, created_at, updated_at) \
                        VALUES (%s, %s, %s, 1, NOW(), CURRENT_TIMESTAMP)", (employee_id, venue_uid, venue_employee_uid))
        self.db.commit()


    def insertLevyMenuItem(self, levy_item_number, menu_item_uid, venue_uid, levy_pos_product_class_id, levy_tax_group_id, levy_revenue_category_id):
        """Adds a row to integrations.menu_items_levy, used when a valid mapping is found"""
        cursor = self.db.cursor()
    
        cursor.execute("INSERT IGNORE INTO integrations.menu_items_levy \
                        (levy_item_number, menu_item_uid, venue_uid, levy_pos_product_class_id, levy_tax_group_id, levy_revenue_category_id, is_active, created_at, updated_at) \
                        VALUES (%s, %s, %s, %s, %s, %s, 1, NOW(), CURRENT_TIMESTAMP)",
                        (levy_item_number, menu_item_uid, venue_uid, levy_pos_product_class_id, levy_tax_group_id, levy_revenue_category_id))
        self.db.commit()

    def updateLevyMenuItem(self, levy_item_number, venue_uid,  levy_pos_product_class_id, levy_tax_group_id, levy_revenue_category_id):
        cursor = self.db.cursor()
            
        cursor.execute('''UPDATE integrations.menu_items_levy
                          SET 
                            levy_pos_product_class_id = %s,
                            levy_tax_group_id = %s,
                            levy_revenue_category_id = %s
                          WHERE levy_item_number = %s AND venue_uid = %s''',
                       (levy_pos_product_class_id, levy_tax_group_id, levy_revenue_category_id, levy_item_number, venue_uid))

        self.db.commit()


    def insertMenuTaxesXTaxGroupsLevy(self, menuTaxUid, taxId, revId, catId, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT IGNORE INTO integrations.menu_taxes_x_tax_groups_levy(
                            menu_tax_uid, 
                            tax_id, 
                            cat_id, 
                            rev_id, 
                            created_at)
                          VALUES (%s, %s, %s, %s, NOW())''', (menuTaxUid, taxId, catId, revId))
        self.db.commit()

    """
        /////////////////////////
        // Get Parametic Data //
        ////////////////////////

        The following methods are responsible for fetching parametic data.  All lookups are done via unique identifiers (id, uid, etc...)
    """

    def getVenueTimeZone(self, venue_uid):
        
        cursor = self.db.cursor()
        cursor.execute("SELECT local_timezone_long \
                        FROM setup.venues \
                        WHERE id = %s",
                        (venue_uid))
        timezoneString = cursor.fetchone()[0]
        timezone = pytz.timezone(timezoneString)
        return timezone

    def findVenueMenu(self, venue_uid, menu_type_uid):
        cursor = self.db.cursor()
        print str(venue_uid) + " " + str(menu_type_uid)
	cursor.execute("SELECT id FROM menus.menus \
                        WHERE venue_uid  = %s AND menu_type_uid = %s \
                        LIMIT 1", (venue_uid, menu_type_uid))
        return cursor.fetchone()[0]

    def getMenuXMenuItemData(self,menu_item_uid, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT menu_x_menu_items.id,price, menu_type_uid FROM menus.menu_x_menu_items \
                        JOIN menus.menus ON menus.id = menu_x_menu_items.menu_uid \
                        WHERE menu_item_uid = %s and venue_uid = %s", (menu_item_uid, venue_uid));
        return cursor.fetchall()

    def getParametricPatron(self, patron_uid):
        cursor = self.db.cursor()
        print "PATRON UID: " + str(patron_uid)
        cursor.execute("SELECT * FROM patrons.patrons WHERE id = %s LIMIT 1", (patron_uid));

        return cursor.fetchone()

    def getParametricClonePatron(self, patron_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM patrons.clone_patrons WHERE id = %s LIMIT 1", (patron_uid))
        return cursor.fetchone()

    def getLevyUnits(self, venue_uid, suite_id):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.units_levy WHERE venue_uid = %s AND suite_id = %s",
                        (venue_uid, suite_id))

        key = 'levy_unit_by_suite_id:{0}:{1}'.format(venue_uid, suite_id)
        self.redis.delete(key)

        return cursor.fetchall()

    def getLevyMenuItem(self, levy_item_number, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.menu_items_levy WHERE levy_item_number = %s AND venue_uid = %s", 
                        (levy_item_number, venue_uid))
        return cursor.fetchall() # there actually might be multiple results in this case

    def getLevyVenuesXEmployees(self, employee_id): #employee_id = the levy id
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.venues_x_employees_levy WHERE employee_id = %s", 
                        (employee_id))
        return cursor.fetchall()

    def getEvent(self, event_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT events.id, events.venue_uid, event_date, event_type_uid, event_name, events_x_venues.id FROM setup.events \
                        JOIN setup.events_x_venues ON events.id = events_x_venues.event_uid \
                        WHERE events.id = %s', (event_uid))
        return cursor.fetchone()

    def getEmployee(self, venues_x_employees_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT employees.* FROM setup.employees \
                        JOIN setup.venues_x_employees ON venues_x_employees.employee_uid = employees.id \
                        WHERE venues_x_employees.id = %s", (venues_x_employees_uid))
        return cursor.fetchone()

    def getUnit(self, unit_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM setup.units WHERE id = %s", (unit_uid));
        return cursor.fetchone()

    def getVenueMenus(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT id, menu_name, menu_type_uid FROM menus.menus \
                        WHERE venue_uid = %s \
                        AND menu_type_uid IN (1, 2, 3)", 
                        (str(venue_uid)))
        return cursor.fetchall()
                              
 
    def getMenuItem(self, menu_item_uid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT id,
                                 venue_uid,
                                 menu_tax_uid,
                                 pos_menu_item_id,
                                 name,
                                 display_name,
                                 description,
                                 server_description,
                                 requires_age_verification,
                                 price,
                                 points,
                                 minimum_qty,
                                 maximum_qty,
                                 servings_per_item,
                                 printer_category,
                                 show_image,
                                 cost,
                                 service_charge_rate,
                                 created_at,
                                 updated_at
                         FROM menus.menu_items WHERE id = %s''',
                        (str(menu_item_uid)))
        return cursor.fetchone()

    def getMenuXMenuItem(self, menu_uid, menu_item_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT * FROM menus.menu_x_menu_items \
                        JOIN menus.menus ON menu_x_menu_items.menu_uid = menus.id \
                        WHERE menu_uid = %s AND menu_item_uid = %s LIMIT 1',
                        (menu_uid, menu_item_uid))
        return cursor.fetchone()

    def getUnitUid(self, suite_uid):
        cursor = self.db.cursor()
        
        code = cursor.execute("SELECT unit_uid FROM integrations.units_levy WHERE suite_id = %s LIMIT 1", (suite_uid))
        if code == 0:
            return None
        else:
            return cursor.fetchone()[1]
   
    def getAllPatronsByNameHash(self, customer_name_hashed, venue_uid):
        cursor = self.db.cursor()

        cursor.execute("SELECT * FROM patrons.patrons \
                        JOIN info.unit_x_patrons ON patrons.id = unit_x_patrons.patron_uid \
                        JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                        WHERE company_name_hashed = %s AND venue_uid = %s", (customer_name_hashed, venue_uid))
        return cursor.fetchall()

    def getMenus(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT id, menu_type_uid FROM menus.menus \
                        WHERE venue_uid = %s', (venue_uid))
        return cursor.fetchall()

    def getMenuItems(self, menu_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT * FROM menus.menu_x_menu_items WHERE menu_uid = %s', (menu_uid))
        return cursor.fetchall() 
 
    def getTaxId(self, taxId, revId,  venue_uid, catId):
        #try:
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT menu_tax_uid 
                        FROM integrations.menu_taxes_x_tax_groups_levy 
                        JOIN menus.menu_taxes ON menu_taxes.id = menu_taxes_x_tax_groups_levy.menu_tax_uid 
                        WHERE menu_taxes_x_tax_groups_levy.tax_id = %s AND rev_id = %s AND venue_uid = %s AND cat_id = %s
                       ''', (taxId, revId, venue_uid, catId))
        taxRow = cursor.fetchone()
        #except:
        #    taxRow = None

        #if taxRow is None:
        #    defaultCursor = self.db.cursor()
        #    cursor.execute('SELECT id FROM menus.menu_taxes \
        #                    WHERE venue_uid = %s and name = "no tax"', (venue_uid))
        #    return cursor.fetchone()[0]
        #else:
        #    return taxRow[0]
        return taxRow[0] #just let it crash and log an error?

    def getPackageTaxData(self, packageItemNumber):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT tax_id, rev_id, cat_id FROM integrations.levy_temp_menu_items WHERE item_number = %s
                       ''', (packageItemNumber))
        return cursor.fetchall()[0]



    def getPrinterCategory(self, levy_revenue_category_id):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            printer_category \
                        FROM integrations.levy_revenue_category_x_printer_categories \
                        WHERE levy_revenue_category_id = %s \
                        LIMIT 1",
                        (levy_revenue_category_id))
        printerCategory = cursor.fetchone()
        if printerCategory is None:
            printerCategory = 'none'
            return printerCategory
        return printerCategory[0]


    """
        //////////////////////
        // Get Levy Data //
        /////////////////////

        These methods are for fetching data from _levy tables.  There is a very real possibility that these functions will return None
        if there is not a mapping yet.  The input parameters should mostly come from the levy raw dump files.
    """

    def getTempLevyItem(self, itemNumber, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            item_classification, \
                            main_price, \
                            doe_price \
                        FROM integrations.levy_temp_menu_items \
                        JOIN integrations.levy_temp_menu_prices ON levy_temp_menu_items.item_number = levy_temp_menu_prices.item_number \
                        JOIN integrations.venues_levy ON venues_levy.levy_entity_code = levy_temp_menu_prices.entity_code AND venues_levy.is_active = 1 \
                        WHERE levy_temp_menu_items.item_number = %s AND venue_uid = %s",
                        (str(itemNumber), str(venue_uid)));
        return cursor.fetchone()
        
    def getTempLevyCustomer(self, customer_number):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            customer_number, \
                            customer_name, \
                            entity_code, \
                            suite_number \
                        FROM integrations.levy_temp_customers \
                        WHERE customer_number = %s", (customer_number))
        return cursor.fetchone()


    def getLevyEvents(self, venue_uid, event_number):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.events_levy WHERE venue_uid = %s AND event_id = %s",
                        (venue_uid, event_number))

        return cursor.fetchall()

    def getEventTypeUid(self, event_type):
        cursor = self.db.cursor()
        count = cursor.execute("SELECT * FROM integrations.event_types_levy WHERE levy_event_type = %s",
                        (event_type))
        if count is None or count == 0:
            return 9
        return cursor.fetchone()[0]

    ### returns a customer from the patrons_levy table based on its customer number or null if there is none
    def getLevyPatron(self, customer_number):
        cursor = self.db.cursor()
        
        count = cursor.execute("SELECT * FROM integrations.patrons_levy WHERE customer_number = %s LIMIT 1", (customer_number))
        if count == 0:
            return None
        else:
            return cursor.fetchone()
    
   
    def getLevyTaxRate(self, taxId, revId, catId, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_tax_uid, tax_rate
                          FROM integrations.menu_taxes_x_tax_groups_levy
                          JOIN menus.menu_taxes ON menu_taxes.id = menu_taxes_x_tax_groups_levy.menu_tax_uid
                          WHERE menu_taxes_x_tax_groups_levy.tax_id = %s 
                            AND menu_taxes_x_tax_groups_levy.rev_id = %s
                            AND menu_taxes_x_tax_groups_levy.cat_id = %s
                            AND menu_taxes.venue_uid = %s''', (taxId, revId, catId, venueUid))
        return cursor.fetchall()

    def getLevyTaxName(self, taxGroupId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT levy_tax_group_name 
                          FROM integrations.tax_groups_levy
                          WHERE levy_tax_group_id = %s''',
                        (taxGroupId))
        return cursor.fetchone()[0]

    def getLevyServiceChargeRate(self, entity_code, rev_id, cat_id = None):
        cursor = self.db.cursor()

        if cat_id is None:
            cursor.execute('''SELECT 
                                category_name, 
                                sc_rate, 
                                tax_flag, 
                                discount_flag
                              FROM integrations.levy_temp_service_charge_rates
                              WHERE entity_code = %s
                                AND rev_id = %s
                            ''', (entity_code, rev_id))
        else:
            cursor.execute('''SELECT 
                                category_name, 
                                sc_rate, 
                                tax_flag, 
                                discount_flag
                              FROM integrations.levy_temp_service_charge_rates
                              WHERE entity_code = %s
                                AND rev_id = %s
                                AND cat_id = %s''', (entity_code, rev_id, cat_id))
        return cursor.fetchone()

    """
        //////////////////////////
        // Find Parametric Data //
        //////////////////////////

        These methods are used to look up parametric data based on info provided from the levy integration raw files.  They
        are called to attempt to find parametic data that matches the levy data

        All of the input params should come from the raw levy files
    """

    def findMenuItems(self, venue_uid, name):
        cursor = self.db.cursor()
        cursor.execute('SELECT * FROM menus.menu_items WHERE venue_uid = %s AND name = %s', 
                        (venue_uid, name))
        return cursor.fetchall() #there should only be one

    def findEvents(self, venue_uid, event_name, event_type, event_datetime):
        #print str(venue_uid) + " " + str(event_name) + " " +str(event_type) + " " + str(event_datetime)
        cursor = self.db.cursor()
        cursor.execute('SELECT events.id, events.venue_uid, event_date, event_type_uid, event_name, events_x_venues.id FROM setup.events \
                        JOIN setup.events_x_venues ON events.id = events_x_venues.event_uid \
                        WHERE event_name = %s AND events.venue_uid = %s AND event_type_uid = %s AND event_date = %s',
                        (event_name, venue_uid, event_type, event_datetime))
        #print cursor._last_executed
        return cursor.fetchall()

    def findUnits(self, venue_uid, name):
        cursor = self.db.cursor()
        cursor.execute("SELECT * \
                        FROM setup.units \
                        WHERE venue_uid = %s \
                        AND name = %s",
                        (venue_uid, name))
        return cursor.fetchall()
    
 
    def findEmployees(self, first_name, last_name, venue_uid):
        cursor = self.db.cursor()
       
        cursor.execute("SELECT employees.id, venues_x_employees.id FROM setup.employees \
                        JOIN setup.venues_x_employees ON venues_x_employees.employee_uid = employees.id \
                        WHERE employees.first_name = %s AND employees.last_name = %s AND venues_x_employees.venue_uid = %s \
		        AND is_active = 1" ,
                        (first_name, last_name, venue_uid))
        return cursor.fetchall()

    def findMatchingCustomers(self,venue_uid, patron_uid, unit_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT patron_uid FROM info.event_info \
                        JOIN setup.events ON event_uid = events.id \
                        WHERE venue_uid = %s AND patron_uid = %s AND unit_uid = %s",
                        (venue_uid, patron_uid, unit_uid))

        return cursor.fetchall()

    def findUnit(self, suite_number, venue_uid): 
        cursor = self.db.cursor()
       # print str(suite_number)
        cursor.execute("SELECT \
                            unit_uid \
                        FROM integrations.units_levy \
                        JOIN integrations.levy_temp_suites on levy_temp_suites.suite_id = units_levy.suite_id \
                        WHERE suite_number = %s AND venue_uid = %s;",
                        (suite_number, venue_uid))
        return cursor.fetchone()


### gets the parametric venue_uid from the levy entity code
    def getVenueUid(self, levy_entity_code):
       
        #print levy_entity_code
        #the following line is very fucking important!!! It strips out pointless invisible characters that are
        #put in but the fucking dumb windows computers that make these stupid fucking dump files.
        #Fuck thats 2 hours of my life I am never getting back GGGGRRRRRRRRRRR!!!!!
        levy_entity_code = "".join(i for i in levy_entity_code if ord(i)<128)
       

        cursor = self.db.cursor()
       
  
        cursor.execute('SELECT venue_uid FROM integrations.venues_levy WHERE levy_entity_code = %s and is_active = 1', (levy_entity_code))
        venue = cursor.fetchall()
         
        return venue[0][0]


### adds a row to the purgatory table.
    def addPurgatoryRow(self, 
                      venue_uid,        #
                      import_uuid,
                      pointer_schema,   #
                      pointer_table,    #
                      pointer_field,    #
                      pointer_uid,      #
                      old_value,        #
                      new_value,        #
                      required_action,
                      levy_temp_pointer = None,
                      ignoreUniqueConstraint = False,
                      auto_apply = False
                      ):   
        
        concatString = str(venue_uid)  +  str(pointer_schema) + str(pointer_table) + str(pointer_field) + str(pointer_uid) + str(old_value) + str(new_value) + str(required_action) + str(levy_temp_pointer)


        #in some cases we want to be able to ignore the unique hash. 
        #for example when we insert rows into units_x_patron, one levy_temp_pointer row may result in multiple purgatory rows
        #NOTE: this will still not allow you to add two exact duplicate rows (import_uuid still must be unique)
        if ignoreUniqueConstraint:
            concatString = concatString + str(import_uuid)

        uniqueHash = hashlib.md5(concatString).hexdigest()
       

        if auto_apply == True:
            action = 'apply'
        else:
            action = 'pending'

        cursor = self.db.cursor()
        try:
            cursor.execute('INSERT IGNORE INTO integrations.purgatory (venue_uid, \
                                                            import_uuid, \
                                                            pointer_schema, \
                                                            pointer_table, \
                                                            pointer_field, \
                                                            pointer_uid, \
                                                            old_value, \
                                                            new_value, \
                                                            required_action, \
                                                            action, \
                                                            levy_temp_pointer, \
                                                            unique_hash, \
                                                            created_at, \
                                                            updated_at \
                                                           ) VALUES \
                                                           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  NOW(), CURRENT_TIMESTAMP)',
                       (venue_uid, import_uuid,  pointer_schema, pointer_table, pointer_field, pointer_uid, old_value, new_value, required_action, action, levy_temp_pointer, uniqueHash))
        except:
            print (cursor._last_executed)
            raise
        self.db.commit()
        return cursor.lastrowid

    def insertUnitXPatrons(self, unit_uid, patron_uid):
        print str(unit_uid) + " " + str(patron_uid)
        cursor = self.db.cursor()
        try:
            cursor.execute("INSERT IGNORE INTO info.unit_x_patrons ( \
                                                                unit_uid, \
                                                                patron_uid, \
                                                                event_type, \
                                                                created_at \
                                                               ) VALUES ( \
                                                                %s, \
                                                                %s, \
                                                                'default', \
                                                                NOW())", (unit_uid, patron_uid));
            self.db.commit()
            print "Unit x patron inserted"
            x = cursor.lastrowid
            print "new unit patron uid: " + str(x)
            return x
        except:
            print "An exception occured"
            print (cursor._last_executed)

    def insertUnitsLevy(self, suite_id, venue_uid, unit_uid):
        cursor = self.db.cursor()
        cursor.execute('INSERT INTO integrations.units_levy (suite_id, venue_uid, unit_uid, is_active, created_at, updated_at) VALUES \
                        (%s, %s, %s, 1, NOW(), CURRENT_TIMESTAMP)', (suite_id, venue_uid, unit_uid))
        self.db.commit()
    
    def insertUnitPatronInfo(self, unit_patron_uid, venueUid):
        print str(unit_patron_uid)
        
        if unit_patron_uid == 0:
            return

        cursor = self.db.cursor()
        cursor.execute('INSERT INTO info.unit_patron_info ( \
                            unit_patron_uid, \
                            created_at \
                        ) VALUES ( \
                            %s, \
                            NOW() \
                        )', (unit_patron_uid))

        unitPatronInfoUid = cursor.lastrowid

        print "Inserting cart info"
        #insert into carts
        cursor.execute("INSERT INTO info.unit_patron_cart_info( \
                            unit_patron_info_uid, \
                            cart_type_uid, \
                            pay_method, \
                            created_at \
                        )(SELECT  \
                            %s,  \
                            cart_type_uid, \
                            'unknown', \
                            NOW() \
                          FROM setup.revenue_centers_x_cart_types \
                          JOIN setup.revenue_centers ON revenue_centers.id = revenue_centers_x_cart_types.revenue_center_uid \
                          WHERE venue_uid = %s)",
                        (unitPatronInfoUid, venueUid))


        self.db.commit()

    def insertMenuXMenuCategory(self, menuUid, menuCategoryUid):
        cursor = self.db.cursor()
        cursor.execute("INSERT IGNORE INTO menus.menu_x_menu_categories ( \
                            menu_uid, \
                            menu_category_uid, \
                            ordinal, \
                            created_at \
                        ) (SELECT \
                                %s, \
                                %s, \
                                MAX(ordinal) + 1, \
                                NOW() \
                            FROM menus.menu_x_menu_categories)", (menuUid, menuCategoryUid))
        self.db.commit()

################# ENCRYPTION    
    
    def getEKey(self, pointer_schema, pointer_table, pointer_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            e_key \
                        FROM operations.data_keys \
                        WHERE pointer_schema = %s AND \
                            pointer_table = %s AND \
                            pointer_uid = %s \
                        LIMIT 1",
                        (pointer_schema, pointer_table, pointer_uid))
        return cursor.fetchone()


    def insertDataKey(self, pointer_uid, pointer_table, pointer_schema, e_key):
        
        cursor = self.db.cursor()
        cursor.execute('INSERT INTO operations.data_keys (pointer_uid, pointer_table, pointer_schema, e_key, created_at, updated_at) \
                        VALUES (%s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (pointer_uid, pointer_table, pointer_schema, e_key));
        self.db.commit()

    def updateEncryptionKey(self, pointerSchema, pointerTable, purgatoryUid, newUid):
        cursor = self.db.cursor()
        cursor.execute("UPDATE operations.data_keys \
                        SET \
                            pointer_uid = %s, \
                            pointer_table = %s, \
                            pointer_schema = %s \
                        WHERE  \
                            pointer_uid = %s AND \
                            pointer_table = 'purgatory' AND  \
                            pointer_schema = 'integrations'",
                            (newUid, pointerTable, pointerSchema, purgatoryUid));
        self.db.commit()
          

####################  rabbit "worker"
    def getPurgatoryRowsToApply(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.purgatory WHERE action = 'apply' AND status IS NULL")
        return cursor.fetchall()

    def removeRow(self, schema, table, field, uid):
        try:
            cursor = self.db.cursor()
            query = "DELETE FROM " + schema + "." + table + " WHERE id = " + str(uid)
            rowsAffected = cursor.execute(query)
            self.db.commit()
            self.addLogRow("Delete: SUCCESS")
            return True, None
        except self.db.error as e:
            self.db.rollback()
            self.addLogRow("Delete: FAILURE")
            return False, str(e)

    def updateRow(self, schema, table, field, uid, new_value):
        try:
            cursor = self.db.cursor()
            query = "UPDATE " + schema + "." + table + " SET " + field + "  = %s WHERE id = %s"
            self.addLogRow("Attempting Update: " + query + " id = " + str(uid))
            rowsAffected = cursor.execute(query, (new_value, uid))
            self.db.commit()
            self.addLogRow("Update Success")
            return True, cursor.lastrowid
        except self.db.error as e:
            self.db.rollback()
            self.addLogRow("Update: FAILURE")
            return False, str(e)

        

    def insertRow(self, schema, table, fields, values, insertIgnore=False):
        try:
            cursor = self.db.cursor()
            fieldList = ", ".join(fields)
            placeholders = list()
            for value in values:
               placeholders.append("%s")

            valueList = ", ".join(placeholders)
            
            queryBase = "INSERT INTO "
            if insertIgnore:
                queryBase = "INSERT IGNORE INTO "
            query = queryBase + schema + "." + table + "( " + fieldList + ", created_at, updated_at) VALUES (" + valueList + ", NOW(), CURRENT_TIMESTAMP)"
            self.addLogRow("Attempting Insert: " + query) 
            rowsAffected = cursor.execute(query, values)
            self.db.commit()
            print "Inserted: " + str(rowsAffected)
            self.addLogRow("Insert Success")
            return True, cursor.lastrowid
        except self.db.Error as e:
            self.db.rollback()
            self.addLogRow("Insert: FAILURE")
            return False, str(e)

    def purgatoryRowApplied(self, row_uid):
        cursor=self.db.cursor()
        cursor.execute("UPDATE integrations.purgatory SET status = 'applied' WHERE id = %s", (row_uid))
        self.db.commit()
    
    def purgatoryRowFailed(self, row_uid, error_message):            
        cursor = self.db.cursor()
        cursor.execute("UPDATE integrations.purgatory SET status = 'failed', error_message = %s WHERE id = %s", (error_message, row_uid))
        self.db.commit()
 
    def purgePurgatory(self):
        cursor = self.db.cursor()
        
        #don't delete the rows marked ignore, leave them around
        cursor.execute("DELETE FROM integrations.purgatory \
                        WHERE action != 'ignore'")
        self.db.commit()   

 
###################  category mapper

    def getParametricCategory(self, item_name, venue_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT menu_category_uid FROM menus.menu_x_menu_items \
                        JOIN menus.menu_items ON menu_x_menu_items.menu_item_uid = menu_items.id \
                        WHERE name = %s \
                            AND venue_uid = %s \
                            AND menu_x_menu_items.menu_uid IN (3, 4, 9, 10) \
                        LIMIT 1', (item_name, venue_uid))
    
    def getMenuCategory(self, prod_class_id, venue_uid):
        cursor = self.db.cursor()
        cursor.execute('SELECT category_uid FROM integrations.categories_levy \
                        WHERE levy_pos_product_class_id = %s AND venue_uid = %s', (prod_class_id, venue_uid))
        menuCategory = cursor.fetchone() 
        if menuCategory is None:
            miscCursor = self.db.cursor()
            cursor.execute('SELECT id FROM menus.menu_categories \
                            WHERE name = "Misc" and venue_uid = %s', (venue_uid))
            try:
                return cursor.fetchone()[0]
            except:
                return 999
        else:
            return menuCategory[0]

    def countRowsToApply(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT COUNT(*) FROM integrations.purgatory \
                        WHERE action = "apply" AND status IS NULL \
                        LIMIT 1')

        return cursor.fetchone()[0]


############## logging

    def addLogRow(self, action):
        cursor = self.db.cursor()
        cursor.execute('INSERT INTO integrations.integration_actions \
                        (action, created_at, updated_at) VALUES \
                        (%s, NOW(), CURRENT_TIMESTAMP)',(action))

        self.db.commit()
        return cursor.lastrowid

    def getLastAction(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT action FROM integrations.integration_actions ORDER BY id DESC LIMIT 1')
        try:
            return cursor.fetchone()[0]
        except:
            return None


############## Levy Temp Menu Tables

    def clearTempLevyEmployeeTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_employees')
        self.db.commit()

    def insertTempLevyEmployee(self, row):
        cursor = self.db.cursor()
        cursor.execute('REPLACE INTO integrations.levy_temp_employees \
                           (employee_id, \
                            entity_code, \
                            employee_site, \
                            pos_name, \
                            first_name, \
                            last_name, \
                            created_at, \
                            updated_at \
                           )VALUES( \
                            %s, %s, %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (row))
        self.db.commit()

    def clearTempLevySuiteTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_suites')
        self.db.commit()

    def insertTempLevySuite(self, row):
        cursor = self.db.cursor()
        cursor.execute('REPLACE INTO integrations.levy_temp_suites \
                           (suite_id, \
                            suite_number, \
                            entity_code, \
                            suite_end_date, \
                            created_at, \
                            updated_at \
                           )VALUES( \
                            %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (row))
        self.db.commit()

    def clearTempLevyCustomerTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_customers')
        self.db.commit()

    def insertTempLevyCustomer(self, row):
        cursor = self.db.cursor()
        cursor.execute('REPLACE INTO integrations.levy_temp_customers \
                           (customer_number, \
                            customer_name, \
                            entity_code, \
                            suite_number, \
                            customer_suite_end_date, \
                            created_at, \
                            updated_at \
                           )VALUES( \
                            %s, %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (row))
        self.db.commit()

    def clearTempLevyEventTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_events')
        self.db.commit()
    

    def insertTempLevyEvent(self, row):
        cursor = self.db.cursor()
        cursor.execute('REPLACE INTO integrations.levy_temp_events \
                           (entity_code, \
                            event_id, \
                            event_name, \
                            event_type, \
                            event_datetime, \
                            cancelled, \
                            created_at, \
                            updated_at \
                           )VALUES( \
                            %s, %s, %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (row))
        self.db.commit()
                        
    def clearTempLevyItemTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_menu_items')
        self.db.commit()    

    def insertTempLevyItem(self, row):
        cursor = self.db.cursor()
        cursor.execute('''REPLACE INTO integrations.levy_temp_menu_items 
                           (item_number, 
                            package_flag, 
                            item_name, 
                            pos_button_1, 
                            pos_button_2, 
                            pos_printer_label, 
                            pos_prod_class_id, 
                            pos_product_class, 
                            item_classification,
                            rev_id, 
                            tax_id,
                            cat_id, 
                            created_at, 
                            updated_at 
                           )VALUES( 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)''', (row))       
        self.db.commit()

    def clearTempLevyItemPriceTable(self):
        cursor = self.db.cursor()
        cursor.execute('DELETE FROM integrations.levy_temp_menu_prices')
        self.db.commit()

    def insertTempLevyItemPrice(self, row):
        cursor = self.db.cursor()
        cursor.execute('REPLACE INTO integrations.levy_temp_menu_prices \
                            (entity_code, \
                             item_number, \
                             min_order, \
                             max_order, \
                             pos_level_id, \
                             main_price, \
                             pos_doe_level_id, \
                             doe_price, \
                             location_name, \
                             created_at, \
                             updated_at \
                            )VALUES( \
                             %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), CURRENT_TIMESTAMP)', (row))
        self.db.commit()

    def clearTempLevyServiceChargesTable(self):
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM integrations.levy_temp_service_charge_rates''')
        self.db.commit()

    def insertTempLevyServiceCharge(self, row):
        cursor = self.db.cursor()
        cursor.execute('''REPLACE INTO integrations.levy_temp_service_charge_rates
                            (entity_code,
                             category_name,
                             rev_id,
                             sc_rate,
                             tax_flag,
                             discount_flag,
                             cat_id,
                             created_at
                            )VALUES(
                             %s, %s, %s, %s, %s, %s, %s, NOW())''', (row))
        self.db.commit()



    def clearTempLevyTaxesTable(self):
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM integrations.levy_temp_tax_rates''')
        self.db.commit

    def insertTempLevyTax(self, row):
        cursor = self.db.cursor()
        cursor.execute('''REPLACE INTO integrations.levy_temp_tax_rates
                            (entity_code,
                             category_name,
                             tax_id,
                             tax_rate,
                             rev_id,
                             cat_id,
                             created_at
                            )VALUES(
                             %s, %s, %s, %s, %s, %s, NOW())''', (row))
        self.db.commit()

    def clearTempLevyPackageDefinitionsTable(self):
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM integrations.levy_temp_package_definitions''')
        self.db.commit()

    def getTempLevyPackageDefinitions(self):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            entity_code,
                            package_item_number,
                            assigned_item_number,
                            matrix_1,
                            matrix_2,
                            matrix_3,
                            matrix_4
                          FROM integrations.levy_temp_package_definitions''')
        return cursor.fetchall()   
 
    def insertTempLevyPackageDefinition(self, row):
        cursor = self.db.cursor()
        cursor.execute('''REPLACE INTO integrations.levy_temp_package_definitions
                            (entity_code,
                             package_item_number,
                             assigned_item_number,
                             matrix_1,
                             matrix_2,
                             matrix_3,
                             matrix_4,
                             created_at
                            )VALUES(
                             %s, %s, %s, %s, %s, %s, %s, NOW())''', (row))
        self.db.commit()

    def getTempEmployees(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            employee_id, \
                            entity_code, \
                            employee_site, \
                            pos_name, \
                            first_name, \
                            last_name \
                        FROM integrations.levy_temp_employees')
        return cursor.fetchall()

    def getTempSuites(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            suite_id, \
                            suite_number, \
                            entity_code, \
                            suite_end_date \
                        FROM integrations.levy_temp_suites')
        return cursor.fetchall()

    def getTempCustomers(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            customer_number, \
                            customer_name, \
                            entity_code, \
                            suite_number, \
                            customer_suite_end_date \
                        FROM integrations.levy_temp_customers \
                        GROUP BY entity_code, customer_number')

        return cursor.fetchall()
 
    def getTempEvents(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            entity_code, \
                            event_id, \
                            event_name, \
                            event_type, \
                            event_datetime, \
                            cancelled \
                        FROM integrations.levy_temp_events \
			ORDER BY event_datetime ASC')
        return cursor.fetchall()

    def getLevyTempMenuData(self):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            levy_temp_menu_items.item_number, 
                            item_name, 
                            pos_prod_class_id, 
                            tax_id, 
                            entity_code, 
                            venue_uid, 
                            min_order, 
                            max_order, 
                            main_price, 
                            doe_price, 
                            rev_id, 
                            location_name,
                            item_classification,
                            cat_id
                        FROM integrations.levy_temp_menu_items 
                        JOIN integrations.levy_temp_menu_prices on levy_temp_menu_items.item_number = levy_temp_menu_prices.item_number 
                        JOIN integrations.venues_levy ON venues_levy.levy_entity_code = levy_temp_menu_prices.entity_code AND venues_levy.is_active = 1''')
        return cursor.fetchall()

    def getTempMenuItems(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            levy_temp_menu_items.item_number, \
                            levy_temp_menu_items.package_flag, \
                            levy_temp_menu_items.item_name, \
                            levy_temp_menu_items.pos_button_1, \
                            levy_temp_menu_items.pos_button_2, \
                            levy_temp_menu_items.pos_printer_label, \
                            levy_temp_menu_items.pos_prod_class_id, \
                            levy_temp_menu_items.pos_product_class, \
                            levy_temp_menu_items.rev_id, \
                            levy_temp_menu_items.tax_id \
                        FROM integrations.levy_temp_menu_items \
                        INNER JOIN integrations.levy_temp_menu_prices ON levy_temp_menu_items.item_number = levy_temp_menu_prices.item_number')
        return cursor.fetchall() 
   
    def getTempMenuPrices(self):
        cursor = self.db.cursor()
        cursor.execute('SELECT \
                            entity_code, \
                            item_number, \
                            min_order, \
                            max_order, \
                            pos_level_id, \
                            main_price, \
                            pos_doe_level_id, \
                            doe_price \
                        FROM integrations.levy_temp_menu_prices')
        return cursor.fetchall()
    
                          
    def getTempData(self, tempId, tempField,  tempTable):
        cursor = self.db.cursor()
        cursor.execute('SELECT * FROM integrations.' + tempTable + ' WHERE ' + tempField + ' = %s', (tempId))
        return cursor.fetchall()[0]


    def getTempTaxes(self):
        cursor = self.db.cursor()
        cursor.execute('''SELECT entity_code, category_name, tax_id, tax_rate, rev_id, cat_id FROM integrations.levy_temp_tax_rates''')
        return cursor.fetchall()

    def getCustomerSuites(self, customerNumber, entityCode):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            suite_number \
                        FROM integrations.levy_temp_customers \
                        WHERE customer_number = %s AND entity_code = %s",
                        (customerNumber, entityCode))

        return cursor.fetchall()


    def insertPatronXUnitsLevy(self, unit_uid, patrons_levy_uid):
        cursor = self.db.cursor()        

        #insert into patrons_x_units_levy
        cursor.execute("INSERT IGNORE INTO `integrations`.`patrons_x_units_levy`( \
                            `patrons_levy_uid`, \
                            `unit_uid`, \
                            `is_active`, \
                            `created_at` \
                        )VALUES( \
                            %s, \
                            %s, \
                            1, \
                            NOW())",
                        (patrons_levy_uid, unit_uid))
          
                
        self.db.commit()        


    def getDefaultPrinterSet(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT id \
                        FROM setup.printer_sets \
                        WHERE venue_uid = %s AND is_default = 1;",
                        (venue_uid))
        return cursor.fetchone()[0]

    def setEventPrinterSet(self, event_uid, default_printer_set_id):
        print str(event_uid) + " " + str(default_printer_set_id)
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO setup.events_x_printer_sets ( \
                            event_uid,  \
                            printer_set_uid \
                        ) VALUES ( \
                            %s,  \
                            %s);",
                        (event_uid, default_printer_set_id))
        self.db.commit()

    

    def getDefaultEventSettings(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT event_setting_uid, default_value \
                        FROM setup.default_event_settings \
                        WHERE venue_uid = %s",
                        (venue_uid))
        return cursor.fetchall()

    def setDefaultEventSetting(self, event_uid, event_setting_uid, default_value):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO setup.events_x_settings (\
                            event_uid, \
                            event_setting_uid, \
                            value, \
                            created_at \
                        ) VALUES ( \
                            %s, \
                            %s, \
                            %s, \
                            NOW() \
                        )",
                        (event_uid, event_setting_uid, default_value))
        self.db.commit()

    def getDefaultSuitemateSettings(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT home_background, nav_bar_bundle, widgets_bundle, menus_bundle, attendant_call_reasons_bundle, screensavers_bundle \
                        FROM settings.venue_settings \
                        WHERE venue_uid = %s AND is_active = 1",
                        (venue_uid))
        return cursor.fetchone()

    def setEventSuitemateSettings(self, venue_uid, event_uid): 
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO settings.event_settings (\
                            is_active, \
                            venue_uid, \
                            event_uid, \
                            home_background, \
                            nav_bar_bundle, \
                            widgets_bundle, \
                            menus_bundle, \
                            attendant_call_reasons_bundle, \
                            screensavers_bundle, \
                            created_at \
                        )VALUES( \
                            1, \
                            %s, \
                            %s, \
                            NULL, \
                            NULL, \
                            NULL, \
                            NULL, \
                            NULL, \
                            NULL, \
                            NOW()\
                        )", (venue_uid, event_uid))

        self.db.commit()

    def markEventXVenueHasImage(self, events_x_venue_uid):
        cursor = self.db.cursor()
        cursor.execute("UPDATE setup.events_x_venues \
                        SET has_image = 1 \
                        WHERE id = %s", (events_x_venue_uid))
        self.db.commit()

    def insertImageRow(self, pointer_uid, image_type, image_hash):
        try:
            cursor = self.db.cursor()
            cursor.execute("INSERT INTO media.images (\
                             pointer_uid, \
                             image_type, \
                             image_hash, \
                             created_at, \
                             image_version \
                         )VALUES( \
                             %s, \
                             %s, \
                             %s, \
                             NOW(), \
                             1 \
                         )  ON DUPLICATE KEY UPDATE image_hash = %s, image_version = image_version + 1", 
                         (pointer_uid, image_type, image_hash, image_hash))
        
            self.db.commit()
            
            
            return True, cursor.lastrowid
        except Exception as e:                
            self.db.rollback()
            self.addLogRow("Image Insert: FAILURE")
            return False, str(e)

    def getDefaultVenueEgo(self, venue_uid, event_type_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            COALESCE ( \
                                (SELECT ego_uid FROM setup.default_egos WHERE venue_uid = %s AND event_type_uid = %s LIMIT 1), \
                                (SELECT ego_uid FROM setup.default_egos WHERE venue_uid = %s AND event_type_uid IS NULL LIMIT 1) \
                            )",  
                        (venue_uid, event_type_uid, venue_uid))
        return cursor.fetchone()[0] 
                            

    def setEventXEgo(self, event_uid, ego_uid):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO setup.events_x_egos ( \
                            event_uid, \
                            ego_uid, \
                            is_home, \
                            created_at \
                        )VALUES( \
                            %s, \
                            %s, \
                            1, \
                            NOW() \
                        )", (event_uid, ego_uid))
        self.db.commit()

    def getDefaultOptionGroups(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT menu_option_group_uid \
                        FROM menus.default_option_groups \
                        WHERE venue_uid = %s",
                        (venue_uid))
        return cursor.fetchall()

    def setOptionGroup(self, menu_item_uid, option_group_uid):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO menus.menu_item_x_option_groups ( \
                            menu_item_uid, \
                            menu_option_group_uid, \
                            created_at \
                        )VALUES( \
                            %s, %s, NOW())", (menu_item_uid, option_group_uid))
        self.db.commit()

    def getAllLevyIntegrationVenues(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT venue_uid
                        FROM integrations.venues_levy
                        WHERE is_active = 1''')
        return cursor.fetchall()

    def countAddPurgatoryRows(self, venue_uid, pointer_table):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.purgatory \
                        WHERE pointer_table = %s \
                        AND venue_uid = %s \
                        AND required_action = 'add' \
                        AND action = 'pending' \
                        GROUP BY import_uuid", (pointer_table, venue_uid))
        return cursor.rowcount

    def countEditPurgatoryRows(self, venue_uid, pointer_table): 
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM integrations.purgatory \
                        WHERE pointer_table = %s \
                        AND venue_uid = %s \
                        AND required_action = 'edit' \
                        AND action = 'pending'", (pointer_table, venue_uid))
        return cursor.rowcount

    def countDeactivatePurgatoryRows(self, venue_uid, pointer_table):
        cursor = self.db.cursor()
        cursor.execute('''SELECT * FROM integrations.purgatory 
                        WHERE pointer_table = %s
                        AND venue_uid = %s
                        AND required_action = "deactivate" 
                        AND action = "pending"''', (pointer_table, venue_uid))
        return cursor.rowcount 
    
    def getVenuesXSuiteHolders(self, venue_uid, patron_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM patrons.venues_x_suite_holders \
                        WHERE venue_uid = %s AND patron_uid = %s",
                        (venue_uid, patron_uid))
        return cursor.fetchall()

    def getUnitsXPatrons(self, unit_uid, patron_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT * FROM info.unit_x_patrons \
                        WHERE unit_uid = %s AND patron_uid = %s",
                        (unit_uid, patron_uid))
        return cursor.fetchall()

    def getUnitXPatronsToInactivate(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                                unit_x_patrons.id, 
                                unit_x_patrons.patron_uid 
                            FROM info.unit_x_patrons
                            LEFT JOIN (SELECT unit_uid, patron_uid FROM integrations.levy_temp_customers
                                       JOIN integrations.venues_levy ON venues_levy.levy_entity_code = levy_temp_customers.entity_code AND venues_levy.is_active = 1
                                       JOIN (SELECT units_levy.unit_uid, units_levy.suite_id, levy_temp_suites.suite_number, units_levy.venue_uid FROM integrations.units_levy
                                       JOIN integrations.levy_temp_suites on levy_temp_suites.suite_id = units_levy.suite_id) as levy_suite_data ON levy_suite_data.venue_uid = venues_levy.venue_uid AND levy_suite_data.suite_number = levy_temp_customers.suite_number
                            LEFT JOIN integrations.patrons_levy ON patrons_levy.customer_number = levy_temp_customers.customer_number) as levy_data ON unit_x_patrons.unit_uid = levy_data.unit_uid AND unit_x_patrons.patron_uid = levy_data.patron_uid
                            JOIN patrons.patrons ON patrons.id = unit_x_patrons.patron_uid
                            JOIN setup.units ON units.id = unit_x_patrons.unit_uid
                            WHERE unit_x_patrons.is_active = 1
                            AND patrons.patron_type_uid != 3
                            AND patrons.id NOT IN (SELECT patrons.id FROM patrons.patrons
                                                   LEFT JOIN integrations.patrons_levy on patrons_levy.patron_uid = patrons.id
                                                   WHERE patrons_levy.id is null)
                            AND levy_data.unit_uid is null
                            AND venue_uid = %s''', (venue_uid))
        return cursor.fetchall()

    def getMenuXMenuItemOrdinal(self, menuUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT MAX(ordinal) + 1 
                            FROM menus.menu_x_menu_items WHERE menu_uid = %s''',
                       (menuUid))
        return cursor.fetchone()[0] 

    def insertParMenuItem(self, venueUid, menuUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT IGNORE INTO info.par_menu_items (
                            venue_uid, 
                            menu_item_uid, 
                            menu_x_menu_item_uid)
                          VALUES (
                            %s, 
                            %s, 
                            (SELECT 
                                id 
                            FROM menus.menu_x_menu_items 
                            WHERE menu_uid = %s 
                            AND menu_item_uid = %s))''',
                         (venueUid, menuItemUid, menuUid, menuItemUid))
        self.db.commit()

    def getAutoApplyAddActions(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            venue_uid, 
                            import_uuid, 
                            pointer_schema, 
                            pointer_table, 
                            pointer_field, 
                            new_value, 
                            levy_temp_pointer
                        FROM integrations.purgatory 
                        WHERE action = 'apply'
                       ''')
        actions = {}
        rows = cursor.fetchall()
        for row in rows:
            import_uuid = row[1]
            if import_uuid in actions:
                actions[import_uuid].append(row)
            else:
                actions[impoort_uuid] = []
                actions[import_uuid].append(row)
            
        return actions

    def isPackageItems(self, item_number):
        cursor = self.db.cursor()
        cursor.execute('SELECT package_flag \
                        FROM integrations.levy_temp_menu_items \
                        WHERE item_number = %s', (item_number))
        packageFlag = cursor.fetchone()[0] 

        if packageFlag == 'N':
            return False
        else:
            return True

    def getMenuXMenuItemsToInactivate(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute('''
                                                 SELECT 
                            menu_x_menu_items.id,
                            menu_x_menu_items.menu_item_uid
                        FROM menus.menu_x_menu_items
                        JOIN menus.menu_items ON menu_x_menu_items.menu_item_uid = menu_items.id
                        JOIN integrations.venues_levy ON menu_items.venue_uid = venues_levy.venue_uid AND  venues_levy.is_active = 1
                        JOIN menus.menus ON menu_x_menu_items.menu_uid = menus.id
                        LEFT JOIN integrations.menu_items_levy ON menu_items_levy.menu_item_uid = menu_x_menu_items.menu_item_uid AND menu_items.venue_uid = menu_items_levy.venue_uid
                        LEFT JOIN integrations.levy_temp_menu_prices ON menu_items_levy.levy_item_number = levy_temp_menu_prices.item_number AND venues_levy.levy_entity_code = levy_temp_menu_prices.entity_code
                        WHERE menu_x_menu_items.is_active = 1
                        AND levy_temp_menu_prices.item_number is null
                        AND menu_items_levy.id is NOT NULL
                        AND menu_items.venue_uid = %s
                        AND menus.menu_type_uid != 8
                        ''', (venue_uid))
        return cursor.fetchall()


    def insertDailyPatron(self, venue_uid, customerNumber, customerName):
        '''
            Inserts a new patron (including all support tables without going through purgatory

            This is used for immediate customer integration. 

        '''        
       
        import IntegrationTools
 
        cursor = self.db.cursor()
        
        companyNameHashed = IntegrationTools.hashString(customerName);
        companyNameEncrypted, companyNameEKey = IntegrationTools.encryptPatron(customerName)

        cursor.execute('''
                        INSERT INTO patrons.patrons (
                            company_name,
                            company_name_hashed,
                            is_encrypted, 
                            created_at
                        )VALUES(
                            %s, %s, 1, NOW())''', (companyNameEncrypted, companyNameHashed)) 

        patron_uid = cursor.lastrowid

        cursor.execute('''INSERT INTO operations.data_keys (
                            pointer_uid, 
                            pointer_table, 
                            pointer_schema, 
                            e_key, 
                            created_at) 
                          VALUES (
                            %s, 
                            'patrons', 
                            'patrons', 
                            %s, 
                            NOW())''', (patron_uid, companyNameEKey))

        cursor.execute('''INSERT INTO patrons.clone_patrons (
                            id,
                            company_name,
                            created_at
                          )VALUES(
                            %s, %s, NOW())''', (patron_uid, customerName))

        cursor.execute('''INSERT INTO integrations.patrons_levy (
                            customer_number,
                            venue_uid,
                            patron_uid,
                            created_at
                          )VALUES(%s, %s, %s, NOW())''', (customerNumber, venue_uid, patron_uid))

        cursor.execute('''INSERT INTO patrons.venues_x_suite_holders (
                            venue_uid,
                            patron_uid,
                            created_at
                          )VALUES(%s, %s, NOW())''', (venue_uid, patron_uid))

        self.db.commit()

        return patron_uid
                
    def insertDailySuiteAssignment(self, customerNumber, venueUid, suiteNumber):

        cursor = self.db.cursor()
        
        patron = self.getLevyPatron(customerNumber)

        if patron is None:
            raise Exception("Could not find patron")

        patronUid = patron[3]

        cursor.execute('''SELECT unit_uid 
                          FROM integrations.levy_temp_suites
                          JOIN integrations.units_levy ON units_levy.suite_id = levy_temp_suites.suite_id
                          WHERE suite_number = %s and venue_uid = %s''', (suiteNumber, venueUid))
        
        unitUid = cursor.fetchone()[0]

        unitsXPatrons = self.getUnitsXPatrons(unitUid, patronUid)
        newMapping = False
        reactivated = False

        if len(unitsXPatrons) > 0:
            
            if unitsXPatrons[0][5] == 0:
                reactivated = True
                
        else:
            newMapping = True
        cursor.execute('''INSERT INTO info.unit_x_patrons (
                            unit_uid,
                            patron_uid,
                            created_at
                          )VALUES(
                            %s, %s, NOW())
                          ON DUPLICATE KEY UPDATE is_active = 1''', (unitUid, patronUid))

        unitXPatronUid = cursor.lastrowid

        if newMapping:
            self.insertUnitPatronInfo(unitXPatronUid, venueUid)
        
        return newMapping or reactivated

    def getMenuCategoryFromClassification(self, classification, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT category_uid FROM integrations.classifications_levy 
                        WHERE item_classification = %s AND venue_uid = %s''', (classification, venueUid))
        menuCategory = cursor.fetchone()
        if menuCategory is None:
            miscCursor = self.db.cursor()
            cursor.execute('SELECT id FROM menus.menu_categories \
                            WHERE name = "Misc" and venue_uid = %s', (venueUid))
            try:
                return cursor.fetchone()[0]
            except:
                return 999
        else:
            return menuCategory[0]

    def getMenuPackageXItems(self, packageUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            id,
                            package_uid,
                            menu_item_uid,
                            qty,
                            qty_per
                          FROM menus.menu_packages_x_items
                          WHERE package_uid = %s AND menu_item_uid = %s''', (packageUid, menuItemUid))
        return cursor.fetchall()


    def isPackageOnlyItem(self, entityCode, assignedItemNumber):
        cursor = self.db.cursor()
        cursor.execute('''SELECT * 
                          FROM integrations.levy_temp_menu_items
                          JOIN integrations.levy_temp_menu_prices ON levy_temp_menu_items.item_number = levy_temp_menu_prices.item_number
                          WHERE levy_temp_menu_items.item_number = %s AND levy_temp_menu_prices.entity_code = %s''', (assignedItemNumber, entityCode))
        results = cursor.fetchall()
        if results is None or len(results) < 1:
            return True
        else:
            return False

    def getLevyPackageOnlyTempMenuItemData(self, itemNumber):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            item_number,
                            package_flag,
                            item_name,
                            pos_prod_class_id,
                            item_classification,
                            rev_id,
                            tax_id
                          FROM integrations.levy_temp_menu_items
                          WHERE item_number = %s''', (itemNumber))
        return cursor.fetchall()



    def findPackageMenuItems(self, venue_uid, name):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                                * 
                          FROM menus.menu_items 
                          JOIN menus.menu_x_menu_items ON menu_items.id = menu_x_menu_items.menu_item_uid
                          JOIN menus.menus ON menu_x_menu_items.menu_uid = menus.id
                          WHERE menu_x_menu_items.is_active = 1 AND menu_type_uid = 8 AND menu_items.venue_uid = %s AND name = %s''', 
                        (venue_uid, name))
        return cursor.fetchall() #there should only be one

    def insertNewPackageOnlyMenuItem(self, venueUid, menuTaxUid, itemName, price, minimumQty, maximumQty, printerCategory, serviceChargeRate, menuUid, menuCategoryUid, itemNumber, levyPosProductClassId, levyTaxGroupId, levyRevenueCategoryId):

        cursor = self.db.cursor()

        print str(levyPosProductClassId)

        cursor.execute('''INSERT INTO menus.menu_items(
                                    venue_uid,
                                    menu_tax_uid,
                                    name,
                                    display_name,
                                    price,
                                    minimum_qty,
                                    maximum_qty,
                                    printer_category,
                                    service_charge_rate,
                                    created_at
                          )VALUES(
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())''',
                              (venueUid, menuTaxUid, itemName, itemName, price, minimumQty, maximumQty, printerCategory, serviceChargeRate))
        
        menuItemUid = cursor.lastrowid

        #425 for some reason (kate, cough, cough) does not have a packges menu, shit's fucked overthere
        if menuUid is not None:
            cursor.execute('''INSERT INTO menus.menu_x_menu_items(
                            menu_uid,
                            menu_category_uid,
                            menu_item_uid,
                            price,
                            created_at
                         )VALUES(
                            %s, %s, %s, %s, NOW())''',
                         (menuUid, menuCategoryUid, menuItemUid, price))

            menuXMenuItemUid = cursor.lastrowid
        
        else:
            menuXMenuItemUid = None

        cursor.execute('''INSERT INTO integrations.menu_items_levy(
                            levy_item_number,
                            menu_item_uid,
                            venue_uid,
                            levy_pos_product_class_id,
                            levy_tax_group_id,
                            levy_revenue_category_id,
                            created_at
                         )VALUES(
                            %s, %s, %s, %s, %s, %s, NOW())''',
                         (itemNumber, menuItemUid, venueUid, levyPosProductClassId, levyTaxGroupId, levyRevenueCategoryId))

        self.db.commit()
                                            
                                            
        
    def getPackagesMenu(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            id 
                          FROM menus.menus
                          WHERE venue_uid = %s AND menu_type_uid = 8''',
                        (venueUid))
        menus = cursor.fetchall()
        try:
            return menus[0][0]
        except:
            print "no package menu for venue " + str(venueUid)
            return None


    def getItemsRemovedFromPackages(self):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            id 
                          FROM menus.menu_packages_x_items 
                          WHERE menu_item_uid NOT IN(
                            SELECT menu_item_uid FROM integrations.levy_temp_package_definitions
                            JOIN integrations.menu_items_levy ON menu_items_levy.levy_item_number = levy_temp_package_definitions.assigned_item_number)''')

        return cursor.fetchall()

    def markVenueIntegrated(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE controlcenter.venue_build_status
                          SET integration = "complete"
                          WHERE venue_uid = %s AND integration != "complete"''', (venueUid))
        self.db.commit()

    def insertAnonPatron(self, unitUid):
        cursor = self.db.cursor()
        #277 = anon patron
        cursor.execute('''INSERT INTO info.unit_x_patrons(
                            unit_uid,
                            patron_uid,
                            event_type,
                            created_at
                          )VALUES(%s, 277, "default", NOW())''',
                          (unitUid))
        
        unitPatronUid = cursor.lastrowid

        cursor.execute('''INSERT IGNORE INTO info.unit_patron_info (unit_patron_uid) VALUES (%s)''', (unitPatronUid))

        self.db.commit()

    def getMenuItemLevyUid(self, venueUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT id
                        FROM integrations.menu_items_levy
                        WHERE menu_item_uid = %s
                        AND venue_uid = %s
                       ''', (menuItemUid, venueUid))

        return cursor.fetchall()[0][0]

    def checkItemActiveStatus(self, venueUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT is_active
                        FROM integrations.menu_items_levy
                        WHERE venue_uid = %s
                        AND menu_item_uid = %s''',
                        (venueUid, menuItemUid))
        
        isActive = cursor.fetchall()[0][0]

        if isActive == 1:
            return True
        else:
            return False

    def getMXMRowsByItemUid(self, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT id 
                        FROM menus.menu_x_menu_items
                        WHERE menu_item_uid = %s''',
                        (menuItemUid))
        return cursor.fetchall()
                      

    def reactivateMenuXMenuItems(self):
        cursor = self.db.cursor()
        cursor.execute('''
                         UPDATE menus.menu_x_menu_items 
                        SET is_active = 1
                        WHERE id in (
                            SELECT id FROM (SELECT menu_x_menu_items.id FROM menus.menu_x_menu_items
                            JOIN menus.menu_items ON menu_items.id = menu_x_menu_items.menu_item_uid
                            JOIN integrations.menu_items_levy on menu_items_levy.menu_item_uid = menu_x_menu_items.menu_item_uid
                            JOIN integrations.venues_levy on menu_items.venue_uid = venues_levy.venue_uid
                            JOIN integrations.levy_temp_menu_prices ON menu_items_levy.levy_item_number = levy_temp_menu_prices.item_number AND levy_temp_menu_prices.entity_code = venues_levy.levy_entity_code
                            WHERE menu_x_menu_items.is_active = 0 
                            AND menu_items_levy.is_active = 1
                            AND menu_x_menu_items.id NOT IN (SELECT menu_x_menu_item_uid 
                                                                 FROM menus.menu_x_menu_item_sticky_deactivations)) AS ids)''')

        self.db.commit()

    def deactivatePackageItems(self):
        cursor = self.db.cursor()
        cursor.execute('''
    DELETE FROM menus.menu_packages_x_items WHERE id IN (SELECT menu_package_x_item_uid FROM (
                            SELECT A_package_item_uid AS menu_package_x_item_uid, A_package_uid AS package_item_uid, A_package_item_number as package_item_number, B_menu_item_uid as menu_item_uid, B_item_item_number as assigned_item_number 
                            FROM
            #get package data, with out uids and their numbers
            (SELECT 
                menu_packages_x_items.id AS A_package_item_uid, 
                package_uid as A_package_uid, 
                menu_packages_x_items.menu_item_uid AS A_menu_item_uid, 
                levy_item_number AS A_package_item_number, 
                menu_items_levy.menu_item_uid as A_menu_items_levy_menu_iem_uid, 
                venue_uid as A_venue_uid 
             FROM menus.menu_packages_x_items
             JOIN integrations.menu_items_levy on menu_items_levy.menu_item_uid = menu_packages_x_items.package_uid) AS A
        JOIN
            #get package ITEM data with our uids and their numbers
            (SELECT 
                menu_packages_x_items.id AS B_package_item_uid, 
                package_uid as B_package_uid, 
                menu_packages_x_items.menu_item_uid as B_menu_item_uid, 
                levy_item_number AS B_item_item_number, 
                menu_items_levy.menu_item_uid as B_menu_items_levy_menu_item_uid, 
                venue_uid as B_venue_uid 
             FROM menus.menu_packages_x_items
             JOIN integrations.menu_items_levy on menu_items_levy.menu_item_uid = menu_packages_x_items.menu_item_uid) AS B

        ON A.A_package_item_uid = B.B_package_item_uid
    ) AS package_info
    #join package_x_items with levy pacakge data, look for missing rows in levy package data which will indicate items removed from pacakges
    LEFT JOIN integrations.levy_temp_package_definitions ON levy_temp_package_definitions.package_item_number = package_info.package_item_number AND levy_temp_package_definitions.assigned_item_number = package_info.assigned_item_number
    WHERE levy_temp_package_definitions.entity_code is NULL
    GROUP BY menu_package_x_item_uid)
    ''')
    
        self.db.commit()

    

    def batchInsertTempTableRows(self, tableName, columns, rows):
        
        localColumns = columns
 
        #build the query string
        query = "INSERT IGNORE INTO integrations." + tableName + "("

        query = query + ",".join(x for x in columns)

        query = query + ") VALUES "
        valuesString = "(" + ",".join("%s" for _ in columns[:-1]) + ",NOW())"
        query = query + ",".join(valuesString for _ in rows)
        
        flattenedRows = [item for sublist in rows for item in sublist]

        try:
            cursor = self.db.cursor()
            rowCount =  cursor.execute(query, tuple(flattenedRows))
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
    
        failedRows = []
        if rowCount < len(rows):
            failedRows = self.getFailedRows(tableName, columns, rows)
            print "FAILED ROWS: " + str(failedRows)        

        self.db.commit()
        
        if len(failedRows) == 0:
            return None
        else:
            return failedRows

    def getFailedRows(self, tableName, columns, rows):

        failedRows = []

        query = "SELECT * FROM integrations." + tableName + " WHERE "
        query = query + " AND ".join(column + "=%s " for column  in columns[:-1])

        cursor = self.db.cursor()
        for row in rows:
            rowCount = cursor.execute(query, row)
            if rowCount == 0: 
                failedRows.append(row)

        return failedRows

    def getItemClassificationFromMenuItemUid(self, venueUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT item_classification 
                        FROM integrations.menu_items_levy
                        JOIN integrations.levy_temp_menu_items on menu_items_levy.levy_item_number = levy_temp_menu_items.item_number
                        WHERE venue_uid = %s AND menu_item_uid = %s
                       ''', (venueUid, menuItemUid))

        return cursor.fetchall()[0][0]

    def countAllVenuePurgatoryRows(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT count(*) FROM integrations.purgatory
                        WHERE venue_uid = %s
                       ''', (venueUid))
        return cursor.fetchall()[0][0]
