from config import CheckMateConfig
from keymaster import KeyMaster
import IntegrationTools
import pytz, datetime

class Aramark_Db:

    def __init__(self, db):
        self.db = db

    def commit(self):
        self.db.commit()

    def getFacilityId(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT facility_id from integrations.venues_aramark 
                          WHERE venue_uid = %s''', (venueUid))
        return cursor.fetchone()[0]
    
    ##################
    #  GET MAPPINGS  #
    ##################

    def getUnitMappings(self, suiteId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            id,
                            suite_id, 
                            unit_uid, 
                            is_active, 
                            last_updated 
                          FROM integrations.units_aramark
                          WHERE suite_id = %s 
                            AND is_active = 1''', (suiteId))

        return cursor.fetchall()

    def getEventMappings(self, eventId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                        event_id,
                        event_uid,
                        menu_id,
                        last_updated
                      FROM integrations.events_aramark
                      WHERE event_id = %s''', (eventId))
        return cursor.fetchall()

    def getMenuItemMappings(self, menuItemId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                            menu_item_uid,
                            menu_item_id,
                            last_updated
                          FROM integrations.menu_items_aramark
                          WHERE menu_item_id = %s''', (menuItemId))
        return cursor.fetchall()

    def getMenuMappings(self, menuId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                            menu_uid,
                            menu_id
                          FROM integrations.menus_aramark
                          WHERE menu_id = %s''', (menuId))
        return cursor.fetchall()

    def getMenuCategoryMappings(self, categoryId, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                            menu_category_uid,
                            category_id
                          FROM integrations.menu_categories_aramark
                          WHERE category_id = %s and venue_uid = %s''', (categoryId, venueUid)) 
        return cursor.fetchall()

    def getCustomerMappings(self, customerId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                            superpatron_uid,
                            customer_id
                          FROM integrations.superpatrons_aramark
                          WHERE customer_id = %s''',
                          (customerId))
        return cursor.fetchall()

    def getAccountMappings(self, accountId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id,
                            patron_uid,
                            account_id
                          FROM integrations.patrons_aramark
                          WHERE account_id = %s''', (accountId))
        return cursor.fetchall()


    #####################
    #  CREATE MAPPINGS  #
    #####################

    def addUnitMapping(self, venueUid, unitUid, suiteId, lastUpdated, suiteNumber):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `integrations`.`units_aramark`(
                            `venue_uid`,
                            `suite_id`,
                            `unit_uid`,
                            `suite_number`,
                            `is_active`,
                            `last_updated`,
                            `created_at`
                          )VALUES(%s, %s, %s, %s, 1, %s, NOW())''',
                        (venueUid, suiteId, unitUid, suiteNumber, lastUpdated))

    def addEventMapping(self, eventUid, eventId, menuId):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `integrations`.`events_aramark`(
                            `event_uid`,
                            `event_id`,
                            `menu_id`,
                            `created_at`
                          )VALUES(%s, %s, %s, NOW())''', (eventUid, eventId, menuId))

    '''
        This add mapping function works differently than the others,
            it doesn't actually make a mapping to a category_uid, someone
            will have to review this row and choose to make a mapping manually
            or not.
    '''
    def addMenuCategoryMapping(self, categoryId, title, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO integrations.menu_categories_aramark(
                            venue_uid,
                            category_id,
                            title,
                            created_at
                          )VALUES(%s, %s, %s, NOW())''', (venueUid, categoryId, title))

    def addMenuMapping(self, menuUid, menuId):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO integrations.menus_aramark(
                            menu_uid,
                            menu_id,
                            created_at
                          )VALUES(%s, %s, NOW())''', (menuUid, menuId))


    def addMenuItemMapping(self, venueUid,  menuItemUid, menuItemId, title, lastUpdated):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO integrations.menu_items_aramark(
                            venue_uid,
                            menu_item_uid,
                            menu_item_id,
                            title,
                            last_updated,
                            created_at
                          )VALUES(%s, %s, %s, %s, %s, NOW())''', (venueUid, menuItemUid, menuItemId, title,  lastUpdated))     

    def addPatronMapping(self, venueUid, patronUid, accountId, accountFirst, accountLast):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO integrations.patrons_aramark(
                            venue_uid,
                            patron_uid,
                            account_id,
                            account_first,
                            account_last,
                            is_active,
                            created_at
                          )VALUES(%s, %s, %s, %s, %s, 1, NOW())''', (venueUid, patronUid, accountId, accountFirst, accountLast))

    def addSuperpatronMapping(self, venueUid, superpatronUid, customerId, customerName):

        cursor = self.db.cursor()

        cursor.execute('''INSERT INTO integrations.superpatrons_aramark(
                            venue_uid,
                            superpatron_uid,
                            customer_id,
                            customer_name,
                            created_at
                          )VALUES(
                            %s, %s, %s, %s, NOW())''',
                        (venueUid, superpatronUid, customerId, customerName))

    #############
    #  UPDATES  #
    #############

    def updateLastUpdated(self, table, lastUpdated, uid):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE integrations.''' + table +
                       ''' SET last_updated = %s
                          WHERE id = %s''', (lastUpdated, uid))
        

    def updateSuite(self, unitUid, unitName):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE setup.units
                          SET name = %s
                          WHERE id = %s''', (unitName, unitUid))

    def updateEvent(self, eventUid, eventDate):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE setup.events
                          SET event_date = %s
                          WHERE id = %s''', (eventDate, eventUid))

    def updateEventXVenue(self, venueUid, eventUid, eventName):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE setup.events_x_venues
                          SET event_name = %s
                          WHERE event_uid = %s AND venue_uid = %s''',
                        (eventName, eventUid, venueUid))

    def updateMenuItem(self, menuItemUid, name, price, servingsPerItem, cost):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE menus.menu_items
                          SET 
                            name = %s,
                            price = %s,
                            servings_per_item = %s,
                            cost = %s
                          WHERE id = %s''',
                        (name, price, servingsPerItem, cost, menuItemUid))

    def updateMenuXMenuItem(self, menuItemUid, menuUid, menuCategoryUid, price):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE menus.menu_x_menu_items
                          SET 
                            menu_category_uid = %s,
                            price = %s
                          WHERE
                            menu_item_uid = %s
                          AND
                            menu_uid = %s''',
                        (menuCategoryUid, price, menuItemUid, menuUid))
    #############
    #  INSERTS  #
    #############

    def insertUnit(self, venueUid, name):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `setup`.`units`(
                            `venue_uid`,
                            `name`,
                            `location_uid`,
                            `unit_type`,
                            `is_active`,
                            `created_at`
                          )VALUES(%s, %s, 6, 'suite', 1, NOW())''',
                        (venueUid, name))
        return cursor.lastrowid

    def insertEvent(self, venueUid, eventDate):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `setup`.`events`(
                            `venue_uid`,
                            `event_date`,
                            `event_type_uid`,
                            `tablet_alert_sent`,
                            `created_at`
                          )VALUES(%s, %s, 9, 0, NOW() )''', (venueUid, eventDate))
        return cursor.lastrowid

    def insertEventXVenue(self, venueUid, eventUid, eventName):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `setup`.`events_x_venues`(
                            `event_uid`,
                            `venue_uid`,
                            `event_name`,
                            `has_image`,
                            `created_at`
                          )VALUES(%s, %s, %s, 0, NOW() )''', (eventUid, venueUid, eventName))
        
        eventXVenueUid = cursor.lastrowid

        cursor.execute('''INSERT INTO setup.events_x_egos (event_uid, ego_uid, is_home) (
                          SELECT %s, ego_uid, 1 FROM setup.default_egos WHERE venue_uid = %s AND event_type_uid is NULL)''',
                          (eventUid, venueUid))

        cursor.execute('''INSERT INTO setup.events_x_printer_sets (event_uid, printer_set_uid) 
                          (SELECT %s, id FROM setup.printer_sets WHERE venue_uid = %s AND is_default = 1)''',
                          (eventUid, venueUid))

        cursor.execute('''INSERT INTO setup.events_x_units (event_uid, unit_uid) 
                          (SELECT %s, id FROM setup.units WHERE venue_uid = %s)''',
                          (eventUid, venueUid))


        cursor.execute('''INSERT INTO setup.events_x_settings (event_uid, event_setting_uid, value) 
                          (SELECT %s, event_setting_uid, default_value FROM setup.default_event_settings WHERE venue_uid = %s)''', (eventUid, venueUid))

        return eventXVenueUid

    def insertMenu(self, venueUid, menuName, menuTypeUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO menus.menus(
                            venue_uid, 
                            menu_name,
                            menu_type_uid,
                            created_at
                          )VALUES(%s, %s, %s, NOW())''', (venueUid, menuName, menuTypeUid))
        return cursor.lastrowid

    def insertMenuItem(self, venueUid, menuTaxUid, name, price, servingsPerItem, printerCategory, cost):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `menus`.`menu_items`(
                            `venue_uid`,
                            `menu_tax_uid`,
                            `name`,
                            `display_name`,
                            `price`,
                            `servings_per_item`,
                            `printer_category`,
                            `cost`,
                            `created_at`)
                          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, NOW())''',
                          (venueUid, menuTaxUid, name, name, price, servingsPerItem, printerCategory, cost))
        return cursor.lastrowid    

    def insertMenuXMenuItem(self, menuUid, menuCategoryUid, menuItemUid, price):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO menus.menu_x_menu_items(
                            menu_uid,
                            menu_category_uid,
                            menu_item_uid,
                            price,
                            ordinal,
                            created_at
                         )VALUES(
                            %s,
                            %s,
                            %s,
                            %s,
                            (SELECT ordinal FROM (SELECT MAX(ordinal) + 1 AS ordinal FROM menus.menu_x_menu_items WHERE menu_uid = %s) AS a),
                            NOW())''',
                        (menuUid, menuCategoryUid, menuItemUid, price, menuUid))

    def insertSuperpatron(self, venueUid, customerName):
        cursor = self.db.cursor()
    
        superpatronUid = cursor.execute('''INSERT IGNORE INTO patrons.superpatrons (venue_uid, name, created_at)
                          VALUES (%s, %s, NOW())''', (venueUid, customerName))

        return cursor.lastrowid

    def insertPatron(self, firstName, lastName, superpatronUid):
        cursor = self.db.cursor()
        
        companyName = firstName + " " + lastName

        km = KeyMaster()
        values = {}
        values['first_name'] = firstName
        values['last_name'] = lastName  
        values['company_name'] = companyName 

        encoded = km.encryptMulti(values)

        if 'first_name' in encoded and 'encoded' in encoded['first_name'] and 'e_key' in encoded['first_name'] and 'last_name' in encoded and 'encoded' in encoded['last_name'] and 'e_key' in encoded['last_name'] and 'company_name' in encoded and 'encoded' in encoded['company_name'] and 'e_key' in encoded['company_name']:
            
            firstNameEncoded = encoded['first_name']['encoded']
            lastNameEncoded = encoded['last_name']['encoded']
            companyNameEncoded = encoded['company_name']['encoded']

            companyNameHashed = IntegrationTools.hashString(companyName)
            


            eKey = encoded['first_name']['e_key'] # will be the same for both, since we encrypt by row

            cursor.execute('''INSERT INTO patrons.patrons(
                                first_name,
                                last_name,
                                company_name,
                                company_name_hashed,
                                is_encrypted,
                                superpatron_uid,
                                created_at
                              )VALUES(
                                %s, %s, %s, %s, 1, %s, NOW())''', (firstNameEncoded, lastNameEncoded, companyNameEncoded, companyNameHashed, superpatronUid))

            patronUid = cursor.lastrowid


            cursor.execute('''INSERT INTO patrons.clone_patrons(
                                id,
                                first_name,
                                last_name,
                                company_name,
                                superpatron_uid,
                                created_at
                              )VALUES(
                                %s, %s, %s, %s, %s, NOW())''', (patronUid, firstName, lastName, companyName, superpatronUid))

            cursor.execute('''INSERT INTO operations.data_keys(
                                pointer_uid,
                                pointer_table,
                                pointer_schema, 
                                e_key,
                                created_at
                             )VALUES(%s, %s, %s, %s, NOW())''', (patronUid, 'patrons', 'patrons', eKey))

            return patronUid
        else:
            raise Exception("Bad data returned from encryptMulti: " + str(encoded))


    def insertVenuesXSuiteHolders(self, venueUid, patronUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO patrons.venues_x_suite_holders(
                            venue_uid,
                            patron_uid,
                            is_active,
                            created_at
                         )VALUES(%s, %s, 1, NOW())''', (venueUid, patronUid))
    ##########
    #  FIND  #
    ##########

    def findUnitByName(self, name, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT id FROM setup.units
                          WHERE is_active = 1 AND name = %s AND venue_uid = %s''',
                          (name, venueUid))
        return cursor.fetchall();

    def findEventByNameAndDate(self, eventName, eventDate):
        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            events.id,
                            events.venue_uid,
                            event_date
                          FROM setup.events
                          JOIN setup.events_x_venues ON events.id = events_x_venues.event_uid
                          WHERE event_name = %s and event_date = %s''', (eventName, eventDate))
        return cursor.fetchall()   

    def findMenuItemByNameAndVenue(self, venueUid, menuItemName):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            id
                          FROM menus.menu_items
                          WHERE venue_uid = %s
                            AND name = %s''', (venueUid, menuItemName))
        return cursor.fetchall()


    ##############
    #  Feedback  #
    ##############

    def getErrorEmailRecipients(self, venueUid):
        ARAMARK_INTEGRATION_ERROR_NOTIFICATION_UID = 9
        cursor = self.db.cursor()
        cursor.execute('''SELECT email from notifications.notifications
                          JOIN notifications.notifications_x_venues ON notifications.id = notifications_x_venues.notification_uid
                          JOIN notifications.email_notifications ON notifications_x_venues.id = email_notifications.notification_venue_uid
                          WHERE venue_uid = %s AND notification_uid = %s''', (venueUid, ARAMARK_INTEGRATION_ERROR_NOTIFICATION_UID))
        return cursor.fetchall()

    ##########
    #  MISC  #
    ##########

    #TODO : this needs to actually do something
    def getMenuTaxUid(self):
        return 35

    def getPrinterCategory(self):
        return 'none'

    def getMenuTypeUid(self):
        return 3

    def getMenuUid(self, menuId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_uid FROM integrations.menus_aramark WHERE menu_id = %s''', (menuId))
        menuUid = cursor.fetchone()[0]
        return menuUid

    def getMenuCategoryUid(self, venueUid, categoryId):
        cursor  = self.db.cursor()
        cursor.execute('''SELECT COALESCE(
                            (SELECT menu_category_uid 
                             FROM integrations.menu_categories_aramark 
                             WHERE category_id = %s), 
                            (SELECT id FROM menus.menu_categories 
                             WHERE venue_uid = %s AND name = 'New Items')
                          )''', (categoryId, venueUid))
        return cursor.fetchone()[0]

    def getUnCategorizedMenuXMenuItems(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_x_menu_items.id, menu_items.name FROM menus.menu_x_menu_items
                          JOIN menus.menu_items ON menu_items.id = menu_x_menu_items.menu_item_uid
                          WHERE menu_category_uid = (
                                    SELECT id 
                                    FROM menus.menu_categories 
                                    WHERE name = "New Items" and venue_uid = %s)''',
                       (venueUid))
        return cursor.fetchall()

    def getPatronUidFromAccountId(self, accountId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT patron_uid 
                          FROM integrations.patrons_aramark
                          WHERE account_id = %s''', (accountId))
        patronUids = cursor.fetchall()
        if len(patronUids) != 1: 
            raise Exception("Too many or too few patron mappings: " + str(patronUids))
        
        return patronUids[0][0]


    def getUnitUidFromSuiteId(self, suiteId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT unit_uid
                          FROM integrations.units_aramark
                          WHERE suite_id = %s''', (suiteId))
        
        #print cursor._last_executed
        unitUids = cursor.fetchall()
        if len(unitUids) != 1:
            raise Exception("Too many or too few unit mappings: SuiteId = " + suiteId + " Mappings= "  + str(unitUids ))

        return unitUids[0][0]

    def getEventIdFromEventUid(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT event_id
                          FROM integrations.events_aramark
                          WHERE event_uid = %s''', (eventUid))
        eventUids = cursor.fetchall()
        if len(eventUids) != 1:
            raise Exception("Too many or too few event mappings")
        
        return eventUids[0][0]

    def insertUnitXPatrons(self, unitUid, patronUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT IGNORE INTO info.unit_x_patrons ( 
                            unit_uid, \
                            patron_uid, \
                            event_type, \
                            created_at \
                          ) VALUES (%s, %s, 'default', NOW())''', (unitUid, patronUid))
        return cursor.lastrowid

    def insertUnitPatronInfo(self, unitPatronUid, venueUid):
        print "Unit patron uid: " + str(unitPatronUid)

        cursor = self.db.cursor()
        cursor.execute('INSERT INTO info.unit_patron_info ( \
                            unit_patron_uid, \
                            created_at \
                        ) VALUES ( \
                            %s, \
                            NOW() \
                        )', (unitPatronUid))

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

    def getAramarkEmailRecipients(self, venueUid):
        cursor = self.db.cursor()

        cursor.execute('''
                        SELECT email 
                        FROM notifications.email_notifications
                        JOIN notifications.notifications_x_venues ON email_notifications.notification_venue_uid = notifications_x_venues.id
                        WHERE venue_uid = %s AND notification_uid = 10
                       ''',
                        (venueUid))

        return cursor.fetchall()

    def insertUnitXPatron(self, patronUid, unitUid, venueUid):

        cursor = self.db.cursor()
        
        cursor.execute('''INSERT IGNORE INTO info.unit_x_patrons(
                            unit_uid,
                            patron_uid,
                            event_type,
                            created_at
                         )VALUES(
                            %s, %s, 'default', NOW())''', (unitUid, patronUid))

        unitPatronUid = cursor.lastrowid

        if unitPatronUid == 0 or unitPatronUid is None:
            cursor.execute('''SELECT id FROM info.unit_x_patrons
                              WHERE unit_uid = %s AND patron_uid = %s''',
                              (unitUid, patronUid))

            unitPatronUid = cursor.fetchall()[0][0]

        

        print "unitPatronUid: " + str(unitPatronUid)
        cursor.execute('''SELECT * FROM info.unit_patron_info WHERE unit_patron_uid = %s''', (unitPatronUid))
        if(len(cursor.fetchall()) > 0):
            self.db.commit()
            return;

        cursor.execute('''INSERT IGNORE INTO info.unit_patron_info (
                            unit_patron_uid,
                            created_at
                         )VALUES(
                            %s,
                            NOW())''', (unitPatronUid))

        unitPatronInfoUid = cursor.lastrowid


        cursor.execute('''INSERT INTO info.unit_patron_cart_info( \
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
                          WHERE venue_uid = %s)''', (unitPatronInfoUid, venueUid))

        self.db.commit()

    def getSuperpatronUid(self, venueUid, customerId):
        cursor = self.db.cursor()
        cursor.execute('''SELECT superpatron_uid 
                          FROM integrations.superpatrons_aramark 
                          WHERE venue_uid = %s AND customer_id = %s''',
                          (venueUid, customerId))

        return cursor.fetchall()[0][0]


    ######################## 
    #  PREORDER INGESTION  #
    ########################

    def getPreorderInProgressEvents(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT event_uid
                          FROM integrations.events_aramark
                          JOIN setup.events on events.id = events_aramark.event_uid
                          WHERE events.venue_uid = %s AND preorder_status = "in progress"''', (venueUid))
        return cursor.fetchall()

    def getAllAccounts(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT account_id
                                   FROM integrations.patrons_aramark
                                   JOIN patrons.venues_x_suite_holders ON patrons_aramark.patron_uid = venues_x_suite_holders.patron_uid
                                   WHERE venues_x_suite_holders.venue_uid = %s''', (venueUid))
        return cursor.fetchall()

    def markPreordresComplete(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE integrations.events_aramark
                          SET preorder_status = "done"
                          WHERE event_uid = %s''',
                          (eventUid))
        self.db.commit()

    def markPreordersFailed(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE integrations.events_aramark
                          SET preorder_status = "failed"
                          WHERE event_uid = %s''',
                          (eventUid))
        self.db.commit()

    ####################
    #  ORDER TRANSFER  #
    ####################

    def getEventsToTransfer(self):
        cursor = self.db.cursor()
        cursor.execute('''SELECT event_uid, events.venue_uid FROM setup.event_controls
                          JOIN setup.events on events.id = event_controls.event_uid
                          JOIN integrations.venues_aramark ON events.venue_uid = venues_aramark.venue_uid
                          WHERE is_locked = 1 AND is_transfered = 0''')

        return cursor.fetchall()

    def getOrdersToTransfer(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT orders.id, order_type_uid, patron_uid, unit_uid
                          FROM orders.orders
                          LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id
                          LEFT JOIN orders.order_modifications ON order_modifications.id = orders_x_modifications.order_modification_uid
                          WHERE event_uid = %s AND is_open = 0 AND orders.id NOT IN (SELECT order_uid FROM integrations.aramark_transfered_orders) 
                            AND (action_type IS NULL OR action_type != 'void')''',
                          (eventUid))

        return cursor.fetchall()


    def getOrderItems(self, orderUid):

        cursor = self.db.cursor()
        cursor.execute('''SELECT 
                            order_items.id, 
                            sub_orders.revenue_center_uid, 
                            menu_x_menu_item_uid, 
                            menu_item_uid, 
                            (CASE WHEN order_modifications.action_type = 'void' and order_modifications.status = 'approved' 
                                THEN 
                                    0 
                                ELSE 
                                    order_items.price 
                                END 
                            ) as 'price', 
                            COUNT(*) AS quantity 
                        FROM orders.order_items 
                        JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid 
                        LEFT JOIN (SELECT * FROM orders.order_items_x_modifications GROUP BY order_item_uid) AS order_items_x_modifications ON order_items_x_modifications.order_item_uid = order_items.id 
                        LEFT JOIN orders.order_modifications ON order_modifications.id = order_items_x_modifications.order_modification_uid 
                        LEFT JOIN orders.orders ON sub_orders.order_uid = orders.id 
                        LEFT JOIN menus.menu_x_menu_items ON menu_x_menu_items.id = order_items.menu_x_menu_item_uid 
                        LEFT JOIN (SELECT order_uid, amount FROM orders.orders_x_discounts WHERE order_uid = %s GROUP BY order_uid) AS orders_x_discounts ON orders_x_discounts.order_uid = orders.id 
                        WHERE orders.id = %s  AND (order_modifications.action_type IS NULL OR order_modifications.action_type != 'void')
                        GROUP BY orders.id, price, menu_x_menu_item_uid;''', (orderUid, orderUid))

        return cursor.fetchall()

    def getOrderItemsAddedToPreorder(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                         SELECT 
                             order_items.id AS order_item_uid, 
                             revenue_center_uid, 
                             menu_x_menu_item_uid, 
                             menu_item_uid,
                             order_items.price, 
                             COUNT(*) AS quantity FROM orders.orders
                         JOIN orders.sub_orders on sub_orders.order_uid = orders.id
                         JOIN orders.order_items on order_items.sub_order_uid = sub_orders.id
                         JOIN menus.menu_x_menu_items on menu_x_menu_items.id = menu_x_menu_item_uid
                         LEFT JOIN orders.order_items_x_modifications ON order_items.id = order_items_x_modifications.order_item_uid
                         LEFT JOIN orders.order_modifications ON order_modifications.id = order_items_x_modifications.order_modification_uid
                         WHERE orders.id = %s AND sub_orders.order_type_uid != 8 AND (action_type != 'void' OR action_type is NULL)
                         GROUP BY menu_x_menu_item_uid                       
                        ''', (orderUid))

        return cursor.fetchall()
    def getAramarkCustomerData(self, patronUid):

        cursor = self.db.cursor()
        cursor.execute('''SELECT customer_id, account_id FROM patrons.patrons 
                          JOIN integrations.patrons_aramark ON patrons.id = patrons_aramark.patron_uid
                          JOIN integrations.superpatrons_aramark ON patrons.superpatron_uid = superpatrons_aramark.superpatron_uid
                          WHERE patrons.id = %s''', (patronUid))

        return cursor.fetchall()[0]

    def getEventId(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT event_id FROM integrations.events_aramark WHERE event_uid = %s''', (eventUid))
        return cursor.fetchall()[0][0]

    def getAramarkSuiteId(self, unitUid):

        cursor = self.db.cursor()
        cursor.execute('''SELECT suite_id FROM integrations.units_aramark WHERE unit_uid = %s''', (unitUid))

        return cursor.fetchall()[0][0]

    def getAramarkItemData(self, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_item_id, title FROM integrations.menu_items_aramark WHERE menu_item_uid = %s''',
                        (menuItemUid))

        return cursor.fetchall()[0]


    def getOrderPayments(self, orderUid):

        cursor = self.db.cursor()
        totalPayments = cursor.execute('''SELECT 
                            order_payment_uid, 
                            SUM(subtotal) AS subtotal, 
                            SUM(discount) AS discount, 
                            SUM(tip) AS tip, 
                            SUM(tax) AS tax, 
                            order_pay_method_uid 
                        FROM orders.order_payments_x_revenue_centers 
                        JOIN orders.order_payments ON order_payments_x_revenue_centers.order_payment_uid = order_payments.id 
                        WHERE order_uid = %s 
                        GROUP BY order_payment_uid''',
                        (orderUid))
    
        if totalPayments == 0:
            return []
        
        return cursor.fetchall()

    def addOrderTransferAction(self, ordersAramarkUid, taskType, taskDescription):
        cursor = self.db.cursor()
        cursor.execute('''
                        INSERT INTO integrations.orders_aramark_tasks(
                            orders_aramark_uid,
                            type,
                            description,
                            created_at
                        )VALUES(
                            %s, %s, %s, NOW())
                       ''', (ordersAramarkUid, taskType, taskDescription))

        self.db.commit()

    def saveOrderAramark(self, orderId, venueUid, orderUid, orderNumber):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO integrations.orders_aramark(
                            order_id,
                            venue_uid,
                            order_uid,
                            order_number,
                            created_at
                          )VALUES(
                            %s, %s, %s, %s, NOW())''', (orderId, venueUid, orderUid, orderNumber))
        self.db.commit()
        return cursor.lastrowid
       
    def getOrderPaymentTotal(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT 
                            SUM(subtotal) + SUM(gratuity) + SUM(TAX) - SUM(discount), SUM(gratuity)
                        FROM orders.orders_x_revenue_centers
                        WHERE order_uid = %s''',
                        (orderUid))
        return cursor.fetchall()[0]
 
    def savePreorderNumber(self, orderUid, orderNumber):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE integrations.orders_aramark
                          SET order_number = %s
                          WHERE orderUid = %s''',
                          (orderNumber, orderUid))

        self.db.commit()
                    
    def findOrdersAramarkUid(self, orderUid, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT id, order_id  FROM integrations.orders_aramark
                        WHERE order_uid = %s AND venue_uid = %s''',
                        (orderUid, venueUid))
        return cursor.fetchall()[0]

    def getAramarkPaymentData(self, venueUid, orderPayMethodUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT payment_name, payment_type_id, type_code
                        FROM integrations.payment_types_aramark
                        WHERE venue_uid = %s AND order_pay_methods_uid = %s
                       ''', (venueUid, orderPayMethodUid))

        return cursor.fetchall()[0]

    def getVoidedOrderItems(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            name
                        FROM orders.order_items
                        JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid
                        JOIN orders.order_items_x_modifications ON order_items_x_modifications.order_item_uid = order_items.id
                        JOIN orders.order_modifications ON order_modifications.id = order_items_x_modifications.order_modification_uid
                        WHERE sub_orders.order_uid = %s
                        AND order_modifications.status = 'approved'
                        AND order_modifications.action_type = 'void'
                        ''', (orderUid))
        return cursor.fetchall()

    def markOrderTransferSuccessful(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        INSERT INTO integrations.aramark_transfered_orders(
                            order_uid,
                            transfered_at,
                            transfer_successful,
                            created_at
                        ) VALUES (
                            %s, NOW(), 1, NOW()) 
                        ON DUPLICATE KEY UPDATE transfer_successful = 1''',
                        (orderUid))
        self.db.commit() 

    def markOrderTransferFailure(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        INSERT INTO integrations.aramark_transfered_orders(
                            order_uid,
                            transfered_at,
                            transfer_successful,
                            created_at
                        ) VALUES (
                            %s, NOW(), 0, NOW()) 
                        ON DUPLICATE KEY UPDATE transfer_successful = 0''',
                        (orderUid))
        self.db.commit()

    def markEventTransfered(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        UPDATE setup.event_controls
                        SET is_transfered = 1
                        WHERE event_uid = %s''',
                        (eventUid))
        self.db.commit()

    def clearOrderTransferTasks(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        DELETE FROM integrations.orders_aramark_tasks
                        WHERE orders_aramark_uid IN (
                            SELECT id 
                            FROM integrations.orders_aramark 
                            WHERE order_uid = %s)
                       ''', (orderUid))
        self.db.commit()
#####################
#  SPECIAL PRICING  #
#####################

    def getSpecialPricingCustomers(self):
        cursor = self.db.cursor()
        cursor.execute('''
                       SELECT 
                            special_pricing_patrons.venue_uid, 
                            special_pricing_patrons.superpatron_uid, 
                            customer_id FROM menus.special_pricing_patrons
                        JOIN patrons.superpatrons ON superpatrons.id = special_pricing_patrons.superpatron_uid
                        JOIN integrations.superpatrons_aramark ON superpatrons.id = superpatrons_aramark.superpatron_uid''')
        return cursor.fetchall()
    
    def getAramarkFacilityId(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT facility_id
                        FROM integrations.venues_aramark
                        WHERE venue_uid = %s''',
                        (venueUid))
        return cursor.fetchall()[0][0]

    def getMenuXMenuItemData(self, menuId, menuItemId):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT menu_x_menu_items.menu_uid, menu_x_menu_items.id, price, title, is_tax_exemptable FROM menus.menu_x_menu_items
                        JOIN integrations.menu_items_aramark ON menu_items_aramark.menu_item_uid = menu_x_menu_items.menu_item_uid
                        JOIN integrations.menus_aramark ON menus_aramark.menu_uid = menu_x_menu_items.menu_uid
                        WHERE menu_id = %s AND menu_item_id = %s''', (menuId, menuItemId))

        return cursor.fetchall()[0]

    def insertSpecialPricingRow(self, venueUid, patronUid, mxmUid, specialPrice, isTaxExempt):
        cursor = self.db.cursor()
        cursor.execute('''
                        INSERT INTO menus.patron_menu_overrides (
                            venue_uid, 
                            patron_uid, 
                            menu_x_menu_item_uid, 
                            price, 
                            is_tax_exemptable,
                            created_at) 
                        VALUES (%s, %s, %s, %s, %s, NOW()) ON DUPLICATE KEY UPDATE  price = %s, is_tax_exemptable = %s''',
                        (venueUid, patronUid, mxmUid, specialPrice, isTaxExempt, specialPrice, isTaxExempt))
        self.db.commit()

    def getPatronsFromSuperPatron(self, superpatronUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT id 
                        FROM patrons.patrons
                        WHERE superpatron_uid = %s''',
                        (superpatronUid))
        return cursor.fetchall() 

    def getVenueTimeZone(self, venue_uid):

        cursor = self.db.cursor()
        cursor.execute("SELECT local_timezone_long \
                        FROM setup.venues \
                        WHERE id = %s",
                        (venue_uid))
        timezoneString = cursor.fetchone()[0]
        timezone = pytz.timezone(timezoneString)
        return timezone

    def getPackageCategory(self, menuUid, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''        
                        SELECT menu_categories.id
                        FROM menus.menu_x_menu_categories
                        JOIN menus.menu_categories ON menu_categories.id = menu_x_menu_categories.menu_category_uid
                        JOIN menus.menu_supercategories ON menu_categories.menu_supercategory_uid = menu_supercategories.id
                        WHERE menu_supercategories.name = 'Packages'
                        AND menu_categories.venue_uid = %s
                       ''', (venueUid))

        menuCategories = cursor.fetchall()

        if menuCategories is None or len(menuCategories) == 0:
            #no category exists yet, create one
            cursor.execute('''
                            INSERT INTO menus.menu_categories(
                                venue_uid,
                                menu_supercategory_uid,
                                name)(
                            SELECT 
                                %s, 
                                id, 
                                "Packages" 
                            FROM menus.menu_supercategories 
                            WHERE venue_uid = %s AND name = "Packages")
                            ''', (venueUid, venueUid))

            menuCategoryUid = cursor.lastrowid

            print "Created new menuCategory: " + str(menuCategoryUid)

            cursor.execute('''
                            INSERT INTO menus.menu_x_menu_categories(
                                menu_uid,
                                menu_category_uid,
                                ordinal,
                                created_at
                            )VALUES(
                                %s, %s, 1000, NOW())''', (menuUid, menuCategoryUid))
                                

            self.db.commit()



            return menuCategoryUid
        else:
            return menuCategories[0][0]
   

    #HACKY HACK HACK HACK
    def getPackageDetailsData(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT event_id FROM integrations.events_aramark
                        JOIN setup.events ON events.id = events_aramark.event_uid
                        WHERE venue_uid = %s;
                        ''', (venueUid))
        eventId = cursor.fetchall()[0][0]

        cursor.execute('''
                        SELECT customer_id FROM integrations.superpatrons_aramark WHERE venue_uid = %s; 
                       ''', (venueUid))
        customerId = cursor.fetchall()[0][0]

        return eventId, customerId

    def insertMenuPackageXItem(self, packageItemUid, menuItemUid, qty, qtyPer):
        cursor = self.db.cursor()
        print str(packageItemUid)
        print str(menuItemUid)
        print str(qty)
        print str(qtyPer)
        cursor.execute('''  
                        INSERT IGNORE INTO menus.menu_packages_x_items(
                            package_uid, 
                            menu_item_uid,
                            qty,
                            qty_per,
                            created_at
                        )VALUES(
                            %s, %s, %s, %s, NOW())
                        ''', (packageItemUid, menuItemUid, qty, qtyPer))

        self.db.commit()

    def isMenuItemIntegrated(self, menuItemId):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT * FROM integrations.menu_items_aramark
                        WHERE menu_item_id = %s''', (menuItemId))
        items = cursor.fetchall()

        if len(items) == 0:
            return False
        else:
            return True
