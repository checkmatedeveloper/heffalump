#TrainingEventDB

from config import CheckMateConfig
import os.path
import json
import uuid

class TrainingEventDB:

    BASE_PATH = "/home/ec2-user/rabbitmq_workers/repo/"

    def __init__(self, db):
        self.db = db

    '''
    Reads in and executes an sql query from a file.  
    WARNING: This function will NOT commit any INSERTS, UPDATES, or DELETES.
             You must call self.db.commit() from your calling function.  This
             way you may use this function to chain together multiple file based
             queries and commit only if all of them run successfully.
    '''
    def executeSQLFromFile(self, filePath):
        cursor = self.db.cursor()
        if os.path.isfile(filePath):
            with open(filePath, "r") as sqlFile:
                query = sqlFile.read().replace('\n', ' ')
                
                try:
                    cursor.execute(query)
                except MySQLdb.Error, e:
                    raise Exception("A MySQL Error has occured: " + str(e))
        else:
            raise Exception('filePath is not a valid file')

    '''
    Reads in and executes multiple sql queries from a file
    WARNING: nothing will be commited, you will need to call
    self.db.commit() sometime after calling this function
    '''
    def executeMultipleSQLFromFile(self, filePath):
        cursor = self.db.cursor()
        if os.path.isfile(filePath):
            with open(filePath, "r") as sqlFile:
                queriesString = sqlFile.read().replace('\n', ' ')
                queries = queriesString.split(';')
                queries.pop()  # we use the pop because the last query SHOULD end in a semi colon which will give us and empty query
                for query in queries:
                    cursor.execute(query)
        else:
            raise Exception("filePath is not a valid file")

    '''
    Returns the path to the sql file
    '''
    #def getSQLFilePath(self, venueUid, tableName):
    #    return "TrainingEvents/venue_%s-%s.sql" % (venueUid, tableName)

    def getSuiteAssignmentsFilePath(self, venueUid, eventUid):
        return self.BASE_PATH + "TrainingEvents/event_%s-suite_assignments.json" % (eventUid)

    #def getPatronCartInfoFilePath(self, venueUid):
    #    return "TrainingEvents/venue_%s-patron_cart_info.json" % (venueUid)

    def getPreorderFilePath(self, eventUid):
        return self.BASE_PATH + "TrainingEvents/event_%s-preorders.json" % (eventUid)

    def getUnitsXPatronsFilePath(self, venueUid):
        return self.BASE_PATH + "TrainingEvents/venue_%s-unit_x_patrons.json" % (venueUid)

    def getBaseShinfoFilePath(self, venueUid): 
        return self.BASE_PATH + "TrainingEvents/venue_%s-unit_patron_info.json" % (venueUid)

    def getEmployeeAssignmentsPath(self, eventUid):
        return self.BASE_PATH + "TrainingEvents/event_%s-employee_assignments.json" % (eventUid)

    def getPointsFilePath(self, venueUid):
        return self.BASE_PATH + "TrainingEvents/venue_%s-points.json" % (venueUid)
    '''
    Returns the training events
    '''
    def getTrainingEvents(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            events.id, \
                            events.venue_uid \
                        FROM setup.events \
                        JOIN setup.event_types ON events.event_type_uid = event_types.id \
                        WHERE event_types.name = 'training' \
                        AND events.id NOT IN (2299, 2306, 2309, 2726, 2727, 2728, 3432)")
        return cursor.fetchall()
  
    def moveEventToToday(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("UPDATE \
                            setup.events \
                        SET event_date = concat(DATE(NOW()), ' ', TIME(event_date)) \
                        WHERE events.id = %s", (eventUid))
        self.db.commit()
 
    '''
    Clears all the training events orders, relies on CASCADES to clear out all
    of the supporting orders. tables
    '''
    def clearEventOrders(self, eventUid):
        
        cursor = self.db.cursor()
        
        cursor.execute("DELETE \
                        FROM orders.order_payment_log WHERE id in \
                        (SELECT id FROM (SELECT order_payment_log.id \
                         FROM orders.order_payment_log \
                         JOIN orders.order_payment_identifier ON order_payment_log.order_payment_identifier_uid = order_payment_identifier.id \
                         WHERE event_uid = %s) as i)", (eventUid))

        cursor.execute("DELETE \
                        FROM orders.orders \
                        WHERE event_uid = %s", (eventUid))
        self.db.commit()

    def clearEventMessages(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("DELETE \
                        FROM messages.messages \
                        WHERE event_uid = %s", (eventUid))
        self.db.commit()                 

    def clearEventActivities(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("DELETE \
                        FROM activities.site_activities \
                        WHERE site_activities.id IN (SELECT site_activity_uid \
                                                     FROM activities.object_activities \
                                                     WHERE activity_object_uid = 3 \
                                                     AND object_pointer_uid = %s)",
                        (eventUid))
        self.db.commit()
        

    def insertUnitXPatrons(self, venueUid):
        cursor = self.db.cursor()
        unitXPatronsFilePath = self.getUnitsXPatronsFilePath(venueUid)

        with open(unitXPatronsFilePath, 'r') as unitXPatronsFile:
            unitXPatronsJSONArray = json.load(unitXPatronsFile)
            for unitXPatronJSON in unitXPatronsJSONArray:
                patronUid = unitXPatronJSON['patron_uid']
                unitNames = unitXPatronJSON['unit_names'];
                for unitName in unitNames:
                    print "Unit Name: " + unitName
                    try:
                        cursor.execute("INSERT IGNORE INTO info.unit_x_patrons( \
                                        unit_uid, \
                                        patron_uid, \
                                        created_at \
                                    )VALUES( \
                                        (SELECT id FROM setup.units WHERE is_active = 1 AND units.name = %s AND venue_uid = %s), \
                                        %s, \
                                        NOW() \
                                    )", (unitName, venueUid, patronUid))
                    except:
                        print "Unit is not active :("

        self.db.commit()
        
    def clearEventInfo(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("DELETE \
                        FROM info.event_info \
                        WHERE event_uid = %s",
                        (eventUid))
        self.db.commit()


    def getUnitPatronUids(self, patronUid, venueUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT unit_x_patrons.id \
                        FROM info.unit_x_patrons \
                        JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                        WHERE patron_uid = %s AND venue_uid = %s",
                        (patronUid, venueUid))
        return cursor.fetchall()

    def restoreBaseShinfo(self, venueUid):
        cursor = self.db.cursor()

        deleteFilePath = self.BASE_PATH + "TrainingEvents/venue_%s-DELETE_BASE_SHINFO.sql" % venueUid
        print deleteFilePath    
        self.executeMultipleSQLFromFile(deleteFilePath)  

        with open(self.getBaseShinfoFilePath(venueUid)) as baseShinfoFile:
        
        
            baseShinfoArray = json.load(baseShinfoFile)
    
            for baseShinfo in baseShinfoArray:
                patronUid = baseShinfo['patron_uid']
                
                #unit patron info
                unitPatronUids = self.getUnitPatronUids(patronUid, venueUid)
                preorderPayMethod = baseShinfo['preorder_pay_method']
                preorderPayAuth = baseShinfo['preorder_pay_auth']
                doePayMethod = baseShinfo['doe_pay_method']
                doePayAuth = baseShinfo['doe_pay_auth']
                replenishPayMethod = baseShinfo['replenish_pay_method']
                replenishPayAuth = baseShinfo['replenish_pay_auth']
                liquorCabinetOpen = baseShinfo['liquor_cabinet_open']
                liquorCabinetAuth = baseShinfo['liquor_cabinet_auth']
                refrigeratorOpen = baseShinfo['refrigerator_open']
                refrigeratorAuth = baseShinfo['refrigerator_auth']
                restockPayMethod = baseShinfo['restock_pay_method']
                restockPayAuth = baseShinfo['restock_pay_auth']
                presentBill = baseShinfo['present_bill']
                provideReceipt = baseShinfo['provide_receipt']
                canGuestInvoice = baseShinfo['can_guest_invoice']
                          

                data = []
                for unitPatronUid in unitPatronUids:
                    unitPatronUid = unitPatronUid[0] #becuase fetch all results
                    
                    #unit_patron_info
                    cursor.execute("INSERT INTO `info`.`unit_patron_info`( \
                                        `unit_patron_uid`, \
                                        `preorder_pay_method`, \
                                        `preorder_pay_auth`, \
                                        `doe_pay_method`, \
                                        `doe_pay_auth`, \
                                        `replenish_pay_method`, \
                                        `replenish_pay_auth`, \
                                        `liquor_cabinet_open`, \
                                        `liquor_cabinet_auth`, \
                                        `refrigerator_open`, \
                                        `refrigerator_auth`, \
                                        `restock_pay_method`, \
                                        `restock_pay_auth`, \
                                        `present_bill`, \
                                        `provide_receipt`, \
                                        `can_guest_invoice`, \
                                        `created_at` \
                                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                                        (unitPatronUid,
                                         preorderPayMethod,
                                         preorderPayAuth,
                                         doePayMethod,
                                         doePayAuth,
                                         replenishPayMethod, 
                                         replenishPayAuth, 
                                         liquorCabinetOpen,
                                         liquorCabinetAuth, 
                                         refrigeratorOpen, 
                                         refrigeratorAuth, 
                                         restockPayMethod, 
                                         restockPayAuth,     
                                         presentBill, 
                                         provideReceipt, 
                                         canGuestInvoice))

                    unitPatronInfoUid = cursor.lastrowid

                    #auth_signers
                    #THOMAS JEFFERSON = 45174
                    cursor.execute('''INSERT IGNORE INTO info.unit_patron_authorized_signers (unit_patron_uid, patron_uid)

                                      VALUES (%s, 45174)''', (unitPatronUid))
             
                    #unit_patron_gratuities
                    gratuitiesArray = baseShinfo['gratuities']
                    for gratuity in gratuitiesArray:
                        if gratuity['automatic_gratuity'] is not None:
                            
                            cursor.execute("INSERT INTO info.unit_patron_gratuities( \
                                                unit_patron_uid, \
                                                revenue_center_uid, \
                                                automatic_gratuity, \
                                                gratuity_percentage, \
                                                gratuity_minimum, \
                                                is_gratuity_adjustable, \
                                                gratuity_maximum, \
                                                gratuity_flat_amount, \
                                                created_at \
                                            )VALUES( \
                                                %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                                                (unitPatronUid,
                                                 gratuity['revenue_center_uid'],
                                                 gratuity['automatic_gratuity'],
                                                 gratuity['gratuity_percentage'],
                                                 gratuity['gratuity_minimum'],
                                                 gratuity['is_gratuity_adjustable'],
                                                 gratuity['gratuity_maximum'],
                                                 gratuity['gratuity_flat_amount']))

                    #discounts
                    discount = baseShinfo['discount']
                    cursor.execute("INSERT INTO info.unit_patron_discounts( \
                                            unit_patron_uid, \
                                            discount, \
                                            created_at \
                                         )VALUES( \
                                            %s, %s, NOW())", (unitPatronUid, discount))

                    #notes
                    notes = baseShinfo['notes']
                    cursor.execute("INSERT INTO info.unit_patron_notes( \
                                        unit_patron_uid, \
                                        notes, \
                                        created_at \
                                    )VALUES( \
                                        %s, %s, NOW())", (unitPatronUid, notes))                   

                    #carts
                    carts = baseShinfo['carts']
                    for cart in carts:
                        cursor.execute("INSERT INTO info.unit_patron_cart_info( \
                                            unit_patron_info_uid, \
                                            cart_type_uid, \
                                            should_stop, \
                                            pay_method, \
                                            is_pay_auth_required, \
                                          created_at \
                                        )VALUES( \
                                            %s, %s, %s, %s, %s, NOW())",
                                        (unitPatronInfoUid,
                                         cart['cart_type_uid'],
                                         cart['should_stop'],
                                         cart['pay_method'],
                                         cart['is_pay_auth_required']))
                    #par
                    parItems = baseShinfo['par_items']
                    if len(parItems) > 0:
                        cursor.execute("INSERT INTO info.unit_patron_pars( \
                                            unit_patron_uid, \
                                            notes, \
                                            created_at \
                                        )VALUES( \
                                            %s, '', NOW())", (unitPatronUid))
                        unitPatronParUid = cursor.lastrowid

                        for parItem in parItems:
                            cursor.execute("INSERT INTO info.unit_patron_par_items( \
                                                unit_patron_par_uid, \
                                                menu_item_uid, \
                                                menu_x_menu_item_uid, \
                                                qty, \
                                                notes, \
                                                created_at \
                                            )VALUES( \
                                                %s, \
                                                %s, \
                                                %s, \
                                                %s, \
                                                '', \
                                               NOW())",
                                            (unitPatronParUid,
                                             parItem['menu_item_uid'],
                                             parItem['menu_x_menu_item_uid'],
                                             parItem['qty']))

            self.db.commit()
                                          


                    

    def restoreSuiteAssignments(self, eventUid, venueUid):
        suiteAssignmentsFilePath = self.getSuiteAssignmentsFilePath(venueUid, eventUid)
        cursor = self.db.cursor()
        print "RESTORING SUITE ASSIGNMENTS"
        with open(suiteAssignmentsFilePath, 'r') as suiteAssignmentsFile:
            suiteAssignmentsJSONArray = json.load(suiteAssignmentsFile)
            for suiteAssignmentJSON in suiteAssignmentsJSONArray:
                patronUid = suiteAssignmentJSON['patron_uid']
                unitNames = suiteAssignmentJSON['unit_names']
                for unitName in unitNames:
                    

                    print str(eventUid) + " " + str(patronUid) + " " + str(unitName) + " " + str(patronUid)
                    cursor.execute("INSERT INTO `info`.`event_info`( \
                                        `unit_uid`, \
                                        `event_uid`, \
                                        `patron_uid`, \
                                        `preorder_pay_method`, \
                                        `preorder_pay_auth`, \
                                        `doe_pay_method`, \
                                        `doe_pay_auth`, \
                                        `replenish_pay_method`, \
                                        `replenish_pay_auth`, \
                                        `liquor_cabinet_open`, \
                                        `liquor_cabinet_auth`, \
                                        `refrigerator_open`, \
                                        `refrigerator_auth`, \
                                        `restock_pay_method`, \
                                        `restock_pay_auth`, \
                                        `present_bill`, \
                                        `provide_receipt`, \
                                        `can_guest_invoice`, \
                                        `permit_suitemate_alcohol`, \
                                        `is_altered_from_base`, \
                                        `created_at` \
                                    ) \
                                    (SELECT \
                                        units.id, \
                                        %s, \
                                        %s, \
                                        preorder_pay_method, \
                                        preorder_pay_auth, \
                                        doe_pay_method, \
                                        doe_pay_auth, \
                                        replenish_pay_method, \
                                        replenish_pay_auth, \
                                        liquor_cabinet_open, \
                                        liquor_cabinet_auth, \
                                        refrigerator_open, \
                                        refrigerator_auth, \
                                        restock_pay_method, \
                                        restock_pay_auth, \
                                        present_bill, \
                                        provide_receipt, \
                                        can_guest_invoice, \
                                        0, \
                                        0, \
                                        NOW() \
                                     FROM info.unit_patron_info \
                                     JOIN info.unit_x_patrons on unit_x_patrons.id = unit_patron_info.unit_patron_uid \
                                     JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                                     WHERE units.name = %s AND patron_uid = %s)", 
                                     (eventUid, patronUid, unitName, patronUid))
                    print "New Event Info Rows: " + str(cursor.rowcount)
                    eventInfoUid = cursor.lastrowid

                    cursor.execute("INSERT INTO `info`.`event_discounts`( \
                                        `event_info_uid`, \
                                        `discount`, \
                                        `created_at`) \
                                    (SELECT %s, discount, NOW() FROM info.unit_patron_discounts \
                                     JOIN info.unit_x_patrons ON unit_x_patrons.id = unit_patron_discounts.unit_patron_uid \
                                     JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                                     WHERE unit_x_patrons.patron_uid = %s AND units.name = %s)",
                                    (eventInfoUid, patronUid, unitName))
                    
                    cursor.execute("INSERT INTO `info`.`event_gratuities`( \
                                        `event_info_uid`, \
                                        `revenue_center_uid`, \
                                        `automatic_gratuity`, \
                                        `gratuity_percentage`, \
                                        `is_gratuity_adjustable`, \
                                        `gratuity_minimum`, \
                                        `gratuity_maximum`, \
                                        `gratuity_flat_amount`, \
                                        `created_at`) \
                                    (SELECT %s, revenue_center_uid, automatic_gratuity, gratuity_percentage, is_gratuity_adjustable, gratuity_minimum, gratuity_maximum, gratuity_flat_amount, NOW() FROM info.unit_patron_gratuities \
                                     JOIN info.unit_x_patrons ON unit_x_patrons.id = unit_patron_gratuities.unit_patron_uid \
                                     JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                                     WHERE unit_x_patrons.patron_uid = %s AND units.name = %s)",
                                    (eventInfoUid, patronUid, unitName))

                    cursor.execute("INSERT INTO `info`.`event_notes`( \
                                        `event_info_uid`, \
                                        `notes`, \
                                        `created_at`) \
                                    (SELECT %s, notes, NOW() FROM info.unit_patron_notes \
                                     JOIN info.unit_x_patrons ON unit_x_patrons.id = unit_patron_notes.unit_patron_uid \
                                     JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                                     WHERE unit_x_patrons.patron_uid = %s AND units.name = %s)",
                                    (eventInfoUid, patronUid, unitName))
    
                    cursor.execute("INSERT INTO `info`.`event_cart_info`( \
                                        `event_info_uid`, \
                                        `cart_type_uid`, \
                                        `should_stop`, \
                                        `pay_method`, \
                                        `is_pay_auth_required`, \
                                        `created_at`) \
                                    (SELECT  \
                                        %s, \
                                        cart_type_uid, \
                                        should_stop, \
                                        pay_method, \
                                        is_pay_auth_required, \
                                        NOW() \
                                    FROM info.unit_patron_cart_info \
                                    JOIN info.unit_patron_info on unit_patron_info.id = unit_patron_cart_info.unit_patron_info_uid \
                                    JOIN info.unit_x_patrons ON unit_patron_info.unit_patron_uid = unit_x_patrons.id \
                                    JOIN setup.units ON units.id = unit_x_patrons.unit_uid \
                                    WHERE patron_uid = %s AND units.name = %s)",
                                    (eventInfoUid, patronUid, unitName))

        self.db.commit()

    def restorePoints(self, venueUid, eventUid):
        print " --- RESTORING POINTS --- "
        cursor = self.db.cursor()
        pointsFilePath = self.getPointsFilePath(venueUid)
        if os.path.isfile(pointsFilePath):
            with open(pointsFilePath, 'r') as pointsFile:
                pointsJSONArray = json.load(pointsFile)
                for pointsJSON in pointsJSONArray:
                    patronUid = pointsJSON['patron_uid']
                    eventStartingBalance = pointsJSON['event_starting_balance']
                    balance = pointsJSON['balance']
                    
                    cursor.execute('''INSERT INTO `info`.`event_info_points`
                                        (`event_info_uid`,
                                         `event_starting_balance`,
                                         `balance`,
                                         `created_at`)
                                        (SELECT id, %s, %s, NOW() FROM info.event_info WHERE patron_uid = %s AND event_uid = %s)''',
                                    (eventStartingBalance, balance, patronUid, eventUid))
                self.db.commit()
        
        
        else:
            return #if the venue has no points file then just return, there are no points to worry about             
                
        
#        parFile = self.getSQLFilePath(venueUid, 'unit_patron_pars')
#        with open(parFile, "r") as parSQLFile:
#            queriesString = parSQLFile.read().replace('\n', ' ')
#            queries = queriesString.split(';')
#            queries.pop()
#            
#            '''
#            The unit_patrons_par file is full of alternating queries.  The first query
#            inserts into unit_patron pars, we need to save it's insert id for the second
#            query which inserts into unit_patron_par_items, using that id.  This is why
#            we have this goofy, alternating, odd/even flipping, code below.  
#            '''
#            odd = True
#            lastInsertId = 0
#            for query in queries:
#                if odd:
#                    
#                    #The we will insert into unit_patron_par, and we need to save the last insert id for the next query
#                    cursor.execute(query)
#                    lastInsertId = cursor.lastrowid
#                else:
#                    
#                    #We need to build a 'values' array that will format the %s's in the even queries with the last insert id from the odd queries       
#                    tokenCount = query.count('%s')
#                    values = []
#                    for i in range(tokenCount):
#                        values.append(lastInsertId)
#                    cursor.execute(query, values)
#                self.db.commit()       
#                odd = not odd
        
    '''
    orders.order items wants one row per ITEM ordered and the preorders.preorder_items table
    wants a row for each unique item with a qty field, this function exists to compress the 
    first format into the second format.  By doing this is prevents us from having to keep two
    lists of items in our json file
    '''
    def addItemToPreorderItems(self, preorderItems, menuXMenuItemUid, name, components, equiptment):
        for item in preorderItems:
            if item['menuXMenuItemUid'] == menuXMenuItemUid:
                item['qty'] = item['qty'] + 1
                item['components'] = components
                item['equiptment'] = equiptment
                return preorderItems
        preorderItems.append({"menuXMenuItemUid":menuXMenuItemUid, "name":name, "qty":1, "components":components, "equiptments":equiptment})
        return preorderItems

    def createEventPreorders(self, venueUid, eventUid):
        
        preordersFilePath = self.getPreorderFilePath(eventUid);
        with open(preordersFilePath, "r") as preorderFile:
            preordersJSON = json.load(preorderFile)

            cursor = self.db.cursor()

            for preorderJSON in preordersJSON:
                unitUid = preorderJSON['unit_uid']
                patronUid = preorderJSON['patron_uid']
                employeeUid = preorderJSON['employee_uid']


                cursor.execute("INSERT INTO `orders`.`orders`( \
                                    `event_uid`, \
                                    `unit_uid`, \
                                    `patron_uid`, \
                                    `employee_uid`, \
                                    `order_type_uid`, \
                                    `order_split_method_uid`, \
                                    `order_pay_method_uid`, \
                                    `started_at`, \
                                    `last_modified_at`, \
                                    `created_at`) \
                                VALUES \
                                    (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())",
                                (eventUid, unitUid, patronUid, employeeUid, preorderJSON['order_type_uid'], preorderJSON['order_split_method_uid'], preorderJSON['order_pay_method_uid']))

                orderUid = cursor.lastrowid
  
                print str(unitUid) + " " + str(eventUid) + " " + str(patronUid)

                cursor.execute("INSERT INTO `preorders`.`preorders`( \
                                    `event_info_uid`, \
                                    `created_at`) \
                                VALUES( \
                                    (SELECT id FROM info.event_info WHERE unit_uid = %s AND event_uid = %s AND patron_uid = %s), \
                                    NOW())", (unitUid, eventUid, patronUid))

                preorderUid = cursor.lastrowid

                revenueCenter = preorderJSON['revenue_centers'][0]

                cursor.execute("INSERT INTO `orders`.`orders_x_revenue_centers`( \
                                    `order_uid`, \
                                    `revenue_center_uid`, \
                                    `subtotal`, \
                                    `discount`, \
                                    `gratuity`, \
                                    `tax`, \
                                    `service_charge_amount`, \
                                    `created_at` \
                                )VALUES( \
                                    %s,%s,%s,%s,%s,%s,%s,NOW())",
                                (orderUid, 
                                 revenueCenter['revenue_center_uid'],
                                 revenueCenter['subtotal'],
                                 revenueCenter['discount'],
                                 revenueCenter['gratuity'],
                                 revenueCenter['tax'],
                                 revenueCenter['service_charge_amount']))
                                 

                subOrders = preorderJSON['sub_orders']
                preorderItems = []
                for subOrder in subOrders:
                    #there should only ever be one suborder for a preorder, but I put this in a for loop in case Becky wants to get creative

                    orderToken = uuid.uuid4()

                    cursor.execute("INSERT INTO `orders`.`sub_orders`( \
                                        `order_uid`, \
                                        `revenue_center_uid`, \
                                        `order_token`, \
                                        `employee_uid`, \
                                        `gratuity`, \
                                        `device_uid`, \
                                        `order_type_uid`, \
                                        `created_at`) \
                                    VALUES \
                                        (%s, %s, %s, %s, %s, %s, %s,NOW())",
                                    (orderUid, subOrder['revenue_center_uid'], orderToken, employeeUid, subOrder['gratuity'], subOrder['device_uid'], subOrder['order_type_uid']))

                    subOrderUid = cursor.lastrowid

                    orderItems = subOrder['order_items']
                     
                    for orderItem in orderItems:
                        menuXMenuItemUid = orderItem['menu_x_menu_item_uid']
                        lineId = orderItem['line_id']
                        name = orderItem['name']
                        price = orderItem['price']
                        taxRate = orderItem['tax_rate']
                        
                        components = orderItem['components']
                        equiptment = orderItem['equiptment']
    
                        self.addItemToPreorderItems(preorderItems, menuXMenuItemUid, name, components, equiptment)

                        print "Order Items: " + str(subOrderUid) + " " + str(menuXMenuItemUid) + " " + name
                       
                        cursor.execute("INSERT INTO `orders`.`order_items`( \
                                            `sub_order_uid`, \
                                            `menu_x_menu_item_uid`, \
                                            `line_id`, \
                                            `name`, \
                                            `price`, \
                                            `tax_rate`, \
                                            `notes`, \
                                            `created_at`) \
                                        VALUES( \
                                            %s, \
                                            %s, \
                                            %s, \
                                            '%s', \
                                            %s, \
                                            %s, \
                                            ' ', \
                                            NOW())" % (subOrderUid,
                                         menuXMenuItemUid,
                                         lineId,
                                         name, 
                                         price,
                                         taxRate))
                
                for preorderItem in preorderItems:
                    cursor.execute("INSERT INTO preorders.preorder_items( \
                                        preorder_uid, \
                                        menu_x_menu_item_uid, \
                                        name, \
                                        qty, \
                                        created_at \
                                    )VALUES( \
                                        %s, \
                                        %s, \
                                        %s, \
                                        %s, \
                                        NOW() \
                                    )", (preorderUid, preorderItem['menuXMenuItemUid'], preorderItem['name'], preorderItem['qty']))
                    preorderItemUid = cursor.lastrowid
                    cursor.execute("INSERT INTO preorders.preorder_components( \
                                        preorder_item_uid, \
                                        menu_component_uid, \
                                        qty, \
                                        created_at \
                                    )(SELECT %s, \
                                             menu_component_uid, \
                                             qty, \
                                             NOW() \
                                      FROM menus.menu_items_x_menu_components \
                                      WHERE menu_items_x_menu_components.menu_item_uid = (SELECT menu_item_uid  \
                                                                                           FROM menus.menu_x_menu_items \
                                                                                           WHERE menu_x_menu_items.id = %s))",
                                    (preorderItemUid, preorderItem['menuXMenuItemUid']))

                    cursor.execute("INSERT INTO preorders.preorder_equipments( \
                                        preorder_item_uid, \
                                        menu_equipment_uid, \
                                        qty, \
                                        created_at \
                                    )(SELECT %s, \
                                             menu_equipment_uid, \
                                             qty, \
                                             NOW() \
                                      FROM menus.menu_items_x_menu_equipment \
                                      WHERE menu_items_x_menu_equipment.menu_item_uid = (SELECT menu_item_uid \
                                                                                          FROM menus.menu_x_menu_items \
                                                                                          WHERE menu_x_menu_items.id = %s))",
                                      (preorderItemUid, preorderItem['menuXMenuItemUid']))

                payment = preorderJSON['payment']
                
                cursor.execute("INSERT INTO orders.order_payment_preauths( \
                                    order_uid, \
                                    merchant_uid, \
                                    event_uid, \
                                    unit_uid, \
                                    device_uid, \
                                    payment_id, \
                                    patron_card_uid, \
                                    amount, \
                                    sale_closed_subtotal, \
                                    sale_closed_tip, \
                                    sale_closed_tax, \
                                    unique_id, \
                                    token_merchant_uid, \
                                    invoice_uid, \
                                    authorization_code, \
                                    authorization_tolerance, \
                                    cc_type, \
                                    receipt_text, \
                                    is_complete, \
                                    created_at) \
                                VALUES( \
                                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (orderUid, 
                                 payment['merchant_uid'], 
                                 eventUid, 
                                 unitUid, 
                                 payment['device_uid'], 
                                 payment['payment_id'], 
                                 payment['patron_card_uid'], 
                                 payment['amount'], 
                                 payment['sale_closed_subtotal'], 
                                 payment['sale_closed_tip'], 
                                 payment['sale_closed_tax'], 
                                 payment['unique_uid'], 
                                 payment['token_merchant_uid'], 
                                 payment['invoice_uid'], 
                                 payment['authorization_code'], 
                                 payment['authorization_tolerance'], 
                                 payment['cc_type'], 
                                 payment['receipt_text'], 
                                 payment['is_complete']))
 

                cursor.execute("INSERT IGNORE INTO `patrons`.`patron_wallet`( \
                                        `patron_uid`, \
                                        `unit_uid`, \
                                        `patron_card_uid`, \
                                        `is_active`, \
                                        `created_at`) \
                                    VALUES (%s, %s, %s, 1, NOW())",
                                    (patronUid, unitUid, payment['patron_card_uid']))            
               
##############################
                cursor.execute('''INSERT INTO `orders`.`orders_x_patron_cards`(
                                    `order_uid`,
                                    `patron_card_uid`,
                                    `is_preorder_card`,
                                    `created_at`
                                )VALUES(%s, %s, 1, NOW())''',
                                (orderUid, payment['patron_card_uid']))

            self.db.commit()    


    def restoreEmployeeAssignments(self, eventUid, venueUid):
        print "Restoring Employee Assignments"
        cursor = self.db.cursor()
        
        print "Deleting Old Assignments"
        cursor.execute("DELETE \
                        FROM setup.units_x_employees \
                        WHERE event_unit_uid IN (SELECT id\
                                                 FROM setup.events_x_units \
                                                 WHERE event_uid = %s)", (eventUid))

        employeeAssignmentsPath = self.getEmployeeAssignmentsPath(eventUid)     

        print "Inserting new assignments"
        with open(employeeAssignmentsPath, 'r') as employeeAssignmentsFile:
            employeeAssignments = json.load(employeeAssignmentsFile)
    
            print "Employee Assignment File opened"
            for employeeAssignment in employeeAssignments:
                employeeUid = employeeAssignment['employee_uid']
                unitNames = employeeAssignment['unit_names']

                for unitName in unitNames:
                    print "Assigning employee " + str(employeeUid) + " to unit " + str(unitName) + " at " + str(venueUid)
                    cursor.execute("INSERT INTO setup.units_x_employees( \
                                        event_unit_uid, \
                                        venue_employee_uid, \
                                        created_at \
                                    )VALUES( \
                                      (SELECT events_x_units.id FROM setup.events_x_units \
                                         JOIN setup.units ON events_x_units.unit_uid = units.id \
                                         WHERE units.name = %s AND events_x_units.event_uid = %s AND units.venue_uid = %s), \
                                        (SELECT id FROM setup.venues_x_employees \
                                         WHERE venues_x_employees.venue_uid = %s AND venues_x_employees.employee_uid = %s), \
                                         NOW())", (unitName, eventUid, venueUid, venueUid, employeeUid))


                                


            self.db.commit()
