class OrderTransfer_Db:

    CREDIT_CARD_PAYMENT_METHOD = 3
    CREDIT_CARD_ON_FILE = 2
    CREDIT_CARD_PREORDER = 6

    DIRECT_BILL_PAYMENT_METHOD = [1, 2] 
    CASH_PAYMENT_METHOD = 4


    LEVY_DIRECT_BILL = 9
    LEVY_CASH = 1


    def __init__(self, db):
        self.logger = None
        self.db = db
        

    def setLogger(self, logger):
        self.logger = logger

    def getClosedEvents(self, venueUid):
        #self.logger.log("DB: Selecting closed events")
        cursor = self.db.cursor()
        cursor.execute("SELECT events.id AS event_uid, venue_uid \
                        FROM setup.events \
                        JOIN setup.event_controls ON event_controls.event_uid = events.id \
                        JOIN setup.event_types ON events.event_type_uid = event_types.id \
                        WHERE is_locked = 1 AND is_transfered = 0 AND venue_uid = %s \
                        AND events.id NOT IN (SELECT event_uid FROM integrations.levy_transfered_events)\
                        AND event_types.name != 'training'",
			(venueUid))
        results = cursor.fetchall()
        #self.logger.log("DB: Select found " + str(len(results)) + " row(s)")
        return results

    def markEventTransfered(self, eventUid):
        self.logger.log("DB: Updating event as transfered")
        cursor = self.db.cursor()
        updatedRows = cursor.execute("UPDATE setup.event_controls \
                                      SET is_transfered=1, transfer_completed_at=NOW() \
                                      WHERE event_uid = %s", (eventUid))
        self.db.commit()
        self.logger.log("DB: updated " + str(updatedRows) + " row(s)")

    def getLevyVenueInfo(self, venueUid):
        self.logger.log("DB: Selecting levy venue info")
        cursor = self.db.cursor()
        cursor.execute("SELECT levy_short_code, levy_profit_center_id \
                        FROM integrations.venues_levy \
                        WHERE venue_uid = %s AND is_active = 1", (venueUid))
        result = cursor.fetchone()
        return result 

    def getLevyUnit(self, unitUid):
        self.logger.log("Getting Levy Unit")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("SELECT suite_id, name \
                        FROM integrations.units_levy \
                        JOIN setup.units on units.id = units_levy.unit_uid \
                        WHERE unit_uid = %s", (unitUid))
        suiteId = cursor.fetchone()
        return suiteId

    def getLevyEmployeeId(self, venueUid, employeeUid):
        self.logger.log("DB: Selecting from integrations.venues_x_employees_uid")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("SELECT employee_id \
                        FROM setup.employees \
                        JOIN setup.venues_x_employees ON venues_x_employees.employee_uid = employees.id \
                        JOIN integrations.venues_x_employees_levy ON venues_x_employees_levy.venue_employee_uid = venues_x_employees.id \
                        WHERE venues_x_employees.venue_uid = %s AND  employees.id = %s", (venueUid, employeeUid))
        employeeId = cursor.fetchone()
        if employeeId is None:
            self.logger.log("No employee rows found!  Using Venue Default")
            defaultCursor = self.db.cursor()
            cursor.execute("SELECT employee_id \
                            FROM integrations.default_employee_levy \
                            WHERE venue_uid = %s",
                            (venueUid))
            defaultEmployeeId = cursor.fetchone()
            if defaultEmployeeId is None:
                self.logger.log("DEFAULT EMPLOYEE IS NULL, THIS IS BAD -- CRASH IMMINENT")
            return defaultEmployeeId[0]
        return employeeId[0]

    def getOrders(self, eventUid):
        self.logger.log("DB: Selecting orders")
        cursor = self.db.cursor()
        cursor.execute("SELECT orders.id, orders.order_type_uid, unit_uid, employee_uid, orders.is_tax_exempt, patron_uid \
                        FROM orders.orders \
                        LEFT JOIN orders.orders_x_modifications ON orders.id = orders_x_modifications.order_uid \
                        LEFT JOIN orders.order_modifications ON orders_x_modifications.order_modification_uid = order_modifications.id \
                        WHERE event_uid = %s \
                            AND is_open = 0 \
                            AND orders.id NOT IN (SELECT order_uid FROM integrations.levy_transfered_orders) \
                            AND (action_type is NULL) \
                            AND order_type_uid != 13 \
                        GROUP BY orders.id", (eventUid))
        results = cursor.fetchall()
        self.logger.log("DB: Select found " + str(len(results)) + " rows")
        return results

    def getOpenOrders(self, eventUid):
        self.logger.log("DB: Selecting open orders (There should be NONE!!!)")
        cursor = self.db.cursor()
        cursor.execute("SELECT orders.id \
                        FROM orders.orders \
                        LEFT JOIN orders.orders_x_modifications ON orders.id = orders_x_modifications.order_uid \
                        LEFT JOIN orders.order_modifications ON orders_x_modifications.order_modification_uid = order_modifications.id \
                        WHERE event_uid = %s \
                            AND is_open = 1 \
                            AND orders.id NOT IN (SELECT order_uid FROM integrations.levy_transfered_orders) \
                            AND (action_type is NULL) \
                        GROUP BY orders.id", (eventUid))
        return cursor.fetchall()

    def getOrderSubtotalAndTaxes(self, order_uid):
        self.logger.log("Getting Order Subtotal and taxes")
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            subtotal, \
                            tax, \
                            discount \
                        FROM orders.order_payments \
                        JOIN orders.order_payments_x_revenue_centers ON order_payments_x_revenue_centers.order_payment_uid = order_payments.id \
                        WHERE order_uid = %s", (order_uid))  
        return cursor.fetchall()

    def getOrderServiceChargeSum(self, orderUid):
        self.logger.log("Getting Order Service Charge Sum")
        cursor = self.db.cursor()
        cursor.execute("SELECT SUM(service_charge_amount) FROM orders.order_payments \
                        JOIN orders.order_payments_x_revenue_centers ON order_payments.id = order_payments_x_revenue_centers.order_payment_uid \
                        WHERE order_uid = %s \
                        GROUP BY order_uid", (orderUid))
        
        results = cursor.fetchone()
        self.logger.log("DB: Total service charge = " + str(results[0]))
        return results[0]

    def getVoidedPreorders(self, event_uid):
        self.logger.log("SELECTING voided orders")
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            orders.id, \
                            orders.order_type_uid, \
                            orders.unit_uid, \
                            orders.employee_uid, \
                            orders_levy.reference_number \
                        FROM orders.orders \
                        JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id \
                        JOIN orders.order_modifications ON orders_x_modifications.order_modification_uid = order_modifications.id \
                        JOIN integrations.orders_levy ON orders.id = orders_levy.order_uid \
                        WHERE order_modifications.action_type = 'void' AND order_modifications.status = 'approved' \
                        AND order_type_uid = 8 \
                        AND orders.event_uid = %s",
                        (event_uid))
        return cursor.fetchall()

    def markOrderTransfered(self, orderUid):
        self.logger.log("Marking order transfered")
        cursor = self.db.cursor()
        cursor.execute("INSERT IGNORE INTO  integrations.levy_transfered_orders \
                        (order_uid, transfered_at, created_at) VALUES \
                        (%s, NOW(), NOW())",
                        (orderUid))
        self.db.commit()

    def markPreorderTransferFailed(self, orderUid):
        self.logger.log("Marking the preorder failed")
        cursor = self.db.cursor()
        cursor.execute("INSERT IGNORE INTO integrations.levy_transfered_orders \
                        (order_uid, transfer_successful, transfered_at, created_at) VALUES \
                        (%s, 0, NOW(), NOW())",
                        (orderUid))
        self.db.commit()

    def getLevyItemNumber(self, menu_item_uid):
        self.logger.log("DB: Getting Levy Item Number mapping for menu_item_uid = " + str(menu_item_uid))
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("SELECT levy_item_number \
                        FROM integrations.menu_items_levy \
                        WHERE menu_item_uid = %s", (menu_item_uid))
        results = cursor.fetchall()
        self.logger.log("DB: Select found " + str(len(results)) + " rows (should be 1)")
        if results is None or len(results) == 0:
		self.logger.log("***ERROR*** MISSING ITEM MAPPING: " + str(menu_item_uid))
	return results[0]

    def getCheckType(self, venue_uid, preorder=False):
        self.logger.log("Getting Check Type for this order")
        check_type = 'doe'
        if preorder:
            check_type = 'preorder'

        cursor = self.db.cursor()
        cursor.execute("SELECT levy_check_type_id \
                        FROM integrations.check_types_levy \
                        WHERE venue_uid = %s AND check_type = %s",
                        (venue_uid, check_type))
        return cursor.fetchone()[0]

    def getOrderItems(self, orderUid):
        self.logger.log("DB: Selecting items for order " + str(orderUid))
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            order_items.id, \
                            sub_orders.revenue_center_uid, \
                            menu_x_menu_item_uid, \
                            menu_item_uid, \
                            (CASE WHEN order_modifications.action_type = 'void' and order_modifications.status = 'approved' \
                                THEN \
                                    0 \
                                ELSE \
                                    order_items.price \
                                END \
                            ) as 'price', \
                            COUNT(*) AS quantity \
                        FROM orders.order_items \
                        JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid \
                        LEFT JOIN (SELECT * FROM orders.order_items_x_modifications GROUP BY order_item_uid) AS order_items_x_modifications ON order_items_x_modifications.order_item_uid = order_items.id \
                        LEFT JOIN orders.order_modifications ON order_modifications.id = order_items_x_modifications.order_modification_uid \
                        LEFT JOIN orders.orders ON sub_orders.order_uid = orders.id \
                        LEFT JOIN menus.menu_x_menu_items ON menu_x_menu_items.id = order_items.menu_x_menu_item_uid \
                        LEFT JOIN (SELECT order_uid, amount FROM orders.orders_x_discounts WHERE order_uid = %s GROUP BY order_uid) AS orders_x_discounts ON orders_x_discounts.order_uid = orders.id \
                        WHERE orders.id = %s \
                        GROUP BY orders.id, price, menu_x_menu_item_uid;", (orderUid, orderUid))

        #TODO: there is going to be many-much extra stuff here to handle all fo the edge case
        results = cursor.fetchall()
        self.logger.log("DB: Select found " + str(len(results)) + " row(s)")
        return results

    def getOrderDiscount(self, orderUid):
        self.logger.log("SELECTing order discount")
        cursor = self.db.cursor()
        cursor.execute("SELECT amount \
                        FROM orders.orders_x_discounts \
                        WHERE order_uid = %s",
                        (orderUid))
        discount = cursor.fetchone()
        if discount is None:
            return 0
        else:
            return discount[0]

    def getOrderPayment(self, orderUid):
        self.logger.log("DB: Selecting payments (orders.order_payments_x_revenue_centers JOIN orders.order_payments) for order " + str(orderUid))
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            order_payment_uid, \
                            SUM(subtotal) AS subtotal, \
                            SUM(discount) AS discount, \
                            SUM(tip) AS tip, \
                            SUM(tax) AS tax, \
                            order_pay_method_uid \
                        FROM orders.order_payments_x_revenue_centers \
                        JOIN orders.order_payments ON order_payments_x_revenue_centers.order_payment_uid = order_payments.id \
                        WHERE order_uid = %s \
                        GROUP BY order_payment_uid", (orderUid))

        #there will probably be endpoints here to worry about
        results = cursor.fetchall()
        self.logger.log("DB: Select found " + str(len(results)) + " rows")
        return results

    def getLevyTenderType(self, orderPaymentUid, orderPayMethodUid):
        self.logger.log("DB: Getting Levy Tender Type")
       	self.logger.logParams(locals())
 
        if orderPayMethodUid in [self.CREDIT_CARD_PAYMENT_METHOD, self.CREDIT_CARD_ON_FILE, self.CREDIT_CARD_PREORDER]:
            creditCardCursor = self.db.cursor()
            creditCardCursor.execute("SELECT card_types.id FROM orders.order_payment_credit_cards \
                                      JOIN patrons.patron_cards ON order_payment_credit_cards.patron_card_uid = patron_cards.id \
                                      JOIN setup.card_types ON patron_cards.card_type = card_types.name \
                                      WHERE order_payment_uid = %s", (orderPaymentUid))
            cardTypeUid = creditCardCursor.fetchone()[0]
        
            tenderTypeCursor = self.db.cursor()
            tenderTypeCursor.execute("SELECT levy_pos_tender_id FROM integrations.tender_types_levy \
                                      WHERE tender_uid = %s AND card_type_uid = %s", (orderPayMethodUid, cardTypeUid))
            
            levyTenderType = tenderTypeCursor.fetchone()[0]
            return levyTenderType

        if orderPayMethodUid in self.DIRECT_BILL_PAYMENT_METHOD:
            return self.LEVY_DIRECT_BILL
        
        if orderPayMethodUid == self.CASH_PAYMENT_METHOD:
            return self.LEVY_CASH

        raise Exception("Unknown orderPayMethodUid: " + str(orderPayMethodUid) + " in getLevyTenderType")   
 
    def getTPGReferenceNumber(self, orderUid):
        self.logger.log("Selecting TPG reference number for order " + str(orderUid))
        cursor = self.db.cursor()
        cursor.execute("SELECT reference_number \
                        FROM integrations.orders_levy \
                        WHERE order_uid = %s", (orderUid))
        results = cursor.fetchall()
        self.logger.log("DB: Select found: " + str(results))
        return results[0]

    def recordTPGOrderNumber(self, order_id, venue_uid, order_uid):
        self.logger.log("Inserting into integrations.orders_levy")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO integrations.orders_levy ( \
                            order_id, \
                            status, \
                            venue_uid, \
                            order_uid, \
                            event_info_uid, \
                            preorder_uid, \
                            reference_number, \
		            created_at \
                        ) VALUES ( \
                            %s, \
                            'success', \
                            %s, \
                            %s, \
                            NULL, \
                            NULL, \
                            %s, \
		            NOW() ) \
			ON DUPLICATE KEY UPDATE reference_number = %s",
                            (order_uid, venue_uid, order_uid, order_id, order_id ))
        self.db.commit()
        
    def recordOrderDifference(self, order_uid, reference_number, parametric_total, ig_total, difference):
        self.logger.log("Recording the difference")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO integrations.order_differences ( \
                            order_uid, \
                            reference_number, \
                            parametric_total, \
                            ig_total, \
                            difference, \
                            created_at, \
                            updated_at \
                        ) VALUES ( \
                            %s, \
                            %s, \
                            %s, \
                            %s, \
                            %s, \
                            NOW(), \
                            CURRENT_TIMESTAMP \
                        ) \
			ON DUPLICATE KEY UPDATE reference_number = %s, parametric_total = %s, ig_total = %s, difference = %s",
                        (order_uid, reference_number, parametric_total, ig_total, difference, reference_number, parametric_total, ig_total, difference))
        self.db.commit()
                       
    def recordOrderTipTotal(self, order_number, order_uid, tip_total, paid_tips):
        self.logger.log("Recording the order tip total")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        
        cursor.execute("INSERT INTO integrations.order_gratuity_levy ( \
                            tpg_order_number, \
                            parametric_order_uid, \
                            tip_total, \
                            paid_tips, \
                            created_at \
                        ) VALUES ( \
                            %s, \
                            %s, \
                            %s, \
                            %s, \
                            NOW() \
                        ) \
                        ON DUPLICATE KEY UPDATE tip_total = %s, paid_tips = %s", 
                        (order_number, order_uid, tip_total, paid_tips, tip_total, paid_tips))
        
        
        self.db.commit()
        
    def getTips(self, event_uid): 
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            parametric_order_uid, \
                            tpg_order_number, \
                            tip_total, \
                            paid_tips \
                        FROM integrations.order_gratuity_levy \
                        JOIN orders.orders ON parametric_order_uid = orders.id \
                        WHERE orders.event_uid = %s", 
                        (event_uid))
        return cursor.description, cursor.fetchall()
       
    def getLevyExportData(self, event_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            orders.id AS 'parametric_order_uid', \
                            orders_levy.reference_number AS 'tpg_order_number', \
                            order_differences.parametric_total /100 AS 'parametric_total', \
                            order_differences.ig_total / 100 AS 'ig_total', \
                            order_differences.difference /100 AS 'difference', \
                            is_tax_exempt, \
                            (SELECT \
                                SUM(tax) \
                             FROM orders.order_payments \
                             JOIN orders.order_payments_x_revenue_centers ON order_payments.id = order_payments_x_revenue_centers.order_payment_uid \
                             WHERE order_uid = orders.id) AS 'tax_total', \
                            tip_total, \
                            paid_tips, \
                            (SELECT \
                             SUM(discount) \
                             FROM orders.order_payments \
                             JOIN orders.order_payments_x_revenue_centers ON order_payments.id = order_payments_x_revenue_centers.order_payment_uid \
                             WHERE order_uid = orders.id) AS 'total_discount', \
                            (SELECT \
                                SUM(service_charge_amount) \
                             FROM orders.order_payments \
                             JOIN orders.order_payments_x_revenue_centers ON order_payments.id = order_payments_x_revenue_centers.order_payment_uid \
                             WHERE order_uid = orders.id) AS 'service_charge' \
                        FROM orders.orders \
                        JOIN integrations.orders_levy ON orders.id = orders_levy.order_uid \
                        LEFT JOIN integrations.order_gratuity_levy ON order_gratuity_levy.parametric_order_uid = orders.id \
                        LEFT JOIN integrations.order_differences ON orders.id = order_differences.order_uid \
                        WHERE orders.event_uid = %s \
                        AND orders.order_type_uid != 13 \
                        AND tpg_order_number IS NOT NULL", (event_uid))
        return cursor.description, cursor.fetchall()
   
    def getOrderTransferEmailRecipientList(self, venue_uid):
        if self.logger is not None:
            self.logger.log("getting Transfer Email Recipient List")
        cursor = self.db.cursor()
        cursor.execute("SELECT email FROM notifications.notifications_x_venues \
                        JOIN notifications.email_notifications ON notifications_x_venues.id = email_notifications.notification_venue_uid \
                        WHERE venue_uid = %s AND notification_uid = 4",
                        (venue_uid))
        return cursor.fetchall() 

    def getFailedTransferOrders(self, event_uid):
        if self.logger is not None:
            self.logger.log("Getting Failed Transfer Orders")
        cursor = self.db.cursor()
        cursor.execute("SELECT reference_number FROM orders.orders \
                        JOIN integrations.levy_transfered_orders on orders.id = levy_transfered_orders.order_uid \
                        JOIN integrations.orders_levy on orders.id = orders_levy.order_uid \
                        WHERE event_uid = %s AND transfer_successful = 0",
                        (event_uid))
        return cursor.fetchall()

    def getTransferedOrderData(self, event_uid):
        if self.logger is not None:
            self.logger.log("Getting Transfered Event Data")
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            event_name, \
                            event_date, \
                            event_id, \
                            local_timezone_long, \
                            venues.name, \
                            first_name, \
                            last_name, \
                            (SELECT SUM(subtotal) + SUM(tip) + SUM(tax) - SUM(discount) \
                                FROM orders.orders \
                                JOIN orders.order_payments ON orders.id = order_payments.order_uid \
                                JOIN orders.order_payments_x_revenue_centers ON order_payments.id = order_payments_x_revenue_centers.order_payment_uid \
                                JOIN integrations.levy_transfered_orders ON levy_transfered_orders.order_uid = orders.id \
                                WHERE orders.event_uid = %s AND transfer_successful = 1) AS total, \
                            (SELECT count(*) \
                             FROM integrations.levy_transfered_orders \
                             JOIN orders.orders on orders.id = levy_transfered_orders.order_uid \
                             WHERE event_uid = %s and transfer_successful = 1) as order_count \
                            FROM setup.events \
                            JOIN setup.events_x_venues on events.id = events_x_venues.event_uid \
                            JOIN setup.venues ON events.venue_uid = venues.id \
                            JOIN integrations.events_levy ON events_levy.event_uid = events.id \
                            JOIN setup.event_controls ON events.id = event_controls.event_uid \
                            JOIN setup.employees ON employees.id = event_controls.locking_employee_uid \
                            WHERE events.id = %s", (event_uid, event_uid, event_uid))
        return cursor.fetchone() #there should only ever be one row

    def getLevyEventNumber(self, event_uid):
        self.logger.log("Getting Levy Event Number")
        cursor = self.db.cursor()
        cursor.execute("SELECT event_id \
                        FROM integrations.events_levy \
                        WHERE event_uid = %s", 
                        (event_uid))
        return cursor.fetchone()[0]


    def getLevyVenueEntityCode(self, venue_uid):
        self.logger.log("Getting Levy Venue Enitiy Code")
        cursor = self.db.cursor()
        cursor.execute("SELECT levy_entity_code \
                        FROM integrations.venues_levy \
                        WHERE venue_uid = %s",
                        (venue_uid))
        return cursor.fetchone()[0]

             
    def getOrderTotal(self, order_uid):
        self.logger.log("Getting order total")
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            SUM(orders_x_revenue_centers.subtotal - orders_x_revenue_centers.discount) AS target_total \
                        FROM orders.orders_x_revenue_centers \
                        WHERE orders_x_revenue_centers.order_uid=%s",
                        (order_uid))
        return cursor.fetchone()[0]                 


    def isOrderVoided(self, order_uid):
        self.logger.log("Determining if the order was voided")
        cursor = self.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders.orders \
                        JOIN orders.orders_x_modifications ON orders.id = orders_x_modifications.order_uid \
                        JOIN orders.order_modifications ON orders_x_modifications.order_modification_uid = order_modifications.id \
                        WHERE orders.id = %s \
                            AND action_type = 'void' \
                            AND status = 'approved'",
                        (order_uid))
        rowCount = cursor.fetchone()[0]
        if rowCount == 0:
            return False
        else:
            return True
        
    def getLevyPatronData(self, patron_uid, venue_uid):
        self.logger.log("SELECTING levy patron data")
        self.logger.logParams(locals())
        cursor = self.db.cursor()
        cursor.execute("SELECT patrons_levy.customer_number, customer_name \
                        FROM integrations.patrons_levy \
                        JOIN integrations.levy_temp_customers ON levy_temp_customers.customer_number = patrons_levy.customer_number \
                        WHERE patron_uid = %s AND venue_uid = %s", (patron_uid, venue_uid))
        return cursor.fetchone()
    
    def markEventTransferStarted(self, event_uid):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO integrations.levy_transfered_events ( \
                            event_uid, \
                            transfer_successful, \
                            created_at \
                        )VALUES( \
                            %s, \
                            0, \
                            NOW())", (event_uid))        
        self.db.commit()

    def markEventTransferSuccessful(self, event_uid):
        self.logger.log("Marking event transfer successful")
        cursor = self.db.cursor()

        cursor.execute("UPDATE integrations.levy_transfered_events \
                        SET transfer_successful = 1 \
                        WHERE event_uid = %s", (event_uid))
        self.db.commit()

    def TPGDisabled(self, eventUid): 
        print "Checking TPG disabled state"
        cursor = self.db.cursor()
        cursor.execute("SELECT tpg_disabled FROM integrations.integration_event_settings \
                        WHERE event_uid = %s", (eventUid))
        settings = cursor.fetchall()
        print "Settings: " + str(settings)
        if len(settings) == 0:
            return False

        if settings[0][0] == 1:
            return True
        else:
            return False

    def getVenueUids(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT venue_uid FROM integrations.venues_levy WHERE is_active = 1"
        return cursor.fetchall()
