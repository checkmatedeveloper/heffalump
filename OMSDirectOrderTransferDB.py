class OMSDirectOrderTransferDB:
    
    def __init__(self, db):
        self.db = db

    def shouldRun(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute('''SELECT events.id AS event_uid, venue_uid 
                        FROM setup.events 
                        JOIN setup.event_controls ON event_controls.event_uid = events.id 
                        JOIN setup.event_types ON events.event_type_uid = event_types.id 
                        WHERE is_locked = 1 AND is_transfered = 0 
                        AND venue_uid IN( ''' + ",".join(("%s",) * len(venueUids)) + ")" +
                       ''' AND events.id NOT IN (SELECT event_uid FROM integrations.levy_transfered_events)
                        AND event_types.name != "training"''', (venueUids))
            
        eventsToTransfer = cursor.fetchall()            
        if len(eventsToTransfer) > 0:
            print "Events to Transfer: " + str(eventsToTransfer)
            return True
        else:
            return False

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

    def getLevyVenueEntityCode(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT levy_entity_code \
                        FROM integrations.venues_levy \
                        WHERE venue_uid = %s",
                        (venue_uid))
        return cursor.fetchone()[0]

    def getLevyEventNumber(self, event_uid):
        print str(event_uid)
        cursor = self.db.cursor()
        cursor.execute("SELECT event_id \
                        FROM integrations.events_levy \
                        WHERE event_uid = %s",
                        (event_uid))
        return cursor.fetchone()[0]
    def getColumnNames(self, description):
        return [ i[0] for i in description ]

    def stringifyList(self, venueUids):
        return ','.join(venueUids)

    def getVoidReasons(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            display_name AS 'NAME', \
                            '' AS 'DESCRIPTION', \
                            'Y' AS 'MGR OVERRIDE NEEDED' \
                        FROM setup.void_reasons")
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getCheckTypes(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            display_name AS 'NAME', \
                            '' AS 'DESCRIPTION', \
                            'N' AS 'MGR OVERRIDE NEEDED' \
                        FROM orders.order_types")
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getUsers(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            employees.id AS 'ID', \
                            first_name AS 'FIRST NAME', \
                            last_name AS 'LAST NAME', \
                            '' AS 'DESCRIPTION' \
                        FROM setup.employees \
                        JOIN setup.venues_x_employees ON venues_x_employees.employee_uid = employees.id \
                        WHERE venues_x_employees.venue_uid IN (" +
                         ",".join(("%s",) * len(venueUids)) + ")", (venueUids))
        return self.getColumnNames(cursor.description), cursor.fetchall()


    def getUserLocations(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            employee_uid AS 'USER ID', \
                            levy_entity_code AS 'LOCATION' \
                        FROM setup.venues_x_employees \
                        JOIN integrations.venues_levy ON venues_levy.venue_uid = venues_x_employees.venue_uid AND venues_levy.is_active = 1\
                        WHERE venues_x_employees.venue_uid IN (" + 
                         ",".join(("%s",) * len(venueUids)) + ")", (venueUids))
        print cursor._last_executed
        return self.getColumnNames(cursor.description), cursor.fetchall()


    def getUserRoles(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            roles.id AS 'ID', \
                            venues_x_roles.display_name AS 'NAME', \
                            '' AS 'DESCRIPTION' \
                        FROM setup.roles \
                        JOIN setup.venues_x_roles ON venues_x_roles.role_uid = roles.id \
                        WHERE venues_x_roles.venue_uid IN (" + 
                        ",".join(("%s",) * len(venueUids)) + ") \
                        GROUP BY roles.id", (venueUids))
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getRoleAssociations(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            setup.employees_x_roles.id AS 'ID', \
                            setup.employees_x_roles.role_uid AS 'ROLE ID', \
                            setup.venues_x_employees.employee_uid AS 'USER ID' \
                        FROM setup.employees_x_roles \
                        JOIN setup.venues_x_employees ON venues_x_employees.id = employees_x_roles.venue_employee_uid \
                        WHERE venues_x_employees.venue_uid IN (" +
                        ",".join(("%s",) * len(venueUids)) + ")", (venueUids))
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getDiscounts(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            display_name AS 'NAME', \
                            '' AS 'DESCRIPTION', \
                            'N' AS 'MGR OVERRIDE NEEDED' \
                        FROM orders.order_discount_types")
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPaymentTypes(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            display_name AS 'NAME', \
                            '' AS 'DESCRIPTION', \
                            'N' AS 'MGR OVERRIDE NEEDED' \
                        FROM orders.order_pay_methods")
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getMenus(self, venueUids):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id AS 'ID', \
                            menu_name AS 'NAME', \
                            '' AS 'DESCRIPTION' \
                        FROM menus.menus \
                        WHERE venue_uid IN (" +
                        ",".join(("%s",) * len(venueUids)) + ")", (venueUids))
        #print cursor._last_executed
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getOrderHeaders(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT  
                            orders.id AS 'ORDER ID',
                            orders.id AS 'ORDER NUMBER',
                            '' AS 'SOURCE ORDER NUMBER',
                            CASE
                                WHEN orders.orders.order_split_method_uid > 1 THEN 'Y'
                                ELSE 'N' END
                            AS 'SPLIT ORDER FLAG',
                            CASE
                                WHEN (om.action_type = 'void' AND om.status = 'approved') THEN 'VOIDED'
                                WHEN orders.closed_at IS NULL THEN 'OPEN'
                                ELSE 'CLOSED' END
                            AS 'ORDER STATUS',
                            units_levy.suite_id AS 'SUITE NUMBER', 
                            0 AS 'GUEST COUNT',
                            orders.patron_uid,
                            patrons_levy.customer_number AS 'OMS CUSTOMER ID',
                            orders.started_at AS 'OPEN ORDER DATETIME', # convert to venue-local TZ in procedure
                            CASE
                                WHEN opened.device_uid = 'CMlevyomspreorders' THEN 'PREORDER'
                                WHEN opened.device_uid IS NULL THEN pay.device_uid
                                ELSE opened.device_uid END
                            AS 'OPEN TABLET ID',
                            orders.closed_at AS 'CLOSE ORDER DATETIME', # convert to venue-local TZ in procedure
                            pay.device_uid AS 'CLOSE TABLET ID',    
                            payments.subtotal + payments.tax + payments.tip + payments.service_charge - payments.discount AS 'TOTAL SALE',  
                            CASE WHEN orders.is_tax_exempt = 1 THEN 'Y' ELSE 'N' END AS 'TAX EXEMPT FLAG',  
                            payments.tax AS 'TOTAL TAX',    
                            payments.tip AS 'TOTAL TIPS',   
                            orders_x_discounts.order_discount_type_uid AS 'DISCOUNT ID',    
                            payments.discount AS 'TOTAL DISCOUNTS', 
                            payments.service_charge AS 'TOTAL SERVICE CHARGE',  
                            CASE    
                                WHEN opened.employee_uid IS NULL THEN orders.employee_uid
                                ELSE opened.employee_uid END
                            AS 'CREATE USER ID',
                            CASE    
                                WHEN closed.employee_uid IS NULL THEN orders.employee_uid
                                ELSE closed.employee_uid END    
                            AS 'LAST UPDATE USER ID',   
                            om.reason_uid AS 'VOID REASON ID',
                            order_types.id AS 'CHECK TYPE'
                        FROM orders.orders     
                        JOIN orders.order_types ON orders.order_type_uid = order_types.id 
                        LEFT JOIN (SELECT order_uid, device_uid, employee_uid FROM (SELECT order_uid, device_uid, employee_uid FROM orders.sub_orders ORDER BY id ASC) AS opened_inner GROUP BY order_uid) AS opened ON opened.order_uid = orders.id        
                        LEFT JOIN (SELECT order_uid, device_uid, employee_uid FROM (SELECT order_uid, device_uid, employee_uid FROM orders.sub_orders ORDER BY id DESC) AS closed_inner GROUP BY order_uid) AS closed ON closed.order_uid = orders.id       
                        LEFT JOIN (SELECT order_uid, device_uid FROM orders.order_payments WHERE payment_id = 0) AS pay ON pay.order_uid = orders.id        
                        JOIN integrations.units_levy ON units_levy.unit_uid = orders.unit_uid       
                        LEFT JOIN integrations.patrons_levy ON patrons_levy.patron_uid = orders.patron_uid  AND patrons_levy.venue_uid = units_levy.venue_uid  
                        LEFT JOIN integrations.patrons_x_units_levy ON patrons_x_units_levy.patrons_levy_uid = patrons_levy.id AND patrons_x_units_levy.unit_uid = orders.unit_uid       
                        JOIN (SELECT        
                                order_payments.order_uid,       
                                SUM(oprc.subtotal) AS 'subtotal',       
                                SUM(oprc.tax) AS 'tax',     
                                SUM(oprc.tip) AS 'tip',     
                                SUM(oprc.discount) AS 'discount',       
                                SUM(oprc.service_charge_amount) AS 'service_charge'     
                              FROM orders.order_payments      
                              JOIN orders.order_payments_x_revenue_centers AS oprc ON oprc.order_payment_uid = order_payments.id      
                              GROUP BY order_payments.order_uid       
                            ) AS payments ON payments.order_uid = orders.id     
                        LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id     
                        LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid     
                        LEFT JOIN (SELECT order_uid, order_discount_type_uid FROM orders.orders_x_discounts GROUP BY order_uid) AS orders_x_discounts ON orders_x_discounts.order_uid = orders.id       
                        WHERE orders.event_uid IN (%s)
                        AND (om.action_type IS NULL OR !(om.action_type = 'void_and_reopen' AND om.status='approved'))      
                        AND orders.id NOT IN (SELECT child_order_uid FROM orders.order_combinations)''', (eventUid))

        return self.getColumnNames(cursor.description), cursor.fetchall()


    def getOrderDetails(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT    
                            order_uid AS 'ORDER ID',    
                            order_items.line_id AS 'LINE NUMBER',   
                            menu_items_levy.levy_item_number AS 'ITEM ID',  
                            COUNT(*) AS 'ITEM QTY', 
                            sub_orders.order_type_uid AS 'CHECK TYPE ID',   
                            sub_orders.created_at AS 'ADD ITEM DATETIME', # convert to venue-local TZ in procedure  
                            CASE    
                                WHEN sub_orders.device_uid = 'CMlevyomspreorders' THEN 'PREORDER'
                                ELSE sub_orders.device_uid END
                            AS 'ADD TABLET ID',
                            order_item_original_prices.price AS 'ORIG PRICE',   
                            order_items.price AS 'ADJ PRICE',   
                            order_items.price * COUNT(*) AS 'GROSS SALE',   
                            order_items.price * COUNT(*) * order_items.tax_rate/100.0 AS 'TAX AMT', 
                            NULL AS 'DISCOUNT ID',  
                            0.0 AS 'DISCOUNT AMT',  
                            sub_orders.employee_uid AS 'CREATE USER ID',    
                            sub_orders.employee_uid AS 'LAST UPDATE USER ID',   
                            order_modifications.reason_uid AS 'VOID REASON ID', 
                            CASE    
                                WHEN order_modifications.consumed = 0 THEN 'N'
                                WHEN order_modifications.consumed = 1 THEN 'Y'
                                ELSE NULL END
                                AS 'VOID CONSUMPTION FLAG',
                            menu_x_menu_items.menu_uid AS 'MENU ID' 
                        FROM orders.order_items 
                        LEFT JOIN orders.order_item_original_prices ON order_item_original_prices.order_item_uid = order_items.id   
                        LEFT JOIN orders.order_items_x_modifications AS oim ON oim.order_item_uid = order_items.id  
                        LEFT JOIN orders.order_modifications ON order_modifications.id = oim.order_modification_uid 
                        JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid 
                        JOIN orders.orders ON orders.id = sub_orders.order_uid  
                        JOIN menus.menu_x_menu_items ON menu_x_menu_items.id = order_items.menu_x_menu_item_uid 
                        JOIN (SELECT levy_item_number, menu_item_uid FROM integrations.menu_items_levy GROUP BY menu_item_uid) AS menu_items_levy ON menu_items_levy.menu_item_uid = menu_x_menu_items.menu_item_uid    
                        WHERE orders.event_uid IN (%s)    
                        GROUP BY order_uid, sub_orders.order_type_uid, order_items.menu_x_menu_item_uid, order_items.price, order_modifications.reason_uid  
                        ORDER BY order_uid, line_id''', (eventUid))
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getOrderPayments(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            order_uid AS 'ORDER ID',
                            order_pay_method_uid AS 'PAYMENT TYPE ID',
                            payment_id AS 'PAYMENT LINE NUMBER',
                            customer_id AS 'OMS CUSTOMER ID',
                            card_type AS 'CARD TYPE',
                            card_name AS 'CUSTOMER NAME',
                            card_four AS 'CC MASK',
                            tax_exempt_flag AS 'TAX EXEMPT FLAG',
                            tax AS 'TAX AMT',
                            tip AS 'TIP AMT',
                            discount AS 'DISCOUNT AMT',
                            service_charge AS 'SERVICE CHARGE AMT',
                            payment AS 'PAYMENT AMT',
                            employee_uid AS 'CREATE USER ID'
                            FROM
                            (SELECT
                            order_payments.order_uid,
                            order_payments.order_pay_method_uid,
                            order_payments.payment_id,
                            CASE
                            WHEN order_payments.order_pay_method_uid = 3 THEN NULL
                            ELSE patrons.customer_number END
                            AS customer_id,
                            clone_patron_cards.card_type,
                            clone_patron_cards.card_name,
                            clone_patron_cards.card_four,
                            CASE
                            WHEN orders.is_tax_exempt = 1 THEN 'Y'
                            ELSE 'N' END
                            AS tax_exempt_flag,
                            ROUND(tax * (payment - tip - points)/(payment-tip),2) AS tax,
                            tip,
                            #ROUND(discount * (payment - tip - points)/(payment-tip),2) AS discount,
                            discount,
                            ROUND(service_charge * (payment - tip - points)/(payment-tip),2) AS service_charge,
                            payment - points AS 'payment',
                            orders.employee_uid
                            FROM orders.order_payments
                            LEFT JOIN (
                            SELECT
                            order_payment_uid,
                            SUM(oprc.subtotal) AS 'subtotal',
                            SUM(oprc.tax) AS 'tax',
                            SUM(oprc.tip) AS 'tip',
                            SUM(oprc.discount) AS 'discount',
                            SUM(oprc.service_charge_amount) AS 'service_charge',
                            SUM(oprc.points) AS 'points',
                            SUM(oprc.subtotal + oprc.tax + oprc.tip + oprc.service_charge_amount - oprc.discount) AS 'payment'
                            FROM orders.order_payments_x_revenue_centers AS oprc
                            GROUP BY order_payment_uid
                            ) AS amounts ON amounts.order_payment_uid = order_payments.id
                            JOIN orders.orders ON orders.id = order_payments.order_uid
                            LEFT JOIN (
                            SELECT patrons_levy.customer_number, patrons_levy.patron_uid, patrons_x_units_levy.unit_uid 
                            FROM integrations.patrons_levy
                            JOIN integrations.patrons_x_units_levy ON patrons_x_units_levy.patrons_levy_uid = patrons_levy.id
                            ) AS patrons ON patrons.patron_uid = order_payments.patron_uid AND patrons.unit_uid = orders.unit_uid
                            LEFT JOIN orders.order_payment_preauths ON order_payment_preauths.order_uid = order_payments.order_uid AND order_payment_preauths.payment_id = order_payments.payment_id
                            LEFT JOIN patrons.clone_patron_cards ON clone_patron_cards.id = order_payment_preauths.patron_card_uid
                            WHERE orders.event_uid IN (%s)
                            AND orders.order_split_method_uid = 1

                            UNION
                           SELECT
                            order_payments.order_uid,
                            1 AS order_pay_method_uid,
                            order_payments.payment_id + 1 AS payment_id,
                            patrons_levy.customer_number AS customer_id,
                            NULL AS card_type,
                            NULL AS card_name,
                            NULL AS card_four,
                            CASE
                            WHEN orders.is_tax_exempt = 1 THEN 'Y'
                            ELSE 'N' END
                            AS 'TAX EXEMPT FLAG',
                            ROUND(tax * (points)/(payment-tip),2) AS tax,
                            0.0 AS tip,
                            ROUND(discount * (points)/(payment-tip),2) AS discount,
                            ROUND(service_charge * (points)/(payment-tip),2) AS service_charge,
                            points AS 'payment',
                            orders.employee_uid
                            FROM orders.order_payments
                            LEFT JOIN (
                            SELECT
                            order_payment_uid,
                            SUM(oprc.subtotal) AS 'subtotal',
                            SUM(oprc.tax) AS 'tax',
                            SUM(oprc.tip) AS 'tip',
                            SUM(oprc.discount) AS 'discount',
                            SUM(oprc.service_charge_amount) AS 'service_charge',
                            SUM(oprc.points) AS 'points',
                            SUM(oprc.subtotal + oprc.tax + oprc.tip + oprc.service_charge_amount - oprc.discount) AS 'payment'
                            FROM orders.order_payments_x_revenue_centers AS oprc
                            GROUP BY order_payment_uid
                            ) AS amounts ON amounts.order_payment_uid = order_payments.id
                            JOIN orders.orders ON orders.id = order_payments.order_uid
                            LEFT JOIN (
                            SELECT patrons_levy.customer_number, patrons_levy.patron_uid, patrons_x_units_levy.unit_uid 
                            FROM integrations.patrons_levy
                            JOIN integrations.patrons_x_units_levy ON patrons_x_units_levy.patrons_levy_uid = patrons_levy.id
                            ) AS patrons ON patrons.patron_uid = order_payments.patron_uid AND patrons.unit_uid = orders.unit_uid
                            LEFT JOIN orders.order_payment_preauths ON order_payment_preauths.order_uid = order_payments.order_uid AND order_payment_preauths.payment_id = order_payments.payment_id
                            LEFT JOIN patrons.clone_patron_cards ON clone_patron_cards.id = order_payment_preauths.patron_card_uid
JOIN setup.events ON events.id = orders.event_uid
LEFT JOIN setup.venue_points_settings ON venue_points_settings.venue_uid = events.venue_uid
LEFT JOIN integrations.patrons_levy ON patrons_levy.patron_uid = venue_points_settings.points_patron_uid
                            WHERE orders.event_uid IN (%s)
                            AND orders.order_split_method_uid = 1
                            AND amounts.points > 0

                            UNION

                            SELECT
                            order_payments.order_uid,
                            CASE
                            WHEN order_payments.order_pay_method_uid = 4 THEN 1
                            ELSE order_payments.order_pay_method_uid END
                            AS order_pay_method_uid,
                            order_payments.payment_id,
                            CASE
                            WHEN order_payments.order_pay_method_uid = 3 THEN NULL
WHEN points>0 THEN patrons_levy.customer_number
                            ELSE patrons.customer_number END
                            AS customer_id,
                            clone_patron_cards.card_type,
                            clone_patron_cards.card_name,
                            clone_patron_cards.card_four,
                            CASE
                            WHEN orders.is_tax_exempt = 1 THEN 'Y'
                            ELSE 'N' END
                            AS tax_exempt_flag,
                            tax,
tip,
                            discount,
                            service_charge,
                            payment,
                            orders.employee_uid
                            FROM orders.order_payments
                            LEFT JOIN (
                            SELECT
                            order_payment_uid,
                            SUM(oprc.subtotal) AS 'subtotal',
                            SUM(oprc.tax) AS 'tax',
                            SUM(oprc.tip) AS 'tip',
                            SUM(oprc.discount) AS 'discount',
                            SUM(oprc.service_charge_amount) AS 'service_charge',
                            SUM(oprc.points) AS 'points',
                            SUM(oprc.subtotal + oprc.tax + oprc.tip + oprc.service_charge_amount - oprc.discount) AS 'payment'
                            FROM orders.order_payments_x_revenue_centers AS oprc
                            GROUP BY order_payment_uid
                            ) AS amounts ON amounts.order_payment_uid = order_payments.id
                            JOIN orders.orders ON orders.id = order_payments.order_uid
                            LEFT JOIN (
                            SELECT patrons_levy.customer_number, patrons_levy.patron_uid, patrons_x_units_levy.unit_uid 
                            FROM integrations.patrons_levy
                            JOIN integrations.patrons_x_units_levy ON patrons_x_units_levy.patrons_levy_uid = patrons_levy.id
                            ) AS patrons ON patrons.patron_uid = order_payments.patron_uid AND patrons.unit_uid = orders.unit_uid
                            LEFT JOIN orders.order_payment_preauths ON order_payment_preauths.order_uid = order_payments.order_uid AND order_payment_preauths.payment_id = order_payments.payment_id AND order_payment_preauths.is_complete = 1
                            LEFT JOIN patrons.clone_patron_cards ON clone_patron_cards.id = order_payment_preauths.patron_card_uid
JOIN setup.events ON events.id = orders.event_uid
LEFT JOIN setup.venue_points_settings ON venue_points_settings.venue_uid = events.venue_uid
LEFT JOIN integrations.patrons_levy ON patrons_levy.patron_uid = venue_points_settings.points_patron_uid
                            WHERE orders.event_uid IN (%s)
                            AND orders.order_split_method_uid = 5

                            ) AS everything
                            ORDER BY order_uid''', (eventUid, eventUid, eventUid))
        return self.getColumnNames(cursor.description), cursor.fetchall()

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

    def markAllOrdersTransfered(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO `integrations`.`levy_transfered_orders`
                            (`order_uid`,
                             `transfered_at`,
                             `transfer_successful`,
                             `created_at`)
                          (SELECT id, NOW(), 1, NOW() FROM orders.orders WHERE event_uid = %s)''', (eventUid))
        self.db.commit()

    def markEventTransferSuccessful(self, event_uid):
        cursor = self.db.cursor()

        cursor.execute("UPDATE integrations.levy_transfered_events \
                        SET transfer_successful = 1 \
                        WHERE event_uid = %s", (event_uid))
        self.db.commit()

    def getOrderTransferEmailRecipientList(self, venue_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT email FROM notifications.notifications_x_venues \
                        JOIN notifications.email_notifications ON notifications_x_venues.id = email_notifications.notification_venue_uid \
                        WHERE venue_uid = %s AND notification_uid = 4",
                        (venue_uid))
        return cursor.fetchall()

    def getTransferedOrderData(self, event_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            event_name, \
                            event_date, \
                            event_id, \
                            local_timezone_long, \
                            venues.name, \
                            first_name, \
                            last_name, \
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
                            WHERE events.id = %s", (event_uid, event_uid))
        return cursor.fetchone() #there should only ever be one row

    def getTransferedOrderTotal(self, eventUid, timezone, venueUid):
        cursor = self.db.cursor()
        result_args = cursor.callproc('reports.sa_get_sales_summary_report', [(eventUid), timezone, venueUid, 0])
        return cursor.fetchall()[0][3]

    def getFailedTransferOrders(self, event_uid):
        cursor = self.db.cursor()
        cursor.execute("SELECT reference_number FROM orders.orders \
                        JOIN integrations.levy_transfered_orders on orders.id = levy_transfered_orders.order_uid \
                        JOIN integrations.orders_levy on orders.id = orders_levy.order_uid \
                        WHERE event_uid = %s AND transfer_successful = 0",
                        (event_uid))
        return cursor.fetchall()

    def getOpenOrders(self, eventUid):
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

    def TPGDisabled(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT tpg_disabled FROM integrations.integration_event_settings \
                        WHERE event_uid = %s", (eventUid))
        settings = cursor.fetchall()
        if len(settings) == 0:
            return True

        if settings[0][0] == 1:
            return True
        else:
            return False

    def markEventTransfered(self, eventUid):
        cursor = self.db.cursor()
        updatedRows = cursor.execute("UPDATE setup.event_controls \
                                      SET is_transfered=1, transfer_completed_at=NOW() \
                                      WHERE event_uid = %s", (eventUid))
        self.db.commit()

    def getVenueUids(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT venue_uid FROM integrations.venues_levy WHERE is_active = 1")
        return cursor.fetchall()

    def getPALACUsers(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            employees.id AS 'ID',
                            employees.first_name AS 'FIRST NAME',
                            employees.last_name AS 'LAST NAME',
                            '' AS 'DESCRIPTION'
                        FROM setup.venues_x_employees
                        JOIN setup.employees ON employees.id = venues_x_employees.employee_uid
                        WHERE venue_uid = 315;
                       ''')
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPALACRoleAssoc(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            employees_x_roles.id AS 'ID',
                            role_uid AS 'ROLE ID',
                            employees.id AS ' USER ID'
                        FROM setup.venues_x_employees
                        JOIN setup.employees ON employees.id = venues_x_employees.employee_uid
                        JOIN setup.employees_x_roles ON employees_x_roles.venue_employee_uid = venues_x_employees.id
                        WHERE venue_uid = 315;
                       ''')
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPALACCreditUsage(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            events_x_venues.event_name AS 'EVENT',
                            orders.id AS 'ORDER ID',
                            units.name AS 'SUITE',
                            CASE WHEN clone_patrons.company_name LIKE '%%FLEX%%' THEN LEFT(clone_patrons.company_name,10) ELSE LEFT(clone_patrons.company_name,5) END AS 'CHARGE ACCOUNT',
                            clone_patrons.company_name AS 'CUSTOMER',
                            SUM(oprc.subtotal + oprc.tax + oprc.service_charge_amount - oprc.discount) AS 'CHECK TOTAL',
                            SUM(oprc.points) AS 'CREDIT USED',
                            orders.order_split_methods.display_name AS 'SPLIT METHOD',
                            order_pay_methods.display_name AS 'PAY METHOD'
                        FROM orders.orders
                        JOIN setup.events_x_venues ON events_x_venues.event_uid = orders.event_uid
                        JOIN orders.order_split_methods ON order_split_methods.id = orders.order_split_method_uid
                        JOIN setup.units ON units.id = orders.unit_uid
                        JOIN patrons.clone_patrons ON clone_patrons.id = orders.patron_uid
                        JOIN orders.order_payments ON order_payments.order_uid = orders.id
                        JOIN orders.order_pay_methods ON order_pay_methods.id = order_payments.order_pay_method_uid
                        JOIN orders.order_payments_x_revenue_centers AS oprc ON oprc.order_payment_uid = order_payments.id
                        LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id
                        LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid
                        WHERE orders.event_uid = %s
                        AND orders.id IN (SELECT order_uid FROM orders.sub_orders WHERE order_type_uid != 13)
                        AND (om.action_type IS NULL OR !((om.action_type = 'void_and_reopen' OR om.action_type = 'void') AND om.status='approved'))
                        AND order_payments.id IN (SELECT order_payment_uid FROM orders.order_payments_x_revenue_centers WHERE points > 0)
                        GROUP BY orders.id
                        ORDER BY units.name
                       ''', (eventUid))
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPALACPatrons(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            patrons_levy.customer_number AS 'CUSTOMER ID',
                            CASE 
                                WHEN clone_patrons.company_name LIKE '%FLEX%' THEN LEFT(clone_patrons.company_name,10) 
                                WHEN CAST(LEFT(clone_patrons.company_name,5) AS UNSIGNED) > 0 THEN LEFT(clone_patrons.company_name,5) 
                                ELSE ''
                            END AS 'CHARGE ACCOUNT',
                            clone_patrons.company_name AS 'CUSTOMER NAME'
                        FROM patrons.clone_patrons
                        JOIN integrations.patrons_levy ON patrons_levy.patron_uid = clone_patrons.id
                        WHERE patrons_levy.venue_uid = 315;                       
                        ''')
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPALACUnits(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT
                            units_levy.suite_id AS 'OMS SUITE NUMBER',
                            units_levy.unit_uid AS 'UNIT UID',
                            units.name AS 'SUITE NAME'
                        FROM integrations.units_levy
                        JOIN setup.units ON units.id = units_levy.unit_uid
                        WHERE units_levy.venue_uid = 315;
                       ''')
        return self.getColumnNames(cursor.description), cursor.fetchall()

    def getPALACItems(self):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT 
                            levy_item_number AS 'ITEM ID',
                            menu_categories.name AS 'CATEGORY',
                            menu_items.name AS 'NAME'
                        FROM integrations.menu_items_levy
                        JOIN menus.menu_items ON menu_items.id = menu_items_levy.menu_item_uid
                        JOIN menus.menu_x_menu_items ON menu_x_menu_items.menu_item_uid = menu_items.id
                        JOIN menus.menu_categories ON menu_categories.id = menu_x_menu_items.menu_category_uid
                        WHERE menu_items_levy.venue_uid = 315
                        GROUP BY menu_x_menu_items.menu_item_uid;
                       ''')
        return self.getColumnNames(cursor.description), cursor.fetchall()

