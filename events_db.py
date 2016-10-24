#!/usr/bin/env python

# import MySQLdb
# from db_connection import DbConnection
from keymaster import KeyMaster
import datetime
from dateutil import tz
from datetime import timedelta
import timezone_converter

class EventsDb:
    '''A class for abstracting Dinexus mySQL queries'''

    def __init__(self, db):
        self.db = db
        # prepare a cursor object using cursor() method
        self.dbc = self.db.cursor()

    def getServiceChargeMessage( self, venueUid ):
        """
        Get service charge message from venue_uid
        
        Parameters
        ----------
        venueUid int

        Returns
        ---------
        out: string

        """
        self.dbc.execute("SELECT\
                            receipt_service_charge_message\
                        FROM\
                            setup.venues\
                         WHERE\
                            id = " + str(venueUid))

        row = self.dbc.fetchone()
        self.db.commit()

        return None if row[0] == None else str(row[0])


    
    def getVenueInfo( self, venueUid ):
        """
        Get venue name from venue_uid
        
        Parameters
        ----------
        venueUid int

        Returns
        ---------
        out: string

        """

        self.dbc.execute("SELECT\
                            name\
                         FROM\
                            setup.venues\
                         WHERE\
                            id = " + str(venueUid));
                         
        row = self.dbc.fetchone()
        self.db.commit()

        return None if row[0] == None else str(row[0])

    def getFailedReceipts( self, venueUid ):
        """
        Retrieve all bounced emails for the previous day, to be used in emailed bounce email report 
        
        Parameters
        __________
        venueUid int

        Returns
        ------
        out: array

        """
        
        # get yesterday        
        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        start_date = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=tz.tzutc())
        end_date = start_date + timedelta(1)

        # get the venues timezone
        self.dbc.execute("SELECT\
                            local_timezone_long\
                        FROM\
                            setup.venues\
                        WHERE\
                            id = " + str(venueUid));

        row = self.dbc.fetchone()
        self.db.commit()

        if row == None :
            return False

        timezone = row[0];    

        # get the failed receipts for the previous day
        self.dbc.execute("CALL reports.get_failed_receipts_by_date_range(%s, %s, %s, %s, @errMessage)", (start_date, end_date, timezone, venueUid ))

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        if rows == None:
            return False     

        # Format rows for return
        data = list()
        for row in rows:
            results = {}
            results['event_name'] = None if row[0] == None else str(row[0])
            results['event_start'] = None if row[1] == None else str(row[1])
            results['attempted_at'] = None if row[2] == None else str(row[2])
            results['order_uid'] = None if row[3] == None else str(row[3])
            results['unit'] = None if row[4] == None else str(row[4])
            results['patron_uid']   = None if row[5] == None else int(row[5])
            results['first_name']        = None if row[6] == None else str(row[6])
            results['last_name']       = None if row[7] == None else str(row[7])
            results['company_name'] = None if row[8] == None else str(row[8])
            results['patron_email_uid'] = None if row[9] == None else str(row[9])
            results['email'] = None if row[10] == None else str(row[10])

            #get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(results["patron_uid"]))
            e_row = self.dbc.fetchone()
  
            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = results["first_name"]
                values['last_name'] = results["last_name"]
                values['company_name'] = results["company_name"]
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)
                if decoded != None and 'first_name' in decoded:
                    results["first_name"] = decoded["first_name"]
                    results["last_name"] = decoded["last_name"]
                    results["company_name"] = decoded["company_name"]
                
                    if results["company_name"] != None and results["company_name"] != '':
                        results["patron"] = results["company_name"]
                    else:
                        results["patron"] = results["first_name"] + " " + results["last_name"]
            
 
            if results['email'] is not None and results['patron_email_uid'] is not None:
        
                # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
                self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_emails' AND pointer_uid = " + str(results['patron_email_uid']))
                e_row = self.dbc.fetchone()
                e_key = e_row[0]

                values = {}
                keys = {}
                values['email'] = results['email']
                keys['email'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)
                results['email'] = str( decoded['email'] )
            data.append(results)

        return data

    def getFailedEmailContacts( self, venueUid ):
        """
        Retrieve the email_notifications contacts for bounce email handling for a venue
 
        Parameters
        __________
        venueUid int

        Returns
        ------
        out: array

        """
        notification_uid = 11;        

        self.dbc.execute("SELECT\
                            email\
                         FROM\
                            notifications.notifications_x_venues\
                         LEFT JOIN notifications.email_notifications ON email_notifications.notification_venue_uid = notifications_x_venues.id\
                         WHERE\
                            notifications_x_venues.notification_uid = %s\
                         AND\
                            notifications_x_venues.venue_uid = %s", (notification_uid, venueUid)) 

        self.db.commit()
        rows = self.dbc.fetchall()
        
        return rows;

    def getAutoSendPrefsForOrder( self, params ):
        """
        Get the unit_patron_email_preferences based on unit patron
        
        Parameters
        ---------
        patron_uid: int
        unit_uid: int
        pay_method_uid: int
        
        Returns
        -------
        out: array
             [ {string: email}]

        """

        self.dbc.execute("SELECT\
                            id\
                         FROM\
                            orders.order_pay_methods\
                         WHERE\
                            name = '" + str(params['pay_method']) + "'")

        row = self.dbc.fetchone()
        if row == None :
            return False

        pay_method_uid = row[0];

        preorder_pay_method_uid = 6;
        cc_on_file_pay_method_uid = 2;
        direct_bill_pay_method_uid = 1;
       
        #Ensure that the patron is the orders.patron_uid
        self.dbc.execute("SELECT\
                            patron_uid\
                         FROM\
                            orders.orders\
                         WHERE\
                            orders.id = " + str(params['order_uid']));

        row = self.dbc.fetchone()
        if row is None:
            return None

        params['patron_uid'] = row[0]        
 
        #Check for prefs before doing an real work
        self.dbc.execute("SELECT\
                    patron_emails.email,\
                    patron_emails.id as patron_email_uid,\
                    order_pay_method_uid\
                FROM\
                    info.unit_x_patrons\
                 LEFT JOIN info.unit_patron_emails ON unit_patron_emails.unit_patron_uid = unit_x_patrons.id\
                 LEFT JOIN info.unit_patron_email_preferences ON unit_patron_email_preferences.unit_patron_email_uid = unit_patron_emails.id\
                 LEFT JOIN patrons.patron_emails ON patron_emails.id = unit_patron_emails.patron_email_uid\
                 WHERE\
                    unit_x_patrons.patron_uid = %s AND\
                    unit_x_patrons.unit_uid = %s AND\
                    unit_x_patrons.is_active = 1", (params['patron_uid'], params['unit_uid']));
        
        preferences = self.dbc.fetchall()
        if preferences == None or len(preferences) == 0:
            return None

        order_pay_method_uid = None;
        patron_email_uid = None;
        email = None;
        response = [];

        """
        if prefs are found, determine the order payment method
        cannot rely on the passed in order_pay_method_uid in the case of preorder cards or cc on file
        """
        if pay_method_uid == direct_bill_pay_method_uid:
            order_pay_method_uid = direct_bill_pay_method_uid
        else:
            # Look for patron_card_uid in wallet, if not found, no prefs
            # If a card is found, determine whether preorder or cc on file by looking at is_cc_on_file flag
            self.dbc.execute("SELECT\
                                id,\
                                is_cc_on_file\
                             FROM\
                                patrons.patron_wallet\
                             WHERE\
                                patron_card_uid = %s\
                             AND\
                                patron_uid = %s\
                             AND\
                                unit_uid = %s\
                             LIMIT 1",(params['patron_card_uid'], params['patron_uid'], params['unit_uid']));

            row = self.dbc.fetchone()
            if row is None:
                return None     
      
            order_pay_method_uid = cc_on_file_pay_method_uid if row[1] == 1 else preorder_pay_method_uid

        # Now we have the true order_pay_method_uid, find any matching preferences
        for pref in preferences:
            
            if pref[2] == order_pay_method_uid:
            
                #we have found the right email pref!    
                email = str( pref[0] )
                patron_email_uid = str( pref[1] )

                # Now make sure we have all the peices
                if email is not None and patron_email_uid is not None and order_pay_method_uid is not None : 
                    # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
                    self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_emails' AND pointer_uid = " + str(patron_email_uid))
                    e_row = self.dbc.fetchone()
                    e_key = e_row[0]

                    values = {}
                    keys = {}
                    values['email'] = email
                    keys['email'] = e_key

                    keymaster = KeyMaster()
                    decoded =  keymaster.decryptMulti(values,keys)

                    email_data = {};
                    email_data['patron_email_uid'] = int(pref[1])
                    email_data['email'] = str( decoded['email'] )
                    response.append( email_data )  
    
            else:
                continue;
 
        return response

    def getEmployeeRole(self, venue_uid, employee_uid):
        """
        Get role data for the specified venue_uid and employee_uid

        Parameters
        ----------
        venue_uid : int
        employee_uid : int

        Returns
        -------
        out : array
            [ { "role_uid" : int, "role" : string }, ... ]

        """

        self.dbc.execute("SELECT\
                            roles.id as role_uid,\
                            roles.name as role\
                          FROM\
                            setup.venues_x_employees\
                          LEFT JOIN\
                            setup.employees_x_roles ON venues_x_employees.id = employees_x_roles.venue_employee_uid\
                          LEFT JOIN\
                            setup.roles ON employees_x_roles.role_uid = roles.id\
                          WHERE\
                            venue_uid = %s AND\
                            employee_uid = %s", (venue_uid, employee_uid))

        rows = self.dbc.fetchall()

        if rows == None or len(rows) == 0:
            return False

        response = []

        for row in rows:
            data = {}
            data['role_uid']    = None if row[0] == None else int(row[0])
            data['role_name']   = None if row[1] == None else str(row[1])
            response.append(data)

        return response




    def getRevCenterNameAndEmployeesByOrderAndRC(self, order_uid, revenue_center_uid):
        """
        Gets RevenueCenter and Employee information for the specified order_uid and revenue_center_uid.

        Parameters
        ----------
        order_uid : int
        revenue_center_uid : int

        Returns
        -------
        out : array [{"revenue_center_uid" : int, "employee_uid" : int, "first_name" : string, "last_name" : string, "display_name" : string}, ...]

        """

        self.dbc.execute("SELECT \
                            sub_orders.revenue_center_uid,\
                            sub_orders.employee_uid,\
                            employees.first_name,\
                            employees.last_name,\
                            revenue_centers.display_name\
                         FROM\
                            orders.sub_orders\
                         JOIN\
                            setup.employees ON sub_orders.employee_uid = employees.id\
                        JOIN\
                            setup.revenue_centers ON sub_orders.revenue_center_uid = revenue_centers.id\
                        WHERE\
                            sub_orders.order_uid = %s AND\
                            sub_orders.revenue_center_uid = %s\
                        GROUP BY\
                            sub_orders.revenue_center_uid,\
                            sub_orders.employee_uid", (order_uid, revenue_center_uid))

        rows = self.dbc.fetchall()

        response = False

        if rows != None:
            response = []
            for row in rows:
                data = {}
                data['revenue_center_uid']  = int( row[0] )
                data['employee_uid']        = int( row[1] )
                data['first_name']          = str( row[2] )
                data['last_name']           = str( row[3] )
                data['display_name']        = str( row[4] )
                response.append(data)

        return response


    def hasOrderBeenReentered(self, order_uid):
        """  
        Checks if order_uid exists in orders.order_reentries table

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : Bool
        """

        self.dbc.execute("SELECT id FROM orders.order_reentries WHERE order_uid = " + str(order_uid) + " LIMIT 1")
        row = self.dbc.fetchone()

        if row != None:
            response = True
        else:
            response = False
        
        return response

    def getVenueTimezoneFromInvoice(self, invoice_uid):
        """
        Gets venue_uid from setup.venues

        Parameters
        ----------
        invoice_uid : int

        Returns
        -------
        local_timezone: str
        """

        self.dbc.execute("SELECT setup.venues.local_timezone FROM setup.venues\
            JOIN setup.events ON setup.events.venue_uid = setup.venues.id\
            JOIN orders.orders ON orders.orders.event_uid = setup.events.id\
            JOIN orders.order_payment_preauths ON orders.order_payment_preauths.order_uid = orders.orders.id\
            WHERE orders.order_payment_preauths.invoice_uid = " + str(invoice_uid))
        row = self.dbc.fetchone()

        if row != None:
            timezone = str(row[0])
        else:
            timezone = None
        return timezone

    def getOrderTypeByOrder(self, order_uid):
        """
        Gets unit_uid from the orders.orders table for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out:
        order_type : str

        """

        self.dbc.execute("SELECT order_type FROM orders.orders WHERE id = " + str(order_uid) + " LIMIT 1")
        row = self.dbc.fetchone()

        if row == None:
            data = None
        else:
            data = str( row[0] )
        return data


    def getEventandUnitByPreorder(self, preorder_uid):
        """  
        Gets Event ID and Unit ID from Preorder ID

        Parameters
        ----------
        preorder_id : int

        Returns
        -------
        out : dictionary
            { "unit_uid" : int, "event_uid" : int}

        """
        self.dbc.execute("SELECT event_uid, unit_uid FROM info.event_info\
                          JOIN preorders.preorders ON preorders.event_info_uid = event_info.id\
                          WHERE preorders.id = " + str(preorder_uid))
        row = self.dbc.fetchone()
        if row == None:
            data = False
        else:
            data = {} 
            data['event_uid'] = int( row[0] )
            data['unit_uid'] = int( row[1] )
        return data 


    def getEventSettingsById(self, event_uid):
        """
        Get event settings for the specified event_uid

        Parameters
        ----------
        event_uid : int

        Returns
        -------
        out : array

        """

        self.dbc.execute("SELECT\
                            events_x_settings.event_setting_uid,\
                            event_settings.name as setting_name,\
                            events_x_settings.value as setting_value\
                          FROM\
                            setup.events_x_settings\
                          JOIN\
                            setup.event_settings ON events_x_settings.event_setting_uid = event_settings.id\
                          WHERE\
                            events_x_settings.event_uid = " + str(event_uid))

        rows = self.dbc.fetchall()

        if rows == None or len(rows) == 0:
            return False

        results = []
        for row in rows:
            data = {}
            data['event_setting_uid']   = None if row[0] == None else int(row[0])
            data['setting_name']        = None if row[1] == None else str(row[1])
            data['setting_value']       = None if row[2] == None else str(row[2])
            results.append(data)

        return results


    def getEventGratuities(self, patron_uid, unit_uid, event_uid):
        """
        Gets shinfo gratuity settings for the passed in patron_uid, unit_uid, and event_uid

        """

        self.dbc.execute("SELECT\
                            revenue_center_uid,\
                            automatic_gratuity,\
                            gratuity_percentage,\
                            is_gratuity_adjustable,\
                            gratuity_minimum,\
                            gratuity_maximum,\
                            gratuity_flat_amount\
                          FROM\
                            info.event_gratuities\
                          JOIN\
                            info.event_info ON event_gratuities.event_info_uid = event_info.id\
                          WHERE\
                            event_info.patron_uid = %s AND\
                            event_info.unit_uid = %s AND\
                            event_info.event_uid = %s", (patron_uid, unit_uid, event_uid))
                            
        rows = self.dbc.fetchall()

        if rows == None or len(rows) == 0:
            return False

        results = []
        for row in rows:
            data = {}
            data['revenue_center_uid']      = int(row[0])
            data['automatic_gratuity']      = str(row[1])
            data['gratuity_percentage']     = float(row[2])
            data['is_gratuity_adjustable']  = int(row[3])
            data['gratuity_minimum']        = float(row[4])
            data['gratuity_maximum']        = float(row[5])
            data['gratuity_flat_amount']    = float(row[6])
            results.append(data)

        return results

    def getPayMethodByOrderID(self, order_uid):
        """
        Gets pay_method from the orders.orders table for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out: pay_method : str

        """

        self.dbc.execute("SELECT order_pay_methods.name as pay_method FROM orders.orders JOIN orders.order_pay_methods ON orders.order_pay_method_uid = order_pay_methods.id WHERE orders.id = " + str(order_uid) + " LIMIT 1")
        row = self.dbc.fetchone()
        if row == None:
            data = ''
        else:
            data = row[0]
        return data

    def updateOrderPayMethod(self, order_uid, order_pay_method_uid):
        """
        Update the order_pay_method_uid for the specified order_uid

        Parameters
        __________
        order_uid : int
        order_pay_method_uid : int

        Returns
        -------
        out : True

        """

        self.dbc.execute("UPDATE orders.orders SET order_pay_method_uid = %s WHERE id = %s", (order_pay_method_uid, order_uid))
        self.db.commit()
        return True


    def getOldOrderItemsByOrderID(self, order_uid):
        """
        Gets a dictionary for every item item: {'name':'Foo', 'qty':1} 
            from the orders.order_combinations table for the specified order_uid
            then combines them in a list

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out: list of dict
            [{'name':'Foo', 'qty':1},{'name':'Bar', 'qty':2}]

        """
        self.dbc.execute("SELECT\
                            COUNT(*) AS qty,\
                            name\
                          FROM\
                            orders.order_items\
                          JOIN\
                            orders.order_combinations ON orders.order_combinations.order_item_uid = order_items.id\
                          WHERE\
                            orders.order_combinations.child_order_uid = " + str(order_uid) + "\
                          GROUP BY\
                            menu_x_menu_item_uid")

        rows = self.dbc.fetchall()

        if rows == None:
            return False
        else:
            response = []
            for row in rows:
                data = {}
                data['qty'] = int( row[0] )
                data['name'] = str( row[1] )
                response.append(data)
            return response

    def getUnitByOrder(self, order_uid):
        """
        Gets unit_uid from the orders.orders table for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out: dictionary
            { "unit_uid" : int, "event_uid" : int, "pay_method" : string, "order_type" : string }

        """

        self.dbc.execute("SELECT\
                            unit_uid,\
                            event_uid,\
                            order_pay_methods.name as pay_method,\
                            order_types.name as order_type\
                          FROM\
                            orders.orders\
                          JOIN\
                            orders.order_types on orders.order_type_uid = order_types.id\
                          JOIN\
                            orders.order_pay_methods ON orders.order_pay_method_uid = order_pay_methods.id\
                          WHERE\
                            orders.id = " + str(order_uid) + "\
                          LIMIT 1")

        row = self.dbc.fetchone()

        if row == None:
            data = False
        else:
            data = {}
            data['unit_uid'] = int( row[0] )
            data['event_uid'] = int( row[1] )
            data['pay_method'] = str( row[2] )
            data['order_type'] = str( row[3] )

        return data

    def getSuitesRevenueCenterByVenue(self, venue_uid):
        """
        Gets the id of the row where revenue_centers.name = 'suites' for the passed in venue_uid.

        Parameters
        ----------
        venue_uid : int

        Returns
        -------
        out : int


        """

        self.dbc.execute("SELECT id from setup.revenue_centers WHERE name = 'suites' AND venue_uid = " + str(venue_uid))

        row = self.dbc.fetchone()

        if row == None:
            return False

        id =  int( row[0] )

        return id

    def getOrderRevenueCenterData(self, order_uid):
        """
        Gets all rows from orders_x_revenue_centers for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : array

        """

        self.dbc.execute("SELECT\
                            orders_x_revenue_centers.order_uid,\
                            COALESCE(orders_x_revenue_centers.subtotal, 0) as subtotal,\
                            COALESCE(orders_x_revenue_centers.discount, 0) as discount,\
                            COALESCE(orders_x_revenue_centers.gratuity, 0) as gratuity,\
                            COALESCE(orders_x_revenue_centers.tax, 0) as tax,\
                            COALESCE(orders_x_revenue_centers.service_charge_amount, 0) as service_charge_amount,\
                            orders_x_revenue_centers.revenue_center_uid,\
                            revenue_centers.name,\
                            revenue_centers.display_name\
                          FROM\
                            orders.orders_x_revenue_centers\
                          JOIN\
                            setup.revenue_centers ON orders_x_revenue_centers.revenue_center_uid = revenue_centers.id\
                          WHERE\
                            orders_x_revenue_centers.order_uid = " + str(order_uid))

        rows = self.dbc.fetchall()

        if rows == None:
            return []

        results = []

        for row in rows:
            data = {}
            data['order_uid']   = int(row[0])
            data['subtotal']    = float(row[1])
            data['discount']    = float(row[2])
            data['gratuity']    = float(row[3])
            data['tax']         = float(row[4])
            data['service_charge_amount'] = float(row[5])
            data['revenue_center_uid']    = int(row[6])
            data['name']                  = str(row[7])
            data['display_name']          = str(row[8])
            results.append(data)

        return results

    def getOrdersXRevenueCentersByOrder(self, order_uid):
        """

        """

        self.dbc.execute("SELECT\
                            orders_x_revenue_centers.order_uid,\
                            sum(COALESCE(orders_x_revenue_centers.subtotal, 0)) as subtotal,\
                            sum(COALESCE(orders_x_revenue_centers.discount, 0)) as discount,\
                            sum(COALESCE(orders_x_revenue_centers.gratuity, 0)) as gratuity,\
                            sum(COALESCE(orders_x_revenue_centers.tax, 0)) as tax,\
                            sum(COALESCE(orders_x_revenue_centers.service_charge_amount, 0)) as service_charge_amount\
                          FROM\
                            orders.orders_x_revenue_centers\
                          WHERE\
                            orders_x_revenue_centers.order_uid = " + str(order_uid))

        row = self.dbc.fetchone()

        data = {}
        if row != None:
            data['order_uid']   = int(row[0])
            data['subtotal']    = float(row[1])
            data['discount']    = float(row[2])
            data['gratuity']    = float(row[3])
            data['tax']         = float(row[4])
            data['service_charge_amount'] = float(row[5])

        return data

    def getUnitAndEmployeeByOrder(self, order_uid):
        """
        Gets unit_uid from the orders.orders table for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out: dictionary
            {   "unit_uid" : int,
                "unit_name" : string,
                "employee_uid" : int,
                "employee_first_name" : string,
                "event_uid" : int,
                "order_type" : string,
                "subtotal" : float,
                "tax" : float,
                "service_charge_amount" : float,
                "created_at" : string }

        """

        self.dbc.execute("SELECT\
                        unit_uid,\
                        units.name as unit_name,\
                        employee_uid,\
                        employees.first_name,\
                        orders.event_uid,\
                        order_types.name as order_type,\
                        COALESCE(orc.subtotal, 0) as subtotal,\
                        COALESCE(orc.tax, 0) as tax,\
                        orders.created_at,\
                        order_pay_methods.name as pay_method\
                      FROM\
                        orders.orders\
                      LEFT JOIN\
                        ( SELECT\
                            orders_x_revenue_centers.order_uid,\
                            orders.tax_is_inclusive,\
                            sum(orders_x_revenue_centers.subtotal) as subtotal,\
                            sum(orders_x_revenue_centers.discount) as discount,\
                            sum(orders_x_revenue_centers.gratuity) as gratuity,\
                            sum(orders_x_revenue_centers.tax) as tax,\
                            sum(orders_x_revenue_centers.service_charge_amount) as service_charge_amount\
                          FROM\
                            orders.orders\
                          JOIN\
                            orders.orders_x_revenue_centers on orders.id = orders_x_revenue_centers.order_uid\
                          WHERE\
                            orders_x_revenue_centers.order_uid = %s\
                          GROUP BY\
                            orders_x_revenue_centers.order_uid ) as orc ON orders.id = orc.order_uid\
                      JOIN\
                        orders.order_types on orders.order_type_uid = order_types.id\
                      JOIN\
                        setup.units ON orders.unit_uid = units.id\
                      JOIN\
                        setup.employees ON orders.employee_uid = employees.id\
                      LEFT JOIN\
                        orders.order_pay_methods ON orders.order_pay_method_uid = order_pay_methods.id\
                      WHERE\
                        orders.id = %s\
                      LIMIT 1", (order_uid, order_uid))

        row = self.dbc.fetchone()

        if row != None:
            data = {}
            data['unit_uid'] = int( row[0] )
            data['unit_name'] = str( row[1] )
            data['employee_uid'] = int( row[2] )
            data['employee_first_name'] = str( row[3] )
            data['event_uid'] = int( row[4] )
            data['order_type'] = str( row[5] )
            data['subtotal'] = float( row[6] )
            data['tax'] = float( row[7] )
            data['created_at'] = row[8]
            data['pay_method'] = None if row[9] == None else str(row[9])
        else:
            data = None

        return data

    def addMessage(self, params):
        """
        Adds a new row into messages.messages table

        Parameters
        ----------
        params : dict
            { "venue_uid" : int,
              "event_uid" : int,
              "action_uid" : int,
              "destination_employee_uid" : int,
              "unit_uid" : int,
              "order_uid" : int,
              "order_item_uid" : int,
              "sent_employee_uid" : int,
              "notes" : string,
              "uuid" : int }

        Returns
        -------
        out : True

        """

        self.dbc.execute("INSERT INTO messages.messages (venue_uid, event_uid, action_uid, destination_employee_uid, unit_uid, order_uid, order_item_uid, sent_employee_uid, sent_at, notes, message_uuid, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, NOW())", (params['venue_uid'], params['event_uid'], params['action_uid'], params['destination_employee_uid'], params['unit_uid'], params['order_uid'], params['order_item_uid'], params['sent_employee_uid'], params['notes'], params['uuid']))
        message_uid = self.dbc.lastrowid
        self.db.commit()
        return message_uid

    def updateOrderItemReentry(self, order_item_uid):
        """
        Update status of order_item in order_item_reentries to 'done'

        Parameters
        ----------
        order_item_uid : int

        Returns
        -------
        out : True

        """

        self.dbc.execute("INSERT INTO orders.order_item_reentries (order_item_uid, status, created_at) VALUES (" + str(order_item_uid) + ", 'done', NOW()) ON DUPLICATE KEY UPDATE status = 'done'")
        self.db.commit()
        return True

    def wasOrderClosedBeforeModification(self, order_uid, order_modification_uid):
        """
        Returns True if order's closed_at is less than the order_modification's created_at date.

        Parameters
        ----------
        order_uid : int
        order_modification_uid : int

        Returns
        -------
        out : True

        """

        self.dbc.execute("SELECT 1 FROM orders.orders WHERE orders.id = %s AND closed_at IS NOT NULL AND closed_at < ( SELECT created_at FROM orders.order_modifications WHERE id = %s LIMIT 1) LIMIT 1", (order_uid, order_modification_uid))
        row = self.dbc.fetchone()

        if row != None:
            response = True
        else:
            response = False

        return response

    def isOrderClosed(self, order_uid):
        """
        Check to see if the order corresponding to the passed in order_uid was a cc_present order and has been closed.

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : bool

        """

        self.dbc.execute("SELECT\
                              orders.closed_at\
                            FROM\
                              orders.order_payments\
                            JOIN\
                              orders.orders ON order_payments.order_uid = orders.id\
                            JOIN\
                              orders.order_pay_methods ON order_payments.order_pay_method_uid = order_pay_methods.id\
                            WHERE\
                              order_payments.order_uid = " + str(order_uid) + " AND\
                              (order_pay_methods.name = 'open_tab' or order_pay_methods.name = 'cc_present')\
                            LIMIT 1")
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            closed_at = row[0]
            if closed_at == None:
                return False
            else:
                return True
   
    def getPrinterByPrinterId(self, printer_uid):
        """
        Gets printer information for the specified printer_uid.
    
        Parameters
        ----------
        printer_uid : int
    
        Returns
        -------
        out : dictionary
              { 'printer_name' : string, 'printer_uid' : int, 'ip_address' : string, 'printer_type' : string, 'chars_per_line'
            
        """
            
        self.dbc.execute("SELECT printers.name, printers.id as printer_uid, INET_NTOA(printers.ip_address) as ip_address, printer_type, chars_per_line FROM setup.printers WHERE printers.id = " + str(printer_uid))

        row = self.dbc.fetchone()
            
        if row == None:
            data = False
        else:
            data = {}
            data['printer_name'] = str( row[0] )
            data['printer_uid'] = int( row[1] )
            data['ip_address'] = str( row[2] )
            data['printer_type'] = str( row[3] )
            data['chars_per_line'] = int( row[4] )

        return data

    def getReceiptPrinterByEventAndUnit(self, event_uid, unit_uid, printer_category):
        """
        Gets Printer data for Receipts for the specified event_uid and unit_uid.

        Parameters
        ----------
        event_uid : int
        unit_uid : int
        printer_category : string

        Returns
        -------
        out : dictionary
            { 'printer_name' : string, 'printer_uid' : int, 'ip_address' : string, 'printer_type' : string, 'chars_per_line' : int }

        """

        self.dbc.execute("SELECT printers.name, printers.id as printer_uid, INET_NTOA(printers.ip_address) as ip_address, printer_type, chars_per_line FROM setup.printers JOIN setup.printers_x_units ON printers_x_units.printer_uid = printers.id JOIN setup.events_x_printer_sets ON events_x_printer_sets.printer_set_uid = printers_x_units.printer_set_uid JOIN setup.unit_groups_x_units ON unit_groups_x_units.unit_group_uid = printers_x_units.unit_group_uid WHERE events_x_printer_sets.event_uid = %s AND unit_groups_x_units.unit_uid = %s AND printers_x_units.printer_category = %s", (event_uid, unit_uid, printer_category))
        row = self.dbc.fetchone()

        if row == None:
            data = False
        else:
            data = {}
            data['printer_name'] = str( row[0] )
            data['printer_uid'] = int( row[1] )
            data['ip_address'] = str( row[2] )
            data['printer_type'] = str( row[3] )
            data['chars_per_line'] = int( row[4] )

        return data

    def orderPrintJobGroupExists(self, order_uid, job_type, job_uuid):
        """
        Checks to see if order print job exists

        Parameters
        ----------
        order_uid : int
        job_type : string
        job_uuid : string

        Returns
        -------
        out : bool

        """

        self.dbc.execute("SELECT 1 FROM orders.order_print_jobs WHERE order_uid = %s AND job_type = %s AND job_uuid = %s", (order_uid, job_type, job_uuid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            return True

    def addOrderPrintJob(self, order_uid, printer_uid, printer_type, ip_address, job_type=None, job_uuid=None):
        """
        Adds a new row to order print jobs table

        Parameters
        ----------
        order_uid : int
        printer_uid : int
        printer_type : string
        ip_address : string
        job_type : string|None
        job_uuid : string|None

        Returns
        -------
        out : int or None

        """

        if job_type == None and job_uuid == None:
            self.dbc.execute("INSERT INTO orders.order_print_jobs (order_uid, status, printer_uid, printer_type, ip_address, created_at) VALUES (%s, 'pending', %s, %s, INET_ATON(%s), NOW())", (order_uid, printer_uid, printer_type, ip_address))
        else:
            self.dbc.execute("INSERT INTO orders.order_print_jobs (order_uid, status, printer_uid, printer_type, ip_address, job_type, job_uuid, created_at) VALUES (%s, 'pending', %s, %s, INET_ATON(%s), %s, %s, NOW())", (order_uid, printer_uid, printer_type, ip_address, job_type, job_uuid))

        print_job_uid = self.dbc.lastrowid
        self.db.commit()

        if print_job_uid == None:
            return False
        else:
            return int( print_job_uid )

    def updateOrderPrintJobIgnore(self, print_job_uid):
        """
        Adds a new row to order print jobs table

        Parameters
        ----------
        print_job_uid : int

        Returns
        -------
        out : Bool

        """
        self.dbc.execute("UPDATE orders.order_print_jobs\
                        SET status = 'ignored'\
                        WHERE id = " + str(print_job_uid))
        self.db.commit()
        return True

    def getOrderVenueAndTime(self, order_uid):
        """
        Gets the Venue ID and Order Time / Timezone Data

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : dict
            {
                'venue_uid':int
                'timezone':str
                'created_at': datetime
            }
            or None

        """

        self.dbc.execute("SELECT\
                              setup.venues.id,\
                              setup.venues.local_timezone_long,\
                              order_items.created_at\
                            FROM\
                              setup.venues\
                            JOIN\
                              setup.events ON setup.events.venue_uid = setup.venues.id\
                            JOIN\
                              orders.orders ON orders.orders.event_uid = setup.events.id\
                            JOIN\
                              orders.sub_orders ON orders.id = sub_orders.order_uid\
                            JOIN\
                              orders.order_items ON sub_orders.id = order_items.sub_order_uid\
                            WHERE\
                              orders.orders.id = " + str(order_uid) + "\
                            ORDER BY\
                              order_items.id DESC\
                            LIMIT 1")
        row = self.dbc.fetchone()
        if row == None:
            return None
        else:
            return { 'venue_uid':int(row[0]), 'timezone':row[1], 'created_at':row[2]}

    def getGroupedOrderItemsByOrder(self, order_uid):
        """
        Get a list of grouped order items for the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : array
            [ { "menu_x_menu_item_uid" : int, "name" : string, "action_type" : string, "total_price" : float, "qty" : int } ]

        """

        self.dbc.execute("SELECT\
                            menu_x_menu_item_uid,\
                            order_items.name,\
                            order_modifications.action_type,\
                            case\
                              when order_modifications.action_type is null then\
                                sum(order_items.price + COALESCE(options.option_price, 0))\
                              else\
                                0\
                              end AS total_price,\
                            count(menu_x_menu_item_uid) AS qty,\
                            menu_item_retail_details.sku\
                          FROM\
                            orders.sub_orders\
                          JOIN\
                            orders.order_items ON sub_orders.id = order_items.sub_order_uid\
                          LEFT JOIN\
                            (SELECT\
                                order_item_uid,\
                                sum(order_item_options.price) as option_price\
                              FROM\
                                orders.order_item_options\
                              JOIN\
                                orders.order_items ON order_item_options.order_item_uid = order_items.id\
                              JOIN\
                                orders.sub_orders ON order_items.sub_order_uid = sub_orders.id\
                              WHERE\
                                sub_orders.order_uid = %s\
                              GROUP BY\
                                order_item_uid ) as options ON order_items.id = options.order_item_uid\
                          LEFT JOIN\
                            orders.order_items_x_modifications ON order_items.id = order_items_x_modifications.order_item_uid\
                          LEFT JOIN\
                            orders.order_modifications ON order_items_x_modifications.order_modification_uid = order_modifications.id\
                          LEFT JOIN\
                            menus.menu_x_menu_items ON order_items.menu_x_menu_item_uid = menu_x_menu_items.id\
                          LEFT JOIN\
                            menus.menu_item_retail_details ON menu_x_menu_items.menu_item_uid = menu_item_retail_details.menu_item_uid\
                          WHERE\
                            sub_orders.order_uid = %s\
                          GROUP BY\
                            menu_x_menu_item_uid,\
                            order_items.name,\
                            order_modifications.action_type\
                          ORDER BY\
                            order_modifications.action_type", (order_uid, order_uid))
        rows = self.dbc.fetchall()

        response = list()
        if rows == None:
            return response
        else:
            for row in rows:
                data = {}
                data['name'] = '' if row[1] == None else str(row[1])
                data['action'] = str(row[2]) if row[2] else ''
                data['price'] = float( row[3] )
                data['qty'] = int( row[4] )
                data['sku'] = None if row[5] == None else str(row[5])
                response.append(data)

        return response

    def getEmailByOrderAndPayment(self, order_uid, payment_id):
        """
        Gets the email associated with the specified order_uid and payment_id.

        Parameters
        ----------
        order_uid : int
        payment_id : int

        Returns
        -------
        out : dicationary or False
            { "id" : int, "email" : string }

        """

        self.dbc.execute("SELECT\
                order_payment_emails.order_payment_uid,\
                            order_payment_emails.patron_email_uid,\
                            patron_emails.id as patron_email_uid,\
                            email\
                          FROM \
                            orders.order_payments \
                          JOIN \
                            orders.order_payment_emails ON order_payments.id = order_payment_emails.order_payment_uid\
                          JOIN \
                            patrons.patron_emails ON order_payment_emails.patron_email_uid = patron_emails.id\
                          WHERE\
                            order_payments.order_uid = {0} AND \
                            order_payments.payment_id = {1} \
                          ORDER BY \
                            order_payment_emails.created_at\
                          DESC LIMIT 1".format(order_uid, payment_id))

        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            data = {}

            patron_email_uid = str( row[2] )
            email = str( row[3] )

            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_emails' AND pointer_uid = " + str(patron_email_uid))
            e_row = self.dbc.fetchone()
            e_key = e_row[0]

            values = {}
            keys = {}
            values['email'] = email
            keys['email'] = e_key

            keymaster = KeyMaster()
            decoded =  keymaster.decryptMulti(values,keys)


            data['order_payment_uid'] = int( row[0] )
            data['patron_email_uid'] = int( row[1] )
            data['email'] = str( decoded['email'] )

        return data

    def getAllPaymentIdsByOrder(self, order_uid):
        """
        Get all payment_ids associated with the specified order_uid.

        Paremters
        ---------
        order_uid : int

        Returns
        -------
        out : array
            [ { payment_id : int } ]


        """

        self.dbc.execute("SELECT\
                            payment_id\
                          FROM\
                            orders.order_payments\
                          JOIN\
                            orders.order_pay_methods ON order_payments.order_pay_method_uid = order_pay_methods.id\
                          WHERE\
                            order_pay_methods.name = 'cc_present' AND\
                            order_uid = " + str(order_uid))

        rows = self.dbc.fetchall()

        if rows == None:
            results = False
        else:
            results = []
            for row in rows:
                data = {}
                data['payment_id'] = row[0]
                results.append(data)

        return results

    def copyClosedOrder(self, order_uid):
        """  
        Copies all non-payment orders data for the specified order_uid.  If the order has credit card payments associated then 
        the procedure will copy the order_payment_preauths rows resetting them to the "tokenized" state.

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : bool

        """

        self.dbc.execute("CALL orders.copy_closed_order_proc(" + str(order_uid) + ", @errMessage)")

        row = self.dbc.fetchone()
        self.dbc.nextset()

        self.dbc.execute("SELECT @errMessage")

        err_row = self.dbc.fetchone()

        if err_row != None:
            if err_row[0] != None:
                return err_row[0]

        return True

    def getReceiptHeaderData(self, order_uid, timezone ):
        """

        Parameters
        ----------
        order_uid : int
        timezone : string ex. 'America/Chicago'

        Returns
        -------
        out : dict
            "cnt" : int,
            "split_method" : string,
            "subttoal" : float,
            "discount" : float,
            "unit_type" : string,
            "unit_name" : string,
            "event_date" : date,
            "event_name" : string,
            "venue_name" : string,
            "event_uid" : int,
            "venue_uid" : int,
            "ego_uid" : int,
            "ego_name: : string,
            "venue_section" : int,
            "receipt_subheader" : str,
            "order_uid" : int,
            "receipt_service_charge_message" : str
    
        """

        self.dbc.execute("CALL orders.get_receipt_header_data_proc(%s, %s, @errMessage)", (order_uid, timezone))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        if row == None:
            return False

        self.dbc.execute("SELECT @errMessage")

        err_row = self.dbc.fetchone()

        if err_row != None:
            if err_row[0] != None:
                return err_row[0]

        results = {}
        results['split_receipt']            = 0 if row[0] == None else int( row[0] )
        results['split_method']             = 'Single' if row[1] == None else str( row[1] )
        results['subtotal']                 = 0 if row[2] == None else float( row[2] )
        results['discount']                 = 0 if row[3] == None else float( row[3] )
        results['tip']                      = 0 if row[4] == None else float( row[4] )
        results['tax']                      = 0 if row[5] == None else float( row[5] )
        results['service_charge_amount']    = 0 if row[6] == None else float( row[6] )
        results['unit_type']                = '' if row[7] == None else str( row[7] )
        results['unit_name']                = '' if row[8] == None else str( row[8] )
        results['event_date']               = '' if row[9] == None else str( row[9] )
        results['event_name']               = '' if row[10] == None else str( row[10] )
        results['venue_name']               = '' if row[11] == None else str( row[11] )
        results['event_uid']                = int( row[12] )
        results['venue_uid']                = int( row[13] )
        results['ego_uid']                  = None if row[14] == None else int( row[14] )
        results['ego_name']                 = '' if row[15] == None else str( row[15] )
        results['venue_section']            = str(row[16])
        results['receipt_subheader']        = str(row[17])
        results['order_uid']                = order_uid
        results['receipt_service_charge_message'] = '' if row[19] == None else str( row[19] )
        results['service_charge_display_name'] = 'Service Charge' if row[20] == None else str( row[20] )
        return results

    def getOrderItemsGroupedByRC(self, order_uid):
        """

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : array of dicts
            "display_name" : string - revenue_center display name
            "menu_x_menu_item_uid" : int
            "name" : string - order item name
            "price" : float
            "qty" : int


        """

        self.dbc.execute("CALL orders.get_order_items_grouped_by_revenue_center_proc(" + str(order_uid) + ")")

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        if rows == None:
            return False

        results = []

        for row in rows:
            temp = {}
            temp['revenue_center_display_name'] = '' if row[0] == None else str( row[0] )
            temp['menu_x_menu_item_uid']        = None if row[1] == None else int( row[1] )
            temp['order_item_name']             = '' if row[2] == None else str( row[2] )
            temp['price']                       = 0 if row[3] == None else float( row[3] )
            temp['qty']                         = 0 if row[4] == None else int( row[4] )
            temp['action_type']                 = None if row[5] == None else str( row[5] )
            temp['sku']                         = None if row[6] == None else str( row[6] )

            results.append(temp)

        return results

    def getReceiptOrderPaymentRevenueCenterData(self, order_uid, payment_id):
        """

        Parameters
        ----------
        order_uid : int
        payment_id : int

        Returns
        -------
        out : array of ditc
            "order_payment_uid" : int
            "subtotal" : float
            "discount" : float
            "tip" : float
            "tax" : float
            "revenue_center_uid" : int
            "name" : string - revenue_center anem
            "display_name" : string - revenue_center display name

        """

        self.dbc.execute("CALL orders.get_receipt_order_payment_revenue_centers_data_proc(%s, %s)", (order_uid, payment_id))

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        if rows == None:
            return False

        results = []

        for row in rows:
            temp = {}
            temp['order_payment_uid']       = int( row[0] )
            temp['subtotal']                = float( row[1] )
            temp['discount']                = float( row[2] )
            temp['tip']                     = float( row[3] )
            temp['tax']                     = float( row[4] )
            temp['service_charge_amount']   = float( row[5] )
            temp['revenue_center_uid']      = int( row[6] )
            temp['name']                    = str( row[7] )
            temp['display_name']            = str( row[8] ) 
            temp['payment_id']              = str(payment_id)
            results.append(temp)

        return results

    def getReceiptOrderPaymentData(self, order_uid, payment_id, timezone):
        """

        Parameters
        ----------
        order_uid : int
        payment_id : int
        timezone : string

        Returns
        -------
        out : dict
            "order_payment_uid" : int
            "subtotal" : float
            "discount" : float
            "tip" : float
            "tax" : float
            "service_charge_amount" : float,
            "grand_total" : float
            "pay_method_name" : string
            "pay_method_display_name" : string
            "receipt_method" : string
            "patron_card_uid" : int
            "card_four" : int
            "card_type" : string
            "pc_patron_uid" : int
            "pc_first_name" : string
            "pc_last_name" : string
            "pc_company_name" : string
            "pn_patron_uid" : int
            "pn_first_name" : string
            "pn_last_name" : string
            "pn_company_name" : string
            "ap_patron_uid" : int
            "ap_first_name" : string
            "ap_last_name" : string
            "ap_company_name" : string
            "created_at" : string
            "invoice_number" : string
            "authorization_code" : string
            "sale_closed_subtotal" : float
            "sale_closed_tip" : float
            "sale_closed_tax" : float
            "is_order_voided" : bool
            "paid_at" : string
            "raw_closed_at" : string
            "card_name" : string
            "payment_uuid" : string
            "updated_at" : string
            "points" : float
        """

        self.dbc.execute("CALL orders.get_receipt_order_payment_data_proc(%s, %s, %s, @errMessage)", (order_uid, payment_id, timezone))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        if row == None:
            return False

        self.dbc.execute("SELECT @errMessage")

        err_row = self.dbc.fetchone()

        if err_row != None:
            if err_row[0] != None:
                return err_row[0]

        results = {}
        results["order_payment_uid"]        = int( row[0] )
        results["subtotal"]                 = 0 if row[1] == None else float( row[1] )
        results["discount"]                 = 0 if row[2] == None else float( row[2] )
        results["tip"]                      = 0 if row[3] == None else float( row[3] )
        results["tax"]                      = 0 if row[4] == None else float( row[4] )
        results['service_charge_amount']    = 0 if row[5] == None else float( row[5] )
        results["grand_total"]              = 0 if row[6] == None else float( row[6] )
        results["pay_method_name"]          = '' if row[7] == None else str( row[7] )
        results["pay_method_display_name"]  = '' if row[8] == None else str( row[8] )
        results["receipt_method"]           = '' if row[9] == None else str( row[9] )
        results["patron_card_uid"]          = None if row[10] == None else int( row[10] )
        results["card_four"]                = None if row[11] == None else str( row[11] )
        results["card_type"]                = None if row[12] == None else str( row[12] )
        results["pc_patron_uid"]            = None if row[13] == None else int( row[13] )
        results["pc_first_name"]            = None if row[14] == None else str( row[14] )
        results["pc_last_name"]             = None if row[15] == None else str( row[15] )
        results["pc_company_name"]          = None if row[16] == None else str( row[16] )
        results["pn_patron_uid"]            = None if row[17] == None else int( row[17] )
        results["pn_first_name"]            = None if row[18] == None else str( row[18] )
        results["pn_last_name"]             = None if row[19] == None else str( row[19] )
        results["pn_company_name"]          = None if row[20] == None else str( row[20] )
        results["ap_patron_uid"]            = None if row[21] == None else int( row[21] )
        results["ap_first_name"]            = None if row[22] == None else str( row[22] )
        results["ap_last_name"]             = None if row[23] == None else str( row[23] )
        results["ap_company_name"]          = None if row[24] == None else str( row[24] )
        results["created_at"]               = '' if row[25] == None else str( row[25] )
        results["closed_at"]                = '' if row[26] == None else str( row[26])
        results["invoice_number"]           = None if row[27] == None else str( row[27] )
        results["authorization_code"]       = '' if row[28] == None else str( row[28] )
        results["sale_closed_subtotal"]     = 0 if row[29] == None else float( row[29] )
        results["sale_closed_tip"]          = 0 if row[30] == None else float( row[30] )
        results["sale_closed_tax"]          = 0 if row[31] == None else float( row[31] )
        results["is_order_voided"]          = 0 if row[32] == None else int( row[32] )
        results["paid_at"]                  = '' if row[33] == None else str( row[33] )
        results["raw_closed_at"]            = '' if row[34] == None else str( row[34] )
        results["card_name"]                = '' if row[35] == None else str( row[35] )
        results["orders_paper_closed"]      = 0 if row[36] == None else int( row[36] )
        results["payment_uuid"]             = '' if row[37] == None else str( row[37] )
        results["updated_at"]               = '' if row[38] == None else str( row[38] )
        results["points"]                   = 0 if row[39] == None else float( row[39] )
        results["superpatron_name"]         = '' if row[40] == None else str( row[40] )
        
        if results["patron_card_uid"] != None and results["card_four"] != None:
            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_cards' AND pointer_uid = " + str(results["patron_card_uid"]))
            e_row = self.dbc.fetchone()

            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['card_four'] = results["card_four"]
                values['card_name'] = results["card_name"]
                keys['card_four'] = e_key
                keys['card_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)

                if decoded != None and 'card_four' in decoded:
                    results["card_four"] = decoded["card_four"]
                if decoded != None and 'card_name' in decoded:
                    results["card_name"] = decoded["card_name"]

        if results["pc_patron_uid"] != None:
            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(results["pc_patron_uid"]))
            e_row = self.dbc.fetchone()

            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = results["pc_first_name"]
                values['last_name'] = results["pc_last_name"]
                values['company_name'] = results["pc_company_name"]
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)

                if decoded != None and 'first_name' in decoded:
                    results["pc_first_name"] = decoded["first_name"]
                    results["pc_last_name"] = decoded["last_name"]
                    results["pc_company_name"] = decoded["company_name"]

        if results["pn_patron_uid"] != None:
            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(results["pn_patron_uid"]))
            e_row = self.dbc.fetchone()

            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = results["pn_first_name"]
                values['last_name'] = results["pn_last_name"]
                values['company_name'] = results["pn_company_name"]
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)

                if decoded != None and 'first_name' in decoded:
                    results["pn_first_name"] = decoded["first_name"]
                    results["pn_last_name"] = decoded["last_name"]
                    results["pn_company_name"] = decoded["company_name"]
                    if results['superpatron_name'] != None:
                        results["pn_company_name"] = results['superpatron_name'] + ' ' + decoded["company_name"]

        if results["ap_patron_uid"] != None:
            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(results["ap_patron_uid"]))
            e_row = self.dbc.fetchone()

            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = results["ap_first_name"]
                values['last_name'] = results["ap_last_name"]
                values['company_name'] = results["ap_company_name"]
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)

                if decoded != None and 'first_name' in decoded:
                    results["ap_first_name"] = decoded["first_name"]
                    results["ap_last_name"] = decoded["last_name"]
                    results["ap_company_name"] = decoded["company_name"]

        return results


    def getOrderPaymentDataByOrderAndPayment(self, order_uid, payment_id):
        """
        Get Order's payment info for the specified order_uid and payment_id.

        Parameters
        ----------
        order_uid : int
        payment_id : int

        Returns
        -------
        out : dictionary
            { 
                "patron_uid" : int,
                "subtotal" : float,
                "tip" : float,
                "tax" : float,
                "created_at" : string,
                "pay_method" : string,
                "discount" : float,
                "order_payment_uid" : int,
                "service_charge_amount" : float,
                "points" : float,
                "first_name" : string,
                "last_name" : string,
                "company_name" : string,
                "patron_card_uid" : int,
            }

        """

        self.dbc.execute("SELECT\
                            patron_uid,\
                            oprc.subtotal,\
                            oprc.tip,\
                            oprc.tax,\
                            order_payments.created_at,\
                            order_pay_methods.name as pay_method,\
                            oprc.discount,\
                            order_payments.id,\
                            oprc.service_charge_amount,\
                            oprc.points,\
                            patrons.first_name,\
                            patrons.last_name,\
                            patrons.company_name,\
                            order_payment_preauths.patron_card_uid\
                          FROM\
                            orders.order_payments\
                          LEFT JOIN\
                            ( SELECT\
                                order_payments_x_revenue_centers.order_payment_uid,\
                                SUM(order_payments_x_revenue_centers.subtotal) as subtotal,\
                                SUM(order_payments_x_revenue_centers.tip) as tip,\
                                SUM(order_payments_x_revenue_centers.tax) as tax,\
                                SUM(order_payments_x_revenue_centers.points) as points,\
                                SUM(order_payments_x_revenue_centers.discount) as discount,\
                                SUM(order_payments_x_revenue_centers.service_charge_amount) as service_charge_amount\
                              FROM\
                                orders.order_payments_x_revenue_centers\
                              JOIN\
                                orders.order_payments ON order_payments_x_revenue_centers.order_payment_uid = order_payments.id\
                              LEFT JOIN\
                                patrons.patrons ON order_payments.patron_uid = patrons.id\
                              WHERE\
                                order_payments.order_uid = %s AND\
                                order_payments.payment_id = %s\
                              GROUP BY\
                                order_payments_x_revenue_centers.order_payment_uid ) as oprc ON order_payments.id = oprc.order_payment_uid\
                          JOIN\
                            orders.order_pay_methods on order_payments.order_pay_method_uid = order_pay_methods.id\
                          LEFT JOIN\
                            patrons.patrons ON order_payments.patron_uid = patrons.id\
                          LEFT JOIN\
                            orders.order_payment_preauths ON order_payments.order_uid = order_payment_preauths.order_uid AND\
                                                             order_payments.payment_id = order_payment_preauths.payment_id\
                          WHERE\
                            order_payments.order_uid = %s AND\
                            order_payments.payment_id = %s\
                          LIMIT 1", (order_uid, payment_id, order_uid, payment_id))

        row = self.dbc.fetchone() 
        if row == None:
            data = False
        else:
            data = {}
            data['patron_uid'] = int( row[0] )
            data['subtotal'] = float( row[1] )
            data['tip'] = float( row[2] )
            data['tax'] = float( row[3] )
            data['created_at'] = row[4]
            data['pay_method'] = str( row[5] )
            data['discount'] = 0 if row[6] == None else float( row[6] )
            data['order_payment_uid'] = int( row[7] )
            data['service_charge_amount'] = 0 if row[8] == None else float( row[8] )
            data['points'] = 0 if row[9] == None else float( row[9] )
            data['first_name'] = None if row[10] == None else str( row[10] )
            data['last_name'] = None if row[11] == None else str( row[11] )
            data['company_name'] = None if row[12] == None else str( row[12] )
            data['patron_card_uid'] = None if row[13] == None else int(row[13])

            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(data['patron_uid']))
            e_row = self.dbc.fetchone()

            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = data['first_name']
                values['last_name'] = data['last_name']
                values['company_name'] = data['company_name']
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)
                if decoded != None and 'first_name' in decoded:
                    data['first_name'] = decoded["first_name"]
                    data['last_name'] = decoded["last_name"]
                    data['company_name'] = decoded["company_name"]

                if data['company_name']  != None and data['company_name'] != '':
                    data['patron'] = data['company_name']
                else:
                    data['patron'] = data['first_name'] + " " + data['last_name']

        return data

    def getOrderPaymentRCByOrderAndPayment(self, order_uid, payment_id):
        """
        Gets order payment revenue centere details for the specified order_uid and payment_id

        Parameters
        ----------
        order_uid - int
        payment_id - int

        Returns
        -------
        out: array

        """

        self.dbc.execute("SELECT\
                        order_payments_x_revenue_centers.order_payment_uid,\
                        order_payments_x_revenue_centers.revenue_center_uid,\
                        SUM(order_payments_x_revenue_centers.subtotal) as subtotal,\
                        SUM(order_payments_x_revenue_centers.tip) as tip,\
                        SUM(order_payments_x_revenue_centers.tax) as tax,\
                        SUM(order_payments_x_revenue_centers.discount) as discount,\
                        SUM(order_payments_x_revenue_centers.service_charge_amount) as service_charge_amount\
                      FROM\
                        orders.order_payments_x_revenue_centers\
                      JOIN\
                        orders.order_payments ON order_payments_x_revenue_centers.order_payment_uid = order_payments.id\
                      WHERE\
                        order_payments.order_uid = %s AND\
                        order_payments.payment_id = %s\
                      GROUP BY\
                        order_payment_uid, revenue_center_uid", (order_uid, payment_id))

        rows = self.dbc.fetchall()

        if rows == None:
            data = False
        else:
            data = []
            for row in rows:
                tmp = {}
                tmp['order_payment_uid']    = int(row[0])
                tmp['revenue_center_uid']   = int(row[1])
                tmp['subtotal']             = 0 if row[2] == None else float(row[2])
                tmp['tip']                  = 0 if row[3] == None else float(row[3])
                tmp['tax']                  = 0 if row[4] == None else float(row[4])
                tmp['discount']             = 0 if row[5] == None else float(row[5])
                tmp['service_charge_amount'] = 0 if row[6] == None else float(row[6])
                data.append(tmp)

        return data

    def deleteAllIGTransferMessagesByOrderItems(self, order_uid):
        """
        Delete all IG Transfer Message for the specified list of order_item_uids

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : True

        """

        self.dbc.execute("DELETE FROM messages.messages WHERE order_uid = " + str(order_uid) + " AND action_uid = 4 and read_at IS NULL")
        self.db.commit()
        return True

    def deleteAllPendingReentriesByOrderItems(self, order_item_uids):
        """
        Delete from reentries for the given order_item_uids

        Parameters
        ----------
        order_item_uids : array

        Returns
        -------
        out : True

        """

        ids = "', '".join(str(v) for v in order_item_uids)

        self.dbc.execute("DELETE FROM orders.order_item_reentries WHERE order_item_uid IN('" + ids + "') AND status = 'pending'")
        self.db.commit()
        return True

    def getRetailDetailsForOrder(self, order_uid):
        """
        Gets any retail details for the items in the specified order_uid

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : list
            [ { "item_group_name" : string, "item_group_description" : string, "item_size" : string, "item_color" : string }, ... ]


        """

        self.dbc.execute("SELECT\
                          menu_item_groups.name as item_group_name,\
                          menu_item_groups.description item_group_description,\
                          menu_item_sizes.name as item_size,\
                          menu_item_colors.name as item_color\
                        FROM\
                          orders.orders\
                        JOIN\
                          orders.sub_orders ON orders.id = sub_orders.order_uid\
                        JOIN\
                          orders.order_items ON sub_orders.id = order_items.sub_order_uid\
                        JOIN\
                          menus.menu_x_menu_items ON order_items.menu_x_menu_item_uid = menu_x_menu_items.id\
                        JOIN\
                          menus.menu_item_retail_details ON menu_x_menu_items.menu_item_uid = menu_item_retail_details.menu_item_uid\
                        JOIN\
                          menus.menu_item_colors ON menu_item_retail_details.menu_item_color_uid = menu_item_colors.id\
                        JOIN\
                          menus.menu_item_sizes ON menu_item_retail_details.menu_item_size_uid = menu_item_sizes.id\
                        JOIN\
                          menus.menu_item_groups ON menu_item_retail_details.menu_item_group_uid = menu_item_groups.id\
                        WHERE\
                          orders.id = " + str(order_uid))

        rows = self.dbc.fetchall()

        results = []

        if rows == None:
            return results

        for row in rows:
            tmp = {}
            tmp['item_group_name']          = str(row[0])
            tmp['item_group_description']   = str(row[1])
            tmp['item_size']                = str(row[2])
            tmp['item_color']               = str(row[3])
            results.append(tmp)

        return results


    def getPendingReentryItemsByOrder(self, order_uid):
        """ 
        Gets all order_item_uids that are not in the list of order_item_uids and have status = pending.

        Parameters
        ----------
        order_uid : int

        Returns
        -------
        out : array

        """

        # get all order_item_uids in reentries table associated with passed in order_uid that have not been reentered 
        # and are not part of the array of order_item_uids in order_item_uids array
        self.dbc.execute("SELECT\
                            order_items.id as order_item_uid,\
                            order_item_reentries.status\
                          FROM\
                            orders.orders\
                          JOIN\
                            orders.sub_orders ON orders.id = sub_orders.order_uid\
                          JOIN\
                            orders.order_items ON sub_orders.id = order_items.sub_order_uid\
                          LEFT JOIN\
                            orders.order_item_reentries ON order_items.id = order_item_reentries.order_item_uid\
                          WHERE\
                            orders.id = " + str(order_uid) + " AND\
                            ( order_item_reentries.status = 'pending' or order_item_reentries.status IS NULL)")
        rows = self.dbc.fetchall()

        if rows == None:
            results = False
        else:
            results = []
            for row in rows:
                order_item_uid = int( row[0] )
                results.append(order_item_uid)

        return results

    def getAllVoidedItemsByOrderItem(self, order_item_uid):
        """
        Get all order_item_uids in order_items_x_modifications that were added with the same modification_token.

        Parameters
        ----------
        order_item_uid : int

        Returns
        -------
        out : array

        """

        self.dbc.execute("SELECT oim.order_item_uid FROM orders.order_items_x_modifications JOIN orders.order_items_x_modifications oim ON order_items_x_modifications.modification_token = oim.modification_token WHERE order_items_x_modifications.order_item_uid = " + str(order_item_uid))
        rows = self.dbc.fetchall()

        if rows == None:
            results = False
        else:
            results = []
            for row in rows:
                order_item_uid = int( row[0] )
                results.append(order_item_uid)

        return results

    def getPreorderCheckListProblems(self, preorder_uid):
        """


        """

        self.dbc.execute("SELECT id, name, qty, checklist_qty, checklist_status FROM preorders.preorder_items WHERE preorder_uid = " + str(preorder_uid) + " AND checklist_status != 'pending' AND checklist_status != 'good'")
        rows = self.dbc.fetchall()

        data = {}
        preorder_item_uids = []

        if rows != None:
            for row in rows:
                preorder_item_uid = int( row[0] )
                preorder_item_uids.append( preorder_item_uid )

                if preorder_item_uid not in data:
                    data[preorder_item_uid] = {}
                    data[preorder_item_uid]['item'] = {}
                    data[preorder_item_uid]['components'] = []
                    data[preorder_item_uid]['equipments'] = []

                data[preorder_item_uid]['item']['name']         = str( row[1] )
                data[preorder_item_uid]['item']['qty']          = 0 if row[2] == None else int( row[2] )
                data[preorder_item_uid]['item']['checklist_qty'] = 0 if row[3] == None else int( row[3] )
                data[preorder_item_uid]['item']['status']       = str( row[4] )

                self.dbc.execute("SELECT menu_components.name, qty, checklist_qty, checklist_status FROM preorders.preorder_components JOIN menus.menu_components ON preorder_components.menu_component_uid = menu_components.id WHERE preorder_item_uid = " + str(preorder_item_uid) + " AND checklist_status != 'pending' AND checklist_status != 'good'")
                components = self.dbc.fetchall()

                if components != None:
                    for component in components:
                        tmp = {}
                        tmp['name']             = str( component[0] )
                        tmp['qty']              = 0 if component[1] == None else int( component[1] )
                        tmp['checklist_qty']    = 0 if component[2] == None else int( component[2] )
                        tmp['status']           = str( component[3] )
                        data[preorder_item_uid]['components'].append( tmp )

                self.dbc.execute("SELECT menu_equipment.name, qty, checklist_qty, checklist_status FROM preorders.preorder_equipments JOIN menus.menu_equipment ON preorder_equipments.menu_equipment_uid = menu_equipment.id WHERE preorder_item_uid = " + str(preorder_item_uid) + " AND checklist_status != 'pending' AND checklist_status != 'good'")
                equipments = self.dbc.fetchall()

                if equipments != None:
                    for equipment in equipments:
                        tmp = {}
                        tmp['name']             = str( equipment[0] )
                        tmp['qty']              = 0 if equipment[1] == None else int( equipment[1] )
                        tmp['checklist_qty']    = 0 if equipment[2] == None else int( equipment[2] )
                        tmp['status']           = str( equipment[3] )
                        data[preorder_item_uid]['equipments'].append( tmp )
                        

        self.dbc.execute("SELECT preorder_items.id, menu_components.name, preorder_components.qty, preorder_components.checklist_qty, preorder_components.checklist_status FROM preorders.preorder_items JOIN preorders.preorder_components ON preorder_items.id = preorder_components.preorder_item_uid JOIN menus.menu_components ON preorder_components.menu_component_uid = menu_components.id WHERE preorder_items.preorder_uid = " + str(preorder_uid) + " AND preorder_components.checklist_status != 'pending' AND preorder_components.checklist_status != 'good'")
        rows = self.dbc.fetchall()

        if rows != None:
            for row in rows:
                preorder_item_uid = int( row[0] )
                
                if preorder_item_uid not in data:
                    data[preorder_item_uid] = {}
                    data[preorder_item_uid]['item'] = {}
                    data[preorder_item_uid]['components'] = []
                    data[preorder_item_uid]['equipments'] = []
                    self.dbc.execute("SELECT id, name, qty, checklist_qty, checklist_status FROM preorders.preorder_items WHERE id = " + str(preorder_item_uid))
                    item = self.dbc.fetchone()

                    if item != None:
                        data[preorder_item_uid]['item']['name']             = str( item[1] )
                        data[preorder_item_uid]['item']['qty']              = 0 if item[2] == None else int( item[2] )
                        data[preorder_item_uid]['item']['checklist_qty']    = 0 if item[3] == None else int( item[3] )
                        data[preorder_item_uid]['item']['status']           = str( item[4] )
                        
 
                tmp = {}                         
                tmp['name']             = str( row[1] )                        
                tmp['qty']              = 0 if row[2] == None else int( row[2] )
                tmp['checklist_qty']    = 0 if row[3] == None else int( row[3] )
                tmp['status']           = str( row[4] )
                data[preorder_item_uid]['components'].append( tmp )
                

        self.dbc.execute("SELECT preorder_items.id, menu_equipment.name, preorder_equipments.qty, preorder_equipments.checklist_qty, preorder_equipments.checklist_status FROM preorders.preorder_items JOIN preorders.preorder_equipments ON preorder_items.id = preorder_equipments.preorder_item_uid JOIN menus.menu_equipment ON preorder_equipments.menu_equipment_uid = menu_equipment.id WHERE preorder_items.preorder_uid = " + str(preorder_uid) + " AND preorder_equipments.checklist_status != 'pending' AND preorder_equipments.checklist_status != 'good'")
        rows = self.dbc.fetchall()

        if rows != None:
            for row in rows:
                preorder_item_uid = int( row[0] )
     
                if preorder_item_uid not in data:
                    data[preorder_item_uid] = {} 
                    data[preorder_item_uid]['item'] = {} 
                    data[preorder_item_uid]['components'] = [] 
                    data[preorder_item_uid]['equipments'] = [] 
                    self.dbc.execute("SELECT id, name, qty, checklist_qty, checklist_status FROM preorders.preorder_items WHERE id = " + str(preorder_item_uid))
                    item = self.dbc.fetchone()

                    if item != None:
                        data[preorder_item_uid]['item']['name']             = str( item[1] )
                        data[preorder_item_uid]['item']['qty']              = 0 if item[2] == None else int( item[2] )
                        data[preorder_item_uid]['item']['checklist_qty']    = 0 if item[3] == None else int( item[3] )
                        data[preorder_item_uid]['item']['status']           = str( item[4] )

                tmp = {}         
                tmp['name']             = str( row[1] )         
                tmp['qty']              = 0 if row[2] == None else int( row[2] )
                tmp['checklist_qty']    = 0 if row[3] == None else int( row[3] )
                tmp['status']           = str( row[4] )
                data[preorder_item_uid]['equipments'].append( tmp )

        return data



    def getUnitDataById(self, unit_uid):
        """
        Get unit's name and timezone by unit_id

        Parameters
        ----------
        unit_uid : int

        Returns
        -------
        out : dictionary
            { "name" : string, "timezone" : string }

        """

        self.dbc.execute("SELECT units.name, local_timezone_long FROM setup.units JOIN setup.venues ON units.venue_uid = venues.id WHERE units.id = " + str(unit_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            data = {}
            data['unit_name']   = str( row[0] )
            data['timezone']    = str( row[1] )
        
        return data

    def getRandomOrderData(self, order_uid, payment_id):
        """
        Gets random order related data for receipts.

        Parameters
        ----------
        order_uid : int
        payment_id : int

        Returns
        -------
        out : dictionary
            { "unit_name" : string, "employee_first_name" : string, "invoice_uid" : int, "cc_type" : string, "receipt_text" : string, "authorization_code" : string, "card_four" : int, "card_last_name" : string }

        """

        self.dbc.execute("SELECT units.name,\
                            employees.first_name,\
                            order_payment_preauths.invoice_uid,\
                            order_payment_preauths.cc_type,\
                            order_payment_preauths.receipt_text,\
                            order_payment_preauths.authorization_code,\
                            patron_cards.id as patron_card_uid,\
                            patron_cards.card_four,\
                            patron_cards.last_name\
                          FROM\
                            orders.order_payment_preauths\
                          JOIN\
                            patrons.patron_cards ON order_payment_preauths.patron_card_uid = patron_cards.id\
                          JOIN\
                            orders.orders ON order_payment_preauths.order_uid = orders.id\
                          JOIN\
                            setup.employees ON orders.employee_uid = employees.id\
                          JOIN\
                            setup.units ON orders.unit_uid = units.id\
                          WHERE\
                            order_payment_preauths.order_uid = %s AND\
                            order_payment_preauths.payment_id = %s", (order_uid, payment_id))
        row = self.dbc.fetchone()

        if row == None:
            data = False
        else:
            patron_card_uid = str( row[6] )
            card_four       = str( row[7] )
            last_name       = str( row[8] )

            # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_cards' AND pointer_uid = " + str(patron_card_uid))
            e_row = self.dbc.fetchone()
            e_key = e_row[0]

            values = {}
            keys = {}
            values['card_four'] = card_four
            values['last_name'] = last_name
            keys['card_four'] = e_key
            keys['last_name'] = e_key

            keymaster = KeyMaster()
            decoded =  keymaster.decryptMulti(values,keys)

            data = {}
            data['unit_name'] = str( row[0] )
            data['employee_first_name'] = str( row[1] )
            data['invoice_uid'] = int( row[2] )
            data['cc_type'] = str( row[3] )
            data['receipt_text'] = str( row[4] )
            data['authorization_code'] = str( row[5] )
            data['card_four'] = str( decoded['card_four'] )
            data['card_last_name'] = str( decoded['last_name'] )

        return data

    def getEmployeeByEventAndUnit(self, event_uid, unit_uid):
        """
        Gets Employee assigned to specified event_uid and unit_uid

        Parameters
        ----------
        event_uid : int
        unit_uid : int

        Returns
        -------
        out : array

        """
 
        self.dbc.execute("SELECT\
                            venues_x_employees.employee_uid\
                          FROM\
                            setup.events_x_units\
                          JOIN\
                            setup.units_x_employees ON events_x_units.id = units_x_employees.event_unit_uid\
                          JOIN\
                            setup.venues_x_employees ON units_x_employees.venue_employee_uid = venues_x_employees.id\
                          WHERE\
                            events_x_units.event_uid = %s AND\
                            events_x_units.unit_uid = %s", (event_uid, unit_uid))
        rows = self.dbc.fetchall()

        if rows == None:
            return False
        else:
            results = []
            for row in rows:
                employee_uid = int( row[0] )
                results.append(employee_uid)

        return results

    def getMenuItemVoidDataByOrderItemId(self, order_item_uid):
        """
        Get unit name and employee name by order_item_uid

        Parameters
        ----------
        order_item_uid : int

        Returns
        -------
        out : dictionary
            { "unit_name" : string, "first_name" : stirng, "last_name" : string }

        """

        self.dbc.execute("SELECT\
                            units.name as unit_name,\
                            employees.first_name,\
                            employees.last_name\
                          FROM\
                            orders.orders\
                          JOIN\
                            orders.sub_orders ON orders.id = sub_orders.order_uid\
                          JOIN\
                            orders.order_items on sub_orders.id = order_items.sub_order_uid\
                          JOIN\
                            setup.employees on orders.employee_uid = employees.id\
                          JOIN\
                            setup.units on orders.unit_uid = units.id\
                          WHERE\
                            order_items.id = " + str(order_item_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            data = {}
            data['unit_name']       = str( row[0] )
            data['first_name']      = str( row[1] )
            data['last_name']       = str( row[2] )

        return data

    def getMenuItemVoidDataByModificationToken(self, modification_token):
        """
        Gets grouped voided item data using modification_token

        Parameters
        ----------
        modification_token : string

        Returns
        -------
        out : array
            [{"display_nane": stirng, "qty" : int},...]

        """

        self.dbc.execute("SELECT\
                            menu_items.display_name,\
                            count(*) as qty\
                          FROM\
                            orders.orders\
                          JOIN\
                            orders.sub_orders ON orders.id = sub_orders.order_uid\
                          JOIN\
                            orders.order_items on sub_orders.id = order_items.sub_order_uid\
                          JOIN\
                            orders.order_items_x_modifications on order_items.id = order_items_x_modifications.order_item_uid\
                          JOIN\
                            menus.menu_x_menu_items on order_items.menu_x_menu_item_uid = menu_x_menu_items.id\
                          JOIN\
                            menus.menu_items on menu_x_menu_items.menu_item_uid = menu_items.id\
                          WHERE\
                            modification_token = '" + str(modification_token) + "'\
                          GROUP BY\
                            display_name")
        rows = self.dbc.fetchall()

        if rows == None:
            return False
        else:
            results = []
            for row in rows:
                tmp = {}
                tmp['display_name'] = str(row[0])
                tmp['qty']          = int(row[1])
                results.append(tmp)

        return results

    def getOrderItemsSoldByPrinterCategory(self, event_uid):
        """

        """

        self.dbc.execute("SELECT\
                            menu_items.display_name,\
                            menu_items.printer_category,\
                            count(*) as qty\
                          FROM\
                            orders.order_payments\
                          JOIN\
                            orders.orders on order_payments.order_uid = orders.id\
                          JOIN\
                            orders.sub_orders ON orders.id = sub_orders.order_uid\
                          JOIN\
                            orders.order_items on sub_orders.order_uid = order_items.sub_.order_uid\
                          JOIN\
                            menus.menu_x_menu_items on order_items.menu_x_menu_item_uid = menu_x_menu_items.id\
                          JOIN\
                            menus.menu_items on menu_x_menu_items.menu_item_uid = menu_items.id\
                          WHERE\
                            orders.event_uid = " + str(event_uid) + "\
                          GROUP BY\
                            menu_items.display_name,\
                            menu_items.printer_category\
                          ORDER BY\
                            printer_category")
        rows = self.dbc.fetchall()

        if rows == None:
            return False

        results = []
        for row in rows:
            tmp = {}
            tmp['display_name']     = str( row[0] )
            tmp['printer_category'] = str( row[1] )
            tmp['qty']              = int( row[2] )
            results.append(tmp)

        return results

    def getOrderItemVoidsByPrinterCategory(self, event_uid):
        """

        Parameters
        ----------
        event_uid : int

        Returns
        -------
        out : array
            [ { "display_name" : string, "printer_category" : string, "qty" : int}, ....]

        """

        self.dbc.execute("SELECT\
                            menu_items.display_name,\
                            menu_items.printer_category,\
                            count(*) as qty\
                          FROM\
                            orders.order_payments\
                          JOIN\
                            orders.orders on order_payments.order_uid = orders.id\
                          JOIN\
                            orders.sub_orders ON orders.id = sub_orders.order_uid\
                          JOIN\
                            orders.order_items on sub_orders.order_uid = order_items.sub_order_uid\
                          JOIN\
                            orders.order_items_x_modifications ON order_items.id = order_items_x_modifications.order_item_uid\
                          JOIN\
                            orders.order_modifications ON order_items_x_modifications.order_modification_uid = order_modifications.id\
                          JOIN\
                            menus.menu_x_menu_items ON order_items.menu_x_menu_item_uid = menu_x_menu_items.id\
                          JOIN\
                            menus.menu_items ON menu_x_menu_items.menu_item_uid = menu_items.id\
                          WHERE\
                            orders.event_uid = " + str(event_uid) + " AND\
                            order_modifications.action_type = 'void'\
                          GROUP BY\
                            menu_items.display_name,\
                            menu_items.printer_category\
                          ORDER BY\
                            printer_category")
        rows = self.dbc.fetchall()

        if rows == None:
            return False

        results = []
        for row in rows:
            tmp = {}
            tmp['display_name']     = str( row[0] )
            tmp['printer_category'] = str( row[1] )
            tmp['qty']              = int( row[2] )
            results.append(tmp)

        return results

    def getEmployeeNameById(self, employee_uid):
        """

        Parameters
        ----------
        employee_uid : int

        Returns
        -------
        out : dictionary
          { "first_name" : string, "last_name" : string }

        """

        self.dbc.execute("SELECT first_name, last_name FROM setup.employees WHERE id = " + str(employee_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            emp = {}
            emp['first_name'] = str( row[0] )
            emp['last_name']  = str( row[1] )

        return emp

    def getEventVenueByEventId(self, venue_uid, event_uid):
        """
        Gets the venue name, event name and event date in venue local time for the passed in
        venue_uid and event_uid.

        Parameters
        ----------
        venue_uid : int
        event_uid : int

        Returns
        -------
        out : { "event_name" : string, "event_date" : string, "venue_name" : string }

        """

        self.dbc.execute("SELECT local_timezone_long FROM setup.venues WHERE id = " + str(venue_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False

        timezone = str( row[0] )

        self.dbc.execute("SELECT event_name, CONVERT_TZ(events.event_date,\'GMT\', %s) AS event_date, venues.name as venue_name FROM setup.events_x_venues JOIN setup.events ON events_x_venues.event_uid = events.id JOIN setup.venues ON events_x_venues.venue_uid = venues.id WHERE event_uid = %s", (timezone, event_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            event_date = str(row[1])
            dt = datetime.datetime.strptime(event_date, "%Y-%m-%d %H:%M:%S")

            data = {}
            data['event_name'] = str(row[0])
            data['event_date'] = dt.strftime('%m/%d/%y %H:%M')
            data['venue_name'] = str(row[2])

        return data


    def getEventNameById(self, event_uid):
        """
        Gets event name by id

        Parameters
        ----------
        event_uid : int

        Returns
        -------
        out : string

        """

        self.dbc.execute("SELECT event_name FROM setup.events_x_venues WHERE event_uid = " + str(event_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            name = str( row[0] )

        return name

    def getMenuItemCountsByPrinterCategory(self, event_uid):
        """
        

        Parameters
        ----------
        event_uid : int

        Returns
        -------
        out :

        """

        # wyatt_wallace 

        '''
        self.dbc.execute("SELECT sold.printer_category, sold.display_name, COALESCE(sold.qty,0), COALESCE(voids.qty,0) FROM ( select menu_items.display_name, menu_items.printer_category, count(*) as qty from orders.order_payments join orders.orders on order_payments.order_uid = orders.id join orders.order_items on order_payments.order_uid = order_items.order_uid join menus.menu_x_menu_items on order_items.menu_x_menu_item_uid = menu_x_menu_items.id join menus.menu_items on menu_x_menu_items.menu_item_uid = menu_items.id where orders.event_uid = %s group by menu_items.display_name, menu_items.printer_category ) AS sold LEFT JOIN ( select menu_items.display_name, menu_items.printer_category, count(*) as qty from orders.order_payments join orders.orders on order_payments.order_uid = orders.id join orders.order_items on order_payments.order_uid = order_items.order_uid join orders.order_items_x_modifications on order_items.id = order_items_x_modifications.order_item_uid join orders.order_modifications on order_items_x_modifications.order_modification_uid = order_modifications.id join menus.menu_x_menu_items on order_items.menu_x_menu_item_uid = menu_x_menu_items.id join menus.menu_items on menu_x_menu_items.menu_item_uid = menu_items.id where orders.event_uid = %s and order_modifications.action_type = 'void' group by menu_items.display_name, menu_items.printer_category ) AS voids ON voids.display_name = sold.display_name ORDER BY printer_category, display_name", (event_uid, event_uid))
        '''

        self.dbc.execute("CALL reports.get_menu_items_counts_by_category_proc(" + str(event_uid) + ", @errMessage)")

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        if rows == None:
            return False

        results = {}
    
        if rows != None:
            for row in rows:
                category = '' if row[0] == None else str( row[0] )

                if category not in results:
                    #print "adding category %s to results" % category
                    results[category] = []

                tmp = {}
                tmp['display_name']     = '' if row[1] == None else str( row[1] )
                tmp['category_name']    = '' if row[2] == None else str( row[2] )
                tmp['supercat_name']    = '' if row[3] == None else str( row[3] )
                tmp['num_sold']         = 0 if row[4] == None else int( row[4] )
                tmp['num_voided']       = 0 if row[5] == None else int( row[5] )

                results[category].append(tmp)

        return results

    def getEmployeeSalesReportDataByEvent(self, event_uid, employee_uid, food_menu_tax_uids, soda_menu_tax_uids, food_supercategory_uids, alcohol_supercategory_uids):
        """

        Parameters
        ----------
        event_uid : int
        employee_uid : int
        food_menu_tax_uids : string - comma separated list of ids
        soda_menu_tax_uids : string - comma separated list of ids
        food_supercategory_uids : string - comma separated list of ids
        alcohol_supercategory_uids - string - comma separated list of ids 

        Returns
        -------
        out : dictionary

        """

        results = {}

        # cash
        results['cash'] = self.getCashPaymentQtyAndTotal(event_uid, employee_uid)
        # credit
        results['credit'] = self.getCreditPaymentQtyAndTotal(event_uid, employee_uid)
        # tips
        results['tips'] = self.getOrderPaymentTipQtyAndTotal(event_uid, employee_uid)
        # card_type
        results['card_types'] = self.creditCardTypesQtyAndTotal(event_uid, employee_uid)
        # venue_uid 203 : menu tax 6 = food, 10 = soda
        ### results['food_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, menu_tax_uids=[6], employee_uid)
        ### results['soda_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, menu_tax_uids=[10], employee_uid)
        results['food_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, food_menu_tax_uids, employee_uid)
        results['soda_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, soda_menu_tax_uids, employee_uid)
        # net total
        results['net_total'] = self.getOrderPaymentsNetTotal(event_uid, employee_uid)
        # total
        results['total'] = self.getOrderPaymentTotal(event_uid, employee_uid)
        # group total
        results['group_total'] = self.getOrderPaymentGroupTotal(event_uid, employee_uid)
        # menu_super_categories: Food = 101, Soda = 102, Beer = 103, Wine = 104, Call Liq = 105, Prem Liq = 108
        ### results['food_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, menu_super_category_uids=[101,102], employee_uid)
        ### results['alcohol_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, menu_super_category_uids=[103,104,105,108], employee_uid)
        results['food_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, food_supercategory_uids, employee_uid)
        results['alcohol_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, alcohol_supercategory_uids, employee_uid)
        # average spend
        results['average_spend'] = self.getOrderPaymentAverageSpend(event_uid, employee_uid )
        return results

    def getEventSalesReportData(self, event_uid, food_menu_tax_uids, soda_menu_tax_uids, food_supercategory_uids, alcohol_supercategory_uids):
        """

        Parameters
        ----------
        event_uid : int
        food_menu_tax_uids : string - comma separated list of ids
        soda_menu_tax_uids : string - comma separated list of ids
        food_supercategory_uids : string - comma separated list of ids
        alcohol_supercategory_uids - string - comma separated list of ids 

        Returns
        -------
        out : dictionary

        """

        results = {}

        # cash
        results['cash'] = self.getCashPaymentQtyAndTotal(event_uid)
        # credit
        results['credit'] = self.getCreditPaymentQtyAndTotal(event_uid)
        # tips
        results['tips'] = self.getOrderPaymentTipQtyAndTotal(event_uid)
        # card_type
        results['card_types'] = self.creditCardTypesQtyAndTotal(event_uid)
        # venue_uid 203 : menu tax 6 = food, 10 = soda
        ### results['food_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, menu_tax_uids=[6])
        ### results['soda_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, menu_tax_uids=[10])
        results['food_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, food_menu_tax_uids)
        results['soda_tax'] = self.getTaxQtyAndTotalByMenuTaxIds(event_uid, soda_menu_tax_uids)
        # net total
        results['net_total'] = self.getOrderPaymentsNetTotal(event_uid)
        # total
        results['total'] = self.getOrderPaymentTotal(event_uid)
        # group total
        results['group_total'] = self.getOrderPaymentGroupTotal(event_uid)
        # menu_super_categories: Food = 101, Soda = 102, Beer = 103, Wine = 104, Call Liq = 105, Prem Liq = 108
        ### results['food_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, menu_super_category_uids=[101,102])
        ### results['alcohol_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, menu_super_category_uids=[103,104,105,108])
        results['food_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, food_supercategory_uids)
        results['alcohol_total'] = self.getMenuSuperCategoriesQtyAndTotal(event_uid, alcohol_supercategory_uids)
        # average spend
        results['average_spend'] = self.getOrderPaymentAverageSpend(event_uid)

        return results


    def getCashPaymentQtyAndTotal(self, event_uid, employee_uid=None):
        """
        Get cash stats for specified event_uid and optional employee_uid

        Parameters
        ----------
        event_uid : int
        employee_uid : None|int

        Return
        ------
        out : dictionary
          { "qty" : int, "total" : float }

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_cash_qty_total_for_event_report_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_cash_qty_total_for_employee_report_proc(%s, %s)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats


    def getCreditPaymentQtyAndTotal(self, event_uid, employee_uid=None):
        """
        Get credit stats for specified event_uid and optional employee_uid

        Parameters
        ----------
        event_uid : int
        employee_uid : None|int

        Return
        ------
        out : dictionary
          { "qty" : int, "total" : float }

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_credit_qty_total_for_event_report_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_credit_qty_total_for_employee_report_proc(%s, %s)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats


    def creditCardTypesQtyAndTotal(self, event_uid, employee_uid=None):
        """
        Gets qty and total for each credit card type used

        Parameters
        ----------
        event_uid : int
        employee_uid : None|int

        Returns
        -------
        out : array
            [{"card_type" : string, "qty" : int, "total" : float}, ... ]

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_card_types_qty_total_for_event_report_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_card_types_qty_total_for_employee_report_proc(%s, %s)", (event_uid, employee_uid))

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        stats = []

        if rows == None:
            return stats

        if rows != None:
            for row in rows:
                tmp = {}
                tmp['qty']          = int( row[0] )
                tmp['total']        = 0 if row[1] == None else float( row[1] )
                tmp['card_type']    = '' if row[2] == None else str( row[2] )
                stats.append(tmp)

        return stats
        
    def getTaxQtyAndTotalByMenuTaxIds(self, event_uid, menu_tax_uids, employee_uid=None):
        """
        Gets Food and Beverage tax qty and totals



        """

        stats = {}

        try:
            num_ids = len(menu_tax_uids)
        except Exception, e:
            num_ids = 0

        if num_ids == 0:
            stats['qty']    = 0
            stats['total']  = 0
            return stats

        ids = ", ".join(str(v) for v in menu_tax_uids)

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_tax_qty_and_total_by_menu_tax_ids_proc(%s, %s)", (event_uid, ids))
        else:
            self.dbc.execute("CALL reports.get_tax_qty_and_total_by_menu_tax_ids_and_employee_proc(%s, %s, %s)", (event_uid, employee_uid, ids))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats


    def getOrderPaymentsNetTotal(self, event_uid, employee_uid=None):
        """
        Get net total from order payments for the specified event_uid and optional employee_uid

        Net Total = cash total + credit total - tax total

        Parameters
        ----------
        event_uid : int
        employee_uid : int

        Returns
        -------
        out : dictionary
         { "total" : float }

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_net_total_by_event_report_proc(" + str(event_uid) + ", @errMessage)")
        else:
            self.dbc.execute("CALL reports.get_net_total_by_employee_report_proc(%s, %s, @errMessage)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {} 

        if row == None:
            stats['total'] = 0
        else:
            stats['total'] = 0 if row[0] == None else float( row[0] )

        return stats

    def getOrderPaymentTotal(self, event_uid, employee_uid=None):
        """
        Get total for the passed in event_uid and optional employee_uid
        
        Total = cash total + credit total + tip total

        Parameters
        ----------
        event_uid : int
        employee_uid : None|int

        Returns
        -------
        out : dictionary
            { "total" : float }

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_all_sales_qty_total_for_event_report_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_all_sales_qty_total_for_employee_report_proc(%s, %s)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats

    def getOrderPaymentGroupTotal(self, event_uid, employee_uid=None):
        """
        Get group total for the passed in event_uid and optional employee_uid

        Group Total = cash total + credit total

        Parameters
        ----------
        event_uid : int
        employee_uid : None|int

        Returns
        -------
        out : dictionary
            { "total" : float }

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_all_sales_subtotal_for_event_report_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_all_sales_subtotal_for_employee_report_proc(%s, %s)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['total']  = 0
        else:
            stats['total']  = 0 if row[0] == None else float( row[0] )

        return stats

    def getOrderPaymentAverageSpend(self, event_uid, employee_uid=None):
        """


        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_order_payment_average_spend_proc(" + str(event_uid) + ")")
        else:
            self.dbc.execute("CALL reports.get_order_payment_average_spend_for_employee_proc(%s, %s)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['average'] = 0
        else:
            stats['average'] = 0 if row[0] == None else float( row[0] )

        return stats


    def getMenuSuperCategoriesQtyAndTotal(self, event_uid, menu_super_category_uids, employee_uid=None):
        """
        Gets qty and total for passed in menu super categories

        Parameters
        ----------
        event_uid : int
        menu_super_categoy_uids : array
        employee_uid : None|int

        Returns
        -------
        out : dictionary
            { "qty" : int, "total" : float }

        """

        stats = {}

        try:
            num_ids = len(menu_super_category_uids)
        except Exception, e:
            num_ids = 0

        if num_ids == 0:
            stats['qty']    = 0
            stats['total']  = 0
            return stats

        ids = ", ".join(str(v) for v in menu_super_category_uids)

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_menu_super_cats_qty_and_total_proc(%s, %s)", (event_uid, ids))
        else:
            self.dbc.execute("CALL reports.get_menu_super_cats_qty_and_total_for_employee_proc(%s, %s, %s)", (event_uid, employee_uid, ids))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats

    def getOrderPaymentTipQtyAndTotal(self, event_uid, employee_uid=None):
        """

        """

        if employee_uid == None:
            self.dbc.execute("CALL reports.get_gratuity_report_for_event_proc(" + str(event_uid) + ", @errMessage)")
        else:
            self.dbc.execute("CALL reports.get_gratuity_report_for_employee_proc(%s, %s, @errMessage)", (event_uid, employee_uid))

        row = self.dbc.fetchone()
        self.dbc.nextset()

        stats = {}

        if row == None:
            stats['qty']    = 0
            stats['total']  = 0
        else:
            stats['qty']    = int( row[0] )
            stats['total']  = 0 if row[1] == None else float( row[1] )

        return stats

    def doesEmployeeHaveVoidsPending(self, event_uid, employee_uid):
        """
        Checks to see if the there are any voided orders or voided items with status = pending for the passed in event_uid and employee_uid.

        Parameters
        ----------
        event_uid : int
        employee_uid : int

        Returns
        -------
        out : bool

        """

        self.dbc.execute("SELECT\
                            1\
                          FROM\
                            (\
                              ( SELECT \
                                  orders.event_uid,\
                                  orders_x_modifications.order_modification_uid\
                                FROM\
                                  orders.orders\
                                LEFT JOIN\
                                  orders.orders_x_modifications on orders.id = orders_x_modifications.order_uid\
                                LEFT JOIN\
                                  orders.order_modifications on orders_x_modifications.order_modification_uid = order_modifications.id\
                                WHERE\
                                  event_uid = %s AND\
                                  orders.employee_uid = %s AND\
                                  order_modifications.status = 'pending'\
                                LIMIT 1 ) \
                          UNION \
                            ( SELECT\
                                orders.event_uid,\
                                order_items_x_modifications.order_modification_uid\
                              FROM\
                                orders.orders\
                              LEFT JOIN\
                                orders.sub_orders on orders.id = sub_orders.order_uid\
                              LEFT JOIN\
                                orders.order_items on sub_orders.id = order_items.sub_order_uid\
                              LEFT JOIN\
                                orders.order_items_x_modifications on order_items.id = order_items_x_modifications.order_item_uid\
                              LEFT JOIN\
                                orders.order_modifications on order_items_x_modifications.order_modification_uid = order_modifications.id\
                              WHERE\
                                event_uid = %s AND\
                                orders.employee_uid = %s AND\
                                order_modifications.status = 'pending'\
                              LIMIT 1 )\
                          ) AS order_voids", (event_uid, employee_uid, event_uid, employee_uid))
        row = self.dbc.fetchone()

        if row == None:
            return False
        else:
            return True

    def addNewPrintoutPrintJob(self, device_uid, printout_uid, status, printer_uid, printer_type, ip_address, payload=None):
        """
        Adds new rows to tablets_x_printouts and printout_print_jobs tables 

        Parameters
        ----------
        device_uid : stirng
        printout_uid : int
        status : string
        printer_uid : int
        printer_type : string
        ip_address : string
        payload : None|string

        Returns
        -------
        out : int

        """

        self.dbc.execute("insert into printing.tablets_x_printouts (device_uid, printout_uid, created_at) values (%s, %s, NOW())", (device_uid, printout_uid))
        tablet_printout_uid = self.dbc.lastrowid
        self.db.commit()

        payload = '' if payload == None else payload

        self.dbc.execute("insert into printing.printout_print_jobs (tablet_printout_uid, status, printer_uid, printer_type, ip_address, payload, created_at) values (%s, %s, %s, %s, INET_ATON(%s), %s, NOW())", (tablet_printout_uid, status, printer_uid, printer_type, ip_address, payload))




        printout_print_job_uid = self.dbc.lastrowid
        self.db.commit()

        return printout_print_job_uid


if __name__ == "__main__":
    #eventsDB = EventsDb()
    #print eventsDB.getVenueTimezoneFromInvoice(209)
    import pprint
    from db_connection import DbConnection
    db = DbConnection().connection
    eventsDb = EventsDb(db)

    
    data = eventsDb.getRandomOrderData(order_uid = 7792, payment_id = 0)
    pprint.pprint(data)

    data = eventsDb.getServiceChargeMessage(202)
    pprint.pprint(data)

    '''
    print "\n-----"
    data = eventsDb.getEmailByOrderAndPayment(4688,0)
    pprint.pprint(data)
    print "-----\n"
   
    params = {}; 
    params['unit_uid'] = 406;
    params['patron_uid'] = 184;
    params['patron_card_uid'] = 10601;
    params['pay_method'] = 'cc_on_file'; 

    print "get AutoSend Prefs for Unit Patron"
    data = eventsDb.getAutoSendPrefsForOrder( params )
    pprint.pprint(data)


    print "getEventVenueByEventId = "
    data = eventsDb.getEventVenueByEventId(202, 998)
    pprint.pprint(data)

    print "getEmployeeRole"
    data = eventsDb.getEmployeeRole(202,1)
    pprint.pprint(data)


    print "---------------------------------------------------"
    data = eventsDb.getEmailByOrderAndPayment(17816,0)
    pprint.pprint(data)
    print "--------------------------------------------------"


    print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"


    # wyatt
    ### data = eventsDb.getGroupedOrderItemsByOrder(17787)
    #data = eventsDb.getGroupedOrderItemsByOrder(17791)
    #pprint.pprint(data)
    #print "nane = {0}".format(data[0]['name'])
    #
    #thename = data[0]['name'].decode('latin-1').encode("utf-8")
    #print thename
    
    #data = eventsDb.getOrderItemsGroupedByRC(17836)
    #pprint.pprint(data)
    #data = eventsDb.getReceiptOrderPaymentData(17836, 0, 'America/New_York')
    #pprint.pprint(data)

    print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

    print "getOrderRevenueCenterData"
    data = eventsDb.getOrderRevenueCenterData(14725)
    pprint.pprint(data)

    data = eventsDb.getReceiptHeaderData(11579, 'America/DogFood')
    pprint.pprint(data)
    data = eventsDb.getReceiptOrderPaymentData(12157, 0, 'America/New_York')
    pprint.pprint(data)
    data = eventsDb.getOrderItemsGroupedByRC(11579)
    pprint.pprint(data)
    data = eventsDb.getReceiptOrderPaymentRevenueCenterData(11579, 0)
    pprint.pprint(data)

    pass
    '''
