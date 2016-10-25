from config import CheckMateConfig
import pytz, datetime
import hashlib
from keymaster import KeyMaster

class TrainingConfigDB:

    def __init__(self, db):
        self.db = db
        self.checkmateconfig = CheckMateConfig()
        
    def commitChanges(self):
        self.db.commit()

    def encryptPatron(self, customerName):
        km = KeyMaster()
        values = {}
        values['customer_name'] = customerName

        encoded = km.encryptMulti((values))

        if False == isinstance(encoded, dict):
            raise Exception("Update Error - Unable to encode new value")

        if 'customer_name' not in encoded:
            raise Exception("Update Error - Unknown encoded")

        if 'encoded' not in encoded['customer_name']:
            raise Exception("Update Error - Missing encoded names")

        encodedValue = encoded['customer_name']['encoded']

        if 'e_key' not in encoded['customer_name']:
            raise Exception("Update Error - Missing e_key")

        eKey = encoded['customer_name']['e_key']

        return encodedValue, eKey

    def hashString(self, string):
        string = "{0}{1}".format(string, self.checkmateconfig.SALT_PATRONS)
        return hashlib.sha256(string).hexdigest()


    def createTrainingEvent(self, venueUid, eventName):
        
        cursor = self.db.cursor()

        cursor.execute('''
                        INSERT INTO setup.events(
                            venue_uid,
                            event_date,
                            event_type_uid,
                            created_at
                        )VALUES(
                            %s, DATE_ADD(DATE(NOW()), INTERVAL 16  HOUR), 11, NOW())
                        ''', (venueUid))
        
        eventUid = cursor.lastrowid

        cursor.execute('''
                        INSERT INTO setup.events_x_venues(
                            event_uid,
                            venue_uid,
                            event_name,
                            subtitle,
                            created_at
                        )VALUES(
                            %s, %s, %s, "Training Event", NOW())
                        ''', (eventUid, venueUid, eventName))

        cursor.execute('''
                       INSERT INTO setup.events_x_units(
                            event_uid,
                            unit_uid,
                            created_at
                        )(SELECT %s, id, NOW() FROM setup.units WHERE venue_uid = %s)
                        ''', (eventUid, venueUid))


        cursor.execute('''
                        INSERT INTO setup.events_x_printer_sets(
                            event_uid, printer_set_uid)(SELECT %s, id FROM setup.printer_sets WHERE venue_uid = %s AND is_default = 1)''', (eventUid, venueUid))

        return eventUid
        
    def createTrainingPatron(self, venueUid, patronName):
        
        cursor = self.db.cursor()
        
        encryptedPatronName, eKey = self.encryptPatron(patronName)

        patronNameHashed = self.hashString(patronName)

        cursor.execute('''
                       INSERT INTO patrons.patrons(
                            company_name,
                            company_name_hashed,
                            is_encrypted,
                            patron_type_uid,
                            created_at
                        )VALUES(
                            %s,
                            %s,
                            1,
                            3,
                            NOW())
                        ''', (encryptedPatronName, patronNameHashed))

        patronUid = cursor.lastrowid

        cursor.execute('''
                        INSERT INTO operations.data_keys(
                            pointer_uid,
                            pointer_table,
                            pointer_schema,
                            e_key,
                            created_at
                        )VALUES(
                            %s, 'patrons', 'patrons', %s, NOW())
                        ''', (patronUid, eKey))

        cursor.execute('''
                        INSERT INTO patrons.clone_patrons(
                            id,
                            company_name,
                            patron_type_uid,
                            created_at
                        )VALUES(
                            %s, %s, 3, NOW())
                        ''', (patronUid, patronName))

        cursor.execute('''
                        INSERT INTO patrons.venues_x_suite_holders(
                            venue_uid,
                            patron_uid,
                            created_at
                        )VALUES(
                            %s, %s, NOW())
                        ''', (venueUid, patronUid))




        return patronUid

    def getVenueName(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT name FROM setup.venues WHERE id = %s''', (venueUid))
        return cursor.fetchall()[0][0]
                    

    def createTrainingEmployee(self, venueUid, employee):
         #   employee -> firstName, lastName, loginCode, role

        employeeEmail = employee.firstName[:1] + employee.lastName + "@" + self.getVenueName(venueUid).replace("\\s+", "") + ".com"
        employeeEmail = employeeEmail.lower()

        print employeeEmail
    
        cursor = self.db.cursor()

        cursor.execute('''
                        INSERT INTO setup.employees(
                            first_name,
                            last_name,
                            email,
                            employee_type_uid,
                            created_at
                        )VALUES(
                            %s, %s, %s, 3, NOW())
                        ''', (employee.firstName, employee.lastName, employeeEmail))

        employeeUid = cursor.lastrowid

        cursor.execute('''
                        INSERT INTO setup.venues_x_employees(
                            venue_uid,
                            employee_uid,
                            login_code,
                            created_at
                        )VALUES(
                            %s, %s, %s, NOW())
                        ''', (venueUid, employeeUid, employee.loginCode))

        venueEmployeeUid = cursor.lastrowid

        cursor.execute('''
                        INSERT INTO setup.employees_x_roles(
                            venue_employee_uid,
                            role_uid,
                            created_at
                        )VALUES(
                            %s, %s, NOW())
                        ''', (venueEmployeeUid, employee.role))

        return employeeUid

    def createTrainingMerchantXVenues(self, venueUid):
        cursor = self.db.cursor()
        
        cursor.execute('''
                        INSERT INTO setup.merchant_x_venues(
                            merchant_uid,
                            venue_uid,
                            merchant_type,
                            created_at
                        )VALUES(
                            1, %s, 'training', NOW())
                        ''', (venueUid))

       
    def getParMXM(self, venueUid, menuItemUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_x_menu_item_uid FROM info.par_menu_items
                          WHERE venue_uid = %s AND menu_item_uid = %s''', 
                        (venueUid, menuItemUid))
        return cursor.fetchall()[0][0]

    def getAllUnitNames(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT name FROM setup.units
                          WHERE venue_uid = %s
                          ORDER BY name ASC''', (venueUid))

        return cursor.fetchall()

    def getFirstUnitName(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT name FROM setup.units
                          WHERE venue_uid = %s
                          ORDER BY name ASC
                          LIMIT 1''', (venueUid))
        return cursor.fetchall()[0][0]

    def getFirstUnitUid(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT id FROM setup.units
                          WHERE venue_uid = %s
                          ORDER BY name ASC
                          LIMIT 1''', (venueUid))
        return cursor.fetchall()[0][0]

    def getAllUnitUids(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT id FROM setup.units
                          WHERE venue_uid = %s
                          ORDER BY name ASC''',
                         (venueUid))
        return cursor.fetchall()

    def getMerchantUid(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT merchant_uid FROM setup.merchant_x_venues
                          WHERE venue_uid = %s AND merchant_type = "food_and_beverage"''',
                       (venueUid))
        return cursor.fetchall()[0][0]

    def configureCCOnFile(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT IGNORE INTO patrons.patron_wallet (patron_uid, unit_uid, patron_card_uid, card_nickname, is_cc_on_file)
                          (SELECT patron_uid, unit_uid, 31585, 'Card on File', 1 FROM info.unit_x_patrons
                          JOIN patrons.patrons ON patrons.id = unit_x_patrons.patron_uid
                          JOIN setup.units on units.id = unit_uid
                          WHERE patrons.patron_type_uid = 3 AND venue_uid = %s);''', (venueUid))

        cursor.execute('''INSERT IGNORE INTO patrons.patron_wallet (patron_uid, unit_uid, patron_card_uid, card_nickname, is_cc_on_file)
                          (SELECT patron_uid, unit_uid, 12832, 'Card on File', 1 FROM info.unit_x_patrons
                          JOIN patrons.patrons ON patrons.id = unit_x_patrons.patron_uid
                          JOIN setup.units on units.id = unit_uid
                          WHERE patrons.patron_type_uid = 3 AND venue_uid = %s);''', (venueUid))

    def configureAuthSigners(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute('''INSERT INTO info.event_authorized_signers (event_info_uid, patron_uid, created_at) SELECT event_info.id, 75, NOW() FROM info.event_info WHERE event_uid = %s''', (eventUid))
   
    def getVenueDiscount(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''
                        SELECT amount FROM setup.venue_discount_settings WHERE venue_uid = %s LIMIT 1;
                       ''', (venueUid))
        discounts = cursor.fetchall()

        if discounts is None or len(discounts) == 0:
            return 20 #put a default 20% discount if the venue doesn't have any discounts configured
        else:
            return discounts[0][0]

    def markTrainingConfigured(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''UPDATE controlcenter.venue_build_status
                          SET training = 'complete'
                          WHERE venue_uid = %s''', (venueUid))
