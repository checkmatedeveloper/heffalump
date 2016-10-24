#Avnet Data Export
import sys
from db_connection import DbConnection
import HipChat
import csv
import traceback
import paramiko

EXPORT_FILE_PATH = "/data/Avnet_Export/"
UPLOAD_FILE_PATH = ''

conn = DbConnection().connection
venueUid = sys.argv[1]



def getFileName(fileNameRoot):
    cursor = conn.cursor()
    cursor.execute("SELECT DATE(CONVERT_TZ(NOW(), 'UTC', (SELECT local_timezone_long FROM setup.venues WHERE id = %s)))", (venueUid))
    date = cursor.fetchone()[0]
    formattedDate = date.strftime("%Y%m%d")
    return fileNameRoot + "_" + formattedDate + ".csv" 

def exportData(fileName, fields, rows):
    with open (EXPORT_FILE_PATH + fileName, 'wb') as fout:
        writer = csv.writer(fout, delimiter='|')
        writer.writerow([ i[0] for i in fields])
        writer.writerows(rows)
    


try:

        #    exit() #remove this when we actualy want to transfer events

    cursor = conn.cursor()

    # ORDERS #
    ordersFileName = getFileName("orders")
    cursor.execute('''
                    SELECT 
                        orders.id,
                        orders.event_uid,
                        orders.unit_uid,
                        orders.patron_uid,
                        orders.employee_uid,
                        orders.order_split_method_uid,
                        orders.is_tax_exempt,
                        orders.order_type_uid,
                        order_types.name,
                        orders.started_at,
                        orders.closed_at
                    FROM orders.orders 
                    JOIN orders.order_types ON order_types.id = orders.order_type_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date)
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen')) ;
                    ''', (venueUid, venueUid))

    fields = cursor.description
    rows = cursor.fetchall()
    exportData(ordersFileName, fields, rows)


    # ORDER ITEMS #
    orderItemsFileName = getFileName('order_items')
    cursor.execute('''
                    SELECT
                        order_items.id AS order_item_uid, 
                        sub_orders.order_uid, 
                        order_items.menu_x_menu_item_uid,
                        order_items.line_id,
                        order_items.name,
                        order_items.price,
                        order_items.created_at
                    FROM orders.order_items
                    JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid
                    JOIN orders.orders ON orders.id = sub_orders.order_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(orderItemsFileName, fields, rows)

    # ORDER ITEM OPTIONS #
    orderItemOptionsFileName = getFileName('order_item_options')
    cursor.execute('''
                    SELECT
                        order_item_options.*
                    FROM orders.order_item_options
                    JOIN orders.order_items ON order_items.id = order_item_options.order_item_uid
                    JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid
                    JOIN orders.orders ON orders.id = sub_orders.order_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(orderItemOptionsFileName, fields, rows)

    # ORDER PAYMENTS #
    orderPaymentsFileName = getFileName('order_payments')
    cursor.execute('''
                    SELECT
                        order_payments.id AS order_payment_uid,
                        order_payments.order_uid,
                        order_payments.patron_uid,
                        order_payments.payment_id,
                        order_payments.order_pay_method_uid,
                        order_payments.order_receipt_method_uid,
                        order_payments.created_at
                    FROM orders.order_payments
                    JOIN orders.orders ON orders.id = order_payments.order_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(orderPaymentsFileName, fields, rows)

    # ORDER PAYMENTS X REVENUE CENTERS #
    orderPaymentsXRevenueCentersFileName = getFileName('order_payments_x_revenue_centers')
    cursor.execute('''
                    SELECT
                        oprc.order_payment_uid,
                        oprc.revenue_center_uid,
                        oprc.subtotal,
                        oprc.discount,
                        oprc.tip,
                        oprc.tax,
                        oprc.service_charge_amount
                    FROM orders.order_payments_x_revenue_centers AS oprc
                    JOIN orders.order_payments ON order_payments.id = oprc.order_payment_uid
                    JOIN orders.orders ON orders.id = order_payments.order_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(orderPaymentsXRevenueCentersFileName, fields, rows)    

    # ORDER PAYMENT EMAILS #
    orderPaymentEmailsFileName = getFileName('order_payment_emails')
    cursor.execute('''
                    SELECT
                        order_payment_uid,
                        clone_patron_emails.email
                    FROM orders.order_payment_emails
                    JOIN patrons.clone_patron_emails ON clone_patron_emails.id = order_payment_emails.patron_email_uid
                    JOIN orders.order_payments ON order_payments.id = order_payment_emails.order_payment_uid
                    JOIN orders.orders ON orders.id = order_payments.order_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(orderPaymentEmailsFileName, fields, rows)

    # PATRONS #
    patronsFileName = getFileName('patrons')
    cursor.execute('''
                    SELECT
                        orders.id AS order_uid,
                        clone_patrons.id AS patron_uid,
                        clone_patrons.company_name AS 'customer'
                    FROM orders.orders
                    JOIN patrons.clone_patrons ON clone_patrons.id = orders.patron_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(patronsFileName, fields, rows)

    # PATRON CARDS #
    patronCardsFileName = getFileName('patron_cards')
    cursor.execute('''
                    SELECT
                        order_payments.id AS order_payment_uid,
                        order_payment_credit_cards.patron_card_uid,
                        clone_patron_cards.card_type,
                        clone_patron_cards.card_name
                    FROM orders.order_payments
                    JOIN orders.orders ON orders.id = order_payments.order_uid
                    JOIN orders.order_payment_credit_cards ON order_payment_credit_cards.order_payment_uid = order_payments.id
                    JOIN patrons.clone_patron_cards ON clone_patron_cards.id = order_payment_credit_cards.patron_card_uid
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date)
                    AND (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen'));
                    ''', (venueUid, venueUid))
    fields = cursor.description
    rows = cursor.fetchall()
    exportData(patronCardsFileName, fields, rows)
    
    #SFTP TIME!!!!!!!

#    exit()

    host = "50.198.6.105"
    port = 22
    username = "Parametric"
    password = "QzXh4389jmKP89Pwwm8"

    transport = paramiko.Transport((host, port))
    transport.connect(username = username, password = password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    
    sftp.put(EXPORT_FILE_PATH + ordersFileName,                         UPLOAD_FILE_PATH + ordersFileName)
    sftp.put(EXPORT_FILE_PATH + orderItemsFileName,                     UPLOAD_FILE_PATH + orderItemsFileName)
    sftp.put(EXPORT_FILE_PATH + orderItemOptionsFileName,               UPLOAD_FILE_PATH + orderItemOptionsFileName)
    sftp.put(EXPORT_FILE_PATH + orderPaymentsFileName,                  UPLOAD_FILE_PATH + orderPaymentsFileName)
    sftp.put(EXPORT_FILE_PATH + orderPaymentsXRevenueCentersFileName,   UPLOAD_FILE_PATH + orderPaymentsXRevenueCentersFileName)
    sftp.put(EXPORT_FILE_PATH + orderPaymentEmailsFileName,             UPLOAD_FILE_PATH + orderPaymentEmailsFileName)
    sftp.put(EXPORT_FILE_PATH + patronsFileName,                        UPLOAD_FILE_PATH + patronsFileName)
    sftp.put(EXPORT_FILE_PATH + patronCardsFileName,                    UPLOAD_FILE_PATH + patronCardsFileName)

    sftp.close()

    transport.close()


    HipChat.sendMessage("Avnet Data Export Complete.  Files transfered via SFTP, we're all done here.", "Avnet Export", HipChat.TECH_ROOM, HipChat.COLOR_GREEN)

except Exception as e:
    tb = traceback.format_exc()
    print tb
    HipChat.sendMessage("Avnet Data Export Script Crashed: " + str(tb), "Avnet Export", HipChat.TECH_ROOM, HipChat.COLOR_RED) 




