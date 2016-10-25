#Avnet Data Export
import sys
from db_connection import DbConnection
import HipChat
import csv
import traceback
import paramiko

try:
    EXPORT_FILE_PATH = "/data/Avnet_Export/"

    conn = DbConnection().connection
    venueUid = sys.argv[1]

    cursor = conn.cursor()
    cursor.execute("SELECT DATE(CONVERT_TZ(NOW(), 'UTC', (SELECT local_timezone_long FROM setup.venues WHERE id = %s)))", (venueUid))
    date = cursor.fetchone()[0]
    formattedDate = date.strftime("%Y%m%d")

    ##########
    # ORDERS #
    ##########
    orderFilename = "order_" + formattedDate + ".csv"

    orderCursor = conn.cursor()
    orderCursor.execute('''SELECT 
                        orders.event_uid, 
                        events_x_venues.event_name, 
                        orders.id AS 'order_uid',
                        orders.unit_uid, 
                        units.name AS 'suite', 
                        orders.patron_uid,
                        clone_patrons.company_name AS 'customer', 
                        order_types.id AS order_type_uid,
                        order_types.display_name AS 'order_type', 
                        employees.id AS employee_uid,
                        employees.last_name AS 'primary employee',
                        started_at AS 'opened (UTC)', 
                        closed_at AS 'closed (UTC)',
                        events.event_date 
                    FROM orders.orders 
                    JOIN setup.events on orders.event_uid = events.id 
                    JOIN setup.events_x_venues ON events_x_venues.event_uid = orders.event_uid 
                    JOIN setup.units ON units.id = orders.unit_uid 
                    JOIN patrons.clone_patrons ON clone_patrons.id = orders.patron_uid 
                    JOIN setup.employees ON employees.id = orders.employee_uid 
                    JOIN orders.order_types ON order_types.id = orders.order_type_uid 
                    LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                    LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                    WHERE (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen')) 
                    AND orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                    ORDER BY started_at''', (venueUid, venueUid))

    orderData = orderCursor.fetchall()
    with open (EXPORT_FILE_PATH + orderFilename, 'wb') as fout:
        writer = csv.writer(fout, delimiter='|')
        writer.writerows(orderData)

    #########
    # ITEMS #
    #########
    itemFilename = "items_" + formattedDate + ".csv"

    itemsCursor = conn.cursor()
    itemsCursor.execute('''SELECT 
                            orders.id AS 'order_uid',
                            order_items.menu_x_menu_item_uid, 
                            order_items.name AS 'item_name', 
                            order_items.price AS 'item_price', 
                            COUNT(*) AS 'item_qty' 
                        FROM orders.order_items 
                        JOIN orders.sub_orders ON sub_orders.id = order_items.sub_order_uid 
                        JOIN orders.orders ON orders.id = sub_orders.order_uid 
                        LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                        LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                        WHERE (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen')) 
                        AND orders.event_uid IN (SELECT 
                                                    events.id
                                                FROM setup.events 
                                                JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                WHERE events.venue_uid = %s
                                                AND events_x_venues.venue_uid = %s
                                                AND event_date > '2016-04-01'
                                                AND event_type_uid = 1
                                                ORDER BY event_date) 
                        GROUP BY orders.id, order_items.menu_x_menu_item_uid 
                        ORDER BY orders.id''', (venueUid, venueUid))
    itemData = itemsCursor.fetchall()
    with open(EXPORT_FILE_PATH + itemFilename, 'wb') as fout:
        writer = csv.writer(fout, delimiter='|')
        writer.writerows(itemData)

    ############
    # PAYMENTS #
    ############
    paymentFilename = "payments_" + formattedDate + ".csv"

    paymentsCursor = conn.cursor()
    paymentsCursor.execute('''SELECT 
                                orders.id AS 'order_uid', 
                                SUM(oprc.subtotal) AS 'subtotal', 
                                SUM(oprc.discount) AS 'discount', 
                                SUM(oprc.tax) AS 'tax', 
                                SUM(oprc.tip) AS 'tip',
                                order_pay_methods.id AS order_pay_method_uid, 
                                order_pay_methods.display_name 'payment method',
                                order_receipt_methods.id AS order_receipt_method_uid, 
                                order_receipt_methods.display_name AS 'receipt method' 
                            FROM orders.order_payments 
                            JOIN orders.order_pay_methods ON order_pay_methods.id = order_payments.order_pay_method_uid 
                            JOIN orders.order_receipt_methods ON order_receipt_methods.id = order_payments.order_receipt_method_uid 
                            JOIN orders.order_payments_x_revenue_centers AS oprc ON oprc.order_payment_uid = order_payments.id 
                            JOIN orders.orders ON orders.id = order_payments.order_uid 
                            LEFT JOIN orders.orders_x_modifications ON orders_x_modifications.order_uid = orders.id 
                            LEFT JOIN orders.order_modifications AS om ON om.id = orders_x_modifications.order_modification_uid 
                            WHERE (om.action_type IS NULL OR !(om.action_type = 'void' OR om.action_type = 'void_and_reopen')) 
                            AND orders.event_uid IN (SELECT 
                                                        events.id
                                                    FROM setup.events 
                                                    JOIN setup.event_controls ON event_controls.event_uid = events.id
                                                    JOIN setup.events_x_venues ON events_x_venues.event_uid = events.id
                                                    WHERE events.venue_uid = %s
                                                    AND events_x_venues.venue_uid = %s
                                                    AND event_date > '2016-04-01'
                                                    AND event_type_uid = 1
                                                    ORDER BY event_date) 
                            GROUP BY order_payments.id''', (venueUid, venueUid))
    paymentData = paymentsCursor.fetchall()
    with open(EXPORT_FILE_PATH + paymentFilename, 'wb') as fout:
        writer = csv.writer(fout, delimiter='|')
        writer.writerows(paymentData)

#    exit() #remove this when we actualy want to transfer events

    #SFTP TIME!!!!!!!

    host = "50.198.6.105"
    port = 22
    username = "Parametric"
    password = "QzXh4389jmKP89Pwwm8"

    transport = paramiko.Transport((host, port))
    transport.connect(username = username, password = password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    UPLOAD_FILE_PATH = ''
    
    sftp.put(EXPORT_FILE_PATH + orderFilename, UPLOAD_FILE_PATH + orderFilename)
    sftp.put(EXPORT_FILE_PATH + itemFilename, UPLOAD_FILE_PATH + itemFilename)
    sftp.put(EXPORT_FILE_PATH + paymentFilename, UPLOAD_FILE_PATH + paymentFilename)

    sftp.close()

    transport.close()


    HipChat.sendMessage("Avnet Data Export Complete.  Files transfered via SFTP, we're all done here.", "Avnet Export", HipChat.TECH_ROOM, HipChat.COLOR_GREEN)

except Exception as e:
    tb = traceback.format_exc()
    HipChat.sendMessage("Avnet Data Export Script Crashed: " + str(tb), "Avnet Export", HipChat.TECH_ROOM, HipChat.COLOR_RED) 




