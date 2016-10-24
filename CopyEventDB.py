'''
Copy Event Data Db
'''

class CopyEventDB:

    def __init__(self, db):
        self.db = db
        


    def getFromEventOrders(self, fromEventUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            event_uid, \
                            unit_uid, \
                            patron_uid, \
                            employee_uid, \
                            order_type_uid, \
                            order_split_method_uid, \
                            order_pay_method_uid, \
                            tax_is_inclusive, \
                            is_tax_exempt, \
                            is_open, \
                            is_patron_closeable, \
                            started_at, \
                            closed_at, \
                            authorized_patron_uid, \
                            last_modified_at \
                        FROM \
                            orders.orders \
                        WHERE event_uid = %s", (fromEventUid))
        
        return cursor.fetchall()

    def addOrderToEvent(self, toEventUid, orderData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`orders` \
                            (`event_uid`, \
                             `unit_uid`, \
                             `patron_uid`, \
                             `employee_uid`, \
                             `order_type_uid`, \
                             `order_split_method_uid`, \
                             `order_pay_method_uid`, \
                             `tax_is_inclusive`, \
                             `is_tax_exempt`, \
                             `is_open`, \
                             `is_patron_closeable`, \
                             `started_at`, \
                             `closed_at`, \
                             `authorized_patron_uid`, \
                             `last_modified_at`, \
                             `created_at`) \
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                        [toEventUid] + list(orderData))

        self.db.commit()
        return cursor.lastrowid

    def getSubOrders(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_uid, \
                            revenue_center_uid, \
                            order_token, \
                            employee_uid, \
                            gratuity, \
                            device_uid, \
                            order_type_uid \
                       FROM orders.sub_orders \
                       WHERE order_uid = %s", (orderUid))
        return cursor.fetchall()

    def addSubOrder(self, orderUid, subOrderData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`sub_orders` \
                            (`order_uid`, \
                             `revenue_center_uid`, \
                             `order_token`, \
                             `employee_uid`, \
                             `gratuity`, \
                             `device_uid`, \
                             `order_type_uid`, \
                             `created_at` \
                            )VALUES(%s, %s, %s, %s, %s, %s, %s, NOW())", [orderUid] + list(subOrderData))

        self.db.commit()
        return cursor.lastrowid

    def getOrderItems(self, subOrderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            sub_order_uid, \
                            menu_x_menu_item_uid, \
                            line_id, \
                            name, \
                            price, \
                            tax_is_inclusive, \
                            tax_rate, \
                            is_voided, \
                            device_uid, \
                            notes \
                        FROM orders.order_items \
                        WHERE sub_order_uid = %s", (subOrderUid))

        return cursor.fetchall()
    
    def addOrderItem(self, subOrderUid, orderItemData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_items`( \
                            `sub_order_uid`, \
                            `menu_x_menu_item_uid`, \
                            `line_id`, \
                            `name`, \
                            `price`, \
                            `tax_is_inclusive`, \
                            `tax_rate`, \
                            `is_voided`, \
                            `device_uid`, \
                            `notes`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", [subOrderUid] + list(orderItemData))
        self.db.commit()
        return cursor.lastrowid

    def getOrderItemsXModifications(self, orderItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            order_modification_uid, \
                            modification_token \
                        FROM orders.order_items_x_modifications \
                        WHERE order_item_uid = %s", (orderItemUid))

        return cursor.fetchall()

    def addOrderItemsXModifications(self, orderItemUid, orderItemsXModificationsData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_items_x_modifications`( \
                            `order_item_uid`, \
                            `order_modification_uid`, \
                            `modification_token`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, NOW())", [orderItemUid] + list(orderItemsXModificationsData))

        self.db.commit()

    def getOrderItemOptions(self, orderItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            menu_item_uid, \
                            name, \
                            price, \
                            tax_is_inclusive, \
                            tax_rate, \
                            is_voided, \
                            menu_option_group_uid \
                        FROM orders.order_item_options \
                        WHERE order_item_uid = %s", (orderItemUid))

        return cursor.fetchall()

    def addOrderItemOptions(self, orderItemUid, orderItemOptionsData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_item_options`( \
                            `order_item_uid`, \
                            `menu_item_uid`, \
                            `name`, \
                            `price`, \
                            `tax_is_inclusive`, \
                            `tax_rate`, \
                            `is_voided`, \
                            `menu_option_group_uid`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, NOW())", [orderItemUid] + list(orderItemOptionsData))

        self.db.commit()

    def getOrderItemSplits(self, orderItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            payment_id, \
                            discount, \
                            number_of_splitters, \
                            shares_taken \
                        FROM orders.order_item_splits \
                        WHERE order_item_uid = %s", (orderItemUid))
        return cursor.fetchall()

    def addOrderItemSplits(self, orderItemUid, orderItemSplitsData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_item_splits`( \
                            `order_item_uid`, \
                            `payment_id`, \
                            `discount`, \
                            `number_of_splitters`, \
                            `shares_taken`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, NOW())", [orderItemUid] + list(orderItemSplitsData))
        self.db.commit()

    def getOrderItemOriginalPrices(self, orderItemUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            employee_uid, \
                            price \
                        FROM orders.order_item_original_prices \
                        WHERE order_item_uid = %s", (orderItemUid))
        return cursor.fetchall()

    def addOrderItemOriginalPrices(self, orderItemUid, orderItemOriginalPricesData):
        cursor = self.db.cursor()
       
        orderItemOriginalPricesData = orderItemOriginalPricesData[0]
        orderItemOriginalPricesList = [orderItemOriginalPricesData[0], orderItemUid, orderItemOriginalPricesData[1]]
        
        print str(orderItemOriginalPricesList)
        cursor.execute("INSERT INTO `orders`.`order_item_original_prices`( \
                            `employee_uid`, \
                            `order_item_uid`, \
                            `price`, \
                            `created_at` \
                        )VALUES (%s, %s, %s, NOW())", orderItemOriginalPricesList)

        self.db.commit()

    def getOrdersXRevenueCenters(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_uid, \
                            revenue_center_uid, \
                            subtotal, \
                            discount, \
                            gratuity, \
                            gratuity_source, \
                            tax, \
                            service_charge_amount, \
                            authorized_patron_uid \
                        FROM orders.orders_x_revenue_centers \
                        WHERE order_uid = %s", (orderUid))
        return cursor.fetchall()


    def addOrdersXRevenueCenters(self, orderUid, ordersXRevenueCenterData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`orders_x_revenue_centers`( \
                            `order_uid`, \
                            `revenue_center_uid`, \
                            `subtotal`, \
                            `discount`, \
                            `gratuity`, \
                            `gratuity_source`, \
                            `tax`, \
                            `service_charge_amount`, \
                            `authorized_patron_uid`, \
                            `created_at` \
                        )VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())", [orderUid] + list(ordersXRevenueCenterData))
        self.db.commit()

    def getOrderPaymentPreauths(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
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
                            is_complete \
                        FROM orders.order_payment_preauths \
                        WHERE order_uid = %s", (orderUid))

        return cursor.fetchall()

    def addOrderPaymentPreauth(self, orderUid, orderPaymentPreauthData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_payment_preauths`( \
                            `order_uid`, \
                            `merchant_uid`, \
                            `event_uid`, \
                            `unit_uid`, \
                            `device_uid`, \
                            `payment_id`, \
                            `patron_card_uid`, \
                            `amount`, \
                            `sale_closed_subtotal`, \
                            `sale_closed_tip`, \
                            `sale_closed_tax`, \
                            `unique_id`, \
                            `token_merchant_uid`, \
                            `invoice_uid`, \
                            `authorization_code`, \
                            `authorization_tolerance`, \
                            `cc_type`, \
                            `receipt_text`, \
                            `is_complete`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", [orderUid] + list(orderPaymentPreauthData))

        self.db.commit()

    def getOrderPayments(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_uid, \
                            patron_uid, \
                            payment_id, \
                            order_pay_method_uid, \
                            order_receipt_method_uid, \
                            authorized_patron_uid, \
                            manager_employee_uid, \
                            order_payment_preauth_uid, \
                            device_uid, \
                            is_guest_paid \
                        FROM orders.order_payments \
                        WHERE order_uid = %s", (orderUid))

        return cursor.fetchall()


    def addOrderPayments(self, orderUid, orderPaymentData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_payments`( \
                            `order_uid`, \
                            `patron_uid`, \
                            `payment_id`, \
                            `order_pay_method_uid`, \
                            `order_receipt_method_uid`, \
                            `authorized_patron_uid`, \
                            `manager_employee_uid`, \
                            `order_payment_preauth_uid`, \
                            `device_uid`, \
                            `is_guest_paid`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", [orderUid] + list(orderPaymentData))

        self.db.commit()
        return cursor.lastrowid

    def getOrderPaymentXRevenueCenters(self, orderPaymentUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_payment_uid, \
                            revenue_center_uid, \
                            subtotal, \
                            discount, \
                            tip, \
                            tax, \
                            service_charge_amount, \
                            service_charge_display_name \
                        FROM orders.order_payments_x_revenue_centers \
                        WHERE order_payment_uid = %s", (orderPaymentUid))
        return cursor.fetchall()

    def addOrderPaymentXRevenueCenters(self, orderPaymentUid, orderPaymentXRevenueCentersData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_payments_x_revenue_centers`( \
                            `order_payment_uid`, \
                            `revenue_center_uid`, \
                            `subtotal`, \
                            `discount`, \
                            `tip`, \
                            `tax`, \
                            `service_charge_amount`, \
                            `service_charge_display_name`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, NOW())", [orderPaymentUid] + list(orderPaymentXRevenueCentersData))

    def getOrderModifications(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            action_type, \
                            status, \
                            reason_uid, \
                            consumed, \
                            submitting_employee_uid, \
                            submitting_notes, \
                            authorizing_employee_uid, \
                            authorizing_notes \
                        FROM orders.orders_x_modifications \
                        JOIN orders.order_modifications ON order_modifications.id = orders_x_modifications.order_modification_uid \
                        WHERE order_uid = %s", (orderUid))
        return cursor.fetchall()

    def addOrderModifications(self, orderUid, orderModificationData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_modifications`( \
                            `action_type`, \
                            `status`, \
                            `reason_uid`, \
                            `consumed`, \
                            `submitting_employee_uid`, \
                            `submitting_notes`, \
                            `authorizing_employee_uid`, \
                            `authorizing_notes`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, %s, %s, NOW())", orderModificationData)
        self.db.commit()
        return cursor.lastrowid

    def addOrderXOrderModification(self, orderUid, orderModificationUid):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`orders_x_modifications`( \
                            `order_uid`, \
                            `order_modification_uid`, \
                            `created_at` \
                        )VALUES(%s, %s, NOW())", (orderUid, orderModificationUid)) 
        self.db.commit()

    def getOrdersXGratuities(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_uid, \
                            order_gratuity_type_uid, \
                            amount, \
                            minimum, \
                            maximum, \
                            apply_to_all_revenue_centers \
                        FROM orders.orders_x_gratuities \
                        WHERE order_uid = %s", (orderUid))

        return cursor.fetchall()       
                
    def addOrdersXGratuities(self, orderUid, ordersXGratuitiesData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`orders_x_gratuities`( \
                            `order_uid`, \
                            `order_gratuity_type_uid`, \
                            `amount`, \
                            `minimum`, \
                            `maximum`, \
                            `apply_to_all_revenue_centers`, \
                            `created_at` \
                        )VALUES(%s, %s, %s, %s, %s, %s, NOW())", [orderUid] + list(ordersXGratuitiesData))
        self.db.commit()
        return cursor.lastrowid

    def getOrderGratuitiesXRevenueCenters(self, orderGratuityUid):
        cursor = self.db.cursor() 
        cursor.execute("SELECT \
                            id, \
                            order_gratuity_uid, \
                            revenue_center_uid \
                        FROM orders.order_gratuities_x_revenue_centers \
                        WHERE order_gratuity_uid = %s", (orderGratuityUid))
        return cursor.fetchall()

    def addOrderGratuitiesXRevenueCenters(self, orderGratuityUid, orderGratuityXRevenueCentersData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_gratuities_x_revenue_centers`( \
                            `order_gratuity_uid`, \
                            `revenue_center_uid`, \
                            `created_at` \
                        )VALUES(%s, %s, NOW())", [orderGratuityUid] + list(orderGratuityXRevenueCentersData))
        self.db.commit()
        return cursor.lastrowid

    def getOrdersXDiscounts(self, orderUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_uid, \
                            order_discount_type_uid, \
                            discount_reason_uid, \
                            note, \
                            amount, \
                            apply_to_all_revenue_centers \
                        FROM orders.orders_x_discounts \
                        WHERE order_uid = %s", (orderUid))
        return cursor.fetchall()
    
    def addOrdersXDiscounts(self, orderUid, ordersXDiscountsData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`orders_x_discounts`( \
                            `order_uid`, \
                            `order_discount_type_uid`, \
                            `discount_reason_uid`, \
                            `note`, \
                            `amount`, \
                            `apply_to_all_revenue_centers`, \
                            `created_at`)VALUES(%s, %s, %s, %s, %s, %s, NOW())", [orderUid] + list(ordersXDiscountsData))
        self.db.commit()
        return cursor.lastrowid

    def getOrderDiscountsXRevenueCenters(self, orderDiscountUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id, \
                            order_discount_uid, \
                            revenue_center_uid \
                        FROM orders.order_discounts_x_revenue_centers \
                        WHERE order_discount_uid = %s", (orderDiscountUid))
        return cursor.fetchall()
    
    def addOrderDiscountsXRevenueCenters(self, orderDiscountUid, orderDiscountsXRevenueCenterData):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO `orders`.`order_discounts_x_revenue_centers`( \
                            `order_discount_uid`, \
                            `revenue_center_uid`, \
                            `created_at` \
                        )VALUES(%s, %s, NOW())", [orderDiscountUid] + list(orderDiscountsXRevenueCenterData))
        self.db.commit()
        return cursor.lastrowid

 
