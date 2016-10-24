class MenuItemImageCopyDB:

    def __init__(self, db):
        self.db = db

    def getMenuItemsWithoutImages(self, venueUid):
        cursor = self.db.cursor()
        cursor.execute('''SELECT menu_items.id, menu_items_levy.levy_item_number, display_name FROM menus.menu_items
                          LEFT JOIN media.images ON images.pointer_uid = menu_items.id AND image_type = "menu_items"
                          JOIN integrations.menu_items_levy ON menu_items.id = menu_items_levy.menu_item_uid
                          WHERE menu_items.venue_uid = %s AND images.id is NULL;''', (venueUid))

        return cursor.fetchall()

    def findMatchingImagePointerUid(self, levyItemNumber):
        cursor = self.db.cursor()
        cursor.execute('''SELECT
                            venue_uid,
                            pointer_uid AS 'menu_item_uid'
                          FROM media.images
                          JOIN integrations.menu_items_levy ON menu_items_levy.menu_item_uid = images.pointer_uid AND images.image_type = 'menu_items'
                          WHERE menu_items_levy.levy_item_number = %s
                          AND images.image_type = "menu_items"
                          LIMIT 1''', (levyItemNumber))
        pointerUid = cursor.fetchone()
        if pointerUid is None:
            return False
        else:
            return pointerUid

    def insertMenuItemImage(self, destinationVenueUid, destinationMenuItemUid, imageHash):

        cursor = self.db.cursor()
        
        cursor.execute('''INSERT INTO `media`.`images`(
                            `pointer_uid`,
                            `image_type`,
                            `image_hash`,
                            `created_at`,
                            `image_version`)
                          VALUES(%s, 'menu_items', %s, NOW(), 1)''',
                        (destinationMenuItemUid, imageHash))

        cursor.execute('''UPDATE menus.menu_items
                          SET show_image = 1
                          WHERE id = %s''', 
                        (destinationMenuItemUid))

        self.db.commit()

