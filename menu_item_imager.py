from db_connection import DbConnection
import os.path
import hashlib

MENU_ITEM_IMAGE_PATH = "/data/media/202/images/menu_items/"

db = DbConnection().connection
menuItemCursor = db.cursor()

menuItemCursor.execute("SELECT * FROM menus.menu_items WHERE show_image =1");

menuItems = menuItemCursor.fetchall()

for menuItem in menuItems:
    image = MENU_ITEM_IMAGE_PATH + str(menuItem[0]) + ".png";
    if os.path.isfile(image):
        fileHash = hashlib.md5(open(image).read()).hexdigest()
        cursor = db.cursor()
        cursor.execute("INSERT INTO media.images(pointer_uid, image_type, image_hash, created_at)\
                        VALUES ({0}, 'menu_items', '{1}', NOW())".format(menuItem[0], fileHash))

        db.commit()
    else:
        print "Can't find it: " + image
