import sys
from db_connection import DbConnection
import traceback
from MenuItemImageCopyDB import MenuItemImageCopyDB
import shutil
import os.path
import hashlib
import boto3


venueUids = sys.argv[1:] 

conn = DbConnection().connection
dbCore = MenuItemImageCopyDB(conn)

NFS_MENU_ITEM_IMAGES_PATH = '/data/media/%s/images/menu_items/%s.png' #where %s is venue uid, menu_item_uid

#TODO
CDN_BUCKET = 'parametric-cdn'

s3 = boto3.resource('s3')

def copyMenuItemImage(destinationVenueUid, destinationMenuItemUid, levyItemNumber):
    matchingPointer = dbCore.findMatchingImagePointerUid(levyItemNumber)
    
    if matchingPointer != False:
        print "Match Found!"
        
        sourceVenueUid, sourceMenuItemUid = matchingPointer        

        #copy the image file from source to destination
        sourceFilePath = NFS_MENU_ITEM_IMAGES_PATH % (sourceVenueUid, sourceMenuItemUid)
        destinationFilePath = NFS_MENU_ITEM_IMAGES_PATH % (destinationVenueUid, destinationMenuItemUid)
        
        if os.path.isfile(sourceFilePath):
            print "COPYING:"
            print "    " + sourceFilePath
            print "TO:"
            print "    " + destinationFilePath
    
            shutil.copyfile(sourceFilePath, destinationFilePath)
        else:
            raise Exception("Source image doesn't exist, can't copy")
          

        #insert a row into media.images AND set show_image high for destination
        imageHash = hashlib.md5(open(sourceFilePath).read()).hexdigest();
        dbCore.insertMenuItemImage(destinationVenueUid, destinationMenuItemUid, imageHash)

        #upload to CDN
        imageData = open(destinationFilePath, 'rb')
        s3ImagePath = "menu/items/%s/%s_1.png" % (destinationVenueUid, destinationMenuItemUid)
        print "Uploading to: " + s3ImagePath
        s3.Bucket(CDN_BUCKET).upload_file(destinationFilePath, s3ImagePath)


###############
# -- START -- #
###############

for venueUid in venueUids:

#    bucket =  s3.Bucket(CDN_BUCKET)
#    bucket.upload_file("test.txt", "menu/items/201/test_1.txt")
#    exit()

    print "Copying menu_item images for venue: " + str(venueUid)

    #Get all images without image_uids
    menuItemsWithoutImages = dbCore.getMenuItemsWithoutImages(venueUid)

    for menuItem in menuItemsWithoutImages:
        menuItemUid, levyItemNumber, displayName = menuItem

        try:        
            copyMenuItemImage(venueUid, menuItemUid, levyItemNumber)
        except: 
            print "Problem copying image"
