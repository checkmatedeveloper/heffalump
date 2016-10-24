import CSVUtils
import IntegrationTools
from db_connection import DbConnection
from Levy_DB import Levy_Db
import uuid


with open('dump/DATA_ITEM_MASTER.TXT', 'rb') as csvFile:

     #Menu Item Row: 
     #0:item number, 1:package flag  2:item name  3:pos button 1 4:pos button 2 5:pos printer label  6:pos prod class id  7:pos product class  8:rev id  9:tax id
    reader = CSVUtils.parseCSVFile(csvFile)
    conn = DbConnection().connection
    levyDB = Levy_Db(conn)

    rowNumber = 0;
    
    for row in reader:
        print row       
 
        venues = levyDB.getAllLevyVenues()

        for venue in venues:
            venue_uid = venue[0] 
            productClassId = row[6]

            matchingParametricCategoryUid = levyDB.getParametricCategory(row[2], venue_uid)
        
            if matchingParametricCategoryUid is None:
                print ""
            else:
               print "MATCH!!!" 
               levyDB.mapCategory(venue_uid, row[6], matchingParametricCategoryUid)
