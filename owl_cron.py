from db_connection import DbConnection
import sys
import _mysql_exceptions

def populateOwlQueue(db):
    
    #Get all of the owls who's time is now
    owlCursor = db.cursor()
    owlCursor.execute("SELECT * FROM owls.owls WHERE prompt_start_date < NOW() AND (prompt_end_date > NOW() OR prompt_end_date IS NULL)")
    currentOwls = owlCursor.fetchall()
    tabletsCursor = db.cursor()
    queueData = list()
    
    #for each of those owls find the tablets who are set to recieve them and have not already received them this interval:
    for owl in currentOwls :
        print "OWL " + str(owl);        
        tabletsCursor.execute("SELECT * FROM owls.owls_x_tablets\
                               WHERE owls_x_tablets.owl_uid = %s\
                               AND (NOW() > DATE_ADD(last_responded, INTERVAL interval_seconds second)\
                               OR last_responded IS NULL);", owl[0])
        tablets =  tabletsCursor.fetchall()
        
        for tablet in tablets :
            print "TABLET "  + str(tablet)
            queueData.append((tablet[2], tablet[1]))
            
    #insert an entry into the queue for them
    owlQueueCursor = db.cursor()
    for data in queueData:
        print "DATA " +  str(data)
        try:
            owlQueueCursor.execute("INSERT INTO owls.owl_queue (device_uid,owl_uid, is_delivered, created_at) VALUES (%s, %s, 0, NOW())",data)
            db.commit() 
            
            
            syncFlagsCursor = db.cursor()
            syncFlagsCursor.execute("UPDATE tablets.sync_flags\
                                     SET sync_owls = 1\
                                     WHERE device_id = %s",
                                     data[0])
            db.commit()
                                    
        except _mysql_exceptions.IntegrityError:
            print "Row Exists"

def purgeExpiredQueueRows(db):
    
    queueCursor = db.cursor()
    queueCursor.execute("SELECT owl_queue.id, owls.id FROM owls.owl_queue JOIN owls.owls ON owl_uid = owls.id\
                         WHERE owls.queue_timeout_in_seconds > 0 AND NOW() > DATE_ADD(owl_queue.created_at, INTERVAL owls.queue_timeout_in_seconds SECOND)")
    queueIdList = queueCursor.fetchall()
    
    if len(queueIdList) > 0:

        listString = "("

        for id in queueIdList:
            listString += str((int(id[0])))
            listString += ', '

        listString = listString[:-2] + ")"
        print listString    

    
        query = "DELETE FROM owls.owl_queue WHERE owl_queue.id IN %s" % listString
        queueCursor.execute(query);
        db.commit()


        actionsCursor = db.cursor();
        for id in queueIdList:
            actionsCursor.execute( "INSERT INTO owls.owl_actions (owl_uid, response_action, created_at, updated_at) VALUES (%s, 'queue_timeout', NOW(), CURRENT_TIMESTAMP)", id[1])

        db.commit()
    #print expiredQueueIds

    #print len(expiredQueueIds)
    
    
#-------------------------------------------------------------------------------------#


db  = DbConnection().connection
populateOwlQueue(db)

purgeExpiredQueueRows(db)

#Force garbage collection of database link
db = None


