from config import CheckMateConfig

class UCRSSDB:

    def __init__(self, db):
        """Initializes a new instance of a Levy_Db object
        
        params:
        db -- a mysqldb database connection object
        """
        self.db = db
        self.checkmateconfig = CheckMateConfig()

    def findEventXVenueUid(self, eventUid):

        cursor = self.db.cursor()
        cursor.execute("SELECT id \
                        FROM setup.events_x_venues \
                        WHERE event_uid = %s AND venue_uid = 201",
                        (eventUid))
        return cursor.fetchone()[0]

    def findEventByDate(self, date):

        print "Date in DB: " + date

        cursor = self.db.cursor()

        cursor.execute("SELECT id \
                        FROM setup.events \
                        WHERE venue_uid = 201 \
                        AND event_date = %s",
                        date)

        
        events = cursor.fetchall()
        
        return events

    def findEventsByDateRange(self, startDate, endDate):
        print "Looking for events from " + str(startDate) + " to " +  str(endDate)
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            id \
                        FROM setup.events \
                        WHERE event_date >= %s AND event_date <= %s\
                        AND venue_uid = 201",
                        (startDate, endDate))
        return cursor.fetchall() 

    def findEventsXVenue(self, eventUid):
        cursor = self.db.cursor()
        cursor.execute("SELECT \
                            event_name, \
                            subtitle, \
                            description \
                        FROM setup.events_x_venues \
                        WHERE \
                            event_uid = %s \
                            AND venue_uid = 201", (eventUid))

        return cursor.fetchall()

