#!/usr/bin/env python

# import MySQLdb
# from db_connection import DbConnection
from keymaster import KeyMaster
import datetime
from dateutil import tz
from datetime import timedelta
import timezone_converter
import json
from config import CheckMateConfig
import sys

class ReportsDb:
    '''A class for abstracting Dinexus mySQL queries to retrieve Report Data'''

    def __init__(self, db):
        self.db = db
        # prepare a cursor object using cursor() method
        self.dbc = self.db.cursor()
        
        checkmateconfig = CheckMateConfig()

        # direct output to logfile 
        day_of_week = datetime.datetime.today().strftime('%A')

        DAILY_BATCH = 'daily'
        WEEKLY_BATCH = 'weekly'
        MONTHLY_BATCH = 'monthly'      
        YEARLY_BATCH = 'yearly'
        EVERY_EVENT_BATCH = 'every_event' 
        ONCE_BATCH = 'once'

        self.GET_EVENTS_BY_DATE_RANGE = 'setup.get_events_by_date_range'
        
        self.batch_range_options = {
            DAILY_BATCH : 'dailyRange',
            WEEKLY_BATCH : 'weeklyRange',
            MONTHLY_BATCH : 'monthlyRange',
            YEARLY_BATCH : 'yearlyRange',
            EVERY_EVENT_BATCH: 'eventLockRange',
            ONCE_BATCH: 'onceRange'
        }


    """
    Get batch by uid
    
    Parameters
    ----------
    batch_uid: int 
        
    Returns
    ------
    out: bool

    """

    def getBatchByUid( self, batch_uid ):

        if batch_uid == None:
            return False

        self.dbc.execute( "SELECT\
                            batch.id as batch_uid,\
                            batch.name as batch_name,\
                            venue_uid\
                        FROM\
                            reports.batch_schedules\
                        LEFT JOIN reports.batch ON batch.batch_schedule_uid = batch_schedules.id\
                        JOIN reports.batches_x_venues ON batches_x_venues.batch_uid = batch.id\
                        WHERE\
                            batch.id = " + str(batch_uid) + "\
                        AND\
                            batch.is_active = 1")

        row = self.dbc.fetchone();

        if row != None:
            r  = {}
            r['batch_uid'] = row[0] 
            r['batch_name'] = row[1]
            r['venue_uid'] = row[2]
            r['range'] = 'once'
            
            return r
        
        return False

    """
    Get the batches for 'every_event' for a specific venue
    
    Parameters
    ----------
    venue_uid: int 
        
    Returns
    ------
    out: bool

    """
    def getEventLockBatches( self, venue_uid ):
       
        if venue_uid == None: 
            return False

        self.dbc.execute( "SELECT\
                            batch.id as batch_uid,\
                            batch.name as batch_name,\
                            venue_uid\
                        FROM\
                            reports.batch_schedules\
                        LEFT JOIN reports.batch ON batch.batch_schedule_uid = batch_schedules.id\
                        JOIN reports.batches_x_venues ON batches_x_venues.batch_uid = batch.id\
                        WHERE\
                            batch_schedules.frequency = %s\
                        AND\
                            batch.is_active = 1\
                        AND\
                            batches_x_venues.venue_uid = %s", ( 'every_event', venue_uid ))

        rows = self.dbc.fetchall();

        data = [];

        for row in rows:
            r  = {}
            r['batch_uid'] = row[0]
            r['batch_name'] = row[1]
            r['venue_uid'] = row[2]
            r['range'] = 'every_event'
            data.append(r)


        return data


    """
    Get batch_uids of a frequency where at least there is at least one batches_x_venues row 
    
    Parameters
    ----------
    frequency: string
        
    Returns
    ------
    out: bool

    """
    def getBatchByFrequency( self, frequency ):

        if frequency == None:
            return False

        self.dbc.execute( "SELECT\
                            batch.id as batch_uid,\
                            batch.name as batch_name,\
                            venue_uid\
                        FROM\
                            reports.batch_schedules\
                        LEFT JOIN reports.batch ON batch.batch_schedule_uid = batch_schedules.id\
                        JOIN reports.batches_x_venues ON batches_x_venues.batch_uid = batch.id\
                        WHERE\
                            batch_schedules.frequency = '" + str(frequency) + "'\
                        AND\
                            batch.is_active = 1")    

        rows = self.dbc.fetchall();
        
        data = [];
        
        for row in rows:
            r  = {}
            r['batch_uid'] = row[0]
            r['batch_name'] = row[1]
            r['venue_uid'] = row[2]
            r['range'] = frequency
            data.append(r)

        return data


    '''
    Get the general data needs to retrieve and display the report
    
    Parameters
    ----------
    params: dict
        venue_uid: int
        batch_uid: int
        range: string
        event_uid: int opt 
    Return
    ---------
    array

    '''
    def getBatchData( self, params ):
        
        if 'venue_uid' not in params or  params['venue_uid'] == None:
            return False

        if 'batch_uid' not in params or params['batch_uid'] == None:
            return False

        # get venue name and timezone
        venue_info = self.getVenueInfo( params['venue_uid'] )
        if venue_info == False:
            return False
        
        params['venue_name'] = venue_info['name'] if 'name' in venue_info else ''
        params['timezone'] = venue_info['timezone'] if 'timezone' in venue_info else ''
        params['events'] = [];

        # get reports in batch
        params['reports'] = self.getBatchReportsInfo( params['batch_uid'] )
    
        if params['reports'] == False:
            return False       
 

        # get batch_emails
        params['emails'] = self.getBatchEmails( params['batch_uid'] )
        
        if params['emails'] == False:
            return False

        # get the correct range and events
        if params['range'] == 'every_event':
            
            # get info about one event 
            self.dbc.execute( 'SELECT\
                                    events_x_venues.event_uid as uid,\
                                    CONVERT_TZ(event_date, \'GMT\', %s) AS date,\
                                    events_x_venues.event_name as name,\
                                    events_x_venues.subtitle as subtitle,\
                                    event_type_groups.name as type\
                                FROM\
                                    setup.events_x_venues\
                                JOIN setup.events ON events.id = events_x_venues.event_uid\
                                LEFT JOIN setup.event_types on events.event_type_uid = event_types.id\
                                LEFT JOIN setup.event_type_groups on event_types.event_type_group_uid = event_type_groups.id\
                                WHERE\
                                    event_uid = %s\
                                AND\
                                    event_type_groups.name != "training"',( params['timezone'] ,params['event_uid'] ) )

            row = self.dbc.fetchone()

            if row == None:
                return False

            e = {}
            e['uid'] = None if row[0] == None else row[0]
            e['name'] = None if row[2] == None else row[2]
            e['subtitle'] = None if row[3] == None else row[3]
            e['type'] = None if row[4] == None else row[4]
            e['date'] = None if row[1] == None else row[1] 

            params['events'].append(e)

            params['batch_range'] = {}
            params['batch_range']['start'] = None if e['date'] == None else e['date']
            params['batch_range']['end'] = None if e['date'] == None else e['date']

        else:
            # get a date range to look up event_uids and info
            if  params['range'] == 'once':
                #find range in db
                self.dbc.execute( 'SELECT\
                                        start_date,\
                                        end_date\
                                   FROM\
                                        reports.batch\
                                   WHERE\
                                        batch.id = ' + str(params['batch_uid']))

                row = self.dbc.fetchone()
 
                if row == None:
                    return False 

                params['batch_range'] = {}
                params['batch_range']['start'] = None if row[0] == None else row[0]
                params['batch_range']['end'] = None if row[1] == None else row[1]
                
            else:

                # find range from frequency
                date_method = getattr( self, self.batch_range_options[ params['range'] ])
                params['batch_range'] = date_method()
               
            # get events in date range
            hideLocked = 0
            self.dbc.execute('CALL ' + self.GET_EVENTS_BY_DATE_RANGE + '(%s, %s, %s, %s, %s, @errMessage)', ( str( params['batch_range']['start'] ), str( params['batch_range']['end'] ), str( params['timezone'] ), str( params['venue_uid'] ) , hideLocked ) )
        
            rows = self.dbc.fetchall()
            self.dbc.nextset()
        
            if rows == None:
                params['events'] = []
                return params
            
            for row in rows:

                e = {}
                e['uid'] = None if row[0] == None else row[0]
                e['name'] = None if row[3] == None else row[3]
                e['subtitle'] = None if row[4] == None else row[4]
                e['type'] = None if row[1] == None else row[1]
                e['date'] = None if row[2] == None else row[2] 

                params['events'].append(e)
        return params

    def dailyRange( self ):
    
        yesterday =  datetime.date.today() - datetime.timedelta(days=1)
        r = {}
        r['start'] = datetime.datetime( yesterday.year, yesterday.month, yesterday.day) 
        r['end'] = r['start'] + timedelta(1)
    
        return r

    def weeklyRange( self ):
        yesterday =  datetime.date.today() - datetime.timedelta(days=1)

        r = {}
        r['end'] = datetime.datetime( yesterday.year, yesterday.month, yesterday.day)
        r['start'] = r['end'] - timedelta(7)

        return r

    def monthlyRange( self ):
        yesterday =  datetime.date.today() - datetime.timedelta(days=1)
        month = datetime.date(yesterday.year, yesterday.month, 1)

        r = {}
        r['start'] = month
        r['end'] =  yesterday

        return r


    def yearlyRange( self ): 
        yesterday =  datetime.date.today() - datetime.timedelta(days=1)
        year = datetime.date(yesterday.year, 1, 1)
       
        r = {}
        r['start'] = year
        r['end'] =  yesterday
    
        return r

    def getBatchReportsInfo( self, batch_uid ):
    
        if batch_uid == None:
            return False

        self.dbc.execute( "SELECT\
                                dynamic_reports.id as report_uid,\
                                dynamic_reports.name as proc,\
                                dynamic_reports.display_name as report_name,\
                                dynamic_reports.is_nested as is_nested\
                            FROM\
                                reports.batch_x_reports\
                            JOIN reports.dynamic_reports ON dynamic_reports.id = batch_x_reports.report_uid\
                            WHERE\
                                batch_x_reports.batch_uid = " + str(batch_uid));

        rows = self.dbc.fetchall();

        data = [];
        for row in rows:
            r = {}
            r['id'] = None if row[0] == None else row[0]
            r['proc'] = None if row[1] == None else row[1]
            r['name'] = None if row[2] == None else row[2]
            r['is_nested'] = None if row[3] == None else row[3]
            data.append(r)
        
        return data        

    def getBatchEmails( self, batch_uid ):
        
        if batch_uid == None:
            return False
            
        self.dbc.execute( "SELECT\
                                email\
                            FROM\
                                reports.batch_x_batch_emails\
                            JOIN reports.batch_emails ON batch_emails.id = batch_x_batch_emails.batch_email_uid\
                            WHERE batch_x_batch_emails.batch_uid = " + str(batch_uid) + "\
                            AND\
                                batch_x_batch_emails.is_active = 1")

        rows = self.dbc.fetchall();
        
        if rows == None:
            return False
 
        data = []
        for row in rows:
            if row[0] != None and row[0] != '':
                data.append( row[0] )

        return data

    def getReportHeaders( self, report_uid, venue_uid ):
       
        if report_uid == None or venue_uid == None:
            return False 

        self.dbc.execute( "SELECT\
                                reports_x_columns.name as name,\
                                reports_x_columns.display_name as display_name,\
                                reports_x_columns.field_display,\
                                reports_x_columns.excel_min_width,\
                                headers_x_reports.clump_key,\
                                headers_x_reports.stats as stats\
                            FROM\
                                reports.headers_x_reports\
                            LEFT JOIN \
                                reports.reports_x_columns ON reports_x_columns.id = headers_x_reports.report_column_uid\
                            LEFT JOIN \
                                reports.dynamic_reports ON dynamic_reports.id = headers_x_reports.dynamic_report_uid\
                            WHERE\
                                headers_x_reports.dynamic_report_uid = " + str(report_uid) + "\
                            ORDER BY\
                                headers_x_reports.ordinality")

        col_keys = []
        for col in self.dbc.description:
            col_keys.append(col[0])

        rows = self.dbc.fetchall();

        data = []

        for row in rows:
            r = {}
            i = 0
            for col in col_keys:
                r[col] = row[i]
                i = i + 1
            data.append(r)

        return data

    def getReportRows( self, params ): 
    
        if 'timezone' not in params or 'event_uids' not in params or 'venue_uid' not in params:
            return False

        if len( params['event_uids'] ) == 0 or params['event_uids'] == None:
            return []

        timezone = params['timezone']
        proc_name =  params['proc']
        procedure = None if proc_name == None else 'reports.' + str( proc_name )       
        if procedure != None:
            self.dbc.execute('CALL ' + procedure + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( timezone ), str( params['venue_uid'] ) ) )
        else:
            return False

        col_keys = []
        for col in self.dbc.description:
            col_keys.append(col[0])        

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        data = []
        for row in rows:
            r = {}
            i = 0
            for col in col_keys:
                r[col] = row[i]
                i = i + 1
            data.append(r)

        return data

    #def getSalesAuditReport( self, params ):
    def getSalesSummary( self, params ): 
        data = {}
        self.dbc.execute('CALL reports.sa_get_sales_summary_report' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone'] ), str( params['venue_uid'] ) ) )
        
        row = self.dbc.fetchone() 
        self.dbc.nextset() 
        data = {}
        data['total_checks'] = None if row[0] == None else row[0]
        data['refunded_checks'] = None if row[1] == None else row[1]
        data['net_checks'] = None if row[2] == None else row[2]
        data['total_receipts'] = None if row[3] == None else row[3]
        data['total_refunds'] = None if row[4] == None else row[4]
        data['gross_revenue'] = None if row[5] == None else row[5]
        data['average_check'] = None if row[6] == None else row[6]
        data['gratuities'] = None if row[7] == None else row[7]
        data['service_charges'] = None if row[8] == None else row[8]
        data['total_addons'] = None if row[9] == None else row[9]

        return data

    def getGrossRevenue( self, params ):

        
        self.dbc.execute('CALL reports.sa_get_gross_revenue_report' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone']), str( params['venue_uid'] ) ) )
        
        rows = self.dbc.fetchall()
        self.dbc.nextset()

        data = [];
        for row in rows:

            r = {}
            r['levy_revenue_category_id']  = None if row[0] == None else row[0]
            r['revenue_category']  = None if row[1] == None else row[1]
            r['items_sold']  = None if row[2] == None else row[2]
            r['gross_revenue']  = None if row[3] == None else row[3]
            r['discounts']  = None if row[4] == None else row[4]
            r['net_revenue']  = None if row[5] == None else row[5]
            r['service_charge']  = None if row[6] == None else row[6]
            r['tax']  = None if row[7] == None else row[7]

            data.append( r )

        return data

    def getTenderTotalsRows( self, params ):

        
        self.dbc.execute('CALL reports.sa_get_tender_totals_rows' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone']), str( params['venue_uid'] ) ) )
        
        rows = self.dbc.fetchall()
        self.dbc.nextset()

        data = [];
        for row in rows:
        
            r = {}
            r['order_pay_method_uid'] = None if row[0] == None else row[0]
            r['pay_method'] = None if row[1] == None else row[1]
            r['qty'] = None if row[2] == None else row[2]
            r['subtotal'] = None if row[3] == None else row[3]
            r['tip'] = None if row[4] == None else row[4]
            r['total'] = None if row[5] == None else row[5]
            r['avg'] = None if row[6] == None else row[6]

            data.append( r )

        return data

    def getTenderTotals( self, params ):
        self.dbc.execute('CALL reports.sa_get_tender_totals_totals' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone']), str( params['venue_uid'] ) ) )
        
        rows = self.dbc.fetchall()
        self.dbc.nextset()

        data = [];
        for row in rows:

            r = {}
            r['pay_method'] = None if row[0] == None else row[0]
            r['qty'] = None if row[1] == None else row[1]
            r['subtotal'] = None if row[2] == None else row[2]
            r['tip'] = None if row[3] == None else row[3]
            r['total'] = None if row[4] == None else row[4]
            r['average'] = None if row[5] == None else row[5]
    
            data.append( r )
        
        return data

    def getSummaryOne( self, params ):
        self.dbc.execute('CALL reports.sa_get_summary_1' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone']), str( params['venue_uid'] ) ) )
        row = self.dbc.fetchone() 
        self.dbc.nextset()
    
        data = {}
        data['net_revenue'] = None if row[0] == None else row[0]
        data['service_charges'] = None if row[1] == None else row[1]
        data['taxes'] = None if row[2] == None else row[2]
        data['tips'] = None if row[3] == None else row[3]
        data['total_receipts'] = None if row[4] == None else row[4]

        return data

    def getSummaryTwo( self, params ):
        self.dbc.execute('CALL reports.sa_get_summary_2' + '(%s, %s, %s, @errMessage)', ( str( params['event_uids'] ), str( params['timezone']), str( params['venue_uid'] ) ) )

        rows = self.dbc.fetchall()
        self.dbc.nextset()

        data = [];
        for row in rows:
            r = {}
            r['pay_method'] = None if row[0] == None else row[0]
            r['total'] = None if row[1] == None else row[1]
            data.append(r)

        return data;

    def decodeRow( self, row ):

        if  'patron_card_uid' in row and row['patron_card_uid'] != None and row['patron_card_uid'] != '':
        
            if 'card_four' in row and 'card_name' in row:
                # get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
                self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patron_cards' AND pointer_uid = " + str(row["patron_card_uid"]))

                e_row = self.dbc.fetchone()

                if e_row != None:
                    e_key = e_row[0]

                    values = {}
                    keys = {}
                    values['card_four'] = row["card_four"]
                    values['card_name'] = row["card_name"]
                    keys['card_four'] = e_key
                    keys['card_name'] = e_key

                    keymaster = KeyMaster()
                    decoded =  keymaster.decryptMulti(values,keys)

                    if decoded != None and 'card_four' in decoded:
                        row["card_four"] = decoded["card_four"]
                    if decoded != None and 'card_name' in decoded:
                        row["card_name"] = decoded["card_name"]

        if 'patron_uid' in row and ( 'first_name' in row or 'company_name' in row ):

            if 'company_name' not in row:
                row['company_name'] = ''
            
            if 'first_name' not in row:
                row['first_name'] = ''

            if 'last_name' not in row:
                row['last_name'] = ''

            #get e_key from operations.data_keys for patrons | patron_cards | patron_card_uid
            self.dbc.execute("SELECT e_key FROM operations.data_keys WHERE pointer_schema = 'patrons' AND pointer_table = 'patrons' AND pointer_uid = " + str(row["patron_uid"]))
            e_row = self.dbc.fetchone()
 
            if e_row != None:
                e_key = e_row[0]

                values = {}
                keys = {}
                values['first_name'] = row["first_name"] if row['first_name'] != None else ''
                values['last_name'] = row["last_name"] if row['last_name'] != None else ''
                values['company_name'] = row["company_name"] if row['company_name'] != None else ''
                keys['first_name'] = e_key
                keys['last_name'] = e_key
                keys['company_name'] = e_key

                keymaster = KeyMaster()
                decoded =  keymaster.decryptMulti(values,keys)
                if decoded != None and 'first_name' in decoded:
                    row["first_name"] = decoded["first_name"]
                    row["last_name"] = decoded["last_name"]
                    row["company_name"] = decoded["company_name"]

                    if row["company_name"] != None and row["company_name"] != '':
                        row["patron"] = row["company_name"]
                    else:
                        row["patron"] = row["first_name"] + " " + row["last_name"]


        return row


    def getVenueInfo( self, venueUid ):

        if venueUid == None:
            return False

        # get the venues info
        self.dbc.execute("SELECT\
                            name,\
                            local_timezone_long\
                        FROM\
                            setup.venues\
                        WHERE\
                            id = " + str(venueUid));

        row = self.dbc.fetchone()

        if row == None:
            return False

        data = {}
        data['id'] = venueUid
        data['timezone'] = None if row[1] == None else row[1]
        data['name'] = None if row[0] == None else row[0]

        return data


    def updateBatchSendStatus( self, batchUid, status ):
        
        if batchUid == None or status == None:
            return False

        self.dbc.execute("UPDATE reports.batch SET last_sent_status = \'{0}\'  WHERE id = {1}".format( str( status ), batchUid ) );
        self.db.commit()

        return True




if __name__ == "__main__":


    import pprint
    from db_connection import DbConnection
    db = DbConnection().connection
    reportsDb = ReportsDb(db)

    reportsDb.updateBatchSendStatus( 33, 'fail');

    '''
    data = reportsDb.getBatchReportsInfo(21)

    #print "data = "
    pprint.pprint(data)

    data = reportsDb.getVenueInfo(202)

    #print "data = "
    pprint.pprint(data)
    
    temp = { "venue_uid" : 425, "batch_uid" : 6, "range" : "once" }
    data = reportsDb.getBatchData(temp)

    #print "data = "
    pprint.pprint(data)
    '''
    #data = reportsDb.getEventLockBatches(202)

    #print "data = "
    #pprint.pprint(data)



