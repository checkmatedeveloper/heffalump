###############################################################################
#!/usr/bin/python
# _*_ coding: utf-8
#

# To write Unicode text in UTF-8 to a xlsxwriter file in Python:
#
# 1. Encode the file as UTF-8.
# 2. Include the "coding" directive at the start of the file.
# 3. Use u'' to indicate a Unicode string.

import datetime
from dateutil.parser import parse
import time
import smtplib
import requests
import os
from db_connection import DbConnection
from reports_db import ReportsDb
from config import CheckMateConfig
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
import json
import logging
from fpdf import FPDF
import locale

class handleBatch:
    
    def __init__(self, db, batch):
    
        self.reports_core = db
        locale.setlocale(locale.LC_ALL, '')
        success = self.createBatchReport( batch )

        if success == False:
            return None

    # lookup constants/array for stat formulas

    FORMULA_TOTAL = 'total'
    FORMULA_COUNT = 'count'
    FORMULA_SPACER_TOTAL = 'spacer_total'
    FORMULA_AVG = 'avg'
    FORMULA_NONE = 'none'

    stat_row_options = {
        FORMULA_TOTAL : 'formulaTotal',
        FORMULA_COUNT : 'formulaCount',
        FORMULA_SPACER_TOTAL : 'formulaSpacerTotal',
        FORMULA_AVG : 'formulaAvg',
        FORMULA_NONE : 'formulaNone'
    }

    # lookup constants/array for cell formatting

    FORMAT_INT = 'int'
    FORMAT_STRING = 'string'
    FORMAT_STRING_CAPPED = 'string_capped'
    FORMAT_BOOL_BINARY = 'bool_binary'
    FORMAT_BOOL_WORD = 'bool_word'
    FORMAT_DOLLAR_DECIMAL = 'dollar_decimal'
    FORMAT_DOLLAR = 'dollar'
    FORMAT_PERCENT_DECIMAL = 'percent_decimal'
    FORMAT_PERCENT = 'percent'
    FORMAT_DATETIME = 'datetime'
    FORMAT_DATE = 'date'
    FORMAT_TIME = 'time'

    cell_format_options = {
        
        FORMAT_INT : 'formatInt' ,
        FORMAT_STRING : 'formatString',
        FORMAT_STRING_CAPPED : 'formatStringCapped',
        FORMAT_BOOL_BINARY : 'formatBoolBinary',
        FORMAT_BOOL_WORD : 'formatBoolWord',
        FORMAT_DOLLAR_DECIMAL : 'formatDollarDecimal',
        FORMAT_DOLLAR : 'formatDollar',
        FORMAT_PERCENT_DECIMAL : 'formatPercentDecimal',
        FORMAT_PERCENT : 'formatPercent',
        FORMAT_DATETIME : 'formatDateTime',
        FORMAT_DATE : 'formatDate',
        FORMAT_TIME : 'formatTime'

    }
    

    """
    Builds Excel Document, Sends Email
    
    Parameters
    __________
    batch: array
        batch_uid: int
        venue_uid: int
        range: string
    Returns
    ------
    out: array

    """
    def createBatchReport( self, batch ):
        # Get batch data
        data = self.reports_core.getBatchData( batch ) 
        if data == False:
            return False
        # Create excel document
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date = str(yesterday).replace (" ", "_")
        
        data['batch_name'] = batch['batch_name'] if 'batch_name' in batch else 'Batch' 
        batch_name = batch['batch_name'].replace (" ", "_") if 'batch_name' in batch else 'Batch'
        root = '/tmp/'
        filename = '{0}Parametric_{3}_{1}_{2}.xlsx'.format( root, batch_name, date, data['venue_name'] );
        try:
            open( filename, 'w+' );
        except IOError:
            return False
        
        workbook = self.createExcelDocument(filename)
        
        # Format excel document
        self.formatExcelDocument( workbook )
        
        # Title Page
        titlePage = self.createExcelPage( workbook, data['batch_name'] )
        write_title_page = self.writeTitlePage( titlePage, data )
        if write_title_page == False:
            os.remove( filename )
            return False
        # Handle Batch Reports
        data['date'] = date
        reports_written = self.handleReportPages( workbook, data )  
        if reports_written == False:
            os.remove( filename )
            return False
        # Close Excel Doc
        try:
            workbook.close()
        except IOError:
            return False

        # Send Batch Report
        checkmateconfig = CheckMateConfig();
        env_var = os.getenv('Chkm8WorkerServerType')
        mailgun_api_key = checkmateconfig.MAILGUN_API_KEY
        mailgun_host = "https://api.mailgun.net/v3/{0}/messages".format(checkmateconfig.MAILGUN_PROD_GENERAL_DOMAIN)

        start = data['batch_range']['start'].strftime('%b %d, %Y')
        end = data['batch_range']['end'].strftime('%b %d, %Y')
        # Email Message
        body = 'Attached is the {0} Batch Report for {1}, sending {2} accounting for {3} to {4} and containing these reports: <br />'.format( data['batch_name'], data['venue_name'], batch['range'], start, end );
       
        include_sales_audit = False; 
        report_list = ''
        for report in data['reports']:
            if report['id'] == 48:
                include_sales_audit = True;
            report_list = report_list + "<tr style='font-size: 12pt'><td>&nbsp;</td><td>" + report['name'] + "</td></tr>"        
        report_list = report_list + "<tr><td>&nbsp;</td><td>&nbsp;</td></tr>"

        message = "<html><body style='font-family: Geneva,Verdana,Arial,Helvetica; font-size: smaller'><div style='width: 500px; height: 100%;'><table border=0; cellpadding=0; width=100% style='font-family: Geneva,Verdana,Arial,Helvetica;'><tr style='font-size: 12pt'><td style='text-align: left'>Hello,</td></tr><tr><td>&nbsp;</td></tr><tr style='font-size: 12pt'><td>&nbsp;&nbsp;&nbsp;{0}</td></tr><tr><td>&nbsp;</td></tr></table><table border=0; cellpadding=0; width=100% style='font-family: Geneva,Verdana,Arial,Helvetica;'>{1}</table><table border=0; cellpadding=0; width=100% style='font-family: Geneva,Verdana,Arial,Helvetica;'> <tr style='font-size: 12pt'><td>Thank you,</td></tr><tr style='font-size: 12pt'><td>Parametric Staff</td></tr></table><br /></div></body></html>".format(body, report_list);

        sales_audit = '/tmp/Parametric_Sales_Audit_{1}_{0}.pdf'.format( date, data['venue_name'] )

        if os.path.isfile( sales_audit ) and include_sales_audit == True:
            for email in data['emails']:
                r = requests.post(
                            mailgun_host,
                            auth=("api", mailgun_api_key),
                            files=[("attachment", open(filename)),("attachment", open(sales_audit))],
                            data={"from": "<mail@bypassmobile.com>",
                                "to": email,
                                "subject": "Parametric " + data['batch_name'],
                                "html": message
                            }
                )
        else:
            for email in data['emails']:
                r = requests.post(
                            mailgun_host,
                            auth=("api", mailgun_api_key),
                            files=[("attachment", open(filename))],
                            data={"from": "<mail@bypassmobile.com>",
                                "to": email,
                                "subject": "Parametric " + data['batch_name'],
                                "html": message
                            }
                )

        # When done, get rid of the excel doc
        os.remove( filename )


    '''

    Create Excel Document

    Parameters
    ---------
    
    Return
    --------
    out: ref

    '''
    def createExcelDocument( self, filename ):

        return xlsxwriter.Workbook(filename)


    '''
    Create/Define Formats for the Excel Doc

    Parameters  
    ---------
    workbook: ref

    Returns
    ------
    
    '''
    def formatExcelDocument( self, workbook ):

        # bold report header
        self.header_format = workbook.add_format({'bold': True})

        # dollar
        self.dollar_format = workbook.add_format({'num_format':'#,##0'})

        # dollar decimal
        self.dollar_decimal_format = workbook.add_format({'num_format': '$#,##0.00'}) 

        # percent
        self.percent_format = workbook.add_format({'num_format': '0"%"'})

        # percent decimal
        self.percent_decimal_format = workbook.add_format({'num_format': '0.00"%"'})
 
        # date 
        self.date_format = workbook.add_format({'num_format': 'mmm d yyyy'})

        # time
        self.time_format = workbook.add_format({'num_format': '%I:%M %p'})

        # datetime
        self.datetime_format = workbook.add_format({'num_format': 'mmm d yyyy'})

        # center
        self.center_format = workbook.add_format({'align': 'center'})


    '''
    Creates a new sheet and adds to the workbook
    
    Parameters
    ---------
    workbook: ref
    name: string (optional)
    
    Return
    --------
    out: page ref

    '''
    def createExcelPage( self, workbook, name ):
        
        ws = workbook.get_worksheet_by_name( name )
        if ws != None:
            return ws

        if name is not None:
            return workbook.add_worksheet(name)
        else:
            return workbook.add_worksheet()

    '''
    Write to Excel Title Page for Batch Report
    
    Parameters
    ----------
    page: ref
    info: dict
        venue_uid: int
        venue_name: string
        batch_name: string
        timezone: string
        events: array
            
    Return
    --------

    '''
    def writeTitlePage( self, page, info ):

        # set some columns width
        page.set_column('B:E', 30)

        x = 0
        y = 0

        # Batch Name
        page.write( y, 0, info['batch_name'], self.header_format )
        y = y + 1

        # VenueUid : Venue Name ( Timezone )
        page.write( y, 0, '{0}: {1} ( {2} )'.format( info['venue_uid'], info['venue_name'], info['timezone'] ) )
        y = y + 1

        # Start Date to End Date
        start = info['batch_range']['start'].strftime('%b %d, %Y')
        end = info['batch_range']['end'].strftime('%b %d, %Y')
        page.write( y, 0, 'Range: {0} to {1}'.format( start , end ) )
        y = y + 1

        # Time Created
        x = x + 1
        now = datetime.datetime.now().date().strftime('%b %d, %Y')
        page.write( y, 0, 'Exported At: {0}'.format(now))
        y = y + 2


        #Events Table
        page.write( y, 0, 'Events' )
        y = y + 1

        events_table_header = ['Id','Name','Subtitle','Type','Start']
        event_row_keys = ['uid','name','subtitle','type','date']
        page.write_row( y, 0, events_table_header, self.header_format );

        # loop over events
        for event in info['events']:

            y = y + 1
            x = 0

            for col in event_row_keys:

                if col == 'date':
                    page.write_datetime( y, x, event[col], self.date_format )
                else:
                    page.write( y, x, event[col] )

                x = x + 1

        page.set_column('A:B', 40 )
        
        return True

    def createSalesAuditReport( self, params ):
    
        # this is a sales audit report
        data = {}
        data['sales_summery'] = self.reports_core.getSalesSummary( params );
        data['gross_revenue'] = self.reports_core.getGrossRevenue( params )
        data['tender_totals'] = self.reports_core.getTenderTotals( params )
        data['tender_total_rows'] = self.reports_core.getTenderTotalsRows( params )
        data['summary_one'] = self.reports_core.getSummaryOne( params )
        data['summary_two'] = self.reports_core.getSummaryTwo( params )

        # find a saved pdf to attach
        pdf = FPDF('L', 'mm', 'A4')
        pdf.add_page()

        # fpdf.cell(w, h = 0, txt = '', border = 0, ln = 0, align = '', fill = False, link = '')

        pdf.set_font("Arial", size=12)
        
        border_left = 5
        border_right = 5
        
        header_cell_width = 42
        header_cell_height = 12

        pdf.ln(h = '')
        pdf.ln(h = '')
        
        pdf.set_fill_color( 224, 224, 224)

        # header
        pdf.cell( header_cell_width, header_cell_height, 'Sales Audit: {0}'.format(params['venue_name']) )
        pdf.cell( header_cell_width + 170, header_cell_height, str( params['date'] ), 0, 0, 'R' )
        pdf.ln(h = '')

        # sales summary header
        pdf.cell( header_cell_width, header_cell_height, 'Total Checks', 'LT', 0, 'C', True) 
        pdf.cell( header_cell_width + 5, header_cell_height, 'Refunded Checks', 'T', 0, 'C', True) 
        pdf.cell( header_cell_width, header_cell_height, 'Net Checks',  'T', 0, 'C', True)
        pdf.cell( header_cell_width, header_cell_height, 'Total Refunds', 'T', 0, 'C', True)
        pdf.cell( header_cell_width, header_cell_height, 'Gross Revenue', 'T', 0, 'C', True)
        pdf.cell( header_cell_width, header_cell_height, 'Average Check', 'TR', 0, 'C', True) 

        pdf.ln(h = '')

        total_checks = '0' if data['sales_summery']['total_checks'] == None else str( data['sales_summery']['total_checks'] )
        refunded_checks = '0' if data['sales_summery']['refunded_checks'] == None else str( data['sales_summery']['refunded_checks'] )
        net_checks = '' if data['sales_summery']['net_checks'] == None else str( data['sales_summery']['net_checks'] )
        total_refunds = '$0.00' if data['sales_summery']['total_refunds'] == None else self.pdfMoneyFormat( data['sales_summery']['net_checks'] ) 
        gross_revenue = '$0.00' if data['sales_summery']['gross_revenue'] == None else self.pdfMoneyFormat( data['sales_summery']['gross_revenue'] ) 
        average_check = '$0.00' if data['sales_summery']['average_check'] == None else self.pdfMoneyFormat( data['sales_summery']['average_check'] )

        # sales summary rows
        pdf.cell( header_cell_width, header_cell_height, total_checks, 'LTB', 0,  align='C')
        pdf.cell( header_cell_width + 5, header_cell_height, refunded_checks, 'TB', 0,   'C')
        pdf.cell( header_cell_width, header_cell_height, net_checks, 'TB', 0,'C')
        pdf.cell( header_cell_width, header_cell_height, total_refunds, 'TB', 0, 'C')
        pdf.cell( header_cell_width, header_cell_height, gross_revenue, 'TB', 0, 'C')
        pdf.cell( header_cell_width, header_cell_height, average_check, 'RTB', 0, 'C')
 
        pdf.ln(h = '')
        pdf.ln(h = '')

        header_cell_width = 35
        
        # gross revenue
        pdf.cell( header_cell_width + 5, header_cell_height, 'Revenue Category', 'LT', 0, 'C', True)
        pdf.cell( header_cell_width, header_cell_height, 'Items Sold', 'T', 0, 'C', True)
        pdf.cell( header_cell_width + 3, header_cell_height, 'Net Revenue',  'T', 0, 'C', True)
        pdf.cell( header_cell_width, header_cell_height, 'Discounts', 'T', 0, 'C', True)
        pdf.cell( header_cell_width + 3, header_cell_height, 'Gross Revenue', 'T', 0, 'C', True)
        pdf.cell( header_cell_width + 3, header_cell_height, 'Service Charge', 'T', 0, 'C', True)
        pdf.cell( header_cell_width - 2, header_cell_height, 'Tax', 'TR', 0, 'C', True)
        pdf.ln(h = '')


        for row in data['gross_revenue']:
            revenue_center = '' if row['revenue_category'] == None else str( row['revenue_category'] )
            items_sold = '0' if row['items_sold'] == None else str( row['items_sold'] )
            net_revenue = '0' if row['net_revenue'] == None else self.pdfMoneyFormat( row['net_revenue'] )
            discounts = '0' if row['discounts'] == None else self.pdfMoneyFormat( row['discounts'] )
            gross_revenue = '0' if row['gross_revenue'] == None else self.pdfMoneyFormat( row['gross_revenue'] )
            service_charge = '0' if row['service_charge'] == None else self.pdfMoneyFormat( row['service_charge'] )
            tax = '0' if row['tax'] == None else self.pdfMoneyFormat( row['tax'] )

            # gross revenue rows
            pdf.cell( header_cell_width + 5, header_cell_height, revenue_center, 'LTB', 0, 'C')
            pdf.cell( header_cell_width, header_cell_height, items_sold, 'TB', 0, 'C') 
            pdf.cell( header_cell_width + 3, header_cell_height, net_revenue, 'TB', 0, 'C') 
            pdf.cell( header_cell_width, header_cell_height,discounts, 'TB', 0, 'C') 
            pdf.cell( header_cell_width + 3, header_cell_height, gross_revenue, 'TB', 0, 'C') 
            pdf.cell( header_cell_width + 3, header_cell_height, service_charge, 'TB', 0, 'C') 
            pdf.cell( header_cell_width - 2, header_cell_height, tax, 'TBR', 0, 'C') 

            pdf.ln(h = '')
       
        pdf.ln(h = '')

        cell_width = 35
        # tender totals
        pdf.cell( 100, header_cell_height, '  Payment Type', 'LT', 0, 'L', True)
        pdf.cell( 15, header_cell_height, 'Qty', 'T', 0, 'C', True)
        pdf.cell( cell_width, header_cell_height, 'Subtotal',  'T', 0, 'C', True)
        pdf.cell( cell_width, header_cell_height, 'Tip', 'T', 0, 'C', True)
        pdf.cell( cell_width, header_cell_height, 'Total', 'T', 0, 'C', True)
        pdf.cell( cell_width, header_cell_height, 'Avg', 'TR', 0, 'C', True)
        pdf.ln(h = '') 

        for row in data['tender_total_rows']:
            
            payment_type = '' if row['pay_method'] == None else str( row['pay_method'] )
            qty = '0' if row['qty'] == None else str( row['qty'] )
            subtotal = '$0.00' if row['subtotal'] == None else self.pdfMoneyFormat( row['subtotal'] )
            tip = '$0.00' if row['tip'] == None else self.pdfMoneyFormat( row['tip'] )
            total = '$0.00' if row['total'] == None else self.pdfMoneyFormat( row['total'] )
            avg = '$0.00' if row['avg'] == None else self.pdfMoneyFormat( row['avg'] )

            # tender total row
            pdf.cell( 100, header_cell_height, '  ' + payment_type, 'LTB', 0, 'L')
            pdf.cell( 15, header_cell_height, qty, 'TB', 0, 'C')
            pdf.cell( cell_width, header_cell_height, subtotal, 'TB', 0, 'C')
            pdf.cell( cell_width, header_cell_height, tip, 'TB', 0, 'C')
            pdf.cell( cell_width, header_cell_height, total, 'TB', 0, 'C')
            pdf.cell( cell_width, header_cell_height, avg, 'TBR', 0, 'C')

            pdf.ln(h = '')

        pdf.ln(h = '')
        # summary one

        net_revenue = '$0.00' if data['summary_one']['net_revenue'] == None else self.pdfMoneyFormat( data['summary_one']['net_revenue'] )
        service_charge = '$0.00' if data['summary_one']['service_charges'] == None else self.pdfMoneyFormat( data['summary_one']['service_charges'] )
        taxes =  '$0.00' if data['summary_one']['taxes'] == None else self.pdfMoneyFormat( data['summary_one']['taxes'] )
        tips = '$0.00' if data['summary_one']['tips'] == None else self.pdfMoneyFormat( data['summary_one']['tips'] )
        total_receipts = '$0.00' if data['summary_one']['total_receipts'] == None else self.pdfMoneyFormat( data['summary_one']['total_receipts'] )

        cell_width = 80

        pdf.cell( cell_width, header_cell_height, '  Net Revenue', 'LTR', 0, 'L', True )
        pdf.cell( cell_width, header_cell_height, '  ' + net_revenue, 'TR', 0, 'L' )
        pdf.ln(h = '')

        pdf.cell( cell_width, header_cell_height, '  Service Charge', 'LTR', 0, 'L', True )
        pdf.cell( cell_width, header_cell_height, '  ' + service_charge, 'TR', 0, 'L' )
        pdf.ln(h = '')

        pdf.cell( cell_width, header_cell_height, '  Taxes', 'LTR', 0, 'L', True )
        pdf.cell( cell_width, header_cell_height, '  ' + taxes, 'TR', 0, 'L' )
        pdf.ln(h = '')


        pdf.cell( cell_width, header_cell_height, '  Tips and Gratuities', 'LTR', 0, 'L', True )
        pdf.cell( cell_width, header_cell_height, '  ' + tips, 'TR', 0, 'L' )
        pdf.ln(h = '')

        pdf.cell( cell_width, header_cell_height, '  Total Receipts', 'LTBR', 0, 'L', True )
        pdf.cell( cell_width, header_cell_height, '  ' + total_receipts, 'LTBR', 0, 'L' )
        pdf.ln(h = '')
        pdf.ln(h = '')

        # summary two
        for row in data['summary_two']:
        
            header = '' if row['pay_method'] == None else '  ' + str( row['pay_method'] )
            amount = '$0.00' if row['total'] == None else '  ' + self.pdfMoneyFormat( row['total'] )  

            pdf.cell( cell_width, header_cell_height, header, 'LTBR', 0, 'L', True )
            pdf.cell( cell_width, header_cell_height, amount, 'TBR', 0, 'L' )
            pdf.ln(h = '')


        root = '/tmp/'
        filename = '{0}Parametric_{1}_{3}_{2}.pdf'.format( root, 'Sales_Audit', params['date'], params['venue_name'] );
        pdf.output(filename)


        return True
       
    
    def pdfMoneyFormat( self, num ):
         
        if num == 0 or num == None or num == '':
            return '$0.00'
        else:
            num = round( float( str( num ) ) , 2 )
            return locale.currency( num, symbol=True, grouping=True)

    '''
    Loop Over an array of reports, get the report data, write a new page for each one
    
    Parameters
    ----------
    workbook: ref
    params: dict
        venue_uid: int
        venue_name: string
        timezone: string
        batch_uid: int
        range: string
        batch_range: dict
            start: datetime
            end: datetime
        events: dict
            uid: int
            name: string
            subtitle: string
            type: date
        reports: dict
            id: int
            proc: string
            name: string
            is_nested: int
    
    Return
    ------


    '''
    def handleReportPages( self, workbook, params ):

        if 'events' not in params or params['events'] == None:
            return False
        if 'reports' not in params or params['reports'] == None:
            return False
        if 'batch_range' not in params or params['batch_range'] == None:
            return False

        event_uids = []
        for event in params['events']:
            uid = event['uid'] if 'uid' in event else None;
            if uid != None:
                event_uids.append(event['uid']) 

        event_uids = ','.join( map(str, event_uids ) )

        if len( event_uids ) < 1:
            return False
        
        for report in params['reports']:
            x = 0
            y = 0
      
            report_params = {}
            report_params['timezone'] = params['timezone']
            report_params['proc'] = report['proc']
            report_params['venue_uid'] = params['venue_uid']
            report_params['venue_name'] = params['venue_name']
            report_params['event_uids'] = event_uids
            report_params['date'] = params['date']
             
            if( report['id'] == 48 ):

                self.createSalesAuditReport( report_params )

            else:
 
                # make a new page
                page = self.createExcelPage( workbook, report['name'] )

                # 1. ) Write Page Header
                
                # report name
                page.write( y, x, str( params['batch_name'] ) + ': ' + str( report['name'] ), self.header_format )
                y = y + 1
                
                # venue name
                page.write( y, x, params['venue_name'] )
                y = y + 1

                # report range

                start = params['batch_range']['start'].strftime('%b %d, %Y')
                end = params['batch_range']['end'].strftime('%b %d, %Y')
                page.write( y, 0, "Range: {0} to {1}".format( start, end ) )

                y = y + 2
        
                # 2.) Write Report Header
                header_row = y
                stat_row = header_row + 1
            
                header = self.reports_core.getReportHeaders( report['id'], params['venue_uid'] )

                if header == False:
                    return False

                for col in header:
                
                    # bold header column
                    page.write( header_row, x, col['display_name'], self.header_format )
                
                    # if min width, set col width
                    if col['excel_min_width'] != None:
                        page.set_column( header_row, x, col['excel_min_width'] )

                    x = x + 1
            
                # save room for stats
                y = stat_row + 1
                first_row = y
        
                # 3.)  Write Report Rows
                
                # rows = reportsDB.getReportRows( report_params ) 
                rows = self.reports_core.getReportRows( report_params ) 
                if rows == False:
                    return False            

                row_count = 0
                if len( rows ) > 0:  
                    # Write UnNested Report Rows
                    if str(report['is_nested']) != '1' :
        
                        for row in rows:
                        
                            x = 0
                            #row = reportsDB.decodeRow( row )        
                            row = self.reports_core.decodeRow( row )
                            for col in header:
                                # use lookup array to call formatting function that writes to the page
                                if col['name'] in row:
                                    display = col['field_display'] if col['field_display'] != None else 'string'

                                    getattr( self, self.cell_format_options[ display ] )( page, row[col['name']], x, y )
                                
                                if col['excel_min_width'] != None:
                                    #set any min width in the doc for readability
                                    page.set_column( y, x, col['excel_min_width'] )
                            
                                x = x + 1
                        
                            y = y + 1
                
                    # Write Nested Report Rows
                    else:
                        # merge_range(first_row, first_col, last_row, last_col, data[, cell_format])
                        for row in rows:
                            
                            x = 0
                            max_row_depth = y
                            top_row = y
                            merge_cells = [];
                            
                            # row = reportsDB.decodeRow( row )            
                            row = self.reports_core.decodeRow( row )            

                            for col in header:
                                
                                name = col['name']    
                                display = col['field_display'] if col['field_display'] != None else 'string'

                                # check for header.clump_key, indicates the val will be json
                                if col['clump_key'] != None and col['clump_key'] != '': 
                                    if col['clump_key'] in row: 
                                        
                                        subrows = self.is_json(row[col['clump_key']])
                                        
                                        if subrows == False:
                                            x = x + 1    
                                            continue
                                            
                                        name = name.replace( str( col['clump_key'] ) + '_' , '' )
                                        row_mark = y    
                                    
                                        # handle subrows
                                        for r in subrows:
                                            
                                            # r = reportsDB.decodeRow( r )
                                            r = self.reports_core.decodeRow( r )

                                            if name in r:
                                                getattr( self, self.cell_format_options[ display ]  )( page, r[name], x, row_mark )
                                            
                                                max_row_depth = row_mark if row_mark > max_row_depth else max_row_depth
                                                row_mark = row_mark + 1
                                    
                                # single value, write to cell
                                else:
                                    if name in row:
                                        
                                        merge_cells.append([x, row[name], display, col['excel_min_width'] ] )
                                        getattr( self, self.cell_format_options[ display ] )( page, row[name], x, y )
             

                                # min width
                                if col['excel_min_width'] != None:
                                    #set any min width in the doc for readability
                                    page.set_column( y, x, col['excel_min_width'] )


                                x = x + 1
                            
                            # merge the multirow cells that have only one row for readability
                            for cell in merge_cells:

                                if max_row_depth > y:

                                    x_pos = cell[0]
                                    cell_data = cell[1]
                                    cell_format = cell[2]
                                    cell_width = cell[3]

                                    # merge the cells
                                    page.merge_range( top_row, x_pos, max_row_depth, x_pos, cell_data )

                                    # have to re format merged cell
                                    getattr( self, self.cell_format_options[cell_format] )( page, cell_data, x_pos, top_row )

                                    # min width
                                    if cell_width != None:
                                        #set any min width in the doc for readability
                                        page.set_column( y, x_pos, col['excel_min_width'] )                                
                                
                            y = max_row_depth + 1
                            row_count = row_count + 1

                    last_row = y - 1

                    # force format of A:B so header is ledgible
                    page.set_column( 'A:B', 40)

                    # 4.) Write Stat Row after rows have been filled for accurate stats
                    x = 0
                    for col in header:
                    
                        stat_type = col['stats']
                
                        if stat_type != None and stat_type != 'none':
                            first_cell = xl_rowcol_to_cell( first_row, x )
                            last_cell = xl_rowcol_to_cell( last_row, x )
                            getattr( self, self.stat_row_options[ stat_type ] )( page, x, stat_row, first_cell, last_cell, row_count )
                        
                        x = x + 1

    # detect json
    def is_json(self, myjson):
        try:
            json_object = json.loads(str( myjson ) )
        except ValueError, e:
            return False
        return json_object

    # Functions to control cell formats
    # Predefined in DB reports.reports_x_columns.field_display ENUM

    def formatDollarDecimal( self, page, val, x, y ):
        val = val if val != None and val != '' else 0
        try:
            page.write_number( y, x, float( val ), self.dollar_decimal_format ) 
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    def formatDollar( self, page, val, x, y ):
        try:
            page.write_number( y, x, val, self.dollar_format ) 
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    def formatPercentDecimal( self, page, val, x, y ):
        val = val if val != None and val != '' else 0
        try:
            page.write_number( y, x, float( val ), self.percent_decimal_format )
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    def formatPercent( self, page, val, x, y ):
        try:
            page.write_number( y, x, val, self.percent_format )
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    def formatBoolBinary( self, page, val, x, y ):
        try:
            page.write_boolean( y, x, int( val ) == 1 )            
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    def formatBoolWord( self, page, val, x, y ):
        val = unicode( str(val), errors='replace')
        val = 'Yes' if val == '1' or val == 'true' or val == True else 'No';
        page.write( y, x, val )
 
    def formatDateTime( self, page, val, x, y ):
        try:
            val = str( val )
            val = datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            page.write_datetime( y, x, val, self.date_format )   
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    
    def formatDate( self, page, val ):
        if val != '':
            try:
                page.write_datetime( y, x, val, self.date_format )
            except ValueError:
                val = unicode( str(val), errors='replace')
                page.write( y, x, val )

    def formatTime( self, page, val, x ,y ):
        if val != '':
            try:
                page.write_datetime( y, x, val, self.time_format )
            except ValueError:
                val = unicode( str(val), errors='replace')
                page.write( y, x, val )

    def formatStringCapped( self, page, val, x, y ):
        val = unicode( str(val), errors='replace')
        val = val.title() if val != None else ''
        page.write( y, x, val )

    def formatString( self, page, val, x, y):
        val = unicode( str(val), errors='replace')
        page.write( y, x, val )

    def formatInt( self, page, val, x, y ):
        val = val if val != None and val != '' else 0
        try:
            page.write_number( y, x, float(val) )    
        except ValueError:
            val = unicode( str(val), errors='replace')
            page.write( y, x, val )
    # functions to write formulas to excel page
    # predefined in reports.headers_x_reports.stats ENUM   
 
    def formulaTotal( self, page, stat_row_x,  stat_row_y, first_cell, last_cell, row_count ):
        formula = '=SUM('+ first_cell  +':' + last_cell + ')'
        page.write_formula( stat_row_y, stat_row_x , formula )

    def formulaSpacerTotal( self, page, stat_row_x, stat_row_y, first_cell, last_cell, row_count ):
        formula = '=SUM('+ first_cell  +':' + last_cell + ')/2'
        page.write_formula( stat_row_y, stat_row_x , formula )

    def formulaCount( self, page, stat_row_x, stat_row_y, first_cell, last_cell, row_count ):
        try:
            page.write_number( stat_row_y, stat_row_x , row_count )
        except ValueError:
            return 0;

if __name__ == '__main__':


    from db_connection import DbConnection
    db = DbConnection().connection 
    reports_core = ReportsDb( db )
    
    payload = {'batch_name': 'Once Batch TestL', 'batch_uid': 37L,'range': 'once','venue_uid': 201L};    

    #payload = {'batch_name': 'Nationals Batch Report Test','batch_uid': 6L,'range': 'once','venue_uid': 425L}
    batchHandler = handleBatch( reports_core, payload )    
    print( batchHandler.pdfMoneyFormat( 2 ) )
    print( batchHandler.pdfMoneyFormat( 0 ) )
    print( batchHandler.pdfMoneyFormat( None ) )
    print( batchHandler.pdfMoneyFormat( '' ) )

    #info =    batchHandler.reports_core.getVenueInfo( 201 )
    #data =    batchHandler.getBatchData( payload )
    #print str( info )



