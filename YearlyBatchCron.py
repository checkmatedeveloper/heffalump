from db_connection import DbConnection
import reports_db
import batch_report_generator 

conn = DbConnection().connection
reportsDB = reports_db.ReportsDb( conn )

batches = reportsDB.getBatchByFrequency( 'yearly')
if batches != False:
    for batch in batches:
        batch_report_generator.handleBatch( reportsDB, batch )


