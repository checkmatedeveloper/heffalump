from db_connection import DbConnection
from Levy_DB import Levy_Db

conn = DbConnection().connection
dbCore = Levy_Db(conn, None)

dbCore.reactivateMenuXMenuItems()
