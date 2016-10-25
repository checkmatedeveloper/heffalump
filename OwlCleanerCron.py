import MySQLdb
import traceback
#import requests
from config import CheckMateConfig
import pprint
import HipChat
checkmateconfig = CheckMateConfig()

dbs = []
db = {}
db['host'] = checkmateconfig.DB_HOST
db['user'] = checkmateconfig.DB_USER
db['passwd'] = checkmateconfig.DB_PASS
dbs.append(db)
#db = {}
#db['host'] = checkmateconfig.DB_DEMO_HOST
#db['user'] = checkmateconfig.DB_DEMO_USER
#db['passwd'] = checkmateconfig.DB_DEMO_PASS
#dbs.append(db)


for db in dbs:

    try:
        conn = MySQLdb.connect(host = db['host'], user = db['user'], passwd = db['passwd'])

        cursor = conn.cursor()
        cursor.execute("DELETE FROM owls.owl_queue")
        cursor.execute("UPDATE tablets.sync_flags SET sync_owls = 0");
        conn.commit()
        HipChat.sendMessage("Owl Queue Cleaned", "Owl Cln Cron", "447878", "purple");
    except Exception, e:
        print 'Something went wrong ' + str(e) + " - " + traceback.format_exc()

