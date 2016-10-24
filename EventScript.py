import MySQLdb
from subprocess import call


devDB = MySQLdb.connect("development.cgfo05y38ueo.us-east-1.rds.amazonaws.com", "devmaster", "As7bwo&d8")

demoDB = MySQLdb.connect("demo-db.cgfo05y38ueo.us-east-1.rds.amazonaws.com", "demomaster",   "2L1ttL32L1ttl3Z!")


prodDB = MySQLdb.connect("production.cgfo05y38ueo.us-east-1.rds.amazonaws.com", "rabbit_worker", "ch3ckm@t3w0rk3r")




prodCursor = prodDB.cursor()

prodCursor.execute("SELECT id, event_date FROM setup.events")

for id, eventDate in prodCursor.fetchall():
    demoCursor = demoDB.cursor()
    date = eventDate.strftime('%Y-%m-%d %H:%M:%S')
    demoCursor.execute("UPDATE setup.events SET event_date = %s WHERE id = %s", (date, str(id)));
    demoDB.commit() 

