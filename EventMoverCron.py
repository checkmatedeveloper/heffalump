import MySQLdb
import traceback
import requests


#add to this list to move other events
dbs = [  #host,                                                     user,           passwd              Server Name                    
        #["demo-db.cgfo05y38ueo.us-east-1.rds.amazonaws.com",        "demomaster",   "2L1ttL32L1ttl3Z!", "Demo"],
        ["development.cgfo05y38ueo.us-east-1.rds.amazonaws.com",    "devmaster",    "d*t*1sb3*^t1f^L",        "Dev"]
      ]

def sendHipChat(message, color):
    '''
        Posts a message to hipchat

        Parameters
        __________
        message : the text of the message to post, will play nicely
            with emoticons, links, or any other hipchat goodness
        color : the color of the message background.  Acceptable 
            colors are: yellow", "red", "green", "purple", "gray",
             or "random"

        Returns
        _______
        out : None
    '''
    hipchat_dict = {'auth_token':'987ce42cc059f3a404e18c9f9c9642',
                         'room_id':'447878',
                         'from':'EventMoverCron',
                         'message':message,
                         'notify':'1',
                         'message_format':'text',
                         'color':color}
    
    resp = requests.post('http://api.hipchat.com/v1/rooms/message', params=hipchat_dict)
    #print resp



for db in dbs:
    try:
        #connect to the db
        conn = MySQLdb.connect(host=db[0], user=db[1], passwd=db[2])

        #set the event_date of event 998 to "Today at 18:00:00"
        cursor = conn.cursor()
        cursor.execute('UPDATE setup.events SET event_date = CONCAT(CURDATE(), " ", "18:00:00") WHERE id IN (998,999)')
        conn.commit()

#        sendHipChat("I moved the " + db[3] + " event to today", "purple");
    except Exception, e:
        sendHipChat('Something went wrong in EventMoverCron: ' + str(e) + " - " + traceback.format_exc(), 'red')
        
