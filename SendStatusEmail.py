"""
    use this script to manually send a table status email.

    usage: python SendStatusEmail.py <venue_uid> <event_uid>
"""

import sys
import TabletStatusMailer
import requests
import traceback
from config import CheckMateConfig

try:

    args = sys.argv[1:]

    #send
    TabletStatusMailer.sendStatusEmail(args[0], args[1], manual=True)

except Exception, e:

    '''
    hipchat_dict = {'auth_token':'987ce42cc059f3a404e18c9f9c9642',
                    'room_id':'518943',
                    'from':'TabletStatCron',
                    'message':'Something went wrong in TabletStatusCron: ' + str(e) + " - " + traceback.format_exc(),
                    'notify':'1',
                    'color':'red'}
    '''

    checkmateconfig = CheckMateConfig()
    auth_token = checkmateconfig.HIPCHAT_AUTH_TOKEN
    room_id = checkmateconfig.HIPCHAT_WORKERS_ROOM_ID

    hipchat_dict = {'auth_token': auth_token,
                    'room_id': room_id,
                    'from':'TabletStatCron',
                    'message':'Something went wrong in TabletStatusCron: ' + str(e) + " - " + traceback.format_exc(),
                    'notify':'1',
                    'color':'red'}

    print hipchat_dict
    resp = requests.post('http://api.hipchat.com/v1/rooms/message', params=hipchat_dict)
    print resp
