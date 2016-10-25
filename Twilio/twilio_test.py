import time
from twilio.rest import TwilioRestClient
 
# Your Account Sid and Auth Token from twilio.com/user/account
account_sid = "AC3ddb0c0958d34110e7dcbbbd714afeb1"
auth_token  = "02289a5e90ee4952194d946621ad9bc9"
client = TwilioRestClient(account_sid, auth_token)
 
call = client.calls.create(url="http://demo.twilio.com/docs/voice.xml",
    to="+16092542121",
    from_="+18567121316",
    timeout="15",
    fallbackurl="http://twimlets.com/callme?PhoneNumber=12672398737&")

sid = call.sid

while(True):
    time.sleep(1)
    call = client.calls.get(sid)
    print "STATUS: " + str(call.status) + " DURATION: " + str(call.duration) + " ANSWERED BY: " + str(call.answered_by)
