import time
from twilio.rest import TwilioRestClient
from twilio.rest.resources import Call

TWILIO_ACCOUNT_SID = "AC3ddb0c0958d34110e7dcbbbd714afeb1"
TWILIO_AUTH_TOKEN = "02289a5e90ee4952194d946621ad9bc9"

GOOD_STATUSES = [Call.COMPLETED]

BAD_STATUSES = [Call.FAILED, Call.BUSY, Call.NO_ANSWER]

client = TwilioRestClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

alertNumbers = ("+16092542121", "+12672398737")


def makeNotificationPhoneCall():
    for alertNumber in alertNumbers:
    
        call = client.calls.create(url="https://nate.paywithcheckmate.com/order_transfer_error_message.xml",
                                   to=alertNumber,
                                   from_="+18567121316",
                                   timeout="15")

        sid = call.sid

        x = 0 #for saftey
        while(x < 60):
            x = x + 1
            time.sleep(1) 
            call = client.calls.get(sid)
            status = call.status

            print str(status)

            if status in GOOD_STATUSES:
                print "GOOD"
                exit()

            if status in BAD_STATUSES:
                print "BAD"
                break; #call the next number
        
            print "OTHER"
    


