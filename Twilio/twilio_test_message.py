
from twilio.rest import TwilioRestClient 
 
# put your own credentials here 
ACCOUNT_SID = "AC3ddb0c0958d34110e7dcbbbd714afeb1" 
AUTH_TOKEN = "02289a5e90ee4952194d946621ad9bc9" 
 
client = TwilioRestClient(ACCOUNT_SID, AUTH_TOKEN) 
 
client.messages.create(
    to="+18567121316", 
    from_="+18567121316", 
    body="This is a test", 
)
