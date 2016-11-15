import config
import requests
from config import CheckMateConfig


def sendEmail(sender, recipient, subject, html, userVars = None, files = None):

    checkmateConfig = CheckMateConfig()
    mailgunApiKey = checkmateConfig.MAILGUN_API_KEY
    mailgunHost = "https://api.mailgun.net/v3/{0}/messages".format(checkmateConfig.MAILGUN_PROD_RECEIPT_DOMAIN)

    data = {"from":sender,
            "to": recipient,
            "subject":subject,
            "html":html}


    if userVars is not None:
        for key, value in userVars.iteritems():
            data['key'] = 'v:' + str(value)

    requests.post(
        mailgunHost,
        auth = ("api", mailgunApiKey),
        files = files,
        data = data)


        
   
