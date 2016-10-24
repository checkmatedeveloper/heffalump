import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def sendGmail(username, password, fromEmail, toEmail, subject, html, text):
    """
        Sends an email from a gmail account

        Parameters
        __________
        username : a gmail username
        password : the password to a gmail account that matches the given username
        fromEmail : the address to appear in the from field
        toEmail : the recipient of the email
        subject : string, the subject field of the email
        html : string, the html body of the email
        text : string, what to dispaly if the recipient's email client does not
               support html emails

        Returns
        _______
        out : None
    """
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = fromEmail
    msg['To'] = toEmail
    msg['Reply-To'] = fromEmail

    msg.attach(MIMEText(text, 'text'))
    msg.attach(MIMEText(html, 'html'))

    server = smtplib.SMTP("smtp.gmail.com", 587)
    #server.ehlo()
    server.starttls()
    #server.ehlo
    server.login(username, password)

    server.sendmail(fromEmail, toEmail, msg.as_string())
    server.quit()
