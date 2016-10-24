from __future__ import division
import sys
from StatusRow import StatusRow
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import timezone_converter
import gmail

def getStatusRows(conn, venueUid):
    """
        Queries the database and returns all of tablet status data represented as
        StatusRow objects

        Parameters
        __________
        conn : DbConnection a active connection to a database
        venueUid : int the venue to get status rows for

        Returns
        _______
        statusRows : array of StatusRow objects
    """

    cursor = conn.cursor()
    cursor.execute("SELECT \
	                   venues_x_tablet.device_id as tablet, \
	                   target_system_version_uid as targetVersion, \
	                   current_system_version_uid as currentVersion, \
	                   systems.`name` as systemType, \
	                   tablets.battery, \
	                   tablets.hw_status as charging, \
                       check_in_date as lastCheckedIn \
                    FROM tablets.venues_x_tablet \
                    JOIN tablets.system_versions ON venues_x_tablet.current_system_version_uid = system_versions.id \
                    JOIN tablets.systems ON system_versions.system_uid = systems.id \
                    JOIN tablets.tablets ON tablets.device_id = venues_x_tablet.device_id \
                    LEFT JOIN tablets.tablet_checkins ON tablets.device_id = tablet_checkins.device_uid  \
                    WHERE venue_uid = %s \
                        AND deploy_status = 'active' \
                    ORDER BY systemType, (lastCheckedIn IS NULL), lastCheckedIn DESC",
                    (venueUid)
                  )
    statusRows = []

    for(tablet, targetVersion, currentVersion, systemType, battery, charging, lastCheckedIn) in cursor:
        statusRows.append(StatusRow(conn, tablet, systemType, lastCheckedIn, currentVersion, targetVersion, battery, charging))

    return statusRows


def getSummary(statusRows):
    """
        Builds a tablet summary based on the statusRows that are passed in.  Summary
        contains the number of ready and not ready tabelt separated by systemType
        arranged into a pretty little table.

        Parameters
        __________
        statusRows : array of StatusRows

        Returns
        _______
        out : the html for the tablet summary
        out : the total number of all tablets
        out : the total number of bad tablets
    """

    cmTablets = 0
    badCmTablets = 0
    smTablets = 0
    badSmTablets = 0
    gTablets = 0
    badGTablets = 0;

    for row in statusRows:

        if(row.systemType == 'USCF Attendant'):
            cmTablets += 1
            if(row.issueLevel > 1):
                badCmTablets += 1
        if(row.systemType == 'USCF Guest'):
            smTablets += 1
            if(row.issueLevel > 1):
                badSmTablets += 1
        if(row.systemType == 'USCF Greeter'):
            gTablets += 1
            if(row.issueLevel > 1):
                badGTablets += 1

    goodCmTablets = cmTablets - badCmTablets
    goodSmTablets = smTablets - badSmTablets
    goodGTablets = gTablets - badGTablets

    summary = '<table width="100%" cellpadding="10">\n'
    summary += '<tr bgcolor=#888888>\n'

    if (cmTablets != 0):
        summary += '<th>CheckMate:</th>'


    if(smTablets != 0):
        summary += '<th>SuiteMate:</th>'


    if(gTablets != 0):
        summary += '<th>Greeter:</th>'

    summary += '</tr><tr>\n'

    if(cmTablets != 0):
        summary += "<td><ul><li>" + str(goodCmTablets) + ' ready</li><li>' + str(badCmTablets) + ' need attention</li></td>\n'
    if(smTablets != 0):
        summary += "<td><ul><li>" + str(goodSmTablets) + ' ready</li><li>' + str(badSmTablets) + ' need attention</li></td>\n'
    if(gTablets != 0):
        summary += "<td><ul><li>" + str(goodGTablets) + ' ready</li><li>' + str(badGTablets) + ' need attention</li></td>\n'

    summary += "</tr>"
    summary += "</table><br><br>\n"

    return summary, cmTablets + smTablets + gTablets, badCmTablets + badSmTablets + badGTablets


def getTableHeader():
    """
        Returns the header row to the tablets status table

        Parameters
        __________

        Returns
        _______
        out: the html for the table header
    """

    header = '<tr bgcolor="#888888">'
    header += "<th>" + 'Type' + "</th>"
    header += "<th>" + 'Tablet' + "</th>"
    header += "<th>" + 'Checked In' + "</th>"
    header += "<th>" + 'Version' + "</th>"
    header += "<th>" + 'Battery' + "</th>"
    header += "<th>" + 'Charging' + "</th>"
    header += "</tr>"

    return header

def getLocalFormattedDate(conn, venueUid, utc_dt):

    """
        Returns a datetime, converted to the venues local timezone and foramtted for
        use in the status email.

        Parameters
        __________
        conn : DbConnection
        venueUid : int used to lookup which timezone to convert the utc date to =
        utc_dt : datetime the utc date to be converted

        Returns
        _______
        out : String the local timezone converted, pretty formatted date
    """



    cursor = conn.cursor()
    cursor.execute("SELECT local_timezone_long FROM setup.venues WHERE id = %s", (venueUid))
    tzString = cursor.fetchone()[0]



    localDate = timezone_converter.convert_utc_to_local(utc_dt, tzString)

    #print "Converting " + str(utc_dt) + " utc to " + tzString + " Result = " + str(localDate)


    localDate = localDate - localDate.dst()

    #format the date like this: August 22, 2014 @ 1:00 PM
    return localDate.strftime('%B %-d, %Y @ %-I:%M %p')


def getIntro(conn, venueUid, eventUid):
    """
        Reuturns the email intro

        Parameters
        __________
        conn : DbConnection
        venueUid : int used to lookup the venue's name
        eventUid : used to lookup the event's name and date

        Returns
        _______
        out : the html for the email intro
    """

    cursor = conn.cursor()
    cursor.execute("SELECT event_name, event_date, venues.name as venue_name FROM setup.events \
                    JOIN setup.events_x_venues ON events.id = events_x_venues.event_uid \
                    JOIN setup.venues ON events_x_venues.venue_uid = venues.id \
                    WHERE events_x_venues.venue_uid = %s and events.id = %s", (venueUid, eventUid))

    eventName, eventDate, venueName = cursor.fetchone()

    prettyDate = getLocalFormattedDate(conn, venueUid, eventDate)

    intro = "<font size=4>" + venueName + " Tablet Report <br>" + eventName + "<br>" + prettyDate + "</font><br><br><br>"

    return intro


def getEmailList(conn, venueUid):
    """
        Returns the list of email addresses that should receive tablet status emails
        for the given venue.

        Parameters
        __________
        conn : DbConnection
        venueUid: used to lookup the email address's

        Returns
        _______
        out : arrays of arrays that contain the email adresses
    """

    cursor = conn.cursor()
    cursor.execute("SELECT email_address FROM setup.venues_x_contacts \
                    JOIN setup.employee_contact_info ON venues_x_contacts.employee_contact_info_uid = employee_contact_info.id \
                    WHERE venue_uid = %s AND purpose = 'tablet_reports'", (venueUid)
                  )

    return cursor.fetchall()


def getTable(statusRows):
    """
        Returns the tablet status table

        Parameters
        __________
        statusRows : array of StatusRow objects

        Returns
        _______
        out : the table html
    """

    table = '<table bgColor="white" width="100%" border="0" cellpadding="10">'

    table += getTableHeader()
    darkRow = False
    for row in statusRows:
        darkRow = not darkRow
        table += row.getTableRow(darkRow)

    table += '</table></font>'

    return table


def getSubject(totalTablets, badTablets):
    """
        Retruns the subject for the email.  If more than 50% of the tablets are not
        in a good state a special 'Warning' subject is returned

        Parameters
        __________
        totalTablets : int the total number of tablets for the venue
        badTablets : int the total not ready tablets for the venue

        Returns
        _______
        out : String the subject for the email
    """
    if(badTablets == 0):
        return "Tablet Report"

    if((badTablets / totalTablets) > .5):
        return "Tablet Report: WARNING! less than 50% of your tablets are ready for the upcoming event"
    else:
        return "Tablet Report"


def sendStatusEmail(venueUid, eventUid, manual=False):
    """
        Sends an email for the given event to everyone on the email list of the given
        venue.
            1. Builds the email html body
            2. gathers the email addresses
            3. sends emails

        Parameters
        __________
        venueUid : int the venue_uid to send status emails for
        evenutUid : int the event_uid to send status emails for

        Reutrns
        _______
        out : boolean True if successful
    """

    from db_connection import DbConnection
    conn = DbConnection().connection

    statusRows = getStatusRows(conn, venueUid)

    body = '<font face="verdana">'

    table = getTable(statusRows)

    body += getIntro(conn, venueUid, eventUid)

    summary, totalTablets, badTablets = getSummary(statusRows)
    body += summary

    body += table

    if(manual):
        body += "<h1><b>*</b></h1>"

    subject = getSubject(totalTablets, badTablets)

    emailList = getEmailList(conn, venueUid);

    for email in emailList:
        gmail.sendGmail("tech@parametricdining.com", "fkTUfbmv2YVy", "tablets@parametricdining.com", email[0], subject, body, 'Please, enable html to view this report')

    return True
