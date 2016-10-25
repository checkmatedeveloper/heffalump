import redis
from config import CheckMateConfig
import logging
import traceback
from Levy_DB import Levy_Db
import HipChat
import uuid
from db_connection import DbConnection
import os
import sys
import HipChat

class LevyIntegrationWorker():
    
    LOCK_STRING = "LOCK ACQUIRED"
    UNLOCK_STRING = "LOCK RELEASED"

    insertCount = 0
    insertFailCount = 0
        
    updateCount = 0
    updateFailCount = 0

    db = None

    def __init__(self, levyDB):
        self.db = DbConnection().connection    
        self.levyDB = levyDB

    def setLock(self):
        self.levyDB.addLogRow(self.LOCK_STRING)

    def releaseLock(self):
        self.levyDB.addLogRow(self.UNLOCK_STRING)
        
    def isLocked(self):
        lockStatus = self.levyDB.getLastAction()
        print str(lockStatus)        
        if lockStatus != self.UNLOCK_STRING or lockStatus is None: #is none in case this is the first row
            return True;
        else:
            return False;

    def handleError(self, errorMessage):
        #might not be the best idea to send all of these we were hammering hipchat pretty hard until it flow rated us
        #print HipChat.sendMessage(errorMessage, "IntWorker", 1066556, "yellow")
        self.levyDB.addLogRow(errorMessage)
       
        
    def consolidatePurgatoryRows(self, applyActionRows):
        actions = {}

        for row in applyActionRows:
            if row[2] != None:
                if actions.get(row[2]) == None:
                    actions[row[2]] = []
                actions[row[2]].append(row)
            else:
                temp_uuid = uuid.uuid4()
                actions[temp_uuid] = []
                actions[temp_uuid].append(row)

        return actions
        

    def processAction(self, action):
        
        requiredAction = action[0][9] # all actions are the same
        print "ID: " + str(action[0][0])        

        if requiredAction == 'edit':
        
            success, errorMessage = self.levyDB.updateRow(action[0][3], action[0][4], action[0][5], action[0][6], action[0][8])
            
            if success:
                self.updateCount += 1
                self.levyDB.purgatoryRowApplied(action[0][0])
            else:
                self.updateFailCount += 1
                self.levyDB.purgatoryRowFailed(action[0][0], errorMessage)
                handleError(errorMessage)
                 
        if requiredAction == 'add':
                
            

            fields = list()
            values = list()

            eventName = ""

            for fieldRow in action:
                        
                if fieldRow[4] == 'events_x_venues' and fieldRow[5] == 'event_name':
                    print "HACKY Events X Venues"
                    eventName = fieldRow[8] 
                    action.remove(fieldRow) #we don't actually want to use the value of the event name for the auto insert
                                            # but we will need it later for inserting in to the cross table
                    print str(action)
                else:
                    fields.append(fieldRow[5])
                    values.append(fieldRow[8])                   
           
            success, message = self.levyDB.insertRow(action[0][3], action[0][4], fields, values)
            
               

            if success:
                #warning, we had to get a little hacky to insert into the X table
                if action[0][3] == 'setup' and action[0][4] == 'employees':
                    print "Action: " + str(action[0])
                    self.levyDB.insertRow('setup', 'venues_x_employees', ['venue_uid', 'employee_uid'], [action[0][1], message] )
               
                if action[0][3] == 'setup' and action[0][4] == 'events' and eventName != "":
                    self.levyDB.insertRow('setup', 'events_x_venues', ['event_uid', 'venue_uid', 'event_name'], [message, action[0][1], eventName]) 

                self.insertCount += 1
                for fieldRow in action:
                    self.levyDB.purgatoryRowApplied(fieldRow[0])
            else:
                self.insertFailCount += 1
                for fieldRow in action:
                    self.levyDB.purgatoryRowFailed(fieldRow[0], message)
                    self.handleError(message)
    
    def main(self):
        print "MAIN"        
        
        self.insertCount = 0
        self.insertFailCount =0
        
        self.updateCount = 0
        self.updateFailCount = 0

       

        if self.isLocked():
            self.handleError("WARNING: Attempted to run an integration worker while another one is running")
            sys.exit()  
        print "SAFE TO RUN"
        self.setLock()          
    
        applyActionRows = self.levyDB.getPurgatoryRowsToApply()
        
        if len(applyActionRows) == 0:
            self.releaseLock()
            sys.exit() 
        actions = self.consolidatePurgatoryRows(applyActionRows)
     

        for action in actions:
            self.processAction(actions[action])
        
        message = "Integrations Applied: \n " + str(self.insertCount) + " new rows inserted "
        if self.insertFailCount > 0:
            message += "(" + str(self.insertFailCount) + " inserts failed)"

        message += "\n"

        message += str(self.updateCount) + " rows updated "
        if self.updateFailCount > 0:
            message += "(" + str(self.updateFailCount) + " updates failed)"

        message += "\n"

        #:TODO
        if self.updateFailCount + self.insertFailCount > 0:
            message += "Failures: http://www.animatedgif.net/underconstruction/btrainbow1_e0.gif"

        HipChat.sendMessage(message, "Integrations", "1066556", "purple")
    
        self.levyDB.addLogRow("Integrations Worker Completed")
        self.releaseLock() 





