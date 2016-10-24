import requests

COLOR_YELLOW = "yellow"
COLOR_RED = "red"
COLOR_GREEN = "green"
COLOR_PURPLE = "purple"
COLOR_GRAY = "gray"
COLOR_RANDOM = "random"


TECH_ROOM = 447878
INTEGRATIONS_ROOM = 1066556


def sendMessage(message, sendFrom, room, color, message_format="text"):
    '''
        Posts a message to hipchat

        Parameters
        __________
        message : the text of the message to post, will play nicely
            with emoticons, links, or any other hipchat goodness
        sendFrom : who should appear in the "from" field, values
            must be less than 15 chars
        room : the room id to post the message into
        color : the color of the message background.  Acceptable 
            colors are: yellow", "red", "green", "purple", "gray",
             or "random"

        Returns
        _______
        out : None
    '''
    hipchat_dict = {'auth_token':'987ce42cc059f3a404e18c9f9c9642',
                         'room_id':str(room),
                         'from':sendFrom,
                         'message':message,
                         'notify':'1',
                         'message_format':message_format,
                         'color':color}

    resp = requests.post('http://api.hipchat.com/v1/rooms/message', params=hipchat_dict)
    #print resp
    return resp
