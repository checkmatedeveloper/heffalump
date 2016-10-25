#!/usr/bin/env python

import traceback
import json
from config import CheckMateConfig
import os
import requests
import pprint
import base64
import urllib
import time

class KeyMaster(object):

    def __init__(self, env=None):
        env_var = os.getenv('Chkm8WorkerServerType')
        checkmateconfig = CheckMateConfig()
        self.fort_knox_url = checkmateconfig.FORT_KNOX_URL

        if env == None:
            if env_var == "PROD":
                self.source = 'production'
            else:
                self.source = 'development'
        else:
            if env == "production" or env == "development":
                self.source = env
            else:
                self.source = 'development'


    def decryptMulti(self, values, keys):
        """


        """

        if False == isinstance(values, dict):
            raise ValueError("Decrypt Multi Error - values is not a list")

        num_values = len(values)

        if 0 == num_values:
            raise ValueError("Decrypt Multi Error - values empty")

        if False == isinstance(keys, dict):
            raise ValueError("Decrypt Multi Error - keys is not a list")

        num_keys = len(keys) 

        if 0 == num_keys:
            raise ValueError("Decrypt Multi Error - keys empty")

        if num_keys != num_values:
            raise ValueError("Decrypt Multi Error - num keys is not equal to num values")

        arr = []

        for x in values:
            if values[x] == None:
                continue

            tmp = {}
            tmp['id'] = x
            tmp['e_key'] = self.urlencode(keys[x])
            tmp['e_field'] = self.urlencode(values[x])
            arr.append(tmp)

        params = {}
        params['action'] = 'decrypt_multi'      # should be a constant
        params['source'] = self.source
        params['data'] = base64.encodestring(json.dumps(arr))

        # MAKE CURL CALL
        response = self.request(params)

        if False == response:
            raise Exception("Decrypt Multi Error - false response")

        response = json.loads(response)

        if 'success' not in response or response['success'] != True:
            raise Exception("Decrypt Multi Error - success not found or not true")

        if 'decoded' not in response or response['decoded'] == None or response['decoded'] == '':
            raise Exception("Decrypt Multi Error - decoded missing or invalid")


        #for x in response['decoded']:
        #    response['decoded'][x] = self.urldecode(response['decoded'][x])

        for key in keys:
            #response['decoded'][key] = '' if key not in response['decoded'] else str( self.urldecode(response['decoded'][key]) )
            response['decoded'][key] = '' if key not in response['decoded'] else self.urldecode(response['decoded'][key])
            response['decoded'][key] = response['decoded'][key].encode('utf-8')

        return response['decoded']


    def encryptMulti(self, values, keys = None):
        """


        """

        if False == isinstance(values, dict):
            raise ValueError("Encrypt Multi Error - values is not a list")

        num_values = len(values)

        if 0 == num_values:
            raise ValueError("Encrypt Multi Error - values empty")

        if keys == None:
            params = {}
            params['action'] = 'new_key'    # should be a constant
            params['source'] = self.source

            # MAKE CURL CALL
            response = self.request(params)

            if False == response:
                raise Exception("Encrypt Multi Error - false response")

            response = json.loads(response)

            if 'success' not in response or response['success'] != True:
                raise Exception("Encrypt Multi Error - success not found or not true")

            if 'e_key' not in response or response['e_key'] == None or response['e_key'] == '':
                raise Exception("Encrypt Multi Error - no or missing e_key")

            response['e_key'] = self.urldecode(response['e_key'])

            keys = dict.fromkeys(values, response['e_key'])


        if False == isinstance(keys, dict):
            raise ValueError("Encrypt Multi Error - keys is not a list")

        num_keys = len(keys)

        if 0 == num_keys:
            raise ValueError("Encrypt Multi Error - keys empty")

        if num_keys != num_values:
            raise ValueError("Encrypt Multi Error - num keys is not equal to num values")

        arr = []

        for x in values:
            tmp = {}
            tmp['id'] = x
            tmp['e_key'] = self.urlencode(keys[x])
            tmp['field'] = self.urlencode(values[x])
            arr.append(tmp)

        params = {}
        params['action'] = 'encrypt_multi'      # should be a constant
        params['source'] = self.source
        params['data'] = base64.encodestring(json.dumps(arr))

        # MAKE CURL CALL
        response = self.request(params)

        if False == response:
            raise Exception("Encrypt Multi Error - false response")

        response = json.loads(response)

        if 'success' not in response or response['success'] != True:
            raise Exception("Encrypt Multi Error - success not found or not true")

        if 'encoded' not in response or response['encoded'] == None or response['encoded'] == '':
            raise Exception("Encrypt Multi Error - encoded missing or invalid")

        result = {}

        for x in response['encoded']:
            tmp = {}
            tmp['e_key'] = keys[x]
            tmp['encoded'] = self.urldecode(response['encoded'][x])
            result[x] = tmp

        return result


    def urlencode(self, value):
        """
        URL Encodes the passed in value

        Parameters
        ----------
        value : string

        Returns
        -------
        out : string

        """

        return urllib.quote_plus(value)

    def urldecode(self, value):
        """
        URL Decodes the passed in value

        Parameters
        ----------
        value : string

        Returns
        -------
        out : string

        """

        return urllib.unquote_plus(value)

    def request(self, data, attempts=1):
        """


        """

        # response = requests.post(self.fort_knox_url, params=data)
        response = requests.post(self.fort_knox_url, data)

        if response.status_code != 200:
            if response.status_code == 504 and attempts < 3:
                time.sleep(.1)
                attempts += 1
                return self.request(data, attempts)
            else:
                raise Exception("Key Master request returned status code {0}".format(response.status_code))

        #return response.json()
        return response.text


if __name__ == "__main__":
    keymaster = KeyMaster();

    values = {}
    values['first_name'] = 'yuYNGzmb/QddgcACq5nmeQx91+GYrHmRj9tkklDajfBf7htJ8VPCQq+hzYulbrZP3RpbeMGtzip74fIMN94//4'
    values['last_name'] = '1usamPHDJSJWjFKe3cBRRgp7nwqQHJPNquIYUjAHZICOIZRr5tfOz/SqXEmOePvia1p5dA7SS1ZTq9dwScGUj2'
    values['company_name'] = 'VogFkTjNVcHcQVtvLNpW9wJ349lw0SjYky/PfWVlMEGKrm7uD205poIvLrfOGMH1EsHoHveyb3TsyY95Rh4E+q'

    keys = {}   
    keys['first_name'] = 'tD/cVA8N8gm6bJbTTgcCCQ5jVvpJ5raBWStgtUuxZkKml3vYsA0qxJv/VLXF5U+8M1cSKYCa7A/1dBB9dU4BN+7R6XOeegAW+ubP48yoKzgcAsfz+xKSn8FhiqwdXtIJijMC05GpgQo0DvfQQlqM7mgng1c8bCHpJn8CLNNp8VPwXgt3yRndal4cVsUhAGxWYGJY1Nkk7f1vf/dDrFh4pxRRcPOGu/6Mu/pt316lDcRA=='
    keys['last_name'] = 'tD/cVA8N8gm6bJbTTgcCCQ5jVvpJ5raBWStgtUuxZkKml3vYsA0qxJv/VLXF5U+8M1cSKYCa7A/1dBB9dU4BN+7R6XOeegAW+ubP48yoKzgcAsfz+xKSn8FhiqwdXtIJijMC05GpgQo0DvfQQlqM7mgng1c8bCHpJn8CLNNp8VPwXgt3yRndal4cVsUhAGxWYGJY1Nkk7f1vf/dDrFh4pxRRcPOGu/6Mu/pt316lDcRA=='
    keys['company_name'] = 'tD/cVA8N8gm6bJbTTgcCCQ5jVvpJ5raBWStgtUuxZkKml3vYsA0qxJv/VLXF5U+8M1cSKYCa7A/1dBB9dU4BN+7R6XOeegAW+ubP48yoKzgcAsfz+xKSn8FhiqwdXtIJijMC05GpgQo0DvfQQlqM7mgng1c8bCHpJn8CLNNp8VPwXgt3yRndal4cVsUhAGxWYGJY1Nkk7f1vf/dDrFh4pxRRcPOGu/6Mu/pt316lDcRA=='

    try:
        print "wallace"
        decoded = keymaster.decryptMulti(values,keys)
        print "wyatt"
        pprint.pprint(decoded)
        print "first name = {0}".format(decoded['first_name'])
        print "last name = {0}".format(decoded['last_name'])
        print "company name = {0}".format(decoded['company_name'])
    except Exception, e:
        # trace = str(traceback.format_exc())
        print 'Something went wrong ' + str(e) + " - " + traceback.format_exc()
 

    values = {}
    values['first_name'] = 'Wyatt'
    values['last_name'] = 'TheDestroyer+'
    values['company_name'] = 'Parametric'

    try:
        print "pre encrypt"
        encoded = keymaster.encryptMulti(values)
        print "encoded = "
        pprint.pprint(encoded)
        print "post encrypt"

        '''
[{'e_key': u'znr7LVD33O1F84IUmEnmNQ28gP2d2VXImQMMyT9EvHsXp7t084gSpo0M8xiCTV70zH/N/Uozdz9A5zoyfPXlJ09+Sz/YGQSC7f4E6RLt7tdIJQcGlPpeIm3KL63iijHxk4TGJPBX5+q4FgccqQPpYZvHF9dhQ80nFdfwC+LTUY7CKDScrrNjk8bKa9U0l2LnC6g7gUeZXTzzaANy8dZLvO+NSMNi4c19jL+XbWk068ew==',
  'encoded': u'qdl335FPalGHEGRM2sKdPgvOPQszts7O7+FPVTI+JT9gkBtkYM6kvIons6KzzxkQAz8IU0P9G7O7RUOQiLFU11'},
 {'e_key': u'znr7LVD33O1F84IUmEnmNQ28gP2d2VXImQMMyT9EvHsXp7t084gSpo0M8xiCTV70zH/N/Uozdz9A5zoyfPXlJ09+Sz/YGQSC7f4E6RLt7tdIJQcGlPpeIm3KL63iijHxk4TGJPBX5+q4FgccqQPpYZvHF9dhQ80nFdfwC+LTUY7CKDScrrNjk8bKa9U0l2LnC6g7gUeZXTzzaANy8dZLvO+NSMNi4c19jL+XbWk068ew==',
  'encoded': u'U8yaTCXYpUtXx60mJMmxTgZy9Sb1sfCXQs418JfDPkYvqRlLSojzEthNrGvA7xYSYCESHU8bFHI1bbipzbkD4Z'},
 {'e_key': u'znr7LVD33O1F84IUmEnmNQ28gP2d2VXImQMMyT9EvHsXp7t084gSpo0M8xiCTV70zH/N/Uozdz9A5zoyfPXlJ09+Sz/YGQSC7f4E6RLt7tdIJQcGlPpeIm3KL63iijHxk4TGJPBX5+q4FgccqQPpYZvHF9dhQ80nFdfwC+LTUY7CKDScrrNjk8bKa9U0l2LnC6g7gUeZXTzzaANy8dZLvO+NSMNi4c19jL+XbWk068ew==',
  'encoded': u'ICZSa2tmiSOQTVonCrNX0QJ2f34/EMcClPnOPsE70950QSULbyFLil9olJZeFXKply5qhe2u4PBt0ulRgtDKtt'}]
        ''' 


        values = {}
        keys = {}

        for x in encoded:
            values[x] = encoded[x]['encoded']
            keys[x] = encoded[x]['e_key']
                        

        print "decoding"
        decoded = keymaster.decryptMulti(values, keys)
        pprint.pprint(decoded)
        print "done ..."



    except Exception, e:
        # trace = str(traceback.format_exc())
        print 'Something went wrong ' + str(e) + " - " + traceback.format_exc()


    print "end"
