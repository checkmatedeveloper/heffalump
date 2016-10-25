from keymaster import KeyMaster
import hashlib
from config import CheckMateConfig

def hashString(string):
    checkmateconfig = CheckMateConfig()
    string = "{0}{1}".format(string, checkmateconfig.SALT_PATRONS) 
    return hashlib.sha256(string).hexdigest()

def confirmInsert(levyDB,
                  venue_uid,
                  import_uuid,
                  pointer_schema,
                  pointer_table,
                  pointer_field,
                  new_value,
                  encrypt=False,
                  levy_temp_pointer=None,
                  ignoreUniqueConstraint=False,
                  auto_apply = False):
        
    if encrypt:
        km = KeyMaster()
        values = {}
        values['new_value'] = new_value
        encoded = km.encryptMulti(values)

        if False == isinstance(encoded, dict):
            raise Exception("confirm insert  Error - Unable to encode insert data")

        if 'new_value' not in encoded:
            raise Exception("confirm insert Error - Unknown encoded")

        if 'encoded' not in encoded['new_value']:
            raise Exception("Add  Error - Missing encoded names")

        if 'e_key' not in encoded['new_value']:
            raise Exception("Add  Error - Missing e_key")

        eKey = encoded['new_value']['e_key']

        insertValue = encoded['new_value']['encoded']
    else:
        insertValue = new_value

    #print venue_uid   
    purgatoryRowUid = levyDB.addPurgatoryRow(venue_uid, import_uuid, pointer_schema, pointer_table, pointer_field, None, None, insertValue, 'add', levy_temp_pointer, ignoreUniqueConstraint, auto_apply) 

    
    
    #add the crypto keys to th db
    if encrypt:
            levyDB.insertDataKey(purgatoryRowUid, 'purgatory', 'integrations', eKey)
 
def confirmUpdate(levyDB,
                  venue_uid,
                  pointer_schema, 
                  pointer_table, 
                  pointer_field, 
                  pointer_uid, 
                  old_value,
                  new_value, 
                  encrypt=False,  #true if the new value should be encrypted before being passed on to the db layer
                  levy_temp_pointer=None): 
   
#    print locals()
 
    if encrypt:         
        km = KeyMaster()
        values = {}
        values['new_value'] = new_value

        key = levyDB.getEKey(pointer_schema, pointer_table, pointer_uid)
        keys = {}    
       
        keys['new_value'] = key[0]
        encoded = km.encryptMulti((values), keys)
        
        if False == isinstance(encoded, dict):
            raise Exception("Update Error - Unable to encode new value")

        if 'new_value' not in encoded:
            raise Exception("Update Error - Unknown encoded")

        if 'encoded' not in encoded['new_value']:
            raise Exception("Update Error - Missing encoded names")

        if 'e_key' not in encoded['new_value']:
            raise Exception("Update Error - Missing e_key")

        eKey = encoded['new_value']['e_key']

        insertValue = encoded['new_value']['encoded']         
    else:
        insertValue = new_value

    purgatoryRowUid = levyDB.addPurgatoryRow(venue_uid, None, pointer_schema, pointer_table, pointer_field, pointer_uid, old_value, insertValue, 'edit', levy_temp_pointer)
    
    #add the crypto keys to th db
    #no don't were using an existing key
    #if encrypt:
    #    levyDB.insertDataKey(purgatoryRowUid, 'purgatory', 'integrations', eKey) 

def confirmRemove(levyDB,
                  venueUid,
                  pointer_schema,
                  pointer_table,
                  pointer_field,
                  pointer_uid):

    levyDB.addPurgatoryRow(venueUid, None, pointer_schema, pointer_table, pointer_field, pointer_uid, None, None, 'remove', None)


def confirmImage(levyDB,
                 venue_uid,
                 pointer_schema,
                 pointer_table,
                 pointer_field,
                 pointer_uid,
                 image_url):
    purgatoryRowUid = levyDB.addPurgatoryRow(venue_uid, None, pointer_schema, pointer_table, pointer_field, pointer_uid, None, image_url, 'image', None)


def confirmDeactivate(levyDB,
                      venue_uid,
                      pointer_schema,
                      pointer_table,
                      pointer_uid,
                      auto_apply = False):

    print str(venue_uid) + " " + pointer_schema + " " + pointer_table + " " + str(pointer_uid)
    purgatoryRowUid = levyDB.addPurgatoryRow(venue_uid, None, pointer_schema, pointer_table, 'is_active', pointer_uid, 1, 0, 'deactivate', levy_temp_pointer = None, auto_apply = auto_apply)


def confrimReActivate(levyDB,
                      venue_uid,
                      pointer_schema,
                      pointer_table,
                      pointer_uid):

    purgatoryRowUid = levyDB.addPurgatoryRow(venue_uid, None, pointer_schema, pointer_table, 'is_active', pointer_uid, 0, 1, 'reactivate', None)




def decryptPatron(emailerDb, patronUid):
    km = KeyMaster()
    
    encryptedCompanyName, eKey = emailerDb.getEncryptedPatronCompanyNameAndKey(patronUid)
    
    values = {}
    values['company_name'] = encryptedCompanyName

    keys = {}
    keys['company_name'] = eKey

    decryptedValues = km.decryptMulti(values, keys)

    return decryptedValues['company_name']

def encryptPatron(customerName):
    km = KeyMaster()
    values = {}
    values['customer_name'] = customerName

    encoded = km.encryptMulti((values))

    if False == isinstance(encoded, dict):
        raise Exception("Update Error - Unable to encode new value")

    if 'customer_name' not in encoded:
        raise Exception("Update Error - Unknown encoded")

    if 'encoded' not in encoded['customer_name']:
        raise Exception("Update Error - Missing encoded names")

    encodedValue = encoded['customer_name']['encoded']

    if 'e_key' not in encoded['customer_name']:
        raise Exception("Update Error - Missing e_key")

    eKey = encoded['customer_name']['e_key']

    return encodedValue, eKey
    
