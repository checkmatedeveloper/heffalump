from db_connection import DbConnection
from TrainingConfigDB import TrainingConfigDB
import HipChat
import sys
import json

conn = DbConnection().connection
dbCore = TrainingConfigDB(conn)


venueUid = sys.argv[1]

print "Creating New Training Events for " + str(venueUid)

# 1. Create 2 new Events
managerTrainingEventUid = dbCore.createTrainingEvent(venueUid, "Manager Training")
print "Manager Training Event Created, Uid = " + str(managerTrainingEventUid)


attendantTrainingEventUid = dbCore.createTrainingEvent(venueUid, "Attendant Training")
print "Attendnet Trainig Event Created, Uid = " + str(attendantTrainingEventUid)

# 2. Create 7 new training patrons
patronNames = [
                "Open/Close Orders", 
                "CC on File and Grat", 
                "Voids and Discounts", 
                "Notes and Order Combine", 
                "Tax Exempt and Split", 
                "Order Void and Reopen", 
                "Preorder, PAR, Auth Signer"
              ]

patronUids = []

for patronName in patronNames:
    patronUid = dbCore.createTrainingPatron(venueUid, patronName)
    print "Training Patron Created: " + patronName + " , Uid = " + str(patronUid)
    patronUids.append(patronUid)

    

    


# 3. Create 3 new training employees
class Employee:
    MANAGER = 1
    ATTENDANT = 2
    COFFEE_CART = 15

    def __init__(self, firstName, lastName, loginCode, role):
        self.firstName = firstName
        self.lastName = lastName
        self.loginCode = loginCode
        self.role = role

employees = []
x = Employee("Molly", "Moore", "12345", Employee.MANAGER)
employees.append(x)
x = Employee("Artie", "Armstrong", "88888", Employee.ATTENDANT)
employees.append(x)
x = Employee("Conner", "Cooper", "44444", Employee.COFFEE_CART)
employees.append(x)

employeeUids = []

for employee in employees:
    employeeUid = dbCore.createTrainingEmployee(venueUid, employee)
    print "Training Employee Created, " + employee.firstName  + " " + employee.lastName + ", Uid = " + str(employeeUid)
    employeeUids.append(employeeUid) 


#4. Pick Par Items
parItem1 = input("First par item uid: ") 
parItem2 = input("Second par item uid: ")

parMXM1 = dbCore.getParMXM(venueUid, parItem1)
parMXM2 = dbCore.getParMXM(venueUid, parItem2)

#5. add merchants_x_venues
merchantUid = dbCore.createTrainingMerchantXVenues(venueUid)


#6. Create Configuration files
FILE_PATH = "/home/ec2-user/crons/repo/TrainingEvents/"


#   per venue

#       DELETE_BASE_SHINFO.sql
concatedPatronUids = ', '.join(str(x) for x in patronUids)

fileContents = ""
fileContents += '''DELETE 
                   FROM info.unit_patron_discounts
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = %s AND patron_uid IN (%s));''' % (venueUid, concatedPatronUids)
fileContents += '''
                   DELETE 
                   FROM info.unit_patron_gratuities
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = %s AND patron_uid IN (%s));''' % (venueUid, concatedPatronUids)
fileContents += '''
                   DELETE 
                   FROM info.unit_patron_info
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = %s AND patron_uid IN (%s));''' % (venueUid, concatedPatronUids)
fileContents += '''
                   DELETE 
                   FROM info.unit_patron_notes
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = %s AND patron_uid IN (%s));''' % (venueUid, concatedPatronUids)

fileContents += '''
                   DELETE 
                   FROM info.unit_patron_pars
                   WHERE unit_patron_uid IN (SELECT
                                                unit_x_patrons.id
                                             FROM info.unit_x_patrons
                                             JOIN setup.units ON unit_x_patrons.unit_uid = units.id
                                             JOIN patrons.clone_patrons ON clone_patrons.id = unit_x_patrons.patron_uid
                                             WHERE venue_uid = %s AND patron_uid IN (%s));''' % (venueUid, concatedPatronUids)
print fileContents
fileName = "venue_%s-DELETE_BASE_SHINFO.sql" % venueUid

with open(FILE_PATH + fileName, 'w') as file:
    file.write(fileContents)
    file.close()




#       patron_cart_info.json
print str(patronUids)
fileContents = '''[
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"yes", "pay_method":"unknown", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"yes", "pay_method":"unknown", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"yes", "pay_method":"direct_bill", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"yes", "pay_method":"direct_bill", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"on_request", "pay_method":"cc_on_file", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"on_request", "pay_method":"cc_on_file", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"on_request", "pay_method":"cc_present", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"on_request", "pay_method":"cc_present", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"no", "pay_method":"preorder_card", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"no", "pay_method":"preorder_card", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"no", "pay_method":"cc_present", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"no", "pay_method":"cc_present", "is_pay_auth_required":0}
    ]
  },
  {
    "patron_uid":%s,
    "carts":[
      {"cart_type_uid":1, "should_stop":"no", "pay_method":"cc_present", "is_pay_auth_required":0},
      {"cart_type_uid":2, "should_stop":"no", "pay_method":"cc_present", "is_pay_auth_required":0}
    ]
  }
]''' % tuple(patronUids)

fileName = "venue_%s-patron_cart_info.json" % (venueUid)
with open(FILE_PATH + fileName, 'w') as file:
    file.write(fileContents)
    file.close()

#       unit_patron_info.json

#suitesRCUid, coffeeCartRCUid = dbCore.getRevenueCenterUids()

fileContents = "["

suitesRevenueCenterUid = input('Suites Revenue Center Uid: ')
cartRevenueCenterUid = input('Cart Revenue Center Uid: ')

fileContents +='''  
{
    "patron_uid":%s,
    "preorder_pay_method":"preorder_card",
    "preorder_pay_auth":0,
    "doe_pay_method":"cc_present",
    "doe_pay_auth":0,
    "replenish_pay_method":"cc_present",
    "replenish_pay_auth":0,
    "liquor_cabinet_open":1,
    "liquor_cabinet_auth":0,
    "refrigerator_open":1,
    "refrigerator_auth":0,
    "restock_pay_method":"cc_present",
    "restock_pay_auth":0,
    "present_bill":1,
    "provide_receipt":1,
    "can_guest_invoice":1,
    "discount":0,
    "notes":"",
    "gratuities":[
      {
        "revenue_center_uid":%s,
        "automatic_gratuity":null,
        "gratuity_percentage":0,
        "gratuity_minimum":0,
        "is_gratuity_adjustable":0,
        "gratuity_maximum":0,
        "gratuity_flat_amount":0
      },
      {
        "revenue_center_uid":%s,
        "automatic_gratuity":null,
        "gratuity_percentage":0,
        "gratuity_minimum":0,
        "is_gratuity_adjustable":0,
        "gratuity_maximum":0,
        "gratuity_flat_amount":0
      }
    ],
    "carts":[
      {
        "cart_type_uid":1,
        "should_stop":"yes",
        "pay_method":"cc_present",
        "is_pay_auth_required":0
      },
      {
        "cart_type_uid":3,
        "should_stop":"yes",
        "pay_method":"cc_present",
        "is_pay_auth_required":0
      }
    ],
    "par_items":[]
  },''' % (patronUids[0], suitesRevenueCenterUid, cartRevenueCenterUid)

fileContents +='''{
      "patron_uid":%s,
      "preorder_pay_method":"cc_on_file",
      "preorder_pay_auth":0,
      "doe_pay_method":"cc_on_file",
      "doe_pay_auth":0,
      "replenish_pay_method":"cc_on_file",
      "replenish_pay_auth":0,
      "liquor_cabinet_open":"1",
      "liquor_cabinet_auth":0,
      "refrigerator_open":1,
      "refrigerator_auth":0,
      "restock_pay_method":"cc_on_file",
      "restock_pay_auth":0,
      "present_bill":1,
      "provide_receipt":1,
      "can_guest_invoice":1,
      "discount":0,
      "notes":"",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"percentage",
          "gratuity_percentage":15,
          "gratuity_minimum":10.0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":200.0,
          "gratuity_flat_amount":0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"percentage",
          "gratuity_percentage":15,
          "gratuity_minimum":10.0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":200.0,
          "gratuity_flat_amount":0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"yes",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":0
        },
        {
          "cart_type_uid":3,
          "should_stop":"yes",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":0
        }
      ],
      "par_items":[]
    },''' % (patronUids[1], suitesRevenueCenterUid, cartRevenueCenterUid)

discount = dbCore.getVenueDiscount(venueUid)
fileContents += '''{
      "patron_uid":%s,
      "preorder_pay_method":"direct_bill",
      "preorder_pay_auth":0,
      "doe_pay_method":"direct_bill",
      "doe_pay_auth":0,
      "replenish_pay_method":"direct_bill",
      "replenish_pay_auth":0,
      "liquor_cabinet_open":0,
      "liquor_cabinet_auth":0,
      "refrigerator_open":0,
      "refrigerator_auth":0,
      "restock_pay_method":"direct_bill",
      "restock_pay_auth":0,
      "present_bill":1,
      "provide_receipt":1,
      "can_guest_invoice":0,
      "discount":%s,
      "notes":"",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"yes",
          "pay_method":"direct_bill",
          "is_pay_auth_required":0
        },
        {
          "cart_type_uid":3,
          "should_stop":"yes",
          "pay_method":"direct_bill",
          "is_pay_auth_required":0
        }
      ],
      "par_items":[]
    },''' % (patronUids[2], discount, suitesRevenueCenterUid, cartRevenueCenterUid)

fileContents += '''{
      "patron_uid":%s,
      "preorder_pay_method":"preorder_card",
      "preorder_pay_auth":0,
      "doe_pay_method":"cc_on_file",
      "doe_pay_auth":0,
      "replenish_pay_method":"cc_on_file",
      "replenish_pay_auth":0,
      "liquor_cabinet_open":0,
      "liquor_cabinet_auth":0,
      "refrigerator_open":0,
      "refrigerator_auth":0,
      "restock_pay_method":"cc_on_file",
      "restock_pay_auth":0,
      "present_bill":1,
      "provide_receipt":1,
      "can_guest_invoice":0,
      "discount":0,
      "notes":"Suite temp should be set to 75",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"percentage",
          "gratuity_percentage":20,
          "gratuity_minimum":50.0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":300.0,
          "gratuity_flat_amount":0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"percentage",
          "gratuity_percentage":20,
          "gratuity_minimum":50.0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":300.0,
          "gratuity_flat_amount":0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"on_request",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":0
        },
        {
          "cart_type_uid":3,
          "should_stop":"on_request",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":0
        }
      ],
      "par_items":[]
    },''' % (patronUids[3], suitesRevenueCenterUid, cartRevenueCenterUid)

fileContents += '''{
      "patron_uid":%s,
      "preorder_pay_method":"preorder_card",
      "preorder_pay_auth":0,
      "doe_pay_method":"cc_present",
      "doe_pay_auth":0,
      "replenish_pay_method":"cc_present",
      "replenish_pay_auth":0,
      "liquor_cabinet_open":1,
      "liquor_cabinet_auth":0,
      "refrigerator_open":1,
      "refrigerator_auth":0,
      "restock_pay_method":"cc_present",
      "restock_pay_auth":0,
      "present_bill":0,
      "provide_receipt":0,
      "can_guest_invoice":0,
      "discount":0,
      "notes":"",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"flat",
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":50.0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":"flat",
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":50.0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"on_request",
          "pay_method":"cc_present",
          "is_pay_auth_required":0
        },
        {
          "cart_type_uid":3,
          "should_stop":"on_request",
          "pay_method":"cc_present",
          "is_pay_auth_required":0
        }
      ],
      "par_items":[]
    },''' % (patronUids[4], suitesRevenueCenterUid, cartRevenueCenterUid)

fileContents += '''{
      "patron_uid":%s,
      "preorder_pay_method":"cc_on_file",
      "preorder_pay_auth":1,
      "doe_pay_method":"cc_on_file",
      "doe_pay_auth":1,
      "replenish_pay_method":"cc_on_file",
      "replenish_pay_auth":1,
      "liquor_cabinet_open":1,
      "liquor_cabinet_auth":0,
      "refrigerator_open":1,
      "refrigerator_auth":0,
      "restock_pay_method":"cc_on_file",
      "restock_pay_auth":1,
      "present_bill":1,
      "provide_receipt":1,
      "can_guest_invoice":1,
      "discount":0,
      "notes":"",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"no",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":1
        },
        {
          "cart_type_uid":3,
          "should_stop":"no",
          "pay_method":"cc_on_file",
          "is_pay_auth_required":1
        }
      ],
      "par_items":[]
    },''' % (patronUids[5], suitesRevenueCenterUid, cartRevenueCenterUid)

fileContents += '''{
      "patron_uid":%s,
      "preorder_pay_method":"preorder_card",
      "preorder_pay_auth":0,
      "doe_pay_method":"preorder_card",
      "doe_pay_auth":1,
      "replenish_pay_method":"preorder_card",
      "replenish_pay_auth":0,
      "liquor_cabinet_open":0,
      "liquor_cabinet_auth":0,
      "refrigerator_open":1,
      "refrigerator_auth":0,
      "restock_pay_method":"cc_on_file",
      "restock_pay_auth":1,
      "present_bill":1,
      "provide_receipt":1,
      "can_guest_invoice":0,
      "discount":0,
      "notes":"",
      "gratuities":[
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        },
        {
          "revenue_center_uid":%s,
          "automatic_gratuity":null,
          "gratuity_percentage":0,
          "gratuity_minimum":0,
          "is_gratuity_adjustable":0,
          "gratuity_maximum":0,
          "gratuity_flat_amount":0
        }
      ],
      "carts":[
        {
          "cart_type_uid":1,
          "should_stop":"no",
          "pay_method":"cc_present",
          "is_pay_auth_required":0
        },
        {
          "cart_type_uid":3,
          "should_stop":"no",
          "pay_method":"cc_present",
          "is_pay_auth_required":0
        }
      ],
      "par_items":[{"menu_item_uid":%s, "menu_x_menu_item_uid":%s, "qty":1},
                   {"menu_item_uid":%s, "menu_x_menu_item_uid":%s, "qty":1}
      ]
    }''' % (patronUids[6], suitesRevenueCenterUid, cartRevenueCenterUid, parItem1, parMXM1, parItem2, parMXM2)

fileContents += ']'

fileName = "venue_%s-unit_patron_info.json" % (venueUid)
with open(FILE_PATH + fileName, 'w') as file:
    file.write(fileContents)
    file.close()




#       unit_x_patrons.json
unitXPatrons = []
unitNames = dbCore.getAllUnitNames(venueUid)
for patronUid in patronUids:
    unitXPatron = {} 
    unitXPatron['patron_uid'] = patronUid
    unitXPatron['unit_names'] = []
    for unitName in unitNames:
        unitXPatron['unit_names'].append(unitName[0])

    unitXPatrons.append(unitXPatron)
    
#print str(json.dumps(unitXPatrons))

fileName = "venue_%s-unit_x_patrons.json" % venueUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(json.dumps(unitXPatrons))
    file.close()

#   per event

#       employee_assignments.json

employeesXUnits = []
for employeeUid in employeeUids:
    employeeXUnit = {}
    employeeXUnit['employee_uid'] = employeeUid
    employeeXUnit['unit_names'] = []
    employeeXUnit['unit_names'].append(dbCore.getFirstUnitName(venueUid))
    employeesXUnits.append(employeeXUnit)

#print str(json.dumps(employeesXUnits))

fileName = "event_%s-employee_assignments.json" % managerTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(json.dumps(employeesXUnits))
    file.close()

fileName = "event_%s-employee_assignments.json" % attendantTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(json.dumps(employeesXUnits))
    file.close()


#       preorders.json
preorderMXMUid = input("Preorder Item MXM uid: " )
preorderTemplate = '''  {
    "event_uid":%s,
    "unit_uid":%s,
    "patron_uid":%s,
    "employee_uid":%s,
    "order_type_uid":8,
    "order_split_method_uid":1,
    "order_pay_method_uid":6,
    "sub_orders":[{
      "revenue_center_uid":%s,
      "employee_uid":%s,
      "gratuity":0.0,
      "device_uid":"CMlevyomspreorders",
      "order_type_uid":8,
      "order_items":[
        {
          "menu_x_menu_item_uid":%s,
          "line_id":1,
          "name":"Chicken Tenders",
          "price":12.75,
          "tax_rate":9.5,
          "components":[],
          "equiptment":[]
        }
      ]
    }],
    "revenue_centers":[{
      "revenue_center_uid":%s,
      "subtotal":12.75,
      "discount":0,
      "gratuity":0,
      "gratuity_source":"guest",
      "tax":1.21,
      "service_charge_amount":0
    }],
    "payment":{
      "merchant_uid":%s,
      "device_uid":"CMlevyomspreorders",
      "payment_id":0,
      "patron_card_uid":12832,
      "amount":13.96,
      "sale_closed_subtotal":13.96,
      "sale_closed_tip":0,
      "sale_closed_tax":1.21,
      "unique_uid":"1119d0ggzfj30cb8",
      "token_merchant_uid":1,
      "invoice_uid":4764,
      "authorization_code":"OK030Y",
      "authorization_tolerance":100,
      "cc_type":"VS",
      "receipt_text":"PURCHASE RESP CD%%3A A INVOICE%%3A 0000004764 ENTRY METHOD%%3A SWIPED APPROVED%%3A %%5BOK030Y%%5D AMOUNT%%3A    137.31 DATE%%2FTIME%%3A 15%%2F10%%2F16 14%%3A21%%3A04 CARD TYPE%%3A Visa CARD %%23%%3A XXXXXXXXXXXX1119     ________________________________________________________________________________ SIGNATURE    ",
      "is_complete":0
    }

  }'''  #9 fields to populate

fileContents = "["

unitUid = dbCore.getFirstUnitUid(venueUid)
merchantUid = dbCore.getMerchantUid(venueUid)


fileContents += preorderTemplate % (managerTrainingEventUid,
                                    unitUid, 
                                    patronUids[6], 
                                    employeeUids[0], 
                                    suitesRevenueCenterUid, 
                                    employeeUids[0],
                                    preorderMXMUid,
                                    suitesRevenueCenterUid,
                                    merchantUid)
fileContents += "]"

fileName = "event_%s-preorders.json" % managerTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(fileContents)
    file.close()

unitUids = dbCore.getAllUnitUids(venueUid)
fileContents = "["
for unitUid in unitUids:
    fileContents += preorderTemplate % (attendantTrainingEventUid,
                                        unitUid[0],
                                        patronUids[6],
                                        employeeUids[0],
                                        suitesRevenueCenterUid,
                                        employeeUids[0],
                                        preorderMXMUid,
                                        suitesRevenueCenterUid,
                                        merchantUid)
    fileContents += ","

fileContents = fileContents[:-1]
fileContents += ']'

fileName = "event_%s-preorders.json" % attendantTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(fileContents)
    file.close()

#       suite_assignments.json
suiteAssignments = []
for patronUid in patronUids:
    suiteAssignment = {}
    suiteAssignment['patron_uid'] = patronUid
    suiteAssignment['unit_names'] = []
    suiteAssignment['unit_names'].append(dbCore.getFirstUnitName(venueUid))
    suiteAssignments.append(suiteAssignment)

fileName = "event_%s-suite_assignments.json" % managerTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(json.dumps(suiteAssignments))
    file.close()

suiteAssignments = []
unitNames = dbCore.getAllUnitNames(venueUid)
for patronUid in patronUids:
    suiteAssignment = {}
    suiteAssignment['patron_uid'] = patronUid
    suiteAssignment['unit_names'] = []
    for unitName in unitNames:
        suiteAssignment['unit_names'].append(unitName[0])

    suiteAssignments.append(suiteAssignment)

fileName = "event_%s-suite_assignments.json" % attendantTrainingEventUid
with open(FILE_PATH + fileName, 'w') as file:
    file.write(json.dumps(suiteAssignments))
    file.close()


dbCore.configureCCOnFile(venueUid)
dbCore.configureAuthSigners(managerTrainingEventUid)
dbCore.configureAuthSigners(attendantTrainingEventUid)
dbCore.markTrainingConfigured(venueUid)

dbCore.commitChanges()
