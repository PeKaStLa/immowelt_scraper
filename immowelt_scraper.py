import requests
import json
import time
import random
import re
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

import datetime
import pandas as pd


##############################################
regexError = 0
errorDiffNumberOfElements = 0
_FLAT_IS_ALREADY_IN_DB = 0
_FLATS_ADDED = 0
_FLATS_DUPLICATED = 0
dontSave = False
print("Connect to AWS DynamoDB table 'flats_immowelt'.")
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('flats_immowelt')
###############################################

mainScript = """
splash:init_cookies({

{
    name="bx",
    value="028bf0cc28a040fba734e516d8b7ad08",
    path="/",
    domain=".immowelt.de",
    expires="2023-03-30T16:28:08Z",
    secure=true,
    httpOnly=false,
}, 
    {
    name="IWDeviceType",
    value="3",
    path="/",
    domain="www.immowelt.de",
    expires="2021-04-13T16:28:08Z",
    secure=true,
    httpOnly=false,
},
    {
    name="IwAGSessionId",
    value="059a2c6a-9441-e1a5-f01b-64448978aadc",
    path="/",
    domain="www.immowelt.de",
    secure=true,
    httpOnly=true,
},
    {
    name="OPTOUTMULTI",
    value="0:0",
    path="/",
    domain=".immowelt.de",
    expires="2021-06-28T16:28:09Z",
    secure=false,
    httpOnly=false,
},
    {
    name="utag_main",
    value="v_id:017883f5957500182557ebde847e03072004806a00bd0$_sn:1$_se:21$_ss:0$_st:1617127257860$ses_id:1617121613175%3Bexp-session$_pn:7%3Bexp-session",
    path="/",
    domain=".immowelt.de",
    expires="2022-03-30T16:28:09Z",
    secure=false,
    httpOnly=false,
},
    {
    name="domain_iwin",
    value="www.immowelt.de",
    path="/",
    domain=".immowelt.de",
    secure=false,
    httpOnly=false,
},
    {
    name="iwScreenRes",
    value="1024,768",
    path="/",
    domain="www.immowelt.de",
    secure=false,
    httpOnly=false,
},
    {
    name="CONSENTMGR",
    value="consent:true%7Cts:1617125457772",
    path="/",
    domain=".immowelt.de",
            expires="2022-03-30T16:28:09Z",
    secure=false,
    httpOnly=false,
},
    {
    name="outbrain_cid_fetch",
    value="true",
    path="/",
    domain="www.immowelt.de",
            expires="2021-03-30T18:28:09Z",
    secure=false,
    httpOnly=false,
},
    {
    name="wd",
    value="b7879cd0d96c42bba14f9283bf5e8474",
    path="/",
    domain=".immowelt.de",
            expires="2023-03-30T00:00:00Z",
    secure=false,
    httpOnly=false,
},
    {
    name="kameleoonVisitorCode",
    value="_js_haw05v7vd2wsgyte",
    path="/",
    domain=".immowelt.de",
            expires="2022-04-14T19:20:00Z",
    secure=false,
    httpOnly=false,
},
    {
    name="_ga",
    value="GA1.2.134645459.1617124412",
    path="/",
    domain=".immowelt.de",
            expires="2023-03-30T19:20:00Z",
    secure=false,
    httpOnly=false,
},
    {
    name="_gid",
    value="GA1.2.68662270.1617124413",
    path="/",
    domain=".immowelt.de",
            expires="2021-03-31T19:20:00Z",
    secure=false,
    httpOnly=false,
}
})

splash:go(args.url)
splash:wait(2)
local scroll_to = splash:jsfunc("window.scrollTo")
scroll_to(0, 500)
splash:wait(0.2)
scroll_to(0, 1000)
splash:wait(0.2)
scroll_to(0, 1500)
splash:wait(0.2)
scroll_to(0, 2000)
splash:wait(0.2)
scroll_to(0, 2500)
splash:wait(0.2)
scroll_to(0, 3000)
splash:wait(0.2)
scroll_to(0, 3500)
splash:wait(0.2)
scroll_to(0, 4000)
splash:wait(0.2)
scroll_to(0, 4500)
splash:wait(0.2)
scroll_to(0, 5000)
splash:wait(0.2)
scroll_to(0, 5500)
splash:wait(0.2)
scroll_to(0, 6000)
splash:wait(0.2)

splash:wait(0.5)

splash:runjs("list = document.getElementsByClassName('clear relative js-listitem');for (var i=0, item; item = list[i]; i++) {item.getElementsByTagName('a')[0].setAttribute('class','linkClass');};")

splash:runjs("list = document.getElementsByClassName('details'); for (var i=0, item; item = list[i]; i++){if (item.getElementsByTagName('li').length == 0){ item.getElementsByTagName('ul')[0].remove() }};")

splash:runjs("list = document.getElementsByClassName('details');for (var i=0, item; item = list[i]; i++){if (item.getElementsByTagName('ul').length == 0){ var ul=document.createElement('ul'); ul.classList.add('js-EqList');  ul.classList.add('eq_list'); ul.classList.add('clear'); var li=document.createElement('li'); ul.appendChild(li);item.appendChild(ul);}};")

splash:runjs("list = document.getElementsByClassName('eq_list clear');for (var i=0, item; item = list[i]; i++){var j;for (j = 0; j < list[i].getElementsByTagName('li').length; j++) {if (!(item.getElementsByTagName('li')[j].className == 'js-moreTag')){item.getElementsByTagName('li')[j].setAttribute('class','extraClass'+[i]);}}}")

splash:wait(1)

local el = splash:select('.price_rent'):text()

local temp = splash:select_all('h2')
local allTitles = {}
for _, el in ipairs(temp) do
  allTitles[#allTitles+1] = el:text()
end
    
local temp = splash:select_all('.listlocation')
local allLocations = {}
for _, el in ipairs(temp) do
  allLocations[#allLocations+1] = el:text()
end
    
local temp = splash:select_all('.price_rent')
local allRents = {}
for _, el in ipairs(temp) do
  allRents[#allRents+1] = el:text()
end	
    
local temp = splash:select_all('.square_meters')
local allSquares = {}
for _, el in ipairs(temp) do
  allSquares[#allSquares+1] = el:text()
end	
    
local temp = splash:select_all('.rooms')
local allRooms = {}
for _, el in ipairs(temp) do
  allRooms[#allRooms+1] = el:text()
end
    
local temp = splash:select_all('.linkClass')
local allLinks = {}
for _, el in ipairs(temp) do
  allLinks[#allLinks+1] = el:info().attributes.href
end





local qswlv = splash:jsfunc([[
  function (x) {
    var el = document.querySelector(x);
    return el;
  }
  ]])

local allExtras = {}

for i=0,19 do 
    local extraClass = ".extraClass"  .. i 
    local tempExtraClass = qswlv(extraClass)

    if (tempExtraClass ~= "") then
        local i = {}
        local temp = splash:select_all(extraClass)
        for _, el in ipairs(temp) do
          i[#i+1] = el:text()
        end
        allExtras[#allExtras+1] = i
    end
end



    
return {
allTitles=allTitles,
allLocations=allLocations,
allRents=allRents,
allSquares=allSquares,
allRooms=allRooms,
allLinks=allLinks,
allExtras=allExtras
}
"""


def decode_JSON_to_main_dict(resp_json):
    #convert response.content (bytes) into python dict
    main_dict = json.loads(resp_json)
    return main_dict

def get_minor_dict_from_main_dict(params):
    minor_dict = params['main_dict_name'].get(params['minor_dict_key'])
    return minor_dict

def get_single_string_from_minor_dict(params):
    return get_minor_dict_from_main_dict({'main_dict_name': params['main_dict_name'],'minor_dict_key': params["minor_dict_key"]}).get(str(params["current_expose"]))

def get_multiple_minor_dicts_from_main_dict(params):
    allMinorDicts = []
    for i in params['list_minor_dict_keys']:
        minor_dict = params['main_dict_name'].get([i][0])
        allMinorDicts.append(minor_dict)
    return allMinorDicts

def return_all_max_values_of_all_minor_dicts(allMinorDicts):
    all_lengths = []
    for i in range(len(allMinorDicts)):
        all_lengths.append(len(allMinorDicts[i]))
    return all_lengths

def do_all_minor_dicts_have_same_element_number(allMinorDicts):
    all_lengths = []
    for i in range(len(allMinorDicts)):
        all_lengths.append(len(allMinorDicts[i]))
    dataframes = pd.DataFrame(all_lengths)
    spannweite = dataframes.apply(lambda x: x.max() - x.min()).item()
    return True if (spannweite==0) else False

def return_max_number_of_elements_in_all_minor_dicts(allMinorDicts):
    all_lengths = []
    for i in range(len(allMinorDicts)):
        all_lengths.append(len(allMinorDicts[i]))
    return int(pd.DataFrame(all_lengths).max())

def return_current_date_and_time():
    return datetime.datetime.now().strftime("%d.%m.%Y#%H:%M:%S")

def return_all_extras_of_current_expose_dict(params):
    return params["allMinorDicts"][6].get(str(params["current_expose"]))

def _create_extras_set(currentExtras):
    # if Expose has no Extras then return None:
    if (currentExtras.get(str("1")) == ""):
        _EXRAS_SET = None
        return _EXRAS_SET
    _EXRAS_SET = set()
    for _K in range(len(currentExtras)):
        _K=_K+1
        _EXRAS_SET.add(currentExtras.get(str(_K)))
    return _EXRAS_SET

def convert_dict_to_list(dict):
    return list(dict.values())

def match_PLZ_in_single_location_string(string):
    match = re.search(r'\b\d{5}\b', string)
    if match:
        return match.group()
    else:
        return "Fehler beim PLZ-matchen"

def _is_flat_already_in_db(pk):
    response = table.get_item(
        Key={
            'pk': pk
        }
    )
    if (len(response) == 2):
        return True
    if (len(response) == 1):
        return False

def _is_flat_already_in_BatchWriteItem(_PK, _ALL_FLATS_ON_PAGE):
    if _PK in _ALL_FLATS_ON_PAGE:
        return True
    
    return False
    

def _send_sns_mail(text):
    SENDER = "Peter Stadler <peter.stadler@stadlersoft.de>"
    RECIPIENT = "14peterstadler@gmail.com"
    CONFIGURATION_SET = "ConfigSet"
    AWS_REGION = "eu-central-1"
    SUBJECT = text
    BODY_TEXT = (text)
                
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>""" + text + """</h1>
    </body>
    </html>
    """            
    CHARSET = "UTF-8"
    client = boto3.client('ses',region_name=AWS_REGION)
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the following line
            #ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])









def get_exposes_single_page_immowelt_save_to_AWS(page):
    global mainScript
    #resp = requests.post('http://3.67.61.168:80/run', json={ #EC2-Server-IP
    resp = requests.post('http://localhost:8050/run', json={
    'lua_source': mainScript,
    'url': "https://www.immowelt.de/liste/berlin/wohnungen/mieten?sort=relevanz&cp=" + page
    })
    print("->",resp,"URL: https://www.immowelt.de/liste/berlin/wohnungen/mieten?sort=relevanz&cp=" + page)
    #print(resp.content)
    python_obj = decode_JSON_to_main_dict(resp.content)
    #print(type(python_obj)) # <class 'dict'>
    #print(python_obj) = dict with keys
    #print("----1-----")
    allMinorDicts = get_multiple_minor_dicts_from_main_dict({'main_dict_name': python_obj,'list_minor_dict_keys': ['allTitles','allLocations','allRents','allSquares','allRooms','allLinks', 'allExtras']})


    #print("----------allMinorDicts-----------")
    #print(allMinorDicts) # = ALL DICTS IN ONE LIST (WITHOUT KEYS)

    #print("----------extras-----------")
    #print(allMinorDicts[6])


    #print(allMinorDicts)
    #allTitles = get_minor_dict_from_main_dict({'main_dict_name': python_obj,          'minor_dict_key': 'allTitles'})
    #allLocations = get_minor_dict_from_main_dict({'main_dict_name': python_obj,             'minor_dict_key': 'allLocations'})
    #allRents = get_minor_dict_from_main_dict({'main_dict_name': python_obj,'minor_dict_key': 'allRents'})
    #allTitles = python_obj.get('allTitles'
    #allLocations = python_obj.get('allLocations')
    #allRents = python_obj.get('allRents')
    #allSquares = python_obj.get('allSquares')
    #allRooms = python_obj.get('allRooms')
    #allLinks = python_obj.get('allLinks')
    #allExtras = python_obj.get('allExtras')
    #print("----2-----")
    # dasist beides das gleiche => Daher erstellen der List <allMinorDicts> unnötig!
    #print(type(allMinorDicts[0]))
    #print(allMinorDicts[0])
    #print(type(python_obj["allTitles"]))
    #print(python_obj["allTitles"])
    print("Gleiche Anzahl an Elementen in Minor Dicts: ", do_all_minor_dicts_have_same_element_number(allMinorDicts))

    global regexError
    global errorDiffNumberOfElements
    global dontSave
    global _FLAT_IS_ALREADY_IN_DB
    global _FLATS_ADDED
    global _FLATS_DUPLICATED
    
    _ALL_FLATS_ON_PAGE = set()

    _RESPONSE = table.get_item(
        Key={
        'pk': 'total'
        }
    )
    _START_FLAT_ID = _RESPONSE['Item']["flats_total"]


    if do_all_minor_dicts_have_same_element_number(allMinorDicts):

        _RESPONSE = table.get_item(
            Key={
            'pk': 'total'
            }
        )
        _START_FLAT_ID = _RESPONSE['Item']["flats_total"]
    
        #print("-----6-----")
        with table.batch_writer() as batch:
            for i in range(return_max_number_of_elements_in_all_minor_dicts(allMinorDicts)):
                #now = datetime.datetime.now()
                #now.strftime("%d.%m.%Y, %H:%M:%S")
                i = i+1
                dontSave = False
                
                currentExtras = return_all_extras_of_current_expose_dict({'allMinorDicts': allMinorDicts, 'current_expose': i})
                #print(type(currentExtras))
                newList = convert_dict_to_list(currentExtras)
                #print(type(newList))

                extrasStringDefault = "FEHLER-Wert! extrasString Objekt ", i,", Seite ", page
                allRentsRegexDefault = "FEHLER-Wert! allRents Objekt ", i,", Seite ", page
                allSquaresRegexDefault = "FEHLER-Wert! allSquares Objekt ", i,", Seite ", page
                allRoomsRegexDefault = "FEHLER-Wert! allRooms Objekt ", i,", Seite ", page
                
                match = re.search(r'\d.*\d', get_single_string_from_minor_dict({'main_dict_name': python_obj,'minor_dict_key': 'allRents', 'current_expose': i}))
                if match:
                    allRentsRegex=match.group()
                    #print('found', match.group(), 'in allRents')
                else:
                    dontSave = True
                    print('! ---> no match in AllRent bei Objekt', i,' , Seite', page, ". dontSave: ", dontSave)
                    regexError = regexError +1

                match = re.search(r'.*\d', get_single_string_from_minor_dict({'main_dict_name': python_obj,'minor_dict_key': 'allSquares', 'current_expose': i}))
                if match:
                    allSquaresRegex=match.group()
                    #print('found', match.group(), 'in allSquares') 
                else:
                    dontSave = True
                    print('! ---> no match in allSquares bei Objekt', i,' , Seite', page, ". dontSave: ", dontSave)
                    regexError = regexError +1

                match = re.search(r'\d.*\d*', get_single_string_from_minor_dict({'main_dict_name': python_obj,'minor_dict_key': 'allRooms', 'current_expose': i}))
                if match:
                    allRoomsRegex=match.group()
                    #print('found', match.group(), 'in allRooms') 
                else:
                    dontSave = True
                    print('! ---> no match in allRooms bei Objekt', i,' , Seite', page, ". dontSave: ", dontSave)
                    regexError = regexError +1

                 
                
                if (dontSave == False):
                    #print("----15----")
                    only_PLZ = match_PLZ_in_single_location_string(get_single_string_from_minor_dict({'main_dict_name': python_obj,'minor_dict_key': 'allLocations', 'current_expose': i}))
                    #print(only_PLZ)

                    _PK = only_PLZ+'#'+allRoomsRegex+'#'+allSquaresRegex+'#'+allRentsRegex
                    
                    
                    if (_is_flat_already_in_db(_PK) == True):
                        print("Flat ist schon in DynamoDB! - Page: ", page, " - " , _PK)
                        _FLAT_IS_ALREADY_IN_DB = _FLAT_IS_ALREADY_IN_DB + 1
                        
                    if (_is_flat_already_in_BatchWriteItem(_PK, _ALL_FLATS_ON_PAGE) == True):
                        print("Flat is duplicated - Page: ", page, " - " , _PK)
                        _FLATS_DUPLICATED = _FLATS_DUPLICATED + 1
                        
                    if (_is_flat_already_in_db(_PK) == False) and (_is_flat_already_in_BatchWriteItem(_PK, _ALL_FLATS_ON_PAGE) == False):
                        _ALL_FLATS_ON_PAGE.add(_PK)
                        _START_FLAT_ID = _START_FLAT_ID + 1
                        _FLATS_ADDED = _FLATS_ADDED + 1

                        batch.put_item(
                           Item={
                                'pk': _PK,
                                'flat_id': _START_FLAT_ID,
                                'cold_rent': allRentsRegex,
                                'date': return_current_date_and_time(),
                                'location': get_minor_dict_from_main_dict({'main_dict_name': python_obj,'minor_dict_key': 'allLocations'}).get(str(i)),
                                'area': allSquaresRegex,
                                'rooms': allRoomsRegex,
                                'title': get_minor_dict_from_main_dict({'main_dict_name': python_obj,'minor_dict_key': 'allTitles'}).get(str(i)),
                                'extras_dict': _create_extras_set(currentExtras),                       
                                'link': "www.immowelt.de" + get_minor_dict_from_main_dict({'main_dict_name': python_obj,'minor_dict_key': 'allLinks'}).get(str(i)),
                            }
                        )

                        table.update_item(
                            Key={
                                'pk': 'total'
                                },
                            UpdateExpression='SET flats_total = :val1',
                            ExpressionAttributeValues={
                                ':val1': _START_FLAT_ID
                            }
                        )


                        
                        #print("----16----")
                        #print(get_minor_dict_from_main_dict({'main_dict_name': python_obj,'minor_dict_key': 'allLinks'}).get(str(i)))
                        #print(extrasString)
                        #print("-----17-----")
    else:
        errorDiffNumberOfElements = errorDiffNumberOfElements + 1
        print(return_all_max_values_of_all_minor_dicts(allMinorDicts))

def main():
    print("IMPORTANT: Dieses Programm kann durch Änderungen an der Website Fehler generieren. Der korrekte Ablauf muss regelmäßig überprüft werden!")
    print("FEHLER: allMinorDicts = python_obj, daher unnötig.")
    print("WARNING: Bitte die maximale Seitenanzahl automatisch heruasfinden :)")
    print("FEATURE: Am Besten du createst eine Tabelle mit allen Item-Anzahlen aus allen Tabellen. Damit du nicht ausversehen die total-number löschst.")
    print("FEATURE: Unbedingt noch die Anzahl der Duplciates auf den Pages zählen!!-!-!-!_!")
    print("FEATURE: Bei ALLEN Status-Meldungen in die Shell unbedingt die Seite und die Flat-Number angeben, damit ich kontrollieren kann!")

    _send_sns_mail("started immowelt scraper")
    
    i = 0
    
    for i in range(32):
        i = i+1
        get_exposes_single_page_immowelt_save_to_AWS(str(i))
        randNr = random.randint(2, 5)
        print("-> Pause - randomSleep: " + str(randNr))
        time.sleep(randNr)

    _END_STRING = "Finished immowelt with " + str(_FLAT_IS_ALREADY_IN_DB) + " FlatIsAlreadyInDB-Errors, " + str(regexError) + " RegEx-Errors, " + str(errorDiffNumberOfElements) + " DifferentNumberOfElements-Errors, " + str(_FLATS_DUPLICATED) + " DuplicateFlatsOnSamePage-Errors and a total of " + str(_FLATS_ADDED) + " Flats added to DynamoDB."    
    print(_END_STRING)
    _send_sns_mail(_END_STRING)

    
if __name__ == "__main__":
    main()
