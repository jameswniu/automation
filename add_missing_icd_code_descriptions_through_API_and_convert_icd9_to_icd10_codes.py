#! /usr/bin/env python3
import os, sys
import re
import json

import psycopg2
import requests
import time

from datetime import datetime, timedelta
from pytz import timezone

from copy import deepcopy
from config import user_db, passwd_db, hipa_token


#----
# def globals
#----
c, d = 0, 0
cup = {}
Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[:4] 


#----
# DB connection
#----
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)


#----
# functions
#----
def get_dx_desc(diffl):
    jar = {
        'N1830': 'N18.30|CHRONIC KIDNEY DISEASE, STAGE 3 UNSPECIFIED' ## SPECIFY
        , 'O99892': 'O99.89|OTH DISEASES AND CONDITIONS COMPL PREG/CHLDBRTH'
        , 'Z0382': 'Z03.82|ENCTR FOR OBSERVATION FOR SUSPECTED FOREIGN BODY RULED OUT'
        , 'V03131A': 'V03.131A|PED ON STND ELECTR SCOOT INJ IN W/CAR/TRK/VAN IN TRAF, INIT'
        , 'Z8616': 'Z86.16|PERSONAL HISTORY OF COVID-19'
        , 'B20': 'B20|HUMAN IMMUNODEFICIENCY VIRUS [HIV] DISEASE'
        , 'J1282': 'J12.82|PNEUMONIA DUE TO CORONAVIRUS DISEASE 2019'
        , 'M5451': 'M54.51|VERTEBROGENIC LOW BACK PAIN'
        , 'N1832': 'N18.32|CHRONIC KIDNEY DISEASE, STAGE 3B'
        , 'S20314A': 'S20.314A|ABRASION OF MIDDLE FRONT WALL OF THORAX, INITIAL ENCOUNTER'
        , 'R7402': 'R74.02|ELEVATION OF LEVELS OF LACTIC ACID DEHYDROGENASE [LDH]'
        , 'G928': 'G92.8|OTHER TOXIC ENCEPHALOPATHY'
        , 'V03138A': 'V03.138A|PED OTHER STND MICR-MOB CONV INJ W/CAR/TRK/VAN IN TRAF, INIT'
        , 'O99893': 'O99.893|OTH DISEASES AND CONDITIONS COMPLICATING PUERPERIUM'
        , 'R3589': 'R35.89|OTHER POLYURIA'
        , 'F32A': 'F32.A|DEPRESSION, UNSPECIFIED'
        , 'V00841A': 'V00.841A|FALL FROM STANDING ELECTRIC SCOOTER, INITIAL ENCOUNTER'
        , 'M1909': 'M19.09|PRIMARY OSTEOARTHRITIS, OTHER SPECIFIED SITE'
        , 'T40715A': 'T40.715A|ADVERSE EFFECT OF CANNABIS, INITIAL ENCOUNTER'
    }

    url0 = 'http://icd10api.com/?code={}&desc=short&r=json'
    url1 = 'http://icd10api.com/?s={}&desc=short&r=json'

    for code in diffl:
        if code not in cup:
            #print(code, file=sys.stderr)
            url00 = url0.format(code)
            url11 = url1.format(code)
            #print(url00, file=sys.stderr)
            #print(url11, file=sys.stderr)

            r0 = requests.get(url00)
            r1 = requests.get(url11)

            #print(r1, type(r1), file=sys.stderr)
            pot0 = r0.json()
            pot1 = r1.json()
            #print(pot0, file=sys.stderr)
            #print(pot1, file=sys.stderr)

            try: 
                formal = pot1['Search'][0]['Name']
            except:
                if code in jar:
                    formal = jar[code].split('|')[0]
                else:
                    formal = ''
            
            try:
                category = re.search(r'\d+', pot0['Type']).group()                
            except:
                category = '10'

            try: 
                info = pot0['Description'].upper()
                #print(info, file=sys.stderr)
            except:
                if code in jar:
                    info = jar[code].split('|')[1]
                else:
                    info = ''

            for p in (',', '.', ':', ';'):
                info = re.sub(rf'(?=\s*[(]\w+[{p}]*\s+.*[)]).*', '', info)    
            #print(info, file=sys.stderr)

            cup[code] = f'{formal}|{category}|{info}'

    #print(cup, file=sys.stderr)


# reading from documentation of icd9 to icd10 conversion
plate = {}

with open(r'modules/ICD9_v2.csv', 'r') as fr:
    next(fr)

    for line in fr:
        line = line.strip()
        
        plate[line] = 1


def add_dx_desc(dicy):
    try:
        global c, d, plate
    except:
        pass

    if dicy['claim_type'] != '837P':
        raise Exception("not 837P")
    
    # if diagnoses non-empty
    if dicy['diagnoses'].strip():
        sql_dx = f"select string_agg(code || '|' || info, '@') from tpl_icd_codes where code = ANY(regexp_split_to_array('{dicy['diagnoses']}', ','));"

        with con:
            cur = con.cursor()
            cur.execute(sql_dx)

        desc = list(cur)[0][0]    # first record first column

        diag_code_l = [code.strip() for code in dicy['diagnoses'].split(',')]

        # if found something in tpl_icd_codes
        try:
            diagdsc_code_l = [codeline.split('|')[0] for codeline in desc.split('@')]
        except: 
            diagdsc_code_l = []

        diffl = list(set(diag_code_l).difference(set(diagdsc_code_l)))

        # find old ICD9
        difflold = []

        for code in diffl:
            if code in plate or re.match(r'\d+$', code):
                difflold.append(code) 

        if len(difflold) > 0:
            strdiffold = ' (old ICD9 "{}")'.format(','.join(difflold))
        else:
            strdiffold = ''


        # complete insertion
        if len(diffl) == 0:                    
            print("""{} : "diagnoses": "{}" (added "dx_code_desc": "{}...")""".format(dicy['vx_pm_sk'], dicy['diagnoses'], desc[:30]), file=sys.stderr)

            dicy['dx_code_desc'] = desc

            c += 1            

            return dicy 
        # non-complete insertion
        else:
            if '{} : no dx_code_desc full | "diagnoses": "{}" missing{}'.format(dicy['vx_pm_sk'], ','.join(diffl), strdiffold) not in processedpmsk:
                print('{} : no dx_code_desc full | "diagnoses": "{}" missing{}'.format(dicy['vx_pm_sk'], ','.join(diffl), strdiffold), file=fwpmsk)
            print('{} : no dx_code_desc full | "diagnoses": "{}" missing{}'.format(dicy['vx_pm_sk'], ','.join(diffl), strdiffold), file=sys.stderr)

            get_dx_desc(diffl)

            d += 1
    else:
        print('{} : no dx_code_desc | "diagnoses": "{}"'.format(dicy['vx_pm_sk'], dicy['diagnoses']), file=sys.stderr)

        d += 1


#----
# extract all records to edit by pm_sk
#----
jar = {}

with open('template.txt', 'r') as fr:
    for line in fr:
        if re.match(r'\d{7}\s:\sno\sdx_code_desc', line):
            line = line.strip()

            pm_sk = re.search(r'\d{7}', line).group()

            jar[pm_sk] = 1


#----
# insert into JSON
#----
newfilepmsk = rf'modules/icd_emails/icdpmsk_{Ymd}.txt'
newfilecode = rf'modules/icd_emails/icdcode_{Ymd}.txt'

processedpmsk = {}
processedcode = {}


if os.path.exists(newfilepmsk):
    with open(newfilepmsk, 'r') as fr:
        for line in fr:
            processedpmsk[line.strip()] = 1    # add to track dupe 

    wapmsk = 'a'    # append if file exists
else:
    wapmsk = 'w'    # write new if file not exists

if os.path.exists(newfilecode):
    with open(newfilecode, 'r') as fr:
        for line in fr:
            processedcode[line.strip()] = 1    # add to track dupe 

    wacode = 'a'    # append if file exists
else:
    wacode = 'w'    # write new if file not exists


fwpmsk = open(newfilepmsk, wapmsk) 
fwcode = open(newfilecode, wacode) 

print('-' * 150, file=sys.stderr)


for line in sys.stdin:
    dicy = json.loads(line)

    if not re.match(r'\s{0,6}{', line):
        continue
    
    if dicy['vx_pm_sk'] in jar:
        dicy = add_dx_desc(dicy)    # insert dx_code_desc  

    if dicy:    # only dump with complete insertion
        r = json.dumps(dicy)

        print(r)


#----
# print onto output file to email for review
#----
print('-' * 150, file=sys.stderr)

if 'code|formal|category|info' not in processedcode:
    print('code|formal|category|info', file=fwcode) 
for code, others in cup.items():
    if f"{code}|{others.split('|')[0]}|{others.split('|')[1]}|{others.split('|')[2]}" not in processedcode:
        if others.split('|')[0].strip() and others.split('|')[2].strip():
            print(f"{code}|{others.split('|')[0]}|{others.split('|')[1]}|{others.split('|')[2]}", file=fwcode)
        else:
            if code not in plate and not re.match(r'\d+$', code):
                print(f"[{code}|{others.split('|')[0]}|{others.split('|')[1]}|{others.split('|')[2]}]", file=sys.stderr)
print('code|formal|category|info', file=sys.stderr) 
[print(f"{code}|{others.split('|')[0]}|{others.split('|')[1]}|{others.split('|')[2]}", file=sys.stderr) for code, others in cup.items() if (code not in plate and not re.match(r'\d+$', code))]

print('-' * 150, file=sys.stderr)

fwpmsk.close()
fwcode.close()


print("""dx_code_desc records added {}""".format(c), file=sys.stderr)
print("""dx_code_desc records removed {}""".format(d), file=sys.stderr)


print('-' * 150, file=sys.stderr)

