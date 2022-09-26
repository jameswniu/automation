#! /usr/bin/python3
import os, sys
import shutil
import json
import string

import pytz

from glob import glob

from datetime import datetime as dt, timedelta

from automation__written_module_allowing_automating_of_emails import automail


pwd = os.getcwd()

alphabet = tuple(string.ascii_lowercase)
t = tuple([letter * 2 + '2' for letter in alphabet])

tz = pytz.timezone('America/New_York')
Ymd = dt.now(tz=tz).strftime('%Y%m%d')
md = Ymd[4:]


config = {    # SPECIFY
    "xx2": "edi" 
    , "yy2": "fax"
    , "ss2": "edi"
    , "tt2": "edi"
    , "uu2": "edi"
    , "vv2": "fax"
    , "zz2": "fax"
}

for tag in t:
    if tag in sys.argv[1]:
        with open(sys.argv[1], 'r') as fr:
            fline = fr.readline().strip()
            dicy = json.loads(fline)
            
            if 'edi_payer_id' in dicy:
                new_name = '{}_{}_edi.json'.format(tag, md)
                new_name1 = '{}_{}_edi.json'.format(tag, Ymd)
            else:
                new_name = '{}_{}_fax.json'.format(tag, md)
                new_name1 = '{}_{}_fax.json'.format(tag, Ymd)
        #print(new_name)
        #print(new_name1)


        #----
        #stop process if billing method incorrect
        #----
        for prefix in config:
            for var in ('new_name', 'new_name1'):
                if prefix in globals()[var] and config[prefix] not in globals()[var]:
                    sys.exit('ERROR billing method')


        shutil.copy('{}.json'.format(tag), r'/tmp/james_claim/{}'.format(new_name)) 
        os.rename('{}.json'.format(tag), r'{}/archives/billing/{}'.format(pwd, new_name1))

        print('file backed up to {}/archives/billing/...'.format(pwd))


if 'xx2' in sys.argv[1]:
    try:
        os.remove('{}_edi.json'.format(md))
    except:
        pass


if 'yy2' in sys.argv[1]:
    toname = ['Yi Yan']
    to = ['yi.yan@medlytix.com']
    
    ccnameicd = ['Melvin Andoor']
    ccicd = ['melvin.andoor@medlytix.com']

    subjaccident = f'Update VNEX Accident Date in Pre-Billing Table - {Ymd}'
    msgaccident = f"""\
Yi,

Please update VNEX accident date(s) in pre-billing table for billing:
/tmp/update_{md}_accident.sql"""
    subjzip = f'Update Zip Code(s) in Pre-Billing Table for Billing - {Ymd}'
    msgzip = f"""\
Yi,

Please update the following zip code(s) in pre-billing table for billing:
/tmp/update_{md}_zip.sql"""
    subjicd = f'Insert ICD Code Description(s) into ICD Codes Table - Accounts Need New Description(s) - {Ymd}'
    msgicd = """\
Yi,

New ICD code description(s):
{}

pm_sk accounts need new description(s):
{}""".format(open(f'modules/icd_emails/icdcode_{Ymd}.txt').read().strip(), open(f'modules/icd_emails/icdpmsk_{Ymd}.txt').read().strip()) 
        
    print('-' * 100)


    #########################################################################################################################################################
    # send email only if update file exists
    #########################################################################################################################################################
    if os.path.exists(rf'/tmp/update_{md}_accident.sql'):
        automail(subjaccident, msgaccident, to, toname)

        print('-' * 100)

    if os.path.exists(rf'/tmp/update_{md}_zip.sql'):
        automail(subjzip, msgzip, to, toname)

        print('-' * 100)

    if os.path.exists(rf'/home/james.niu@revintel.net/production/jsondump/modules/icd_emails/icdpmsk_{Ymd}.txt') and \
            os.path.exists(rf'/home/james.niu@revintel.net/production/jsondump/modules/icd_emails/icdcode_{Ymd}.txt'):
        automail(subjicd, msgicd, to, toname, ccicd, ccnameicd)

        print('-' * 100)


    for suffix in ('zip', 'accident'): 
        for f in glob(r'/tmp/update_*_{}.sql'.format(suffix)):
            if os.path.basename(f) != 'update_{}_{}.sql'.format(md, suffix):
                os.remove(f) 

    for f in glob(r'/tmp/facilitypayto_zip_*.sql'):
        if os.path.basename(f) != 'facilitypayto_zip_{}.sql'.format(md):
            os.remove(f) 


