#!/usr/bin/env python3
import os, sys
import json
import re

import paramiko
from paramiko import SSHClient
from scp import SCPClient
import psycopg2.extras

from datetime import datetime, timedelta
from glob import glob
from pytz import timezone

from usps import USPSApi, Address
from automation__allow_automatically_sending_emails_through_SMTP import automail
from config import user_db, passwd_db


os.chdir(r'/tmp')
Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[4:]


#----
# dump SQL
#----
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)

with open('/home/james.niu@revintel.net/production/jsondump/{}'.format(sys.argv[1]), 'r') as fr:
    sql = fr.read().strip()  
#"""select content from business_analysis.test_responses;"""
with con:
    cur = con.cursor()
    cur.execute(sql)


#----
# create json files
#----
nnp, nni = 0, 0
nyp, nyi = 0, 0
c = 0
for row in cur:
    dicy = row[0]
    typ = dicy['claim_type']

    if dicy['holding_info'] != '':
        info = dicy['holding_info']
    else:
        info = dicy['reject_info']
    
    # sometimes flags are not present in data structure
    try:
        holdd = dicy['holding_flag']
    except:
        holdd = ''

    try:        
        rejj = dicy['reject_flag']
    except:
        rejj = ''

    ind = '{}-{}'.format(holdd, rejj)
    processed = ''

    if ind == 'N-N' and typ == '837P':
        if os.path.exists('prof_{}_pending2client.json'.format(md)):
            with open('prof_{}_pending2client.json'.format(md), 'r') as fr:
                processed = fr.read()                    

            act = 'a'
        else:
            act = 'w'

        with open('prof_{}_pending2client.json'.format(md), act) as fw:
            if json.dumps(dicy) not in processed:
                print(json.dumps(dicy), file=fw)
 
        for f in glob('prof_*_pending2client.json'):
            if f != 'prof_{}_pending2client.json'.format(md):
                os.rename(f, rf'/home/james.niu@revintel.net/production/jsondump/archives/pending/{f}')

        nnp += 1
    elif ind == 'N-N' and typ == '837I':
        if os.path.exists('inst_{}_pending2client.json'.format(md)):
            with open('inst_{}_pending2client.json'.format(md), 'r') as fr:
                processed = fr.read()                    

            act = 'a'
        else:
            act = 'w'

        with open('inst_{}_pending2client.json'.format(md), act) as fw:
            if json.dumps(dicy) not in processed:
                print(json.dumps(dicy), file=fw)
            
        for f in glob('inst_*_pending2client.json'):
            if f != 'inst_{}_pending2client.json'.format(md):
                os.rename(f, rf'/home/james.niu@revintel.net/production/jsondump/archives/pending/{f}')


        nni += 1
    elif ind == 'N-Y' and typ == '837P':
        if os.path.exists('prof_{}_pending2rejected.json'.format(md)):
            with open('prof_{}_pending2rejected.json'.format(md), 'r') as fr:
                processed = fr.read()                    

            act = 'a'
        else:
            act = 'w'

        with open('prof_{}_pending2rejected.json'.format(md), act) as fw:
            if json.dumps(dicy) not in processed:
                print(json.dumps(dicy), file=fw)
            
        for f in glob('prof_*_pending2rejected.json'):
            if f != 'prof_{}_pending2rejected.json'.format(md):
                os.rename(f, rf'/home/james.niu@revintel.net/production/jsondump/archives/pending/{f}')


        nyp += 1
    elif ind == 'N-Y' and typ == '837I':
        if os.path.exists('inst_{}_pending2rejected.json'.format(md)):
            with open('inst_{}_pending2rejected.json'.format(md), 'r') as fr:
                processed = fr.read()                    

            act = 'a'
        else:
            act = 'w'

        with open('inst_{}_pending2rejected.json'.format(md), act) as fw:
            if json.dumps(dicy) not in processed:
                print(json.dumps(dicy), file=fw)
            
        for f in glob('inst_*_pending2rejected.json'):
            if f != 'inst_{}_pending2rejected.json'.format(md):
                os.rename(f, rf'/home/james.niu@revintel.net/production/jsondump/archives/pending/{f}')

        nyi += 1

    #if 'missing W9' not in info:
    #    print(ind, info, typ, dicy)

    c += 1


#----
# email if changes complete
#----
if nnp + nni + nyp + nyi == c:
    sqlfac = """select * from business_analysis.test_npi;"""

    with con:
        cur = con.cursor()
        cur.execute(sqlfac)    

    header_l = [r[0] for r in cur.description]
    header = '|'.join(header_l)

    records_l = []
    [records_l.append(f'{r[0]}|{r[1]}|{r[2]}|{r[3]}|{r[4]}') for r in cur]
        
    if len(records_l) != 0:
        records = '   \n'.join(records_l)

        desc = f"""{header}    
{records}"""
         
        subj = 'Insert Facility Name into Reader and/or Hospital Mapping Table - {}'.format(Ymd)
        msg = """\
Yi

Please insert into reader and/or tpl_hospital_name_mappings table:
{}

Source: National Plan and Provider Enumeration System (NPPES) API""".format(desc)
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']

        print('-' * 200)
        print('facility name(s) update exists...')

        if os.path.exists('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md)):
            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read()
            act = 'a' 
        else:
            rec = ''
            act = 'w'

        if msg not in rec:        
            automail(subj, msg, to, toname)

            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), act) as fw:
                print(msg, file=fw)
    else:
        print('-' * 200)
        print('facility name(s) update not exists...')


    sqldesc = """\
select -- check distinct holding infos (goes into email)
    coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info') holding_reject_info
    , count(*) records
    , count(distinct pat_acct) pat_accts
--  , string_agg(distinct cust_id::text, '; ') cust_ids
from 
    business_analysis.test_responses
where 
    processed = 'f'
    and content->>'holding_flag' = '{}'
    and content->>'reject_flag' = '{}'
    and content->>'claim_type' = '{}'
group by 
    coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info')
order by
    coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info')
    , records desc;"""

    if os.path.exists('prof_{}_pending2client.json'.format(md)):
        with con:
            cur = con.cursor()
            cur.execute(sqldesc.format('N', 'N', '837P'))

        header_l = [r[0] for r in cur.description]
        header = ' -- '.join(header_l)

        records_l = []
        [records_l.append(f'{r[0]} -- {r[1]} -- {r[2]}') for r in cur]
        records = '   \n'.join(records_l)

        desc = f"""{header}    
{records}"""

        subj = 'Load 837P JSON from Pending into Client Raw Bills - {}'.format(Ymd)
        msg = """\
Yi,

Please load fixed 837P JSON from pending into client raw bills:
{}

/tmp/{}""".format(desc, 'prof_{}_pending2client.json'.format(md))
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']
        cc = ['gavin.johnson@medlytix.com']
        ccname = ['Gavin Johnson']

        print('-' * 200)
        print('prof pending2client file exists...')

        if os.path.exists('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md)):
            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read()
            act = 'a' 
        else:
            rec = ''
            act = 'w'

        if msg not in rec:        
            automail(subj, msg, to, toname, cc, ccname)

            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), act) as fw:
                print(msg, file=fw)
    else:
        print('-' * 200)
        print('prof pending2client file not exists...')


    if os.path.exists('inst_{}_pending2client.json'.format(md)):
        with con:
            cur = con.cursor()
            cur.execute(sqldesc.format('N', 'N', '837I'))

        header_l = [r[0] for r in cur.description]
        header = ' -- '.join(header_l)

        records_l = []
        [records_l.append(f'{r[0]} -- {r[1]} -- {r[2]}') for r in cur]
        records = '   \n'.join(records_l)

        desc = f"""{header}    
{records}"""

        subj = 'Load 837I JSON from Pending into Client Raw Bills - {}'.format(Ymd)
        msg = """\
Yi,

Please load fixed 837I JSON from pending into client raw bills:
{}

/tmp/{}""".format(desc, 'inst_{}_pending2client.json'.format(md))
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']
        cc = ['gavin.johnson@medlytix.com']
        ccname = ['Gavin Johnson']
            
        print('-' * 200)
        print('inst pending2client file exists...')

        if os.path.exists('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md)):
            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read()
            act = 'a' 
        else:
            rec = ''
            act = 'w'

        if msg not in rec:        
            automail(subj, msg, to, toname, cc, ccname)

            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), act) as fw:
                print(msg, file=fw)
    else:
        print('-' * 200)
        print('inst pending2client file not exists...')


    if os.path.exists('prof_{}_pending2rejected.json'.format(md)):
        with con:
            cur = con.cursor()
            cur.execute(sqldesc.format('N', 'Y', '837P'))

        header_l = [r[0] for r in cur.description]
        header = ' -- '.join(header_l)

        records_l = []
        [records_l.append(f'{r[0]} -- {r[1]} -- {r[2]}') for r in cur]
        records = '   \n'.join(records_l)

        desc = f"""{header}     
{records}"""

        subj = 'Move 837P JSON from Pending to Rejected Raw Bills - {}'.format(Ymd)
        msg = """\
Yi,

Please move error 837P JSON from pending to rejected raw bills:
{}

/tmp/{}""".format(desc, 'prof_{}_pending2rejected.json'.format(md))
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']

        print('-' * 200)
        print('prof pending2rejected file exists...')

        if os.path.exists('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md)):
            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read()
            act = 'a' 
        else:
            rec = ''
            act = 'w'

        if msg not in rec:        
            automail(subj, msg, to, toname)

            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), act) as fw:
                print(msg, file=fw)
    else:
        print('-' * 200)
        print('prof pending2rejected file not exists...')


    if os.path.exists('inst_{}_pending2rejected.json'.format(md)):
        with con:
            cur = con.cursor()
            cur.execute(sqldesc.format('N', 'Y', '837I'))

        header_l = [r[0] for r in cur.description]
        header = ' -- '.join(header_l)

        records_l = []
        [records_l.append(f'{r[0]} -- {r[1]} -- {r[2]}') for r in cur]
        records = '   \n'.join(records_l)

        desc = f"""{header}    
{records}"""

        subj = 'Move 837I JSON from Pending to Rejected Raw Bills - {}'.format(Ymd)
        msg = """\
Yi,

Please move error 837I JSON from pending to rejected raw bills:
{}

/tmp/{}""".format(desc, 'inst_{}_pending2rejected.json'.format(md))
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']

        print('-' * 200)
        print('inst pending2rejected file exists...')

        if os.path.exists('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md)):
            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read()
            act = 'a' 
        else:
            rec = ''
            act = 'w'

        if msg not in rec:        
            automail(subj, msg, to, toname)

            with open('/home/james.niu@revintel.net/production/jsondump/modules/pending_emails/email_pending_{}.txt'.format(md), act) as fw:
                print(msg, file=fw)
    else:
        print('-' * 200)
        print('inst pending2rejected file not exists...')

else:
    print('changes incomplete ERROR')



#----
# print output message
#----
print('-' * 200)
print("""
load 837P {}
load 837I {}
rejected 837P {}
rejected 837I {}

total pending {}""".format(nnp, nni, nyp, nyi, c))





