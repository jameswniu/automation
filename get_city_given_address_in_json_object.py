import os, sys
import re
import json

import paramiko
import psycopg2.extras

from datetime import datetime, timedelta
from pytz import timezone

from paramiko import SSHClient
from scp import SCPClient
from usps import USPSApi, Address

from automation__written_module_allowing_automating_of_emails import automail
from config import user_db, passwd_db


def get_city(cur):
    # header = [row[0] for row in cur.description]
    # print(header)
    dicy = {}
    b, c, d, p = 0, 0, 0, 0

    for row in cur:
        # print(row)
        ky = row[0]
        addr = row[1]
        typ = row[2]
        info = row[3]

        if typ == '837P':
            p += 1

        c += 1

        dicy[ky] = """{}-{}-{}""".format(addr, typ, info)
    # print(p)
    # print(dicy)

    sql1 = """\
update
    business_analysis.test_responses
set
    content = content || '{{"patient_city": "{}", "patient_state": "{}",
    "holding_flag": "N", "reject_flag": "N"}}'
where
    id = {}
    and coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info') ~ 'bad city name:';"""

    for ky in dicy:
        tmp = dicy[ky].split('-')
        l = tmp[0].split(', ')
        addr1 = l[0]
        cy = l[2]
        st = l[3].split(' ')[0]
        zp = l[3].split(' ')[1]

        if 'bad city name' in tmp[2]:
            print('-' * 200)
            print("""{}, {}, {} {}""".format(addr1, cy, st, zp))

            try:
                address = Address(
                    name=''
                    , address_1=addr1
                    , city=cy
                    , state=st
                    , zipcode=zp
                )

                usps = USPSApi('748MEDLY7441', test=True)
                validation = usps.validate_address(address)
                res = validation.result['AddressValidateResponse']['Address']
                print("""{}: {}|{} (changed to {}|{})""".format(ky, cy, st, res['City'], res['State']))
                s1 = sql1.format(res['City'], res['State'], ky)

                with con:
                    cur.execute(s1)

                if cur.rowcount == 1:
                    # print(s1)
                    pass
                else:
                    print('update not ran...')

                b += cur.rowcount
            except:
                print("""{}: finding address ERROR""".format(ky))

                d += 1
    print('-' * 200)
    print("""
PENDING
Updated addr: {}/{} | 837P: {}/{}""".format(b, b + d, p, c))


os.chdir(r'L:\Auto_Opportunity_Analysis\Load_Pending')
Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[4:]


#----
# get city w API
#----
# connect DB and fetch data
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)
cur = con.cursor()

sql = """\
drop table if exists business_analysis.test_responses;
create table if not exists business_analysis.test_responses as (
select
    is_exception_corrected,
    a.id,
    a.created_at,
    a.cust_id,
    b.cust_name,
    a.pat_acct,
    a.gross_charges,
    case when a.notes ~* 'MLX' then null else a.notes end notes,
    a.processed,
    a.processed_at,
    a.content
from 
    tpl_pending_raw_bills a
left join 
    tpl_cust_infos b on a.cust_id = b.cust_id
where
    current_date - a.created_at::date < 30
    and processed = 'f'
    and not exists (select 1 from tpl_client_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct)
    and not exists (select 1 from tpl_rejected_raw_bills d where a.cust_id = d.cust_id and a.pat_acct = d.pat_acct)
order by 
    a.created_at desc
);
-->
select
    id
    , concat(content->>'patient_addr1', ', ', content->>'patient_addr2', ', ', content->>'patient_city', ', ', content->>'patient_state', ' ', content->>'patient_zip') patient_addr
    , content->>'claim_type' claim_type
    , coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info')
from 
    business_analysis.test_responses
where 
    processed = 'f'
--  and coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info') !~* 'facility name is bad'
order by 
    date(created_at) desc
    , cust_id
    , pat_acct;"""
with con:
    cur.execute(sql)

# update patient city in database data
get_city(cur)


#----
# extract json and scp transfer if fix all addr
#----
if b == c == p != 0:    # updated addr eq total pending eq 837P not zero
    with open('prof_{}_pending2client.json'.format(md), 'w') as fw0:
        print('\nall addr fixed... dumping json...')

        sql2 = """select content from business_analysis.test_responses;"""
        with con:
            cur.execute(sql2)
        [print(json.dumps(row[0]), file=fw0) for row in cur]

    try:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        ssh.connect(hostname='revproc01.revintel.net',
                    username='james.niu',
                    password='Cw1874567.,')

        # SCPCLient takes a paramiko transport as its only argument
        scp = SCPClient(ssh.get_transport())
        scp.put('prof_{}_pending2client.json'.format(md), '/tmp')

        print('\nscp transport success... /tmp/{}'.format('prof_{}_pending2client.json'.format(md)))
    except:
        print('\nconnection ERROR')
else:
    print('\nfile not created...')


#----
# write email
#----
def email_yi():
    if b == c == p != 0:
        if os.path.exists('email_pending_{}.txt'.format(md)):
            with open('email_pending_{}.txt'.format(md), 'r') as fr:
                rec = fr.read().strip()
        else:
            rec = ''
        # print(rec)

        subj = 'Load Json from Pending into Client Raw Bills - {}'.format(Ymd)
        msg = """Yi,
Please load from pending into client raw bills - fixed patient city name(s):
/tmp/{}""".format('prof_{}_pending2client.json'.format(md))
        # print(msg)
        to = ['yi.yan@medlytix.com']
        toname = ['Yi Yan']

        if msg != rec:
            print('\njsonpending file exists...')
            automail(subj, msg, to, toname)
            with open('email_pending_{}.txt'.format(md), 'w') as fw0:
                print(msg, file=fw0)
        else:
            print('\njsonpending file already sent_Justin...')
    else:
        print('\nno jsonpending file sent_Justin...')
email_yi()

