import os, sys
import shutil
import re
import json

import pytz
import paramiko
import requests
import numpy as np
import psycopg2

from copy import deepcopy, copy
from glob import glob
from pprint import pprint

from datetime import datetime, timedelta
from pytz import timezone
from datetime import datetime, timedelta
from paramiko import SSHClient
from scp import SCPClient
from PyPDF3 import PdfFileWriter, PdfFileReader

from config import user_db, passwd_db
from extraction__cleaning__automation__abbreviate_company_names_and_verify_address_through_API import trim_name, transfer_linux
from automation__allow_automatically_sending_emails_through_SMTP import automail


params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db
}
con = psycopg2.connect(**params)

Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')


print("""\
########################################################################################################################
# Handle missing TeamHealth tax IDs
########################################################################################################################""")
os.chdir(r'L:\customer\10082_teamhealth\000538_teamhealth\enrollment\W9s\PUBLISHED')

# extract local tax IDs
base_d = {}
for f in glob('*.pdf'):
    name = f.replace('-', '').replace('.pdf', '')
    base_d[name] = 1

# extract CP pending tax IDs
sql_ip = """\
select distinct 
	coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info') holding_reject_info
	, cust_id
	, content->>'billing_provider_taxid' billing_provider_taxid
	, count(*) over (partition by content->>'billing_provider_taxid')
	, content->>'holding_flag' holding_flag
	, content->>'reject_flag' reject_flag
	, string_agg(distinct created_at::date::text, ';  ') created_at
from 
	business_analysis.test_responses
where 
	coalesce(nullif(notes, ''), nullif(content->>'holding_info', '') , content->>'reject_info', content->>'hodling_info') ~* 'missing W9'
	and content->>'holding_flag' = 'Y'
group by
	coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info')
	, cust_id
	, content->>'billing_provider_taxid'
	, content->>'holding_flag'
	, content->>'reject_flag'
order by
	billing_provider_taxid;"""
with con:
    cur = con.cursor()
    cur.execute(sql_ip)

raw_d = {}
for r in cur:
    r = [str(x) for x in r]
    raw_d[r[2]] = r[1]

os.chdir(r'L:\Auto_Opportunity_Analysis\PUBLISHED_verify')

# Stopper for sent W9s and received non-TeamHeath
sent_Justin = [   # SPECIFY
    '85-3861695.pdf'
]
received_nonTeamHealth = [   # SPECIFY
]

# Compare for TeamHealth W9s
exist_d = {}
print(r'Please install the following TeamHealth W9(s) from /tmp:')
for id, cust in raw_d.items():
    if id in base_d:
        op = f'{id[:2]}-{id[2:]}.pdf'
        print(op)

        exist_d[id] = cust

# remove every file and copy new W9s over
for walk in os.walk(os.getcwd()):
    folder = walk[0]
    files = walk[2]

    for f in files:
        os.remove(os.path.join(folder, f))

for f in glob(r'L:\customer\10082_teamhealth\000538_teamhealth\enrollment\W9s\PUBLISHED\*.pdf'):
    name = os.path.basename(f).replace('-', '').replace('.pdf', '')
    # print(name)

    if name in exist_d:
        shutil.copyfile(f, os.path.basename(f))

# #check first before transfer to linux and email Yi
print()
ind0 = input('Have you checked pay_to and filename in PUBLISHED_verify?').lower()
ind1 = input('Have you ran ./pending.sh run.sql?').lower()
indf = f'{ind0}{ind1}'

if indf == 'yy' and len(glob('*.pdf')) != 0:
    # clean PDF and initialize vars
    fname_l = []

    for fname in glob('*.pdf'):
        # print(fname)
        fname_l.append(fname)

        with open(fname, "rb") as fr:
            inputpdf = PdfFileReader(fr)

            for i in range(inputpdf.numPages):
                output = PdfFileWriter()
                output.addPage(inputpdf.getPage(i))

                nfnm = "new_{}_{}".format(i + 1, fname)

                with open(nfnm, "wb") as fh:
                    output.write(fh)

        os.remove(fname)
        os.rename(nfnm, fname)

    to = ['yi.yan@medlytix.com']
    toname = ['Yi Yan']
    cc = ['gavin.johnson@medlytix.com', 'justin.martin@medlytix.com', 'andrea.davis@medlytix.com']
    ccname = ['Gavin Johnson', 'Justin Martin', 'Andrea Davis']

    subj = f'Install 538/734 TeamHealth W9 to Release Pending Account(s) - {Ymd}'
    msg = """\
Yi,

Please install the following W9(s) from /tmp:
{}""".format('\n'.join(fname_l))

    if os.path.exists(r'L:\Auto_Opportunity_Analysis\email_logs\email_teamhealth_W9s_{}.txt'.format(Ymd)):
        with open(r'L:\Auto_Opportunity_Analysis\email_logs\email_teamhealth_W9s_{}.txt'.format(Ymd), 'r') as fr:
            rec = fr.read()
        act = 'a'
    else:
        rec = ''
        act = 'w'

    if msg not in rec:
        # transfer linux
        for f in fname_l:
            transfer_linux(f, '/tmp')

        # email Yi cc the rest
        automail(subj, msg, to, toname, cc, ccname)

        with open(r'L:\Auto_Opportunity_Analysis\email_logs\email_teamhealth_W9s_{}.txt'.format(Ymd), act) as fw:
            print(msg, file=fw)
    else:
        print('email already sent...')


print("""\
########################################################################################################################
# Handle missing non-TeamHealth tax IDs
########################################################################################################################""")
print(r'Please install the following non-TeamHealth W9(s): (CHECK PAY_TO AND FILENAME FIRST)')
for i in received_nonTeamHealth:
    if i.replace('-', '').replace('.pdf', '') in raw_d:
        print(i)


# set up temp table to expand missing W9 infos
sql_makew9 = """\
drop table if exists business_analysis.test_w9;
create table if not exists business_analysis.test_w9 as (
select
	b.master_id
	, a.cust_id cust_ids
	, content->>'billing_provider' provider
	, format('%s-%s', left(content->>'billing_provider_taxid', 2), right(content->>'billing_provider_taxid', 7)) tax_id
	, content->>'pay_to_addr1' pay_to_addr1
	, content->>'pay_to_addr2' pay_to_addr2
	, content->>'pay_to_city' pay_to_city
	, content->>'pay_to_state' pay_to_state
	, format('%s-%s', left(content->>'pay_to_zip', 5), right(content->>'pay_to_zip', 4)) pay_to_zip
	, content->>'billing_provider_npi' npi
	, null::text phone
	, null::text specialty_code
	, content->>'billing_provider_addr1' addr1
	, content->>'billing_provider_addr2' addr2
	, content->>'billing_provider_city' city
	, content->>'billing_provider_state' state
	, format('%s-%s', left(content->>'billing_provider_zip', 5), right(content->>'billing_provider_zip', 4)) zip
	, null::text provider_short_name
	, content->>'billing_provider_taxid'::text tax_num
from
	business_analysis.test_responses a
right join
	tpl_cust_infos b
	on a.cust_id = b.cust_id
where
	coalesce(nullif(notes, ''), nullif(content->>'holding_info', '') , content->>'reject_info', content->>'hodling_info') ~* 'missing W9'
	and content->>'holding_flag' = 'Y'
	and a.id in
	(select max(id) from business_analysis.test_responses
	where coalesce(nullif(notes, ''), nullif(content->>'holding_info', '') , content->>'reject_info', content->>'hodling_info') ~* 'missing W9' and content->>'holding_flag' = 'Y'
	group by content->>'billing_provider_taxid')
	and a.cust_id not in (538, 734)
);
select * from business_analysis.test_w9;"""
with con:
    cur = con.cursor()
    cur.execute(sql_makew9)

c = cur.rowcount

# update temp table missing provider info
if c != 0:
    sql_getnpiname = """
select a.cust_ids, b.cust_name, tax_num, npi, provider from business_analysis.test_w9 a
left join tpl_cust_infos b on a.cust_ids = b.cust_id;
    """
    with con:
        cur = con.cursor()
        cur.execute(sql_getnpiname)

    print("""\
Update:
cust_ids|cust_name|tax_num|phone|specialty_code|short_name (long_name)""")
    for r in cur:
        cust_ids = r[0]
        cust_name = r[1]
        tax_num = r[2]
        npi = r[3]
        lname = r[4]

        url = f'https://npiregistry.cms.hhs.gov/api?number={npi}&version=2.1'
        r = requests.get(url)
        dicy = r.json()
        # pprint(dicy)

        pot = (dicy['results'][0]['addresses'])

        for jar in pot:
            for k in jar:
                if jar[k] == 'LOCATION':
                    phone = jar['telephone_number']

        taxo = dicy['results'][0]['taxonomies'][0]['code']

        sname = trim_name(lname, 33)

        sql_updatew9 = f"""\
    update
        business_analysis.test_w9 a
    set
        phone = '{phone}'
        , specialty_code = '{taxo}'
        , provider_short_name = '{sname}'
    where
        a.npi = '{npi}';"""
        print(f'{cust_ids}|{cust_name}|{tax_num}|{phone}|{taxo}|{sname} ({lname})')

        with con:
            cur = con.cursor()
            cur.execute(sql_updatew9)

    # transfer linux
    os.chdir(r'L:\Auto_Opportunity_Analysis\w9_DB_insert')

    with open(f'w9_install_{Ymd}.txt', 'w') as fw:
        sql_exportw9 = """\
        select * from business_analysis.test_w9
        --where"""

        with con:
            cur = con.cursor()
            cur.execute(sql_exportw9)

        head_l = [r[0] for r in cur.description]
        head = '|'.join(head_l)
        print(head, file=fw)
        for r in cur:
            rec = '|'.join([str(i) for i in r])
            print(rec, file=fw)

    print()
    transfer_linux(f'w9_install_{Ymd}.txt', '/tmp')
else:
    print()
    print('non-TeamHealth pay_to infos not created...')


print("""\
########################################################################################################################
# Email Justin all not found tax IDs
########################################################################################################################""")
print('Would you be able to advise on the following tax ID(s)?')
for id, cust in raw_d.items():
    if id not in base_d and id not in [i.replace('-', '').replace('.pdf', '') for i in received_nonTeamHealth]:
        op = f'{id[:2]}-{id[2:]}.pdf'
        if op not in sent_Justin:
            print(op)
            # print(id)





