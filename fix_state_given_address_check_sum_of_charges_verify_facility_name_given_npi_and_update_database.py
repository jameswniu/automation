import os, sys
import re
import json

import paramiko
import psycopg2
import requests

from copy import deepcopy
from string import punctuation

from datetime import datetime, timedelta
from pytz import timezone
from usps import USPSApi, Address
from paramiko import SSHClient
from scp import SCPClient

from automation__allow_automatically_sending_emails_through_SMTP import automail
from config import user_db, passwd_db
from extraction__cleaning__automation__abbreviate_company_names_and_verify_address_through_API import validate_addr


os.chdir(r'L:\Auto_Opportunity_Analysis\Load_Pending')


Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[4:]

params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)


def get_pat_city(dicy):
    # mark as fixed
    sql1 = """\
update
    business_analysis.test_responses
set
    content = content || '{{"patient_city": "{}", "patient_state": "{}",
    "holding_flag": "{}", "reject_flag": "N"}}'
where
    id = {};"""    # SPECIFY variable

    if 'city name' in info or 'state' in info:
        print('-' * 200)

        ky = id    ## SPECIFIY

        addr1 = dicy['patient_addr1']
        city = dicy['patient_city']
        state = dicy['patient_state']
        zip = dicy['patient_zip']

        print('{}:'.format(ky))
        print("""{}, {}, {} {}""".format(addr1, city, state, zip))


        # bypass verification through valid city|State
        valid = {   # SPECIFY
            'DPO': 'AE'
        }

        if city in valid and state in valid[city]:
            cy = city
            st = state

            print("""{}|{} (changed to {}|{})""".format(city, state, cy, st))


        # ping API for non-bypass city|State
        else:
            fulladdr = validate_addr(addr1, city, state, zip).split(',')
            # print(fulladdr)

            try:
                cy = fulladdr[1]
                st = fulladdr[2][:2]

                print("""{}|{} (changed to {}|{})""".format(city, state, cy, st))
            except:
                print('finding address ERROR')
            else:
                global a, b

                # check if sole issue and update DB if True
                if len([x.strip() for x in info.split(';') if x.strip()]) == 1:
                    hold = 'N'
                else:
                    hold = 'Y'

                s1 = sql1.format(cy, st, hold, ky)
                # print(s1)

                with con:
                    cur = con.cursor()
                    cur.execute(s1)

                    con.commit()

                a += cur.rowcount

                cur.close()

            b += 1


def check_sum(dicy):
    sum = 0.0
    for ky in dicy:
        if 'LX' in ky and 'charge' in ky and 'non_covered' not in ky:
            sum += float(dicy[ky])
    sum = round(sum, 2)
    return sum


def fix_charges(dicy):
    # fix total charges
    sql2 = """\
update
    business_analysis.test_responses
set
    content = content || '{{"total_charges": "{}", "balance": "{}",
    "holding_flag": "{}", "reject_flag": "N"}}'
where
    id = {};"""  # SPECIFY variable

    # send to rejected
    sql3 = """\
update
	business_analysis.test_responses
set
	content = content || '{{"holding_info": "", "holding_flag": "{}", "reject_flag": "Y"}}' || jsonb_build_object('reject_info', content->>'holding_info')
where id = {} and content->>'holding_info' != '';
"""

    if '<>' in info and 'sum of charges' in info:
        print('-' * 200)

        ky = id    # specify this

        sumsvclines = float(check_sum(dicy))
        totcharges = float(dicy['total_charges'])
        # print(totcharges, sumsvclines)

        print('{}'.format(ky))
        # print(info)

        disp = abs(totcharges - sumsvclines) / sumsvclines
        thres = 1   # specify


        if disp < thres or disp == 1:
            # print(totcharges, sumsvclines)

            # check if sole issue and update DB if True
            if len([x.strip() for x in info.split(';') if x.strip()]) == 1:
                hold = 'N'
            else:
                hold = 'Y'

            s2 = sql2.format(sumsvclines, sumsvclines, hold, ky)
            # print(s2)
            print('total charges {} (changed to {})'.format(totcharges, sumsvclines))

            with con:
                cur = con.cursor()
                cur.execute(s2)

                con.commit()

            cur.close()

            global c
            c += 1
        else:
            s3 = sql3.format(hold, ky)
            print(s3)

            with con:
                cur = con.cursor()
                cur.execute(s3)

                cur.close()

            print('discrepancy {} % > {} %'.format(round(disp * 100, 1), round(thres * 100, 1)))

        global d
        d += 1



def get_fac_name(dicy):
    try:
        global g
    except:
        pass
    # fix facility and update facility table
    sql1 = """\
    update
        business_analysis.test_responses
    set
        content = content || '{{"facility": "{}",
        "holding_flag": "{}", "reject_flag": "N"}}'
    where
        id = {};"""    # SPECIFY name reference
    sql11 = """\
    update 
    	business_analysis.test_npi
    set
    	medlytix_hospital_name = '{}'
    where 
    	facility_npi = '{}'
    	and medlytix_hospital_name = ''
    	and incoming_hospital_name = '{}';"""

    if 'facility name' in info:
        print('-' * 200)

        # set key for updating sql
        ky = id

        npi = dicy['facility_npi']
        fac = dicy['facility']

        print(f'{ky}:')

        # prefilter facility name(s)
        plate = {    # SPECIFY
            'Colonial Heights Emergency Care Center': 'COLONIAL HEIGHT EMERGENCY CARE|1710562723'
        }

        ind = 0
        for k, v in plate.items():
            if k in fac:
                # print(k)
                tmp = v.split('|')

                newnamesuffix = tmp[0]
                newnpi = tmp[1]

                ind = 1

        # ping api for facility name(s)
        if ind == 0:
            str = 'https://npiregistry.cms.hhs.gov/api?number={}&version=2.1'
            r = requests.get(str.format(npi))
            pot = r.json()    # eq to json.loads(response.text)
            # print(pot)

            alias = ('DBA', 'INC', 'LLC', 'CO', 'CORP', 'CORPORATION')    # remove alias everything after for comparison
            oalias = ('A CAMPUS OF', 'A COMPANY OF')

            # grab name(s) and clean
            name = pot['results'][0]['basic']['organization_name']
            for b in punctuation.replace('&', ''):
                name = re.sub(r'\s*{}+\s*|\s+'.format(f'\{b}'), ' ', name).strip()
            for a in alias:
                for word in name.split(' '):
                    if a == word:
                        name = re.sub(r'(?=\b{}\b).*|'.format(a), '', name)
            for c in oalias:
                name = re.sub(r'(?=\b{}\b).*'.format(c), '', name)
            name = name.strip()
            # print(name)

            try:
                oname = pot['results'][0]['other_names'][0]['organization_name']
                for b in punctuation.replace('&', ''):
                    oname = oname.replace(b, ' ')
                    oname = oname.replace("'", "")
                    oname = re.sub('\s+', ' ', oname)
                for a in alias:
                    for word in oname.split(' '):
                        if a == word:
                            oname = re.sub(r'(?=\b{}\b).*'.format(a), '', oname)
                for c in oalias:
                    oname = re.sub(r'(?=\b{}\b).*'.format(c), '', oname)
                oname = oname.strip()
            except:
                oname = ''
            # print(oname)

            rname = ' '.join([x for x in fac.split('_') if len(x) > 3]).upper()
            # print(rname)

            namebase = deepcopy(name)
            onamebase = deepcopy(oname)

            stopwords = ('COMMUNITY HOSPITAL', 'CENTER', 'CTR'
                         , 'MEDICAL', 'MEDICAL CENTER', 'REGIONAL CENTER', 'HOSPITAL COMPANY'
                         , 'HOSPITAL CENTER', 'COMMUNITY', 'HSP', 'HEALTH CENTER', 'HEALTH CTR')    # SPECIFY
            for w in stopwords:
                namebase = namebase.replace('{}S'.format(w), '')
                namebase = namebase.replace(w, '')
                onamebase = onamebase.replace('{}S'.format(w), '')
                onamebase = onamebase.replace(w, '')
            namebase = re.sub('\s+', ' ', namebase).strip()
            onamebase = re.sub('\s+', ' ', onamebase).strip()
            # print()
            # print(name, '->', namebase)
            # print(oname, '->', onamebase)
            # print(rname)

            # determine which name to use and print
            n, o = 0, 0
            for i in namebase.split(' '):
                if i in rname:
                    n += 1
            # print(n)

            try:
                for j in onamebase.split(' '):
                    if j in rname:
                        o += 1
            except:
                o += 0
            # print(o)

            if o > n and onamebase != '':
                newnamebase = onamebase
            else:
                newnamebase = namebase

            box = {}
            for w in stopwords:
                if w in rname:
                    box[w] = 1
            # print(box)

            try:
                suffix = max(box, key=len)
            except:
                suffix = ''
            newnamesuffix = f'{newnamebase} {suffix}'.strip()
            print(newnamesuffix)

            newnpi = npi

        print(f'{npi}|{fac} (changed to {newnpi}|{newnamesuffix})')

        # check if sole issue and update DB if True
        if len([x.strip() for x in info.split(';') if x.strip()]) == 1:
            hold = 'N'
        else:
            hold = 'Y'

        s11 = sql11.format(newnamesuffix, npi, fac)

        with con:
            cur = con.cursor()
            cur.execute(s11)

            con.commit()
        # print(s11)

        cur.close()

        s1 = sql1.format(newnamesuffix, hold, ky)

        with con:
            cur = con.cursor()
            cur.execute(s1)

            con.commit()
        # print(s1)

        # print(cur.rowcount)
        g += cur.rowcount

        cur.close()


def trim_zip(dicy):
    sql1 = """\
update
    business_analysis.test_responses
set
    content = content || '{{"patient_zip": "{}"}}'
where
    id = {};"""    ## SPECIFY name reference

    ky = id  ## SPECIFIY

    zip = dicy['patient_zip']

    if len(str(zip)) > 5:
        zipnew = zip[:5]

        sql11 = sql1.format(zipnew, ky)

        with con:
            cur = con.cursor()
            cur.execute(sql11)

            con.commit()

        cur.close()


#----
# create tmp pending table, maybe prepare npi table for update
#----
sql0 = """\
drop table if exists business_analysis.test_responses;
create table if not exists business_analysis.test_responses as (
select
	a.id
    , is_exception_corrected
	, a.created_at
	, a.cust_id
	, b.cust_name
	, a.pat_acct
	, a.gross_charges
	, case when a.notes ~* 'MLX' then null else a.notes end notes
	, a.processed
	, a.processed_at
	, a.content
from 
	tpl_pending_raw_bills a
left join 
	tpl_cust_infos b on a.cust_id = b.cust_id
where
	current_date - a.created_at::date < 30
and 
	processed = 'f'
	and not exists (select 1 from tpl_client_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct)
	and not exists (select 1 from tpl_rejected_raw_bills d where a.cust_id = d.cust_id and a.pat_acct = d.pat_acct)
    and length(regexp_replace(content->>'patient_zip', '\D', '', 'g')) in (5, 9)	-- foreign addresses stay in pending for now
    and content->>'patient_state' !=  'PR'    -- puerto rico
    and content->>'patient_addr1' !~* 'OASIS DE BURGOS|GUADALUPE RAMIREZ'
    and content->>'patient_city' not in ('NEWTONKITTY' ,'HERMOSILLO', 'SINGAPORE', 'SINGPORE', 'LIMA')
    --and holding_info ~* 'facility'
order by
	a.created_at desc
);
-->
drop table if exists business_analysis.test_npi;
create table if not exists business_analysis.test_npi as (
select distinct 
	cust_id
	, content->>'facility' incoming_hospital_name
	, '' medlytix_hospital_name
	, content->>'facility_npi' facility_npi 
	, count(*) over (partition by content->>'facility') 
from
	business_analysis.test_responses
where 
	coalesce(nullif(notes, ''), nullif(content->>'holding_info', '') , content->>'reject_info', content->>'hodling_info') ~* 'facility name'
);
select * from business_analysis.test_npi"""

with con:
    cur = con.cursor()
    cur.execute(sql0)

    con.commit()
# [print(r) for r in cur]

cur.close()


#----
# create temp table and update pending records
#----
sql = """\
select
	id
    , is_exception_corrected
	, date(created_at)
	, content->>'claim_type' inst_prof
	, cust_id, pat_acct, coalesce(gross_charges, round((content->>'total_charges')::numeric)) total_charges
	, coalesce(nullif(notes, ''), nullif(content->>'holding_info', ''), content->>'reject_info', content->>'hodling_info') holding_reject_info
	, content->>'billing_provider_taxid' billing_provider_taxid
	, concat(content->>'patient_addr1', ', ', content->>'patient_addr2', ', ', content->>'patient_city', ', ', content->>'patient_state', ' ', content->>'patient_zip') patient_addr
	, content->>'facility' facility, content->>'facility_npi' facility_npi
	, concat(content->>'facility_addr1', ', ', content->>'facility_addr2', ', ', content->>'facility_city', ', ', content->>'facility_state', ' ', content->>'facility_zip') facility_addr
	, content->>'holding_flag' holding_flag, content->>'reject_flag' reject_flag
	, processed, processed_at
	, 'James, Yi'::text assigned, now()::date - created_at::date aging, content
from 
	business_analysis.test_responses
where 
	processed = 'f'
order by
	date(created_at) desc
	, cust_id
	, pat_acct;"""

with con:
    cur = con.cursor()
    cur.execute(sql)
# [print(r) for r in cur]

a, b = 0, 0
c, d = 0, 0
g = 0
t = 0
for row in cur:
    id = row[0]
    dicy = row[-1]
    info = dicy['holding_info']

    get_fac_name(dicy)    # fix facility name and create npi table

    get_pat_city(dicy)    # update patient city

    fix_charges(dicy)    # fix tot charges

    trim_zip(dicy)    # trim patient zip to 5 digits

    t += 1

cur.close()


#----
# check distinct accts
#----
sql1 = """\
select 
	count(distinct pat_acct) pat_accts
	, count(*) records
	, content->>'holding_flag' holding_flag 
from 
	business_analysis.test_responses
where 
	processed = 'f'
group by 
	content->>'holding_flag';"""

with con:
    cur = con.cursor()
    cur.execute(sql1)
# [print(r) for r in cur]

a_n, r_n, a_y, r_y = 0, 0, 0 ,0
for r in cur:
    if r[2] == 'N':
        a_n = r[0]
        r_n = r[1]
    else:
        a_y = r[0]
        r_y = r[1]
# print(a_n, r_n, a_y, r_y)

cur.close()
con.close()

print('-' * 200)
print(f"""\
Pat city|state changed {a}, pat city|state remaining {b - a}
Total charges changed {c}
Facility names changed {g}

Pending changed {a + c + g}, pending remaining {t - (a + c + g)}
Acct changed {a_n}, acct remaining {a_y}""")
