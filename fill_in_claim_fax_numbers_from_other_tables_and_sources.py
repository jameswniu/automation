#! /usr/bin/python3
import os
import sys
import re

import psycopg2
import psycopg2.extras


"""
Correct fax #:  586-469-1872
 Failed fax # :   5863231130
"""

Dry_run = False     # update db table
#Dry_run = True     # only generate update sql


db_host = 'revpgdb01'
db_name = ''
db_user = ''
port  = '5432'

dns = "dbname=%s user=%s host=%s port=%s" % (db_name, db_user, db_host, port)
conn = psycopg2.connect(dns)
cur = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)


"""
 Section 1

 Update insurance claim fax number. These insurance's first/third party
 use same Fax

"""
print(' Updating carriers Fax ...' )

select_sql = """
    select insurance_name, fp_fax as fax
    from  tpl_carrier_infos t
    where  t.fp_fax is not null and t.tp_fax is not null and t.fp_fax = t.tp_fax
    """


fax_update_sql = """
    update tpl_pre_billing_records set content=jsonb_set( content,
         '{}send_fax_number{}', '"{}"', true) , data_modified = 't'
    where data_modified = 'f' and status in ( 'W', 'PB', 'E')
            and created_at > '2019-07-01'
            and insurance_name ~* '{}'
  """



cur.execute(select_sql)
rows = cur.fetchall()
for ins in rows:
    if re.search(r'ALLSTATE|STATE FARM|TRAVELERS INDEMNITY',ins[0]):
        continue

    sql = fax_update_sql.format('{', '}', ins[1].strip(), ins[0].strip())
    if Dry_run:
        print(sql)
    else:
        try :
            cur.execute(sql)
        except :
            print(sql)
            conn.rollback()
else:
    print(' Done section 1 !' )
    conn.commit()


"""
 Section 2

 Update insurance claim fax number. These insurance's first/third party
 claims have different fax numbers
"""


print(' Updating carriers with diffretn FP/TP faxes ...' )


select_sql = """
    select insurance_name, fp_fax, tp_fax
    from  tpl_carrier_infos t
    where  t.fp_fax is not null and t.tp_fax is not null and t.fp_fax != t.tp_fax
    """

fax_update_sql = """
    update tpl_pre_billing_records set content=jsonb_set( content,
         '{}send_fax_number{}', '"{}"', true), data_modified = 't'
    where data_modified = 'f' and  status in ('W', 'PB', 'E')
          and created_at > '2019-07-01'
          and insurance_name ~* '{}' and claim_type = '{}'
  """


cur.execute(select_sql)
rows = cur.fetchall()
for ins in rows:
    if re.search(r'ALLSTATE|STATE FARM|TRAVELERS INDEMNITY',ins[0]):
        continue

    sql = fax_update_sql.format('{', '}', ins[1], ins[0], 'FP')
    if Dry_run :
        print(sql)
    else:
        cur.execute(sql)

    sql = fax_update_sql.format('{', '}', ins[2], ins[0], 'TP')
    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()
print(' Done section 2 !' )


"""
 Section 3
 Update  GEICO/Government employee
"""

print(' Update GEICO/Government Employee FAX ...' )

geico_fax_num = {
  'AL' : 2023544691,
  'AK' : 8667602860,
  'AZ' : 8665682132,
  'AR' : 2023544691,
  'CA' : 6198194004,
  'CO' : 2144425164,
  'CT' : 7168980542,
  'DE' : 7037382188,
  'DC' : 7037382188,
  'FL' : 2023545295,
  'GA' : 2023544691,
  'HI' : 8667602860,
  'ID' : 8665682132,
  'IL' : 2023544691,
  'IN' : 2023544691,
  'IA' : 2144425164,
  'KS' : 2144425164,
  'KY' : 2023544691,
  'LA' : 2023544691,
  'ME' : 7168980542,
  'MD' : 7037382188,
  'MA' : 7168980542,
  'MI' : 2023544691,
  'MN' : 2023544691,
  'MS' : 2023544691,
  'MO' : 2023544691,
  'MT' : 8665682132,
  'NE' : 2144425164,
  'NV' : 8665682132,
  'NH' : 7168980542,
  'NJ' : 7168980542,
  'NM' : 2144425164,
  'NY' : 7168980542,
  'NC' : 7037382194,
  'ND' : 2144425164,
  'OH' : 2023544691,
  'OK' : 2144425164,
  'OR' : 8665682132,
  'PA' : 7037382188,
  'RI' : 7168980542,
  'SC' : 2023544691,
  'SD' : 2144425164,
  'TN' : 2023544691,
  'TX' : 2144425164,
  'UT' : 8665682132,
  'VT' : 7168980542,
  'VA' : 7037382194,
  'WA' : 8665682132,
  'WV' : 7037382188,
  'WI' : 2023544691,
  'WY' : 2144425164
};


#-----------------------------------
# for 837P/837I new json format
#-----------------------------------
for state, fax in geico_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                    data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E') and insurance_name ~* 'GEICO|GOVERNMENT EMPL'
                 and created_at > '2019-01-01'
                 and content->>'edi_payer_id' is null
                 and content->>'vx_carrier_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)


#-----------------------------------
# for Matt legacey data format
#-----------------------------------
for state, fax in geico_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                    data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E')
                 and insurance_name ~* 'GEICO|GOVERNMENT EMPL'
                 and content->>'cms_7_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)



conn.commit()
print(' Done section 3 !' )


print(' Updating Columbia Mutual fax ... !' )

columbia_mut_fax_num = {
  'AL' : 8004477916,
  'GA' : 8004477916,
  'MS' : 8004477916,
  'NC' : 8004477916,
  'SC' : 8004477916,
  'TN' : 8004477916,
};


for state, fax in columbia_mut_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                    data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E') and insurance_name ~* 'COLUMBIA MUTU'
                 and content->>'vx_carrier_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)


for state, fax in columbia_mut_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                    data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E') and insurance_name ~* 'COLUMBIA MUTU'
                 and content->>'cms_7_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()
print(' Done section 4 !' )


"""
 Section 5
 Update   Safeway
"""

print(' Updating SAFEWAY fax ... !' )
safeway_fax_num = {
    'AL' : '800-488-2270',
    'GA' : '800-896-5375',
    'MS' : '601-936-6701',
    'IN' : '601-936-6701',
    'IL' : '630-887-9886',
    'LA' : '337-233-7804',
    'CA' : '626-301-1974',
    'TN' : '877-323-8052',
    'TX' : '800-290-8153'
}

for state, fax in safeway_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                data_modified = 't'
           where data_modified = 'f' and status in('W', 'PB', 'E') and insurance_name ~* 'SAFEWAY'
                 and content->>'vx_carrier_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)


for state, fax in safeway_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                data_modified = 't'
           where data_modified = 'f' and status in('W', 'PB', 'E') and insurance_name ~* 'SAFEWAY'
                 and content->>'cms_7_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()
print(' Done section 5 !' )


"""
 Section 6
 Update  AMERISURE INSURANCE
"""

print(' Updating Amerisure fax ... !' )
amerisure_fax_num = {
    'FL' : '813-282-6050',
    'GA' : '770-813-3310',
    'MS' : '901-683-6001',
    'NC' : '704-510-8327',
    'SC' : '704-510-8327',
    'TN' : '901-683-6001',
    'IN' : '800-528-9187',
    'LA' : '214-630-8326',
    'TX' : '214-630-8326',
}

for state, fax in amerisure_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E') and insurance_name ~* 'AMERISURE INSURANCE'
                 and content->>'vx_carrier_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)


for state, fax in amerisure_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"'  , true),
                data_modified = 't'
           where data_modified = 'f' and status in ('W', 'PB', 'E') and insurance_name ~* 'AMERISURE INSURANCE'
                 and content->>'cms_7_insured_state' = '{}'
          """ .format ( '{',  '}', fax, state)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()
print(' Done section 6 !' )


"""
 Section 7
 Update  PENNSYLVANIA NATIONAL MUT CAS INS
"""

print(' Updating PENNSYLVANIA NATIONAL MUT CAS INS fax ... !' )
penns_nat_mut_fax_num = {
    '029' : '877-942-9715',
    '032' : '877-942-9715',
    '044' : '866-495-4764',
    '089' : '866-496-4764',
    '096' : '866-496-4764',
    '171' : '866-523-0583',
}

for pattern, fax in penns_nat_mut_fax_num.items():
    sql = """
           update tpl_pre_billing_records
                set content=jsonb_set(content, '{}send_fax_number{}', '"{}"' , true),
                data_modified = 't'
           where data_modified = 'f' and status = 'PB' and insurance_name ~* 'PENNSYLVANIA NATIONAL MUT'
                 and claim_num ~* '^{}'
          """ .format ( '{',  '}', fax, pattern)

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()
print(' Done section 6 !' )


print(' Updating bad tax IDs ...' )
select_sql = """
         select pm_sk, content->>'w9_tax_id' from tpl_pre_billing_records
         where status = 'PB' and content->>'w9_tax_id' in ('20759583', '20762608', '22847732')
       """


update_sql = """
        update tpl_pre_billing_records set content=jsonb_set( content,
                  '{}w9_tax_id{}', '"{}"', true)
        where pm_sk = '{}'
        """

cur.execute(select_sql)
rows = cur.fetchall()
for pm_sk, taxid in rows:
    print ( 'pm_sk:{},  tax_id: {}'.format(pm_sk, taxid))
    fixed =  '0{}-{}'.format(taxid[:1], taxid[1:])
    sql = update_sql.format('{', '}', fixed,  pm_sk )

    if Dry_run:
        print(sql)
    else:
        cur.execute(sql)

conn.commit()

