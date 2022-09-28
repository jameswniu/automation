#! /usr/bin/python3
import os
import pytz
import psycopg2.extras

from datetime import datetime, timedelta
from glob import glob

from config import user_db, passwd_db
from automation__written_module_allowing_automating_of_emails import automail


#----
# create update flag sqls
#----
os.chdir(r'/tmp')


tz = pytz.timezone('America/New_York')
Ymd = datetime.now(tz=tz).strftime('%Y%m%d')
md = Ymd[4:]
mdbef = (datetime.now(tz=tz) - timedelta(1)).strftime('%m%d')
mdfri = (datetime.now(tz=tz) - timedelta(3)).strftime('%m%d')

params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

s0 = """select a.id from tpl_pending_raw_bills a 
where current_date - a.created_at::date < 30 
and processed = 'f'
and exists (select 1 from tpl_client_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct);"""
s1 = """select a.id from tpl_pending_raw_bills a 
where current_date - a.created_at::date < 30 
and processed = 'f'
and not exists (select 1 from tpl_client_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct)
and exists (select 1 from tpl_rejected_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct and a.holding_info = coalesce(c.content->>'holding_info', c.content->>'reject_info'));"""
s2 = """select a.id from tpl_rejected_raw_bills a 
where current_date - a.created_at::date < 30 
and processed = 'f'
and exists (select 1 from tpl_client_raw_bills c where a.cust_id = c.cust_id and a.pat_acct = c.pat_acct);"""


updatepending = 'update_{}_pendingflag.sql'.format(md)
updaterejected = 'update_{}_rejectedflag.sql'.format(md)

dicy0 = {}
with con:
    cur.execute(s0)
for row in cur:
    dicy0[row[0]] = 1
c0 = 0
if len(dicy0) != 0:
    with open(updatepending, 'w') as fw:
        for ky in dicy0:
            str = """update tpl_pending_raw_bills a set processed = 't', processed_at = b.created_at - '1 hour'::interval from tpl_client_raw_bills b where a.cust_id = b.cust_id and a.pat_acct = b.pat_acct and a.id = {};""".format(ky)
            print(str, file=fw)
            c0 += 1

dicy1 = {}
with con:
    cur.execute(s1)
for row in cur:
    dicy1[row[0]] = 1
c1 = 0
if len(dicy1) != 0:
    if os.path.exists(updatepending):
        act = 'a'
    else:
        act = 'w'
    with open(updatepending, act) as fw:
        c1 = 0
        for ky in dicy1:
            str = """update tpl_pending_raw_bills a set processed = 't', processed_at = b.created_at - '1 hour'::interval from tpl_rejected_raw_bills b where a.cust_id = b.cust_id and a.pat_acct = b.pat_acct and a.id = {};""".format(ky)
            print(str, file=fw)
            c1 += 1

print("""update records from pending already in client: {}
update records from pending already in rejected: {}""".format(c0, c1))

dicy2 = {}
with con:
    cur.execute(s2)
for row in cur:
    dicy2[row[0]] = 1
c2 = 0
if len(dicy2) != 0:
    with open(updaterejected.format(md), 'w') as fw:
        c2 = 0
        for ky in dicy2:
            str = """update tpl_rejected_raw_bills a set processed = 't', processed_at = b.created_at - '1 hour'::interval from tpl_client_raw_bills b where a.cust_id = b.cust_id and a.pat_acct = b.pat_acct and a.id = {};""".format(ky)
            print(str, file=fw)
            c2 += 1
print('Update records from rejected already in client: {}'.format(c2))
print('-' * 200)


#----
# write emails
#----
if os.path.exists(updatepending):
    if c1 == 0:
        desc = 'Update records from pending already in client: {}'.format(c0)
    else:
        desc = """\
Update records from pending already in client: {}   
Update records from pending already in rejected: {}""".format(c0, c1)

    subj = 'Update Processed Flag in Pending Raw Bills Table - {}'.format(Ymd)
    msg = """\
Yi,

Please update processed flag in pending raw bills table:
{}

/tmp/{}""".format(desc, updatepending)
    to = ['yi.yan@medlytix.com']
    toname = ['Yi Yan']

    print('updatepending file exists...')
    automail(subj, msg, to, toname)

    for f in glob('update_*_pendingflag.sql'):
        if os.path.basename(f) != 'update_{}_pendingflag.sql'.format(md):
            os.remove(f)

else:
    print('no updatepending file...')


if os.path.exists(updaterejected):
    subj = 'Update Processed Flag in Rejected Raw Bills Table - {}'.format(Ymd)
    msg = """\
Yi,

Please update processed flag in rejected raw bills table:
Update records from rejected already in client: {}

/tmp/{}""".format(c2, updaterejected)
    to = ['yi.yan@medlytix.com']
    toname = ['Yi Yan']

    print('updaterejected file exists...')
    automail(subj, msg, to, toname)

    for f in glob('update_*_rejectedflag.sql'):
        if os.path.basename(f) != 'update_{}_rejectedflag.sql'.format(md):
            os.remove(f)

else:
    print('no updaterejected file...')




