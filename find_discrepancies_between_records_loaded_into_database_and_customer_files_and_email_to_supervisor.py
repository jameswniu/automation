import os, sys

import psycopg2

from glob import glob

from datetime import datetime, timedelta
from pytz import timezone

from automation__allow_automatically_sending_emails_through_SMTP import automail
from config import user_db, passwd_db


os.chdir(r'L:\worktemp\yi\Melvin')


Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[4:]
outpt = ''


def extract_apollo_gap():
    global outpt

    flist = glob("*tplacct.txt")
    print('reading files...')

    dicy = {}
    dicyrec = {}
    for f in flist:
        # print(f)
        jar = {}

        lin_cnt = 0

        with open(f, 'r') as fr:
            for line in fr:
                # line = line.strip()

                if 'AcctNo' in line:
                    continue

                acct = line.split('|')[1]

                jar[acct] = 1
                line_cnt += 1

                dicy[acct] = 1
                #dicyrec[line] = 1

        print('{} | accts {} | recs {}'.format(f, len(jar), line_cnt))


    accts = ["""('{}')""".format(a) for a in dicy]
    accts = ', '.join(accts)
    new_sql = """\
with ABC (pat_acct) as (values
{}
)
-->
select ABC.pat_acct from ABC
where not exists (Select 1 from tpl_client_raw_bills crb where ABC.pat_acct = crb.pat_acct and crb.cust_id in (321, 405)) 
and not exists (Select 1 from tpl_raw_bills cb where ABC.pat_acct = cb.pat_acct and cb.cust_id in (321, 405));""".format(accts)

    print()
    print('comparing DB...')
    params = {
        'host': 'revpgdb01.revintel.net',
        'database': 'tpliq_tracker_db',
        'user': user_db,
        'password': passwd_db
    }
    con = psycopg2.connect(**params)
    with con:
        cur = con.cursor()
        cur.execute(new_sql)

    unique_acct = [row[0] for row in cur]  # new accts
    # for acct in unique_acct:
    #     print(acct)
    # print(len(unique_acct))

    related_record = []
    new_acctchg = []  # stopper prevent records dupe
    print()
    print('extracting new records from files...')

    for f in flist:
        d = 0

        fileacct = {}

        with open(f, 'r') as fr:
            for line in fr:
                line = line.strip()

                if 'AcctNo' in line:
                    continue

                tmp = line.split('|')
                acct = tmp[1]
                acctchg = (tmp[1], tmp[2], tmp[51]) # service date

                if acct in unique_acct and acctchg not in new_acctchg:
                    related_record.append(line)

                    fileacct[acct] = 1

                    new_acctchg.append(acctchg)

                    d += 1


        print('{} | newaccts {} | newrecs {}'.format(f, len(fileacct), d))
   outpt = """\
    Apollo gap accts: {} out of {}
    Apollo gap records: {} out of {}""".format(len(unique_acct), len(dicy), len(related_record), len(dicyrec))
    print()
    print(outpt)

    with open('apollo_gap_accts_{}.txt'.format(Ymd), 'w') as fw:
        [print(line, file=fw) for line in unique_acct]

    with open('apollo_gap_records_{}.txt'.format(Ymd), 'w') as fw:
        [print(line, file=fw) for line in related_record]

    if os.path.exists('apollo_gap_accts_{}.txt'.format(Ymd)):
        for f in glob('apollo_gap_accts_*.txt'):
            if os.path.basename(f) != 'apollo_gap_accts_{}.txt'.format(Ymd):
                os.rename(f, r'{}\Archive\{}'.format(os.getcwd(), f))

    if os.path.exists('apollo_gap_records_{}.txt'.format(Ymd)):
        for f in glob('apollo_gap_records_*.txt'):
            if os.path.basename(f) != 'apollo_gap_records_{}.txt'.format(Ymd):
                os.rename(f, r'{}\Archive\{}'.format(os.getcwd(), f))


def email_apollo_gap_after():
    global outpt

    print('-' * 200)

    subj = f'New Accounts 405 Apollo - {Ymd}'

    files = '\n'.join([rf'{os.getcwd()}\{file}' for file in glob(f'apollo_gap*{Ymd}.txt')])
    msg = f"""\
Yi,

Please find new Apollo accounts and records:
{outpt}

{files}"""

    to = ['yi.yan@medlytix.com']
    toname = ['Yi Yan']
    cc = ['gavin.johnson@medlytix.com']
    ccname = ['Gavin Johnson']

    if os.path.exists(r'L:\Auto_Opportunity_Analysis\email_logs\email_gap_apollo_{}.txt'.format(Ymd)):
        with open(r'L:\Auto_Opportunity_Analysis\email_logs\email_gap_apollo_{}.txt'.format(Ymd), 'r') as fr:
            rec = fr.read()
        act = 'a'
    else:
        rec = ''
        act = 'w'

    if msg not in rec:
        automail(subj, msg, to, toname)

        with open(r'L:\Auto_Opportunity_Analysis\email_logs\email_gap_apollo_{}.txt'.format(Ymd), act) as fw:
            print(msg, file=fw)
    else:
        print('email already sent...')


extract_apollo_gap()
#----
# email out the gap accts (RUN WITH TOP FUNCTION ON)
#----
email_apollo_gap_after()
