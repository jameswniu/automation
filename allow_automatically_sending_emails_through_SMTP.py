import os
import sys
import re

import pytz
import smtplib

from datetime import datetime as dt

from config import passwd_email


def automail(subj, msg, to, toname, cc=None, ccname=None):
    to_raw = to.copy()
    toname_raw = toname.copy()
    if cc:
        to.extend(cc)
        toname.extend(ccname)

    if len(to) != len(toname):
        print('ERROR: missing receiver/name')
        sys.exit()

    tz = pytz.timezone('America/New_York')
    Ymd = dt.now(tz=tz).strftime('%Y%m%d')
    md = Ymd[4:]

    l0 = []
    for i, j in zip(toname_raw, to_raw):
        entry = '{} <{}>'.format(i, j)
        l0.append(entry)
    toline = ', '.join(l0)

    if cc:
        l1 = []
        for m, n in zip(ccname, cc):
            entry = '{} <{}>'.format(m, n)
            l1.append(entry)
        ccline = ', '.join(l1)
    else:
        ccline = ''

    txt = """\
From: Melvin Andoor <melvin.andoor@medlytix.com>
To: {}
CC: {}
Subject: {}

{}

Sincerely,


Melvin Andoor
Data Engineer
Medlytix, LLC

675 Mansell Road
Suite 100
Roswell, GA 30076
Direct Line: 470-500-6765
www.medlytix.com\n\n""".format(toline, ccline, subj, msg)

    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as mail:
            mail.starttls()
            mail.login('melvin.andoor@medlytix.com', passwd_email)
            # print(to)
            mail.sendmail('melvin.andoor@medlytix.com', to, txt)
        print(txt)
    except:
        print('sending email ERROR...')


if __name__ == '__main__':
    automail(subj, msg, to, toname, cc=None, ccname=None)

