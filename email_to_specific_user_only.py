#! /usr/bin/python3
import os
import sys
import re
from datetime import datetime as dt
import pytz
import smtplib
from config import passwd_email


#----
# send out
#----
def automail_accident():
    tz = pytz.timezone('America/New_York') 
    Ymd = dt.now(tz=tz).strftime('%Y%m%d')
    md = Ymd[4:]
    
    sender = 'james.niu@medlytix.com'
    receivers = ['yi.yan@medlytix.com']
    names = ['Yi Yan']

    frm = 'James Niu <{}>'.format(sender)
    l = []
    for receiver, name in zip(receivers, names):
        entry = '{} <{}>'.format(name, receiver)
        l.append(entry)
    to = ', '.join(l)
    #print(frm)
    #print(to)
    
    msg = """From: {}
To: {}
Subject: Update VNEX Accident Date in Pre-Billing Table - {}

Yi,

Please update VNEX accident date(s) in pre-billing table for billing:
/tmp/update_{}_accident.sql

Sincerely,


James Niu
Data Analyst
Medlytix, LLC

675 Mansell Road
Suite 100
Roswell, GA 30076
Direct Line: (678) 589-7439
www.medlytix.com\n\n""".format(frm, to, Ymd, md)

    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as mail:
            mail.starttls()
            mail.login('james.niu@medlytix.com', passwd_email)
            mail.sendmail(sender, receivers, msg)
        print(msg)
    except:
        print('sending email ERROR...')


def automail_zip():
    tz = pytz.timezone('America/New_York') 
    Ymd = dt.now(tz=tz).strftime('%Y%m%d')
    md = Ymd[4:]
    
    sender = 'james.niu@medlytix.com'
    receivers = ['yi.yan@medlytix.com']
    names = ['Yi Yan']

    frm = 'James Niu <{}>'.format(sender)
    l = []
    for receiver, name in zip(receivers, names):
        entry = '{} <{}>'.format(name, receiver)
        l.append(entry)
    to = ', '.join(l)
    #print(frm)
    #print(to)
    
    msg = """From: {}
To: {}
Subject: Update Zip Code(s) in Pre-Billing Table for Billing - {}

Yi,

Please update the following zip code(s) in pre-billing table for billing:
/tmp/update_{}_zip.sql

Sincerely,


James Niu
Data Analyst
Medlytix, LLC

675 Mansell Road
Suite 100
Roswell, GA 30076
Direct Line: (678) 589-7439
www.medlytix.com\n\n""".format(frm, to, Ymd, md)
    
    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as mail:
            mail.starttls()
            mail.login('james.niu@medlytix.com', passwd_email)
            mail.sendmail(sender, receivers, msg)
        print(msg)
    except:
        print('sending email ERROR...')


def emailing(): 
    tz = pytz.timezone('America/New_York') 
    Ymd = dt.now(tz=tz).strftime('%Y%m%d')
    md = Ymd[4:]
    
    if os.path.exists(r'/tmp/update_{}_accident.sql'.format(md)):
        print('accident file exists...') 
        automail_accident()
        print('sending email success...')
    else:
        print('no accident file')
    
    print('-' * 100)    

    if os.path.exists(r'/tmp/update_{}_zip.sql'.format(md)):
        print('zip file exists...')
        automail_zip()
        print('sending email success...')
    else:
        print('no zip file...')


if __name__ == '__main__':
    automail_accident()
    automail_zip()
    emailing()





