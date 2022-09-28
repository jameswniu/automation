#! /usr/bin/env python3
import os, sys
import psycopg2

from config import user_db, passwd_db


def backup_business_analysis(name):
    sql_backup = f"""\
drop table if exists business_analysis.{name}_backup;
create table if not exists business_analysis.{name}_backup as (
select * from business_analysis.{name}
);"""
    with con:
        cur = con.cursor()
        cur.execute(sql_backup)
    print(f'backed up to CP... business_analysis.{name}_backup')


params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)


backup_business_analysis('jopari_claim_alerts')

backup_business_analysis('analyst_shadow_bills')


